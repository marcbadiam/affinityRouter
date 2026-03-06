"""Redis service registry using Sorted Sets for heartbeat tracking.

Workers register themselves and send periodic heartbeats.  The Router
queries for active workers to maintain its hash ring.

Redis keys used:
    ``affinity:workers:heartbeat``
        Sorted set — score is the Unix timestamp of the last heartbeat,
        member is the worker ID.
    ``affinity:workers:info:{worker_id}``
        Hash — stores the serialised worker metadata.
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio import Redis

from affinity_router.core.exceptions import RegistryError
from affinity_router.core.types import WorkerInfo
from affinity_router.registry.base import ServiceRegistry

logger = logging.getLogger(__name__)

_HEARTBEAT_KEY = "affinity:workers:heartbeat"
_INFO_PREFIX = "affinity:workers:info"


def _info_key(worker_id: str) -> str:
    return f"{_INFO_PREFIX}:{worker_id}"


class RedisServiceRegistry(ServiceRegistry):
    """Service registry backed by Redis Sorted Sets and Hashes.

    Args:
        redis: An async Redis client instance.

    Example::

        from redis.asyncio import Redis
        redis = Redis.from_url("redis://localhost:6379")
        registry = RedisServiceRegistry(redis)
    """

    def __init__(self, redis: Redis) -> None:  # type: ignore[type-arg]
        self._redis = redis

    async def register(self, worker: WorkerInfo) -> None:
        """Register a worker in Redis."""
        now = time.time()
        try:
            pipe = self._redis.pipeline()
            pipe.zadd(_HEARTBEAT_KEY, {worker.worker_id: now})
            pipe.hset(
                _info_key(worker.worker_id),
                mapping={
                    "worker_id": worker.worker_id,
                    "host": worker.host,
                    "port": str(worker.port),
                    "weight": str(worker.weight),
                    "metadata": json.dumps(worker.metadata),
                },
            )
            await pipe.execute()
            logger.info("Registered worker %r", worker.worker_id)
        except Exception as exc:
            raise RegistryError(
                f"Failed to register worker {worker.worker_id!r}"
            ) from exc

    async def deregister(self, worker_id: str) -> None:
        """Remove a worker from the registry."""
        try:
            pipe = self._redis.pipeline()
            pipe.zrem(_HEARTBEAT_KEY, worker_id)
            pipe.delete(_info_key(worker_id))
            await pipe.execute()
            logger.info("Deregistered worker %r", worker_id)
        except Exception as exc:
            raise RegistryError(f"Failed to deregister worker {worker_id!r}") from exc

    async def heartbeat(self, worker_id: str) -> None:
        """Update the heartbeat timestamp for a worker."""
        now = time.time()
        try:
            await self._redis.zadd(_HEARTBEAT_KEY, {worker_id: now})
        except Exception as exc:
            raise RegistryError(f"Heartbeat failed for worker {worker_id!r}") from exc

    async def get_active_workers(self, timeout: float = 30.0) -> list[WorkerInfo]:
        """Return workers that have heartbeated within *timeout* seconds."""
        min_score = time.time() - timeout
        try:
            worker_ids_raw = await self._redis.zrangebyscore(
                _HEARTBEAT_KEY, min_score, "+inf"
            )
        except Exception as exc:
            raise RegistryError("Failed to query active workers") from exc

        workers: list[WorkerInfo] = []
        for wid_raw in worker_ids_raw:
            wid = wid_raw.decode() if isinstance(wid_raw, bytes) else wid_raw
            info = await self._redis.hgetall(_info_key(wid))
            if not info:
                continue
            workers.append(self._deserialize_worker(wid, info))

        return workers

    async def close(self) -> None:
        """Close the Redis connection."""
        await self._redis.aclose()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _deserialize_worker(
        worker_id: str,
        info: dict[bytes | str, bytes | str],
    ) -> WorkerInfo:
        """Build a WorkerInfo from Redis hash fields."""

        def _s(val: bytes | str) -> str:
            return val.decode() if isinstance(val, bytes) else val

        # Safely extract fields with fallbacks
        host = _s(info.get(b"host", info.get("host", b"localhost")))  # type: ignore[arg-type]
        port = int(_s(info.get(b"port", info.get("port", b"0"))))  # type: ignore[arg-type]
        weight = int(_s(info.get(b"weight", info.get("weight", b"1"))))  # type: ignore[arg-type]
        metadata_raw = _s(info.get(b"metadata", info.get("metadata", b"{}")))  # type: ignore[arg-type]
        metadata: dict[str, str] = json.loads(metadata_raw)

        return WorkerInfo(
            worker_id=worker_id,
            host=host,
            port=port,
            weight=weight,
            metadata=metadata,
        )

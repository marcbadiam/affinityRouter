"""Redis Streams transport backend.

Each worker gets a dedicated Redis Stream.  The Router writes tasks with
XADD and the Worker reads with XREADGROUP, providing at-least-once
delivery with consumer-group acknowledgement.
"""

from __future__ import annotations

import contextlib
import json
import logging
from typing import TYPE_CHECKING

from affinity_router.core.exceptions import TransportError
from affinity_router.core.types import Task
from affinity_router.transport.base import TransportBackend

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis.asyncio import Redis

logger = logging.getLogger(__name__)

_STREAM_PREFIX = "affinity"
_CONSUMER_GROUP = "affinity-workers"


def _stream_key(worker_id: str) -> str:
    """Return the Redis Stream key for a given worker."""
    return f"{_STREAM_PREFIX}:{worker_id}:tasks"


class RedisStreamTransport(TransportBackend):
    """Transport backend using Redis Streams with consumer groups.

    Args:
        redis: An async Redis client instance.
        block_ms: Milliseconds to block on XREADGROUP when no messages
            are available.

    Example::

        from redis.asyncio import Redis
        redis = Redis.from_url("redis://localhost:6379")
        transport = RedisStreamTransport(redis)
    """

    def __init__(self, redis: Redis, block_ms: int = 5000) -> None:  # type: ignore[type-arg]
        self._redis = redis
        self._block_ms = block_ms
        # Map (worker_id, task_id) -> stream message_id for acknowledgement
        self._msg_ids: dict[tuple[str, str], str] = {}

    # ------------------------------------------------------------------
    # TransportBackend implementation
    # ------------------------------------------------------------------

    async def send_task(self, worker_id: str, task: Task) -> None:
        """Publish a task to the worker's Redis Stream via XADD."""
        stream = _stream_key(worker_id)
        try:
            fields: dict[str, str] = {
                "task_id": task.task_id,
                "task_name": task.task_name,
                "routing_key": task.routing_key,
                "payload": json.dumps(task.payload),
                "created_at": str(task.created_at),
            }
            await self._redis.xadd(stream, fields)  # type: ignore[arg-type]
            logger.debug("Sent task %r to stream %r", task.task_id, stream)
        except Exception as exc:
            raise TransportError(
                f"Failed to send task {task.task_id!r} to {stream!r}"
            ) from exc

    async def receive_tasks(self, worker_id: str) -> AsyncIterator[Task]:
        """Consume tasks from the worker's Redis Stream using XREADGROUP.

        Creates the consumer group if it does not exist.
        """
        stream = _stream_key(worker_id)
        consumer = worker_id

        # Ensure consumer group exists
        await self._ensure_consumer_group(stream)

        while True:
            try:
                response = await self._redis.xreadgroup(
                    groupname=_CONSUMER_GROUP,
                    consumername=consumer,
                    streams={stream: ">"},  # only new undelivered messages
                    count=1,
                    block=self._block_ms,
                )
            except Exception as exc:
                raise TransportError(f"Failed to read from stream {stream!r}") from exc

            if not response:
                continue  # timeout, no messages — loop again

            for _stream_name, messages in response:
                for msg_id, fields in messages:
                    try:
                        task = self._deserialize_task(fields)
                        # Store msg_id for later acknowledgement
                        if isinstance(msg_id, str):
                            msg_id_str = msg_id
                        else:
                            msg_id_str = msg_id.decode()
                        self._msg_ids[(worker_id, task.task_id)] = msg_id_str
                        yield task
                    except Exception:
                        logger.exception(
                            "Failed to deserialize message %r from %r",
                            msg_id,
                            stream,
                        )

    async def acknowledge(self, worker_id: str, task_id: str) -> None:
        """Acknowledge a task in the Redis Stream via XACK."""
        stream = _stream_key(worker_id)
        msg_id = self._msg_ids.pop((worker_id, task_id), None)
        if msg_id is None:
            logger.warning(
                "No message ID found for task %r on worker %r",
                task_id,
                worker_id,
            )
            return
        try:
            await self._redis.xack(stream, _CONSUMER_GROUP, msg_id)
            logger.debug(
                "Acknowledged task %r (msg %r) on stream %r",
                task_id,
                msg_id,
                stream,
            )
        except Exception as exc:
            raise TransportError(
                f"Failed to ack task {task_id!r} on {stream!r}"
            ) from exc

    async def close(self) -> None:
        """Close the Redis connection."""
        await self._redis.aclose()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _ensure_consumer_group(self, stream: str) -> None:
        """Create the consumer group if it doesn't already exist."""
        with contextlib.suppress(Exception):
            await self._redis.xgroup_create(
                stream, _CONSUMER_GROUP, id="0", mkstream=True
            )

    @staticmethod
    def _deserialize_task(fields: dict[bytes | str, bytes | str]) -> Task:
        """Convert Redis Stream fields back into a Task instance."""

        def _s(val: bytes | str) -> str:
            return val.decode() if isinstance(val, bytes) else val

        def _get_field(key: str) -> str:
            bkey = key.encode()
            return _s(fields[bkey] if bkey in fields else fields[key])

        return Task(
            task_id=_get_field("task_id"),
            task_name=_get_field("task_name"),
            routing_key=_get_field("routing_key"),
            payload=json.loads(_get_field("payload")),
            created_at=float(_get_field("created_at")),
        )

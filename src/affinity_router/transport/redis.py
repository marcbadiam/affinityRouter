"""Redis Streams transport backend.

Each worker gets a dedicated Redis Stream.  The Router writes tasks with
XADD and the Worker reads with XREADGROUP, providing at-least-once
delivery with consumer-group acknowledgement.
"""

from __future__ import annotations

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

    def __init__(self, redis: Redis, block_ms: int = 5000) -> None:
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

        def _decode(val: bytes | str) -> str:
            return val.decode() if isinstance(val, bytes) else val

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
                        msg_id_str = _decode(msg_id)
                        self._msg_ids[(worker_id, task.task_id)] = msg_id_str
                        yield task
                    except Exception:
                        # Log error and ACK malformed message to prevent retry loop
                        logger.exception(
                            "Failed to deserialize message %r from %r - "
                            "acknowledging to skip",
                            msg_id,
                            stream,
                        )
                        # Acknowledge malformed message to prevent redelivery
                        try:
                            await self._redis.xack(
                                stream, _CONSUMER_GROUP, _decode(msg_id)
                            )
                        except Exception as ack_exc:
                            logger.warning(
                                "Failed to ACK malformed message %r: %s",
                                msg_id,
                                ack_exc,
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
        try:
            await self._redis.xgroup_create(
                stream, _CONSUMER_GROUP, id="0", mkstream=True
            )
            logger.debug(
                "Created consumer group %r for stream %r", _CONSUMER_GROUP, stream
            )
        except Exception as exc:
            # Check if it's a "BUSYGROUP" error (group already exists)
            error_msg = str(exc)
            if "BUSYGROUP" in error_msg:
                logger.debug(
                    "Consumer group %r already exists for stream %r",
                    _CONSUMER_GROUP,
                    stream,
                )
            else:
                # Log unexpected errors but don't crash - stream may exist
                logger.warning(
                    "Failed to create consumer group %r for stream %r: %s",
                    _CONSUMER_GROUP,
                    stream,
                    exc,
                )

    @staticmethod
    def _deserialize_task(fields: dict[bytes | str, bytes | str]) -> Task:
        """Convert Redis Stream fields back into a Task instance."""

        def _field(name: str) -> str:
            raw = fields.get(name) or fields.get(name.encode(), b"")
            return raw.decode() if isinstance(raw, bytes) else raw

        return Task(
            task_id=_field("task_id"),
            task_name=_field("task_name"),
            routing_key=_field("routing_key"),
            payload=json.loads(_field("payload")),
            created_at=float(_field("created_at")),
        )

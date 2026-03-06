"""Redis state backend for task idempotency tracking.

Uses Redis SET NX (set-if-not-exists) for atomic task claiming, ensuring
that a task is processed at most once even under concurrent delivery.

Redis keys used:
    ``affinity:task:{task_id}``
        String (JSON) — stores the current status and (once finished) the
        result of the task.  Keys have a TTL so completed tasks are
        automatically cleaned up.
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING

from affinity_router.core.exceptions import StateBackendError
from affinity_router.core.types import TaskResult, TaskStatus
from affinity_router.state.base import StateBackend

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)

_KEY_PREFIX = "affinity:task"


def _task_key(task_id: str) -> str:
    return f"{_KEY_PREFIX}:{task_id}"


class RedisStateBackend(StateBackend):
    """State backend using Redis for task idempotency and status tracking.

    Args:
        redis: An async Redis client instance.
        acquire_ttl: Default TTL (seconds) for the processing lock.
            If a worker crashes, the lock expires and the task can be
            retried by another worker.
        result_ttl: TTL (seconds) for completed/failed task results.
            After this period Redis will automatically delete the key.

    Example::

        from redis.asyncio import Redis
        redis = Redis.from_url("redis://localhost:6379")
        state = RedisStateBackend(redis)
    """

    def __init__(
        self,
        redis: Redis,
        acquire_ttl: float = 300.0,
        result_ttl: float = 86400.0,
    ) -> None:
        self._redis = redis
        self._acquire_ttl = acquire_ttl
        self._result_ttl = result_ttl

    async def get_status(self, task_id: str) -> TaskStatus | None:
        """Return the current status of a task."""
        key = _task_key(task_id)
        try:
            raw = await self._redis.get(key)
        except Exception as exc:
            raise StateBackendError(
                f"Failed to get status for task {task_id!r}"
            ) from exc

        if raw is None:
            return None
        data = json.loads(raw)
        return TaskStatus(data["status"])

    async def try_acquire(self, task_id: str, ttl: float = 300.0) -> bool:
        """Atomically claim a task using SET NX EX.

        Returns True if the task was claimed (key didn't exist).
        Returns False if the task is already processing or completed.
        """
        key = _task_key(task_id)
        record = json.dumps(
            {
                "status": TaskStatus.PROCESSING.value,
                "started_at": time.time(),
                "result": None,
                "error": None,
                "completed_at": None,
            }
        )
        effective_ttl = ttl or self._acquire_ttl
        try:
            was_set = await self._redis.set(key, record, nx=True, ex=int(effective_ttl))
            if was_set:
                logger.debug("Acquired task %r", task_id)
                return True
            logger.debug("Task %r already claimed", task_id)
            return False
        except Exception as exc:
            raise StateBackendError(f"Failed to acquire task {task_id!r}") from exc

    async def mark_completed(self, task_id: str, result: TaskResult) -> None:
        """Mark a task as completed and store its result."""
        await self._store_result(task_id, result)

    async def mark_failed(self, task_id: str, result: TaskResult) -> None:
        """Mark a task as failed and store the error."""
        await self._store_result(task_id, result)

    async def get_result(self, task_id: str) -> TaskResult | None:
        """Retrieve the stored result for a task."""
        key = _task_key(task_id)
        try:
            raw = await self._redis.get(key)
        except Exception as exc:
            raise StateBackendError(
                f"Failed to get result for task {task_id!r}"
            ) from exc

        if raw is None:
            return None
        data = json.loads(raw)
        if data["status"] == TaskStatus.PROCESSING.value:
            return None  # still in progress, no result yet

        return TaskResult(
            task_id=task_id,
            status=TaskStatus(data["status"]),
            result=data.get("result"),
            error=data.get("error"),
            completed_at=data.get("completed_at", 0.0),
        )

    async def close(self) -> None:
        """Close the Redis connection."""
        await self._redis.aclose()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _store_result(self, task_id: str, result: TaskResult) -> None:
        """Overwrite the task key with the final result and a long TTL."""
        key = _task_key(task_id)
        record = json.dumps(
            {
                "status": result.status.value,
                "result": result.result,
                "error": result.error,
                "completed_at": result.completed_at,
            }
        )
        try:
            await self._redis.set(key, record, ex=int(self._result_ttl))
            logger.debug("Stored %s result for task %r", result.status.value, task_id)
        except Exception as exc:
            raise StateBackendError(
                f"Failed to store result for task {task_id!r}"
            ) from exc

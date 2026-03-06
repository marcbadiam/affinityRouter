"""Abstract base class for state backends.

A state backend provides task-level idempotency guarantees.  Before
processing a task, the Worker checks whether the task ID has already been
seen.  After processing, it records the result so that duplicate deliveries
are silently skipped.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from affinity_router.core.types import TaskResult, TaskStatus


class StateBackend(ABC):
    """Interface for task state tracking and idempotency."""

    @abstractmethod
    async def get_status(self, task_id: str) -> TaskStatus | None:
        """Return the current status of a task, or ``None`` if unknown.

        Args:
            task_id: Unique task identifier.
        """

    @abstractmethod
    async def try_acquire(self, task_id: str, ttl: float = 300.0) -> bool:
        """Attempt to claim a task for processing (atomic check-and-set).

        Returns ``True`` if the task was successfully claimed (status set
        to PROCESSING).  Returns ``False`` if it is already being
        processed or has been completed — the caller should skip it.

        Args:
            task_id: Unique task identifier.
            ttl: Maximum time (in seconds) before the claim expires,
                allowing another worker to retry a stuck task.
        """

    @abstractmethod
    async def mark_completed(self, task_id: str, result: TaskResult) -> None:
        """Record that a task finished successfully.

        Args:
            task_id: Unique task identifier.
            result: The result to persist.
        """

    @abstractmethod
    async def mark_failed(self, task_id: str, result: TaskResult) -> None:
        """Record that a task failed.

        Args:
            task_id: Unique task identifier.
            result: The failure result (should contain an error message).
        """

    @abstractmethod
    async def get_result(self, task_id: str) -> TaskResult | None:
        """Retrieve the stored result for a task, if any.

        Args:
            task_id: Unique task identifier.

        Returns:
            The :class:`TaskResult` if the task has completed or failed,
            ``None`` otherwise.
        """

    @abstractmethod
    async def close(self) -> None:
        """Release any resources held by the state backend."""

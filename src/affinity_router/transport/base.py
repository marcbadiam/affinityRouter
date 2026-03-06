"""Abstract base class for transport backends.

A transport backend is responsible for sending tasks from the Router to
Workers and receiving tasks on the Worker side.  Each concrete backend
(Redis Streams, gRPC, NATS, …) implements this interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from affinity_router.core.types import Task


class TransportBackend(ABC):
    """Interface for task delivery between Router and Workers.

    Implementations must support per-worker delivery — the Router decides
    *which* worker receives a task; the transport simply delivers it.
    """

    @abstractmethod
    async def send_task(self, worker_id: str, task: Task) -> None:
        """Deliver a task to a specific worker's queue/channel.

        Args:
            worker_id: The target worker that should process this task.
            task: The task to deliver.

        Raises:
            TransportError: If delivery fails.
        """

    @abstractmethod
    async def receive_tasks(self, worker_id: str) -> AsyncIterator[Task]:
        """Yield tasks destined for *worker_id* as they arrive.

        This is a long-running async generator used by the Worker's
        processing loop.  It should block (await) when no tasks are
        available and yield new tasks as they are received.

        Args:
            worker_id: The worker whose queue to consume.

        Yields:
            Task instances in arrival order.
        """
        yield  # type: ignore[misc]  # pragma: no cover

    @abstractmethod
    async def acknowledge(self, worker_id: str, task_id: str) -> None:
        """Acknowledge successful processing of a task.

        The transport may use this to remove the message from a pending
        list, advance a stream cursor, etc.

        Args:
            worker_id: The worker that processed the task.
            task_id: The unique ID of the processed task.
        """

    @abstractmethod
    async def close(self) -> None:
        """Release any resources held by the transport (connections, etc.)."""

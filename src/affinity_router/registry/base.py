"""Abstract base class for service registry backends.

A service registry tracks which workers are alive and healthy.  The Router
uses it to maintain an up-to-date hash ring, and each Worker uses it to
announce its presence via periodic heartbeats.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from affinity_router.core.types import WorkerInfo


class ServiceRegistry(ABC):
    """Interface for worker discovery and health checking."""

    @abstractmethod
    async def register(self, worker: WorkerInfo) -> None:
        """Register a worker as available.

        Should be called once when a worker starts.  Subsequent liveness
        is maintained via :meth:`heartbeat`.

        Args:
            worker: Metadata about the worker to register.

        Raises:
            RegistryError: If registration fails.
        """

    @abstractmethod
    async def deregister(self, worker_id: str) -> None:
        """Remove a worker from the registry (graceful shutdown).

        Args:
            worker_id: The unique ID of the worker to remove.
        """

    @abstractmethod
    async def heartbeat(self, worker_id: str) -> None:
        """Update the liveness timestamp for a worker.

        Workers must call this periodically (e.g. every 5 s).  If a
        worker misses enough heartbeats it will be considered dead and
        removed by :meth:`get_active_workers` consumers.

        Args:
            worker_id: The worker sending the heartbeat.
        """

    @abstractmethod
    async def get_active_workers(self, timeout: float = 30.0) -> list[WorkerInfo]:
        """Return all workers that have heartbeated within *timeout* seconds.

        Args:
            timeout: Maximum age (in seconds) of the last heartbeat for
                a worker to be considered active.

        Returns:
            A list of currently active :class:`WorkerInfo` instances.
        """

    @abstractmethod
    async def close(self) -> None:
        """Release any resources held by the registry."""

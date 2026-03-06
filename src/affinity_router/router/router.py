"""Router: the load balancer that routes tasks by affinity.

The Router maintains a :class:`ConsistentHashRing` that is kept in sync
with a :class:`ServiceRegistry`.  When a task is submitted, its
``routing_key`` is hashed to determine which worker should handle it,
ensuring data locality.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

from affinity_router.core.exceptions import WorkerNotFoundError
from affinity_router.core.hash_ring import ConsistentHashRing
from affinity_router.core.types import Task

if TYPE_CHECKING:
    from affinity_router.registry.base import ServiceRegistry
    from affinity_router.transport.base import TransportBackend

logger = logging.getLogger(__name__)


class Router:
    """Affinity-aware task router backed by consistent hashing.

    The Router periodically polls the :class:`ServiceRegistry` to discover
    active workers and updates its internal hash ring.  Tasks submitted via
    :meth:`submit` are routed to the worker responsible for the given
    ``routing_key``.

    Args:
        transport: Backend for delivering tasks to workers.
        registry: Backend for discovering live workers.
        replicas: Number of virtual nodes per physical node on the ring.
        sync_interval: Seconds between registry sync polls.
        worker_timeout: Seconds before a worker is considered dead.

    Example::

        router = Router(
            transport=redis_transport,
            registry=redis_registry,
        )
        await router.start()
        await router.submit(
            routing_key="ticket:12345",
            task_name="process_payment",
            payload={"amount": 100},
        )
        await router.stop()
    """

    def __init__(
        self,
        transport: TransportBackend,
        registry: ServiceRegistry,
        replicas: int = 160,
        sync_interval: float = 5.0,
        worker_timeout: float = 30.0,
    ) -> None:
        self._transport = transport
        self._registry = registry
        self._ring = ConsistentHashRing(replicas=replicas)
        self._sync_interval = sync_interval
        self._worker_timeout = worker_timeout

        self._running = False
        self._sync_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    @classmethod
    def from_redis(
        cls,
        redis_url: str = "redis://localhost:6379",
        replicas: int = 160,
        sync_interval: float = 5.0,
        worker_timeout: float = 30.0,
    ) -> Router:
        """Create a Router backed by Redis for transport and registry.

        This is a convenience factory that wires up all Redis adapters.

        Args:
            redis_url: Redis connection URL.
            replicas: Virtual nodes per physical node on the hash ring.
            sync_interval: Seconds between registry sync polls.
            worker_timeout: Seconds before a worker is considered dead.

        Returns:
            A fully configured :class:`Router` instance (call
            :meth:`start` to begin).
        """
        from redis.asyncio import Redis as AsyncRedis

        from affinity_router.registry.redis import RedisServiceRegistry
        from affinity_router.transport.redis import RedisStreamTransport

        redis_transport = AsyncRedis.from_url(redis_url, decode_responses=False)
        redis_registry = AsyncRedis.from_url(redis_url, decode_responses=False)

        return cls(
            transport=RedisStreamTransport(redis_transport),
            registry=RedisServiceRegistry(redis_registry),
            replicas=replicas,
            sync_interval=sync_interval,
            worker_timeout=worker_timeout,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the router: perform initial sync then begin background sync loop."""
        self._running = True
        await self._sync_ring()
        self._sync_task = asyncio.create_task(self._sync_loop(), name="router-sync")
        logger.info("Router started with %d workers", len(self._ring))

    async def stop(self) -> None:
        """Gracefully shut down the router."""
        if not self._running:
            return
        self._running = False
        if self._sync_task is not None:
            self._sync_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sync_task
            self._sync_task = None
        await self._transport.close()
        await self._registry.close()
        logger.info("Router stopped")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def submit(
        self,
        routing_key: str,
        task_name: str,
        payload: dict[str, Any] | None = None,
        task_id: str | None = None,
    ) -> Task:
        """Submit a task for processing.

        The *routing_key* is hashed to select the target worker.  The
        same routing key will always map to the same worker (as long as
        the cluster topology is stable), enabling hot local caches.

        Args:
            routing_key: Affinity key (e.g. ``"ticket:12345"``).
            task_name: Name of the handler to invoke on the worker.
            payload: Arbitrary data to pass to the handler.
            task_id: Optional explicit task ID for idempotency. One is
                generated automatically if omitted.

        Returns:
            The :class:`Task` that was created and dispatched.

        Raises:
            WorkerNotFoundError: If no workers are available.
        """
        worker_id = self._ring.get_node(routing_key)
        if worker_id is None:
            raise WorkerNotFoundError("No workers available to handle the task")

        kwargs: dict[str, Any] = {
            "task_name": task_name,
            "routing_key": routing_key,
            "payload": payload or {},
        }
        if task_id is not None:
            kwargs["task_id"] = task_id

        task = Task(**kwargs)
        await self._transport.send_task(worker_id, task)
        logger.debug(
            "Task %r routed to worker %r (key=%r)",
            task.task_id,
            worker_id,
            routing_key,
        )
        return task

    # ------------------------------------------------------------------
    # Ring synchronization
    # ------------------------------------------------------------------

    async def _sync_loop(self) -> None:
        """Periodically sync the hash ring with the service registry."""
        while self._running:
            await asyncio.sleep(self._sync_interval)
            try:
                await self._sync_ring()
            except Exception:
                logger.exception("Failed to sync worker ring")

    async def _sync_ring(self) -> None:
        """Fetch active workers and update the hash ring.

        Adds new workers, removes dead ones, and updates weights if they
        have changed.  This is designed to be called periodically.
        """
        workers = await self._registry.get_active_workers(timeout=self._worker_timeout)
        active_ids = {w.worker_id for w in workers}
        current_ids = set(self._ring.nodes.keys())

        # Remove workers that are no longer active
        for dead_id in current_ids - active_ids:
            self._ring.remove_node(dead_id)
            logger.info("Removed dead worker %r from ring", dead_id)

        # Add new workers or update weights
        for worker in workers:
            current_weight = self._ring.nodes.get(worker.worker_id)
            if current_weight is None:
                self._ring.add_node(worker.worker_id, weight=worker.weight)
                logger.info(
                    "Added worker %r to ring (weight=%d)",
                    worker.worker_id,
                    worker.weight,
                )
            elif current_weight != worker.weight:
                self._ring.add_node(worker.worker_id, weight=worker.weight)
                logger.info(
                    "Updated worker %r weight: %d -> %d",
                    worker.worker_id,
                    current_weight,
                    worker.weight,
                )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        """Whether the router is currently running."""
        return self._running

    @property
    def worker_count(self) -> int:
        """Number of workers currently in the hash ring."""
        return len(self._ring)

    @property
    def ring(self) -> ConsistentHashRing:
        """The underlying hash ring (read-only access)."""
        return self._ring

    def __repr__(self) -> str:
        return f"Router(workers={len(self._ring)}, running={self._running})"

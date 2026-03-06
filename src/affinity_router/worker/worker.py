"""Worker process: receives tasks, executes handlers, manages heartbeats.

The Worker connects to the transport to receive tasks routed by the Router,
checks idempotency via the state backend, and dispatches tasks to handlers
registered with the :meth:`Worker.task` decorator.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from affinity_router.core.types import TaskResult, TaskStatus, WorkerInfo
from affinity_router.worker.cache import AffinityCache

if TYPE_CHECKING:
    from affinity_router.core.types import Task
    from affinity_router.registry.base import ServiceRegistry
    from affinity_router.state.base import StateBackend
    from affinity_router.transport.base import TransportBackend

logger = logging.getLogger(__name__)

# Type alias for task handlers
TaskHandler = Callable[..., Any]


class Worker:
    """Async worker that processes tasks routed by affinity.

    Args:
        worker_id: Unique identifier for this worker instance.
        transport: Backend for receiving tasks.
        registry: Backend for heartbeats and discovery.
        state: Backend for idempotency checks.
        cache: Optional local cache (one is created if not provided).
        heartbeat_interval: Seconds between heartbeats.
        host: Hostname to register in the registry.
        port: Port to register in the registry.
        weight: Capacity weight for hash ring placement.

    Example::

        worker = Worker(
            worker_id="worker-1",
            transport=redis_transport,
            registry=redis_registry,
            state=redis_state,
        )

        @worker.task("process_payment")
        async def process_payment(task: Task) -> dict:
            return {"status": "paid"}

        @worker.task("generate_pdf")
        def generate_pdf(task: Task) -> bytes:
            # Sync handlers are wrapped with asyncio.to_thread()
            return create_pdf(task.payload)

        await worker.start()
    """

    def __init__(
        self,
        worker_id: str,
        transport: TransportBackend,
        registry: ServiceRegistry,
        state: StateBackend,
        cache: AffinityCache | None = None,
        heartbeat_interval: float = 5.0,
        host: str = "localhost",
        port: int = 0,
        weight: int = 1,
    ) -> None:
        self._worker_id = worker_id
        self._transport = transport
        self._registry = registry
        self._state = state
        self.cache = cache or AffinityCache()
        self._heartbeat_interval = heartbeat_interval
        self._host = host
        self._port = port
        self._weight = weight

        self._handlers: dict[str, TaskHandler] = {}
        self._running = False
        self._tasks: set[asyncio.Task[Any]] = set()

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    @classmethod
    def from_redis(
        cls,
        worker_id: str,
        redis_url: str = "redis://localhost:6379",
        heartbeat_interval: float = 5.0,
        host: str = "localhost",
        port: int = 0,
        weight: int = 1,
        cache: AffinityCache | None = None,
    ) -> Worker:
        """Create a Worker backed by Redis for all backends.

        This is a convenience factory that wires up all Redis adapters.

        Args:
            worker_id: Unique identifier for this worker.
            redis_url: Redis connection URL.
            heartbeat_interval: Seconds between heartbeats.
            host: Hostname to register in the registry.
            port: Port to register in the registry.
            weight: Capacity weight for hash ring placement.
            cache: Optional local cache instance.

        Returns:
            A fully configured :class:`Worker` instance (call
            :meth:`start` to begin processing).
        """
        from redis.asyncio import Redis as AsyncRedis

        from affinity_router.registry.redis import RedisServiceRegistry
        from affinity_router.state.redis import RedisStateBackend
        from affinity_router.transport.redis import RedisStreamTransport

        redis_transport = AsyncRedis.from_url(redis_url, decode_responses=False)
        redis_registry = AsyncRedis.from_url(redis_url, decode_responses=False)
        redis_state = AsyncRedis.from_url(redis_url, decode_responses=False)

        return cls(
            worker_id=worker_id,
            transport=RedisStreamTransport(redis_transport),
            registry=RedisServiceRegistry(redis_registry),
            state=RedisStateBackend(redis_state),
            cache=cache,
            heartbeat_interval=heartbeat_interval,
            host=host,
            port=port,
            weight=weight,
        )

    # ------------------------------------------------------------------
    # Decorator API
    # ------------------------------------------------------------------

    def task(self, task_name: str) -> Callable[[TaskHandler], TaskHandler]:
        """Register a handler for *task_name*.

        The decorated function can be sync or async.  Sync functions are
        automatically wrapped with :func:`asyncio.to_thread`.

        Args:
            task_name: The name that the Router will use to dispatch.

        Returns:
            The original function, unmodified.
        """

        def decorator(func: TaskHandler) -> TaskHandler:
            self._handlers[task_name] = func
            logger.info(
                "Registered handler %r for task %r",
                func.__name__,
                task_name,
            )
            return func

        return decorator

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the worker: register, begin heartbeat and processing loop.

        This method runs until :meth:`stop` is called or the process is
        interrupted.
        """
        self._running = True
        worker_info = WorkerInfo(
            worker_id=self._worker_id,
            host=self._host,
            port=self._port,
            weight=self._weight,
        )
        await self._registry.register(worker_info)
        logger.info("Worker %r registered and starting", self._worker_id)

        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"heartbeat-{self._worker_id}"
        )
        self._tasks.add(heartbeat_task)
        heartbeat_task.add_done_callback(self._tasks.discard)

        try:
            await self._processing_loop()
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Gracefully shut down the worker."""
        if not self._running:
            return
        self._running = False
        logger.info("Worker %r shutting down", self._worker_id)

        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        await self._registry.deregister(self._worker_id)
        await self._transport.close()
        await self._state.close()
        await self._registry.close()
        logger.info("Worker %r stopped", self._worker_id)

    # ------------------------------------------------------------------
    # Internal loops
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Periodically send heartbeats to the registry."""
        while self._running:
            try:
                await self._registry.heartbeat(self._worker_id)
            except Exception:
                logger.exception("Heartbeat failed for %r", self._worker_id)
            await asyncio.sleep(self._heartbeat_interval)

    async def _processing_loop(self) -> None:
        """Consume tasks from the transport and dispatch to handlers."""
        async for task in self._transport.receive_tasks(self._worker_id):
            if not self._running:
                break
            try:
                await self._handle_task(task)
            except Exception:
                logger.exception("Unhandled error processing task %r", task.task_id)

    async def _handle_task(self, task: Task) -> None:
        """Process a single task with idempotency and error handling."""
        # 1. Idempotency check
        acquired = await self._state.try_acquire(task.task_id)
        if not acquired:
            logger.debug("Task %r already processed, skipping", task.task_id)
            await self._transport.acknowledge(self._worker_id, task.task_id)
            return

        # 2. Find the handler
        handler = self._handlers.get(task.task_name)
        if handler is None:
            error_result = TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error=f"No handler for task {task.task_name!r}",
            )
            await self._state.mark_failed(task.task_id, error_result)
            await self._transport.acknowledge(self._worker_id, task.task_id)
            logger.error("No handler for task %r", task.task_name)
            return

        # 3. Execute (auto-detect sync vs async)
        try:
            if inspect.iscoroutinefunction(handler):
                result_value = await handler(task)
            else:
                result_value = await asyncio.to_thread(handler, task)

            success_result = TaskResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                result=result_value,
            )
            await self._state.mark_completed(task.task_id, success_result)
            logger.debug("Task %r completed", task.task_id)

        except Exception as exc:
            error_result = TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error=f"{type(exc).__name__}: {exc}",
            )
            await self._state.mark_failed(task.task_id, error_result)
            logger.exception("Task %r failed", task.task_id)

        # 4. Always acknowledge delivery (processed or failed)
        await self._transport.acknowledge(self._worker_id, task.task_id)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def worker_id(self) -> str:
        """The unique ID of this worker."""
        return self._worker_id

    @property
    def is_running(self) -> bool:
        """Whether the worker is currently processing tasks."""
        return self._running

    @property
    def registered_tasks(self) -> list[str]:
        """List of task names this worker can handle."""
        return list(self._handlers.keys())

    def __repr__(self) -> str:
        return (
            f"Worker(id={self._worker_id!r}, "
            f"tasks={list(self._handlers.keys())}, "
            f"running={self._running})"
        )

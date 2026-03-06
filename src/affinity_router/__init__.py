"""AffinityRouter — intelligent task routing with data locality.

Quick start::

    from affinity_router import Router, Worker

    # Router side
    router = Router.from_redis(redis_url="redis://localhost:6379")
    await router.start()
    await router.submit(routing_key="ticket:123", task_name="process", payload={})

    # Worker side
    worker = Worker.from_redis(redis_url="redis://localhost:6379", worker_id="w1")

    @worker.task("process")
    async def process(task):
        return {"ok": True}

    await worker.start()
"""

from affinity_router.core.exceptions import (
    AffinityRouterError,
    RegistryError,
    StateBackendError,
    TaskAlreadyProcessedError,
    TaskHandlerNotFoundError,
    TransportError,
    WorkerNotFoundError,
)
from affinity_router.core.hash_ring import ConsistentHashRing
from affinity_router.core.types import Task, TaskResult, TaskStatus, WorkerInfo
from affinity_router.registry.base import ServiceRegistry
from affinity_router.router.router import Router
from affinity_router.state.base import StateBackend
from affinity_router.transport.base import TransportBackend
from affinity_router.worker.cache import AffinityCache
from affinity_router.worker.worker import Worker

__all__ = [
    "AffinityCache",
    "AffinityRouterError",
    "ConsistentHashRing",
    "RegistryError",
    "Router",
    "ServiceRegistry",
    "StateBackend",
    "StateBackendError",
    "Task",
    "TaskAlreadyProcessedError",
    "TaskHandlerNotFoundError",
    "TaskResult",
    "TaskStatus",
    "TransportBackend",
    "TransportError",
    "Worker",
    "WorkerInfo",
    "WorkerNotFoundError",
]

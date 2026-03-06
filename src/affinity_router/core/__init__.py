"""Core module — pure-Python types, hash ring, and exceptions."""

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

__all__ = [
    "AffinityRouterError",
    "ConsistentHashRing",
    "RegistryError",
    "StateBackendError",
    "Task",
    "TaskAlreadyProcessedError",
    "TaskHandlerNotFoundError",
    "TaskResult",
    "TaskStatus",
    "TransportError",
    "WorkerInfo",
    "WorkerNotFoundError",
]

"""Core data types for AffinityRouter."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TaskStatus(str, Enum):
    """Status of a task in its lifecycle."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class Task:
    """Represents a unit of work to be routed and processed.

    Attributes:
        task_id: Unique identifier for idempotency.
        task_name: Name of the handler to invoke.
        routing_key: Key used for consistent-hash routing (data affinity).
        payload: Arbitrary data for the handler.
        created_at: Unix timestamp of creation.
    """

    task_name: str
    routing_key: str
    payload: dict[str, Any] = field(default_factory=dict)
    task_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: float = field(default_factory=time.time)


@dataclass(frozen=True, slots=True)
class TaskResult:
    """Result returned after a task is processed.

    Attributes:
        task_id: The ID of the original task.
        status: Final status of the task.
        result: Return value from the handler (if completed).
        error: Error message (if failed).
        completed_at: Unix timestamp of completion.
    """

    task_id: str
    status: TaskStatus
    result: Any = None
    error: str | None = None
    completed_at: float = field(default_factory=time.time)


@dataclass(frozen=True, slots=True)
class WorkerInfo:
    """Metadata about a registered worker.

    Attributes:
        worker_id: Unique identifier for the worker.
        host: Network address or hostname.
        port: Service port (optional, depends on transport).
        weight: Relative capacity weight for the hash ring.
        last_heartbeat: Unix timestamp of last heartbeat.
        metadata: Extra key-value pairs (e.g. supported task names).
    """

    worker_id: str
    host: str = "localhost"
    port: int = 0
    weight: int = 1
    last_heartbeat: float = field(default_factory=time.time)
    metadata: dict[str, str] = field(default_factory=dict)

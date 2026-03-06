"""AffinityRouter exception hierarchy."""


class AffinityRouterError(Exception):
    """Base exception for all AffinityRouter errors."""


class WorkerNotFoundError(AffinityRouterError):
    """Raised when no worker is available for a routing key."""


class TaskAlreadyProcessedError(AffinityRouterError):
    """Raised when a task has already been processed (idempotency check)."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__(f"Task {task_id!r} has already been processed")


class TaskHandlerNotFoundError(AffinityRouterError):
    """Raised when no handler is registered for a task name."""

    def __init__(self, task_name: str) -> None:
        self.task_name = task_name
        super().__init__(f"No handler registered for task {task_name!r}")


class TransportError(AffinityRouterError):
    """Raised when a transport operation fails."""


class RegistryError(AffinityRouterError):
    """Raised when a service registry operation fails."""


class StateBackendError(AffinityRouterError):
    """Raised when a state backend operation fails."""

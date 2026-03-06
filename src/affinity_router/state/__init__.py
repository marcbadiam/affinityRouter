"""State layer — task idempotency and status tracking."""

from affinity_router.state.base import StateBackend
from affinity_router.state.redis import RedisStateBackend

__all__ = ["RedisStateBackend", "StateBackend"]

"""Registry layer — worker discovery and health checking."""

from affinity_router.registry.base import ServiceRegistry
from affinity_router.registry.redis import RedisServiceRegistry

__all__ = ["RedisServiceRegistry", "ServiceRegistry"]

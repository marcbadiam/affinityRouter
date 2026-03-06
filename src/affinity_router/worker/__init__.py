"""Worker module — task processing with affinity-based local caching."""

from affinity_router.worker.cache import AffinityCache
from affinity_router.worker.worker import Worker

__all__ = ["AffinityCache", "Worker"]

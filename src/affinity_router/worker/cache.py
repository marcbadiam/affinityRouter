"""Thread-safe LRU cache with per-entry TTL for worker-local data.

This cache is the reason AffinityRouter exists: when the same routing key
always lands on the same worker, this cache stays warm and DB lookups
drop to near zero.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any


@dataclass
class _CacheEntry:
    """Internal: a cached value with its expiration timestamp."""

    value: Any
    expires_at: float


class AffinityCache:
    """LRU cache with per-entry TTL, designed for worker-local data.

    Args:
        max_size: Maximum number of entries before eviction.
        default_ttl: Default time-to-live in seconds for new entries.

    Example::

        cache = AffinityCache(max_size=10_000, default_ttl=300.0)
        cache.set("ticket:123", {"seat": "A1", "status": "sold"})
        hit = cache.get("ticket:123")  # -> {"seat": "A1", ...}
    """

    def __init__(
        self,
        max_size: int = 10_000,
        default_ttl: float = 300.0,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if default_ttl <= 0:
            raise ValueError("default_ttl must be > 0")
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._data: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        """Return the cached value for *key*, or ``None`` if missing/expired.

        Accessing an entry promotes it to the most-recently-used position.
        """
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            if time.monotonic() >= entry.expires_at:
                del self._data[key]
                return None
            self._data.move_to_end(key)
            return entry.value

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Store *value* under *key* with an optional custom *ttl*.

        If the cache is full the least-recently-used entry is evicted.
        """
        ttl = ttl if ttl is not None else self._default_ttl
        expires_at = time.monotonic() + ttl
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
            self._data[key] = _CacheEntry(value=value, expires_at=expires_at)
            while len(self._data) > self._max_size:
                self._data.popitem(last=False)

    def delete(self, key: str) -> bool:
        """Remove *key* from the cache.  Returns ``True`` if it existed."""
        with self._lock:
            return self._data.pop(key, None) is not None

    def clear(self) -> None:
        """Remove all entries."""
        with self._lock:
            self._data.clear()

    @property
    def size(self) -> int:
        """Current number of entries (including possibly expired ones)."""
        return len(self._data)

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None

    def __repr__(self) -> str:
        return (
            f"AffinityCache(max_size={self._max_size}, "
            f"default_ttl={self._default_ttl}, "
            f"entries={len(self._data)})"
        )

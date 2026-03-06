"""Tests for AffinityCache."""

import time

import pytest

from affinity_router.worker.cache import AffinityCache


class TestAffinityCache:
    def test_set_and_get(self):
        cache = AffinityCache()
        cache.set("k1", {"data": 42})
        assert cache.get("k1") == {"data": 42}

    def test_get_missing_returns_none(self):
        cache = AffinityCache()
        assert cache.get("missing") is None

    def test_ttl_expiration(self):
        cache = AffinityCache(default_ttl=0.1)
        cache.set("k1", "val")
        assert cache.get("k1") == "val"
        time.sleep(0.15)
        assert cache.get("k1") is None

    def test_custom_ttl(self):
        cache = AffinityCache(default_ttl=300)
        cache.set("k1", "val", ttl=0.1)
        time.sleep(0.15)
        assert cache.get("k1") is None

    def test_lru_eviction(self):
        cache = AffinityCache(max_size=3)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        # Access 'a' to make it most recently used
        cache.get("a")
        # Add 'd' — should evict 'b' (least recently used)
        cache.set("d", 4)
        assert cache.get("a") == 1
        assert cache.get("b") is None  # evicted
        assert cache.get("c") == 3
        assert cache.get("d") == 4

    def test_overwrite_existing_key(self):
        cache = AffinityCache()
        cache.set("k1", "old")
        cache.set("k1", "new")
        assert cache.get("k1") == "new"
        assert cache.size == 1

    def test_delete(self):
        cache = AffinityCache()
        cache.set("k1", "val")
        assert cache.delete("k1") is True
        assert cache.get("k1") is None
        assert cache.delete("k1") is False

    def test_clear(self):
        cache = AffinityCache()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.clear()
        assert cache.size == 0
        assert cache.get("a") is None

    def test_contains(self):
        cache = AffinityCache()
        cache.set("k1", "val")
        assert "k1" in cache
        assert "k2" not in cache

    def test_repr(self):
        cache = AffinityCache(max_size=100, default_ttl=60)
        assert "max_size=100" in repr(cache)
        assert "default_ttl=60" in repr(cache)

    def test_invalid_max_size(self):
        with pytest.raises(ValueError, match="max_size must be >= 1"):
            AffinityCache(max_size=0)

    def test_invalid_ttl(self):
        with pytest.raises(ValueError, match="default_ttl must be > 0"):
            AffinityCache(default_ttl=0)

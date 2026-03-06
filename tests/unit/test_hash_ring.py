"""Tests for ConsistentHashRing."""

import pytest

from affinity_router.core.hash_ring import ConsistentHashRing


class TestConsistentHashRing:
    """Test suite for the consistent hash ring."""

    def test_empty_ring_returns_none(self):
        ring = ConsistentHashRing()
        assert ring.get_node("any-key") is None

    def test_single_node_gets_all_keys(self):
        ring = ConsistentHashRing()
        ring.add_node("worker-1")
        for i in range(100):
            assert ring.get_node(f"key-{i}") == "worker-1"

    def test_same_key_same_node(self):
        """The core invariant: same key always maps to same node."""
        ring = ConsistentHashRing()
        ring.add_node("w1")
        ring.add_node("w2")
        ring.add_node("w3")
        key = "ticket:12345"
        target = ring.get_node(key)
        # Verify 100 lookups give the same result
        for _ in range(100):
            assert ring.get_node(key) == target

    def test_distribution_across_nodes(self):
        """Keys should distribute somewhat evenly across nodes."""
        ring = ConsistentHashRing(replicas=160)
        nodes = ["w1", "w2", "w3"]
        for n in nodes:
            ring.add_node(n)

        counts: dict[str, int] = dict.fromkeys(nodes, 0)
        num_keys = 10_000
        for i in range(num_keys):
            node = ring.get_node(f"key-{i}")
            assert node is not None
            counts[node] += 1

        # Each node should get at least 20% of keys (ideal is 33%)
        for n in nodes:
            assert counts[n] > num_keys * 0.20, f"{n} got only {counts[n]}"

    def test_add_node_minimal_redistribution(self):
        """Adding a node should only remap ~1/n of keys."""
        ring = ConsistentHashRing(replicas=160)
        ring.add_node("w1")
        ring.add_node("w2")

        # Record current assignments
        keys = [f"key-{i}" for i in range(1000)]
        before = {k: ring.get_node(k) for k in keys}

        # Add a third node
        ring.add_node("w3")
        after = {k: ring.get_node(k) for k in keys}

        changed = sum(1 for k in keys if before[k] != after[k])
        # Expect roughly 1/3 of keys to move (allow up to 50%)
        assert changed < len(keys) * 0.50, f"Too many keys moved: {changed}"
        assert changed > 0, "At least some keys should move"

    def test_remove_node_minimal_redistribution(self):
        """Removing a node should only remap that node's keys."""
        ring = ConsistentHashRing(replicas=160)
        ring.add_node("w1")
        ring.add_node("w2")
        ring.add_node("w3")

        keys = [f"key-{i}" for i in range(1000)]
        before = {k: ring.get_node(k) for k in keys}

        ring.remove_node("w2")
        after = {k: ring.get_node(k) for k in keys}

        # Only keys that were on w2 should move
        for k in keys:
            if before[k] != "w2":
                msg = f"{k} moved from {before[k]} to {after[k]}"
                assert after[k] == before[k], msg

    def test_weighted_nodes(self):
        """Heavier nodes should receive proportionally more keys."""
        ring = ConsistentHashRing(replicas=100)
        ring.add_node("heavy", weight=3)
        ring.add_node("light", weight=1)

        counts = {"heavy": 0, "light": 0}
        for i in range(10_000):
            node = ring.get_node(f"key-{i}")
            assert node is not None
            counts[node] += 1

        # Heavy should get roughly 3x light (allow 2x-4x)
        ratio = counts["heavy"] / max(counts["light"], 1)
        assert 1.5 < ratio < 5.0, f"Weight ratio off: {ratio:.1f}"

    def test_get_nodes_returns_distinct(self):
        ring = ConsistentHashRing()
        ring.add_node("w1")
        ring.add_node("w2")
        ring.add_node("w3")
        nodes = ring.get_nodes("some-key", count=3)
        assert len(nodes) == 3
        assert len(set(nodes)) == 3

    def test_get_nodes_count_exceeds_nodes(self):
        ring = ConsistentHashRing()
        ring.add_node("w1")
        ring.add_node("w2")
        nodes = ring.get_nodes("key", count=5)
        assert len(nodes) == 2  # can't return more than available

    def test_remove_nonexistent_node_is_noop(self):
        ring = ConsistentHashRing()
        ring.add_node("w1")
        ring.remove_node("nonexistent")
        assert len(ring) == 1

    def test_add_node_replaces_on_weight_change(self):
        ring = ConsistentHashRing(replicas=10)
        ring.add_node("w1", weight=1)
        assert ring.total_virtual_nodes == 10
        ring.add_node("w1", weight=2)
        assert ring.total_virtual_nodes == 20
        assert ring.nodes["w1"] == 2

    def test_len_and_contains(self):
        ring = ConsistentHashRing()
        assert len(ring) == 0
        ring.add_node("w1")
        assert len(ring) == 1
        assert "w1" in ring
        assert "w2" not in ring

    def test_repr(self):
        ring = ConsistentHashRing(replicas=10)
        ring.add_node("w1")
        assert "replicas=10" in repr(ring)
        assert "nodes=1" in repr(ring)

    def test_invalid_replicas(self):
        with pytest.raises(ValueError, match="replicas must be >= 1"):
            ConsistentHashRing(replicas=0)

    def test_invalid_weight(self):
        ring = ConsistentHashRing()
        with pytest.raises(ValueError, match="weight must be >= 1"):
            ring.add_node("w1", weight=0)

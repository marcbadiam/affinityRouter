"""Consistent hash ring implementation using only the Python standard library.

This module provides a ring-based consistent hashing algorithm with virtual
nodes (replicas) for even distribution and weighted node support.  When a
node is added or removed only ~1/n of the keys are remapped, avoiding the
"thundering herd" cache invalidation that modulo-based schemes suffer from.
"""

from __future__ import annotations

import bisect
import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Hashable


class ConsistentHashRing:
    """A consistent hash ring with virtual nodes and weighted placement.

    Each physical node is mapped to ``replicas * weight`` positions on a
    32-bit integer ring.  Key look-ups are O(log n) where *n* is the total
    number of virtual nodes.

    Args:
        replicas: Base number of virtual nodes per physical node.

    Example::

        ring = ConsistentHashRing(replicas=160)
        ring.add_node("worker-1", weight=2)
        ring.add_node("worker-2", weight=1)
        target = ring.get_node("ticket:12345")
    """

    def __init__(self, replicas: int = 160) -> None:
        if replicas < 1:
            raise ValueError("replicas must be >= 1")
        self._replicas = replicas
        self._ring: dict[int, str] = {}
        self._sorted_keys: list[int] = []
        self._nodes: dict[str, int] = {}  # node_id -> weight

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _hash(key: str) -> int:
        """Return a 32-bit integer hash using MD5 (fast, well-distributed)."""
        digest = hashlib.md5(key.encode(), usedforsecurity=False).digest()
        return int.from_bytes(digest[:4], byteorder="big")

    def _virtual_node_key(self, node_id: str, index: int) -> str:
        """Deterministic string for a virtual node position."""
        return f"{node_id}:{index}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_node(self, node_id: str, weight: int = 1) -> None:
        """Add a physical node (or update its weight) on the ring.

        Creates ``replicas * weight`` virtual node positions.
        """
        if weight < 1:
            raise ValueError("weight must be >= 1")
        if node_id in self._nodes:
            self.remove_node(node_id)
        self._nodes[node_id] = weight
        for i in range(self._replicas * weight):
            h = self._hash(self._virtual_node_key(node_id, i))
            self._ring[h] = node_id
            bisect.insort(self._sorted_keys, h)

    def remove_node(self, node_id: str) -> None:
        """Remove a physical node and all its virtual positions."""
        weight = self._nodes.pop(node_id, None)
        if weight is None:
            return
        for i in range(self._replicas * weight):
            h = self._hash(self._virtual_node_key(node_id, i))
            self._ring.pop(h, None)
            idx = bisect.bisect_left(self._sorted_keys, h)
            if idx < len(self._sorted_keys) and self._sorted_keys[idx] == h:
                self._sorted_keys.pop(idx)

    def get_node(self, key: Hashable) -> str | None:
        """Return the node responsible for *key*, or ``None`` if the ring is empty.

        Look-up is O(log n) in the number of virtual nodes.
        """
        if not self._sorted_keys:
            return None
        h = self._hash(str(key))
        idx = bisect.bisect_right(self._sorted_keys, h)
        if idx == len(self._sorted_keys):
            idx = 0  # wrap around the ring
        return self._ring[self._sorted_keys[idx]]

    def get_nodes(self, key: Hashable, count: int = 1) -> list[str]:
        """Return up to *count* distinct nodes for *key* (for replication).

        Walks clockwise from the key's position, skipping duplicate physical
        nodes.
        """
        if not self._sorted_keys or count < 1:
            return []
        h = self._hash(str(key))
        idx = bisect.bisect_right(self._sorted_keys, h)
        seen: set[str] = set()
        result: list[str] = []
        total = len(self._sorted_keys)
        for offset in range(total):
            pos = (idx + offset) % total
            node_id = self._ring[self._sorted_keys[pos]]
            if node_id not in seen:
                seen.add(node_id)
                result.append(node_id)
                if len(result) == count:
                    break
        return result

    @property
    def nodes(self) -> dict[str, int]:
        """Mapping of node IDs to their weights (read-only copy)."""
        return dict(self._nodes)

    @property
    def total_virtual_nodes(self) -> int:
        """Total number of virtual positions on the ring."""
        return len(self._sorted_keys)

    def __len__(self) -> int:
        """Number of physical nodes on the ring."""
        return len(self._nodes)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._nodes

    def __repr__(self) -> str:
        return (
            f"ConsistentHashRing(replicas={self._replicas}, "
            f"nodes={len(self._nodes)}, "
            f"vnodes={len(self._sorted_keys)})"
        )

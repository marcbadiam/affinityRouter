"""Integration tests for RedisServiceRegistry."""

import fakeredis.aioredis
import pytest

from affinity_router.core.types import WorkerInfo
from affinity_router.registry.redis import RedisServiceRegistry


@pytest.fixture
async def registry():
    server = fakeredis.FakeServer()
    r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)
    reg = RedisServiceRegistry(r)
    yield reg
    await reg.close()


class TestRedisServiceRegistry:
    async def test_register_and_get_active(self, registry):
        worker = WorkerInfo(worker_id="w1", host="10.0.0.1", port=8080, weight=2)
        await registry.register(worker)

        active = await registry.get_active_workers(timeout=30.0)
        assert len(active) == 1
        assert active[0].worker_id == "w1"
        assert active[0].host == "10.0.0.1"
        assert active[0].port == 8080
        assert active[0].weight == 2

    async def test_deregister_removes_worker(self, registry):
        worker = WorkerInfo(worker_id="w1")
        await registry.register(worker)
        await registry.deregister("w1")

        active = await registry.get_active_workers(timeout=30.0)
        assert len(active) == 0

    async def test_heartbeat_updates_timestamp(self, registry):
        worker = WorkerInfo(worker_id="w1")
        await registry.register(worker)

        # Heartbeat should keep the worker active
        await registry.heartbeat("w1")
        active = await registry.get_active_workers(timeout=30.0)
        assert len(active) == 1

    async def test_expired_worker_not_returned(self, registry):
        worker = WorkerInfo(worker_id="w1")
        await registry.register(worker)

        # With a very small timeout, the worker should be considered dead
        # immediately after registration (since time has passed)
        active = await registry.get_active_workers(timeout=0.0)
        assert len(active) == 0

    async def test_multiple_workers(self, registry):
        for i in range(3):
            await registry.register(WorkerInfo(worker_id=f"w{i}"))

        active = await registry.get_active_workers(timeout=30.0)
        assert len(active) == 3
        ids = {w.worker_id for w in active}
        assert ids == {"w0", "w1", "w2"}

    async def test_metadata_preserved(self, registry):
        worker = WorkerInfo(
            worker_id="w1",
            metadata={"region": "eu-west", "gpu": "true"},
        )
        await registry.register(worker)

        active = await registry.get_active_workers(timeout=30.0)
        assert active[0].metadata == {"region": "eu-west", "gpu": "true"}

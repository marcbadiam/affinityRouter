"""Integration tests for RedisStreamTransport."""

import fakeredis.aioredis
import pytest

from affinity_router.core.types import Task
from affinity_router.transport.redis import RedisStreamTransport


@pytest.fixture
async def transport():
    server = fakeredis.FakeServer()
    r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)
    t = RedisStreamTransport(r, block_ms=100)
    yield t
    await t.close()


class TestRedisStreamTransport:
    async def test_send_and_receive(self, transport):
        task = Task(
            task_name="pay",
            routing_key="ticket:1",
            payload={"amount": 100},
            task_id="task-001",
        )
        await transport.send_task("w1", task)

        received = []
        async for t in transport.receive_tasks("w1"):
            received.append(t)
            break  # only consume one

        assert len(received) == 1
        assert received[0].task_id == "task-001"
        assert received[0].task_name == "pay"
        assert received[0].routing_key == "ticket:1"
        assert received[0].payload == {"amount": 100}

    async def test_acknowledge(self, transport):
        task = Task(task_name="t", routing_key="k", task_id="ack-test")
        await transport.send_task("w1", task)

        async for _t in transport.receive_tasks("w1"):
            break

        # Should not raise
        await transport.acknowledge("w1", "ack-test")

    async def test_acknowledge_unknown_task(self, transport):
        # Should log warning but not raise
        await transport.acknowledge("w1", "nonexistent")

    async def test_multiple_tasks_ordered(self, transport):
        for i in range(3):
            task = Task(task_name="t", routing_key="k", task_id=f"task-{i}")
            await transport.send_task("w1", task)

        received = []
        async for t in transport.receive_tasks("w1"):
            received.append(t)
            if len(received) == 3:
                break

        assert [t.task_id for t in received] == ["task-0", "task-1", "task-2"]

    async def test_per_worker_isolation(self, transport):
        """Tasks sent to w1 should not be received by w2."""
        await transport.send_task(
            "w1", Task(task_name="t", routing_key="k", task_id="for-w1")
        )
        await transport.send_task(
            "w2", Task(task_name="t", routing_key="k", task_id="for-w2")
        )

        # w1 receives only its task
        async for t in transport.receive_tasks("w1"):
            assert t.task_id == "for-w1"
            break

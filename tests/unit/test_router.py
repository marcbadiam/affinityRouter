"""Tests for the Router."""

from unittest.mock import AsyncMock

import pytest

from affinity_router.core.exceptions import WorkerNotFoundError
from affinity_router.core.types import Task, WorkerInfo
from affinity_router.router.router import Router


def _make_registry(workers: list[WorkerInfo] | None = None) -> AsyncMock:
    registry = AsyncMock()
    registry.get_active_workers = AsyncMock(return_value=workers or [])
    registry.close = AsyncMock()
    return registry


def _make_transport() -> AsyncMock:
    transport = AsyncMock()
    transport.send_task = AsyncMock()
    transport.close = AsyncMock()
    return transport


class TestRouterSubmit:
    async def test_submit_routes_to_correct_worker(self):
        workers = [
            WorkerInfo(worker_id="w1"),
            WorkerInfo(worker_id="w2"),
        ]
        registry = _make_registry(workers)
        transport = _make_transport()

        router = Router(transport=transport, registry=registry)
        await router.start()

        task = await router.submit(
            routing_key="ticket:123",
            task_name="process",
            payload={"amount": 100},
        )

        transport.send_task.assert_called_once()
        call_args = transport.send_task.call_args
        task_sent = call_args[1]["task"] if "task" in call_args[1] else call_args[0][1]
        assert task_sent.task_name == "process"
        assert isinstance(task, Task)
        assert task.routing_key == "ticket:123"
        await router.stop()

    async def test_submit_same_key_same_worker(self):
        workers = [WorkerInfo(worker_id=f"w{i}") for i in range(5)]
        registry = _make_registry(workers)
        transport = _make_transport()

        router = Router(transport=transport, registry=registry)
        await router.start()

        # Submit same key multiple times
        targets = set()
        for _ in range(10):
            await router.submit(routing_key="ticket:999", task_name="pay")
            call_args = transport.send_task.call_args
            targets.add(call_args[0][0])  # worker_id is first positional arg

        assert len(targets) == 1, "Same key should always go to same worker"
        await router.stop()

    async def test_submit_no_workers_raises(self):
        registry = _make_registry([])
        transport = _make_transport()

        router = Router(transport=transport, registry=registry)
        # Don't start — ring is empty
        with pytest.raises(WorkerNotFoundError):
            await router.submit(routing_key="k", task_name="t")

    async def test_submit_with_explicit_task_id(self):
        workers = [WorkerInfo(worker_id="w1")]
        registry = _make_registry(workers)
        transport = _make_transport()

        router = Router(transport=transport, registry=registry)
        await router.start()

        task = await router.submit(
            routing_key="k", task_name="t", task_id="my-custom-id"
        )
        assert task.task_id == "my-custom-id"
        await router.stop()


class TestRouterSync:
    async def test_sync_adds_new_workers(self):
        registry = _make_registry([WorkerInfo(worker_id="w1")])
        transport = _make_transport()

        router = Router(transport=transport, registry=registry)
        await router._sync_ring()

        assert router.worker_count == 1
        assert "w1" in router.ring

    async def test_sync_removes_dead_workers(self):
        registry = _make_registry(
            [WorkerInfo(worker_id="w1"), WorkerInfo(worker_id="w2")]
        )
        transport = _make_transport()

        router = Router(transport=transport, registry=registry)
        await router._sync_ring()
        assert router.worker_count == 2

        # w2 dies
        registry.get_active_workers.return_value = [WorkerInfo(worker_id="w1")]
        await router._sync_ring()
        assert router.worker_count == 1
        assert "w2" not in router.ring

    async def test_sync_updates_weight(self):
        registry = _make_registry([WorkerInfo(worker_id="w1", weight=1)])
        transport = _make_transport()

        router = Router(transport=transport, registry=registry)
        await router._sync_ring()
        assert router.ring.nodes["w1"] == 1

        # Weight changes
        registry.get_active_workers.return_value = [
            WorkerInfo(worker_id="w1", weight=3)
        ]
        await router._sync_ring()
        assert router.ring.nodes["w1"] == 3


class TestRouterLifecycle:
    async def test_start_and_stop(self):
        registry = _make_registry([])
        transport = _make_transport()

        router = Router(transport=transport, registry=registry)
        await router.start()
        assert router.is_running
        await router.stop()
        assert not router.is_running
        transport.close.assert_called_once()
        registry.close.assert_called_once()

    async def test_double_stop_is_safe(self):
        registry = _make_registry([])
        transport = _make_transport()

        router = Router(transport=transport, registry=registry)
        await router.start()
        await router.stop()
        await router.stop()  # should not raise

    def test_repr(self):
        router = Router(transport=AsyncMock(), registry=AsyncMock())
        assert "workers=0" in repr(router)

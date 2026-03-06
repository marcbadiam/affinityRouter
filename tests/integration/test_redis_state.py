"""Integration tests for RedisStateBackend."""

import fakeredis.aioredis
import pytest

from affinity_router.core.types import TaskResult, TaskStatus
from affinity_router.state.redis import RedisStateBackend


@pytest.fixture
async def state():
    server = fakeredis.FakeServer()
    r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)
    s = RedisStateBackend(r)
    yield s
    await s.close()


class TestRedisStateBackend:
    async def test_try_acquire_new_task(self, state):
        acquired = await state.try_acquire("task-1")
        assert acquired is True

    async def test_try_acquire_duplicate_blocked(self, state):
        await state.try_acquire("task-1")
        second = await state.try_acquire("task-1")
        assert second is False

    async def test_get_status_unknown(self, state):
        status = await state.get_status("nonexistent")
        assert status is None

    async def test_get_status_processing(self, state):
        await state.try_acquire("task-1")
        status = await state.get_status("task-1")
        assert status == TaskStatus.PROCESSING

    async def test_mark_completed_and_get_result(self, state):
        await state.try_acquire("task-1")

        result = TaskResult(
            task_id="task-1",
            status=TaskStatus.COMPLETED,
            result={"paid": True},
        )
        await state.mark_completed("task-1", result)

        status = await state.get_status("task-1")
        assert status == TaskStatus.COMPLETED

        stored = await state.get_result("task-1")
        assert stored is not None
        assert stored.task_id == "task-1"
        assert stored.status == TaskStatus.COMPLETED
        assert stored.result == {"paid": True}

    async def test_mark_failed_and_get_result(self, state):
        await state.try_acquire("task-1")

        result = TaskResult(
            task_id="task-1",
            status=TaskStatus.FAILED,
            error="Payment declined",
        )
        await state.mark_failed("task-1", result)

        stored = await state.get_result("task-1")
        assert stored is not None
        assert stored.status == TaskStatus.FAILED
        assert stored.error == "Payment declined"

    async def test_get_result_returns_none_while_processing(self, state):
        await state.try_acquire("task-1")
        result = await state.get_result("task-1")
        assert result is None  # still processing

    async def test_get_result_returns_none_for_unknown(self, state):
        result = await state.get_result("nonexistent")
        assert result is None

    async def test_idempotency_after_completion(self, state):
        """Once completed, task cannot be re-acquired."""
        await state.try_acquire("task-1")
        await state.mark_completed(
            "task-1",
            TaskResult(task_id="task-1", status=TaskStatus.COMPLETED, result=True),
        )

        # Try to re-acquire — should fail because key still exists
        acquired = await state.try_acquire("task-1")
        assert acquired is False

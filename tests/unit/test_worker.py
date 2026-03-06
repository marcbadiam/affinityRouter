"""Tests for the Worker."""

from unittest.mock import AsyncMock

from affinity_router.core.types import Task, TaskStatus
from affinity_router.worker.worker import Worker


def _make_mocks():
    transport = AsyncMock()
    registry = AsyncMock()
    state = AsyncMock()
    return transport, registry, state


class TestWorkerDecorator:
    def test_register_async_handler(self):
        transport, registry, state = _make_mocks()
        worker = Worker("w1", transport, registry, state)

        @worker.task("pay")
        async def handle_pay(task):
            return {"paid": True}

        assert "pay" in worker.registered_tasks

    def test_register_sync_handler(self):
        transport, registry, state = _make_mocks()
        worker = Worker("w1", transport, registry, state)

        @worker.task("pdf")
        def generate_pdf(task):
            return b"pdf-bytes"

        assert "pdf" in worker.registered_tasks

    def test_decorator_returns_original_function(self):
        transport, registry, state = _make_mocks()
        worker = Worker("w1", transport, registry, state)

        @worker.task("pay")
        async def handle_pay(task):
            return True

        # The original function is returned unchanged
        assert handle_pay.__name__ == "handle_pay"


class TestWorkerHandleTask:
    async def test_async_handler_success(self):
        transport, registry, state = _make_mocks()
        state.try_acquire = AsyncMock(return_value=True)

        worker = Worker("w1", transport, registry, state)

        @worker.task("pay")
        async def handle_pay(task):
            return {"paid": True}

        task = Task(task_name="pay", routing_key="t:1", task_id="task-1")
        await worker._handle_task(task)

        state.try_acquire.assert_called_once_with("task-1")
        state.mark_completed.assert_called_once()
        result = state.mark_completed.call_args[0][1]
        assert result.status == TaskStatus.COMPLETED
        assert result.result == {"paid": True}
        transport.acknowledge.assert_called_once_with("w1", "task-1")

    async def test_sync_handler_wrapped_with_to_thread(self):
        transport, registry, state = _make_mocks()
        state.try_acquire = AsyncMock(return_value=True)

        worker = Worker("w1", transport, registry, state)

        @worker.task("pdf")
        def generate_pdf(task):
            return "pdf-content"

        task = Task(task_name="pdf", routing_key="t:1", task_id="task-2")
        await worker._handle_task(task)

        state.mark_completed.assert_called_once()
        result = state.mark_completed.call_args[0][1]
        assert result.result == "pdf-content"

    async def test_idempotency_skip(self):
        transport, registry, state = _make_mocks()
        state.try_acquire = AsyncMock(return_value=False)

        worker = Worker("w1", transport, registry, state)

        @worker.task("pay")
        async def handle_pay(task):
            return {"paid": True}

        task = Task(task_name="pay", routing_key="t:1", task_id="dup-task")
        await worker._handle_task(task)

        # Handler should NOT be called
        state.mark_completed.assert_not_called()
        # But should still acknowledge
        transport.acknowledge.assert_called_once_with("w1", "dup-task")

    async def test_handler_not_found(self):
        transport, registry, state = _make_mocks()
        state.try_acquire = AsyncMock(return_value=True)

        worker = Worker("w1", transport, registry, state)
        # No handler registered for "unknown"

        task = Task(task_name="unknown", routing_key="t:1", task_id="task-3")
        await worker._handle_task(task)

        state.mark_failed.assert_called_once()
        result = state.mark_failed.call_args[0][1]
        assert result.status == TaskStatus.FAILED
        assert "No handler" in (result.error or "")
        transport.acknowledge.assert_called_once()

    async def test_handler_exception_marks_failed(self):
        transport, registry, state = _make_mocks()
        state.try_acquire = AsyncMock(return_value=True)

        worker = Worker("w1", transport, registry, state)

        @worker.task("boom")
        async def handle_boom(task):
            raise ValueError("something broke")

        task = Task(task_name="boom", routing_key="t:1", task_id="task-4")
        await worker._handle_task(task)

        state.mark_failed.assert_called_once()
        result = state.mark_failed.call_args[0][1]
        assert result.status == TaskStatus.FAILED
        assert "ValueError" in (result.error or "")
        transport.acknowledge.assert_called_once()


class TestWorkerProperties:
    def test_worker_id(self):
        transport, registry, state = _make_mocks()
        worker = Worker("my-worker", transport, registry, state)
        assert worker.worker_id == "my-worker"

    def test_is_running_default_false(self):
        transport, registry, state = _make_mocks()
        worker = Worker("w1", transport, registry, state)
        assert not worker.is_running

    def test_repr(self):
        transport, registry, state = _make_mocks()
        worker = Worker("w1", transport, registry, state)
        assert "w1" in repr(worker)

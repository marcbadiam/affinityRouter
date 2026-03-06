"""Tests for core data types."""

import time

from affinity_router.core.types import Task, TaskResult, TaskStatus, WorkerInfo


class TestTask:
    def test_auto_generates_id(self):
        t1 = Task(task_name="pay", routing_key="t:1")
        t2 = Task(task_name="pay", routing_key="t:1")
        assert t1.task_id != t2.task_id
        assert len(t1.task_id) == 32  # uuid4 hex

    def test_explicit_id(self):
        t = Task(task_name="pay", routing_key="t:1", task_id="custom-id")
        assert t.task_id == "custom-id"

    def test_frozen(self):
        t = Task(task_name="pay", routing_key="t:1")
        import pytest

        with pytest.raises(AttributeError):
            t.task_name = "other"  # type: ignore[misc]

    def test_default_payload(self):
        t = Task(task_name="pay", routing_key="t:1")
        assert t.payload == {}

    def test_created_at_auto(self):
        before = time.time()
        t = Task(task_name="pay", routing_key="t:1")
        after = time.time()
        assert before <= t.created_at <= after


class TestTaskResult:
    def test_completed(self):
        r = TaskResult(
            task_id="abc",
            status=TaskStatus.COMPLETED,
            result={"ok": True},
        )
        assert r.status == TaskStatus.COMPLETED
        assert r.error is None

    def test_failed(self):
        r = TaskResult(
            task_id="abc",
            status=TaskStatus.FAILED,
            error="boom",
        )
        assert r.status == TaskStatus.FAILED
        assert r.error == "boom"


class TestTaskStatus:
    def test_values(self):
        assert TaskStatus.PENDING == "pending"
        assert TaskStatus.PROCESSING == "processing"
        assert TaskStatus.COMPLETED == "completed"
        assert TaskStatus.FAILED == "failed"


class TestWorkerInfo:
    def test_defaults(self):
        w = WorkerInfo(worker_id="w1")
        assert w.host == "localhost"
        assert w.port == 0
        assert w.weight == 1
        assert w.metadata == {}

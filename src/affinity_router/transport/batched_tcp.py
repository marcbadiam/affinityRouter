"""Batched TCP transport backend.

Router side maintains persistent asyncio.StreamWriter connections to each Worker
with sender-side batching — tasks are accumulated per worker and flushed based
on batch size or flush interval. Worker side runs an asyncio TCP server listening
for newline-delimited JSON messages, handling both single tasks and batched
messages transparently.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import TYPE_CHECKING, Any

from affinity_router.core.exceptions import TransportError
from affinity_router.core.types import Task
from affinity_router.transport.base import TransportBackend

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from affinity_router.core.types import WorkerInfo
    from affinity_router.registry.base import ServiceRegistry

logger = logging.getLogger(__name__)

__all__ = ["BatchedTcpTransport"]


class BatchedTcpTransport(TransportBackend):
    """Batched TCP transport with sender-side batching for reduced syscall overhead.

    Router side: Maintains persistent asyncio.StreamWriter to each Worker with
    batching — tasks are accumulated per worker and flushed based on batch size
    or flush interval.

    Worker side: Runs an asyncio TCP server listening for newline-delimited JSON.
    Handles both single tasks and batched messages transparently.

    Args:
        registry: Service registry for worker discovery and host/port resolution.
        host: Host address to bind the TCP server on (worker side).
        port: Port to bind the TCP server on (worker side). Use 0 for auto-assign.
        batch_size: Maximum number of tasks to accumulate before flushing.
        flush_interval_ms: Maximum milliseconds to wait before flushing a batch.
    """

    def __init__(
        self,
        registry: ServiceRegistry,
        host: str = "127.0.0.1",
        port: int = 0,
        batch_size: int = 100,
        flush_interval_ms: int = 10,
    ) -> None:
        self.registry = registry
        self.host = host
        self.port = port
        self.batch_size = batch_size
        self.flush_interval_ms = flush_interval_ms

        self.worker_cache: dict[str, WorkerInfo] = {}
        self._lock = asyncio.Lock()

        # Router side state
        self.writers: dict[str, asyncio.StreamWriter] = {}
        self._pending_batches: dict[str, list[dict[str, Any]]] = {}
        self._batch_queues: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._flush_tasks: dict[str, asyncio.Task[Any]] = {}
        self._flush_events: dict[str, asyncio.Event] = {}

        # Worker side state
        self.queue: asyncio.Queue[Task] = asyncio.Queue()
        self.server: asyncio.AbstractServer | None = None

    async def _get_writer(self, worker_id: str) -> asyncio.StreamWriter:
        """Get or create a StreamWriter for the specified worker."""
        writer = self.writers.get(worker_id)
        if writer is None or writer.is_closing():
            # Perform blocking resolution and connection without holding the mutex
            worker_info = self.worker_cache.get(worker_id)
            if not worker_info or worker_info.port == 0:
                workers = await self.registry.get_active_workers()
                for w in workers:
                    self.worker_cache[w.worker_id] = w
                worker_info = self.worker_cache.get(worker_id)

            if not worker_info:
                raise TransportError(
                    f"Could not resolve host/port for worker {worker_id}"
                )

            _, new_writer = await asyncio.open_connection(
                worker_info.host, worker_info.port
            )
            
            async with self._lock:
                writer = self.writers.get(worker_id)
                # Ensure no other coroutine created it while we were connecting
                if writer is None or writer.is_closing():
                    self.writers[worker_id] = new_writer
                    writer = new_writer

                    # Initialize batching state for this worker
                    if worker_id not in self._batch_queues:
                        self._batch_queues[worker_id] = asyncio.Queue()
                        self._pending_batches[worker_id] = []
                        self._flush_events[worker_id] = asyncio.Event()
                        self._flush_tasks[worker_id] = asyncio.create_task(
                            self._flush_loop(worker_id),
                            name=f"batch-flush-{worker_id}",
                        )
                else:
                    new_writer.close()

        return writer

    async def _flush_loop(self, worker_id: str) -> None:
        """Background task that periodically flushes pending batches."""
        try:
            while True:
                try:
                    # Wait for flush interval or until explicitly triggered
                    await asyncio.wait_for(
                        self._flush_events[worker_id].wait(),
                        timeout=self.flush_interval_ms / 1000.0,
                    )
                    self._flush_events[worker_id].clear()
                except asyncio.TimeoutError:
                    pass

                await self._flush_batch(worker_id)
        except asyncio.CancelledError:
            # Final flush before cancellation
            await self._flush_batch(worker_id)
            raise

    async def _flush_batch(self, worker_id: str) -> None:
        """Flush pending tasks for a worker if any exist."""
        async with self._lock:
            pending = self._pending_batches.get(worker_id, [])
            if not pending:
                return

            writer = self.writers.get(worker_id)
            if writer is None or writer.is_closing():
                # Writer disconnected, clear pending (tasks will be lost or retried)
                self._pending_batches[worker_id] = []
                return

            try:
                # Send batched message format
                batch_payload = {"batch": True, "tasks": pending}
                data = json.dumps(batch_payload).encode("utf-8") + b"\n"
                writer.write(data)
                await asyncio.wait_for(writer.drain(), timeout=2.0)

                # Clear pending batch
                self._pending_batches[worker_id] = []
            except Exception as e:
                logger.error(f"Failed to flush batch to worker {worker_id}: {e}")
                self.writers.pop(worker_id, None)
                raise TransportError(f"Failed to flush batch via TCP: {e}") from e

    async def send_task(self, worker_id: str, task: Task) -> None:
        """Deliver a task to a specific worker's queue via batched TCP.

        Args:
            worker_id: The target worker that should process this task.
            task: The task to deliver.

        Raises:
            TransportError: If delivery fails.
        """
        try:
            await self._get_writer(worker_id)

            task_dict = {
                "task_id": task.task_id,
                "task_name": task.task_name,
                "routing_key": task.routing_key,
                "payload": task.payload,
                "created_at": task.created_at,
            }

            async with self._lock:
                self._pending_batches.setdefault(worker_id, []).append(task_dict)
                current_size = len(self._pending_batches[worker_id])

                # Trigger flush if batch size reached
                if current_size >= self.batch_size:
                    self._flush_events[worker_id].set()

        except Exception as e:
            if not isinstance(e, TransportError):
                raise TransportError(f"Failed to queue task via TCP: {e}") from e
            raise

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle incoming client connections on the worker side."""
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break  # Connection closed remotely

                try:
                    payload = json.loads(data.decode("utf-8").strip())
                except json.JSONDecodeError as e:
                    logger.error(f"Malformed JSON payload received: {e} - data: {data[:100]}...")
                    continue

                # Handle batched messages
                if payload.get("batch") is True:
                    tasks_data = payload.get("tasks", [])
                    for task_data in tasks_data:
                        task = Task(
                            task_id=task_data["task_id"],
                            task_name=task_data["task_name"],
                            routing_key=task_data["routing_key"],
                            payload=task_data["payload"],
                            created_at=task_data["created_at"],
                        )
                        await self.queue.put(task)
                else:
                    # Handle single task (backwards compatible)
                    task = Task(
                        task_id=payload["task_id"],
                        task_name=payload["task_name"],
                        routing_key=payload["routing_key"],
                        payload=payload["payload"],
                        created_at=payload["created_at"],
                    )
                    await self.queue.put(task)

        except asyncio.IncompleteReadError:
            pass
        except Exception as e:
            logger.error(f"TCP Server error: {e}")
        finally:
            writer.close()

    async def receive_tasks(self, worker_id: str) -> AsyncIterator[Task]:
        """Yield tasks destined for *worker_id* as they arrive.

        Starts an asyncio TCP server listening for incoming task messages.

        Args:
            worker_id: The worker whose queue to consume.

        Yields:
            Task instances in arrival order.
        """
        self.server = await asyncio.start_server(
            self._handle_client, self.host, self.port, limit=1024 * 1024 * 10
        )
        logger.info(f"Batched TCP Transport listening on {self.host}:{self.port}")

        while True:
            try:
                task = await self.queue.get()
                yield task
            except asyncio.CancelledError:
                break

    async def acknowledge(self, worker_id: str, task_id: str) -> None:
        """Acknowledge successful processing of a task.

        This transport tracks acknowledgements globally via Redis metrics,
        so this method is a no-op.

        Args:
            worker_id: The worker that processed the task.
            task_id: The unique ID of the processed task.
        """
        pass  # Tracked globally via Redis metrics

    async def close(self) -> None:
        """Release all resources held by the transport.

        Cancels flush tasks, flushes remaining batches, closes writers,
        and shuts down the TCP server.
        """
        # Cancel flush tasks
        for task in self._flush_tasks.values():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._flush_tasks.clear()

        # Flush any remaining batches before closing writers
        for worker_id in list(self._pending_batches.keys()):
            await self._flush_batch(worker_id)

        # Close writers
        for w in self.writers.values():
            w.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(w.wait_closed(), timeout=2.0)
        self.writers.clear()

        # Close server
        if self.server:
            self.server.close()
            with contextlib.suppress(Exception):
                await self.server.wait_closed()
            self.server = None

        # Free any lingering queue gets
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break

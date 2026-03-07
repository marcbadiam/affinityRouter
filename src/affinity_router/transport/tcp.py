"""Direct TCP transport backend.

Router side: Maintains a persistent asyncio.StreamWriter to each Worker.
Worker side: Runs an asyncio TCP server listening for newline-delimited JSON.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import TYPE_CHECKING

from affinity_router.core.exceptions import TransportError
from affinity_router.core.types import Task
from affinity_router.transport.base import TransportBackend

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from affinity_router.core.types import WorkerInfo
    from affinity_router.registry.base import ServiceRegistry

logger = logging.getLogger(__name__)

__all__ = ["DirectTcpTransport"]


class DirectTcpTransport(TransportBackend):
    """Direct persistent TCP transport for maximum latency and throughput.

    Args:
        registry: Service registry for worker discovery.
        host: Host address to bind the server (worker side).
        port: Port to bind the server (worker side). Use 0 for auto-assign.

    Example::

        from affinity_router.registry.redis import RedisServiceRegistry
        registry = RedisServiceRegistry(redis)
        transport = DirectTcpTransport(registry, host="0.0.0.0", port=8000)
    """

    def __init__(
        self, registry: ServiceRegistry, host: str = "127.0.0.1", port: int = 0
    ) -> None:
        self._registry = registry
        self._host = host
        self._port = port

        self._worker_cache: dict[str, WorkerInfo] = {}
        self._lock = asyncio.Lock()

        # Router side state
        self._writers: dict[str, asyncio.StreamWriter] = {}

        # Worker side state
        self._queue: asyncio.Queue[Task] = asyncio.Queue()
        self._server: asyncio.AbstractServer | None = None

    async def _get_writer(self, worker_id: str) -> asyncio.StreamWriter:
        """Get or create a StreamWriter for the given worker.

        Args:
            worker_id: The worker to connect to.

        Returns:
            An asyncio StreamWriter for the connection.

        Raises:
            TransportError: If the worker cannot be resolved or connected.
        """
        writer = self._writers.get(worker_id)
        if writer is None or writer.is_closing():
            async with self._lock:
                writer = self._writers.get(worker_id)
                if writer is None or writer.is_closing():
                    worker_info = self._worker_cache.get(worker_id)
                    if not worker_info or worker_info.port == 0:
                        workers = await self._registry.get_active_workers()
                        for w in workers:
                            self._worker_cache[w.worker_id] = w
                        worker_info = self._worker_cache.get(worker_id)

                    if not worker_info:
                        raise TransportError(
                            f"Could not resolve host/port for worker {worker_id}"
                        )

                    _, writer = await asyncio.open_connection(
                        worker_info.host, worker_info.port
                    )
                    self._writers[worker_id] = writer

        return writer

    async def send_task(self, worker_id: str, task: Task) -> None:
        """Send a task to the specified worker via TCP.

        Args:
            worker_id: The target worker ID.
            task: The task to send.

        Raises:
            TransportError: If the task cannot be sent.
        """
        try:
            writer = await self._get_writer(worker_id)

            payload: dict[str, object] = {
                "task_id": task.task_id,
                "task_name": task.task_name,
                "routing_key": task.routing_key,
                "payload": task.payload,
                "created_at": task.created_at,
            }

            data = json.dumps(payload).encode("utf-8") + b"\n"
            writer.write(data)
            await writer.drain()

        except Exception as exc:
            self._writers.pop(worker_id, None)
            raise TransportError(f"Failed to send task via TCP: {exc}") from exc

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle incoming client connections on the worker side.

        Args:
            reader: The StreamReader for the connection.
            writer: The StreamWriter for the connection.
        """
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break  # Connection closed remotely

                payload = json.loads(data.decode("utf-8").strip())
                task = Task(
                    task_id=payload["task_id"],
                    task_name=payload["task_name"],
                    routing_key=payload["routing_key"],
                    payload=payload["payload"],
                    created_at=payload["created_at"],
                )

                await self._queue.put(task)

        except asyncio.IncompleteReadError:
            pass
        except Exception as exc:
            logger.error("TCP Server error: %s", exc)
        finally:
            writer.close()

    async def receive_tasks(self, worker_id: str) -> AsyncIterator[Task]:
        """Receive tasks from the TCP server.

        Args:
            worker_id: The worker ID (used for logging purposes).

        Yields:
            Tasks received from the TCP connection.
        """
        self._server = await asyncio.start_server(
            self._handle_client, self._host, self._port
        )
        logger.info("Direct TCP Transport listening on %s:%s", self._host, self._port)

        while True:
            task = await self._queue.get()
            yield task

    async def acknowledge(self, worker_id: str, task_id: str) -> None:
        """Acknowledge a task (no-op for TCP transport).

                TCP transport does not track acknowledgements; metrics are tracked
        globally via Redis.

                Args:
                    worker_id: The worker ID that processed the task.
                    task_id: The task ID to acknowledge.
        """
        pass  # Tracked globally via Redis metrics

    async def close(self) -> None:
        """Close all TCP connections and the server."""
        for w in self._writers.values():
            w.close()
            with contextlib.suppress(Exception):
                await w.wait_closed()
        self._writers.clear()

        if self._server:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
            self._server = None

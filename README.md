# AffinityRouter

Intelligent task routing with data locality for distributed systems.

## Overview

AffinityRouter is a Python library for smart task routing in distributed systems. Instead of distributing work agnostically (e.g., Round-Robin), it prioritizes **Data Locality** using consistent hashing, ensuring tasks for the same resource always reach the same worker — maximizing in-memory cache hits and minimizing database pressure.

## Key Features

- **Consistent Hashing** — Tasks for the same resource always go to the same worker
- **Dynamic Cluster Management** — Workers register/deregister with heartbeats
- **Native Idempotency** — Built-in duplicate execution prevention
- **Pluggable Architecture** — Swap transport, registry, and state backends
- **Async-First** — Built on `asyncio` with automatic sync function wrapping

## Installation

```bash
pip install affinity-router
```

## Quick Start

```python
from affinity_router import Router, Worker

# --- Router side ---
router = Router.from_redis(redis_url="redis://localhost:6379")
await router.submit(
    routing_key="ticket:12345",
    task_name="process_payment",
    payload={"amount": 100, "currency": "EUR"},
)

# --- Worker side ---
worker = Worker.from_redis(redis_url="redis://localhost:6379", worker_id="worker-1")

@worker.task("process_payment")
async def process_payment(task):
    return {"status": "paid"}

await worker.start()
```

## License

MIT

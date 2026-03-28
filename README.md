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
pip install --pre affinity-router
```

> **Note:** The `--pre` flag is required to install this alpha pre-release.

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

## Transport Backend

AffinityRouter uses **Redis Streams** as the transport backend:

```python
from affinity_router import Router, Worker

router = Router.from_redis(redis_url="redis://localhost:6379")
```

Redis Streams provides:
- **At-least-once delivery** with persistent durability
- **Consumer groups** for distributed processing
- **High resilience** for production deployments

> **Note:** This branch (`task1`) uses Redis Streams only. TCP transports have been removed to simplify the codebase for the specific use case of this project.

## ⚠️ Disclaimer

This software is provided **"AS IS"**, without warranty of any kind, express or implied. The authors and contributors shall not be held liable for any damages, data loss, service disruptions, or any other issues arising from the use of this software. Users are solely responsible for evaluating the suitability of this software for their intended use case.

By using this software, you acknowledge that you have read and understood the [MIT License](LICENSE) under which it is distributed.

## 🚧 Alpha Status

> **This is an alpha release (`0.1.0a1`).**
>
> This version may contain **incomplete implementations** and **undocumented critical bugs**. The API surface is subject to breaking changes without prior notice.
>
> **It is strongly recommended to avoid using this package in real development or production environments.**

## License

MIT

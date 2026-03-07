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

## Transport Backends

AffinityRouter supports multiple transport backends:

### Redis Streams (default)
Best for production deployments with resilience requirements.
```python
from affinity_router import Router, Worker
from affinity_router.transport import RedisStreamTransport

router = Router.from_redis(redis_url="redis://localhost:6379")
```

### Direct TCP
Best for maximum throughput in controlled environments where workers have direct network access.
```python
from affinity_router.transport import DirectTcpTransport

transport = DirectTcpTransport(registry=registry, host="0.0.0.0", port=8000)
```

### Batched TCP
Best for high-throughput scenarios with batching to reduce syscall overhead.
```python
from affinity_router.transport import BatchedTcpTransport

transport = BatchedTcpTransport(
    registry=registry,
    batch_size=100,
    flush_interval_ms=10
)
```

| Transport | Throughput | Latency | Resilience | Use Case |
|-----------|------------|---------|------------|----------|
| Redis Streams | Medium | Medium | High | Production, distributed |
| Direct TCP | High | Low | Low | Controlled networks |
| Batched TCP | Highest | Medium | Low | High-throughput batch jobs |

## License

MIT

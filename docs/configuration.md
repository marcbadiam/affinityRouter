# Configuration and Tuning

AffinityRouter exposes several configurable parameters across its components. Tuning these correctly is essential for optimizing performance, fault tolerance, and memory usage depending on your exact use case.

---

## 1. Router Configuration

The `Router` is responsible for observing the cluster topology and dispatching tasks.

```python
router = Router.from_redis(
    redis_url="redis://localhost:6379",
    replicas=160,
    sync_interval=5.0,
    worker_timeout=30.0
)
```

### `replicas` (default: 160)
* **What it does:** The number of virtual nodes created on the hash ring per physical worker node.
* **Use Case - Low Worker Count (1-5 workers):** Increase `replicas` (e.g., 256 or 512). A higher number guarantees a much more strictly even distribution of keys across a small number of machines.
* **Use Case - High Worker Count (100+ workers):** Decrease `replicas` (e.g., 64). Having too many virtual nodes for hundreds of workers uses excessive memory on the Router and slows down ring recalculations.

### `sync_interval` (default: 5.0)
* **What it does:** How often (in seconds) the router polls the registry to find new workers or prune dead ones.
* **Use Case - Highly Dynamic Infrastructure (Kubernetes Autoscaling):** Set `sync_interval` lower (e.g., `1.0` or `2.0`). This ensures the router quickly detects pods spinning up or tearing down, updating the routing ring before tasks are sent to dead nodes.
* **Use Case - Static Bare Metal Servers:** Set to `10.0` or `30.0`. If your server count rarely changes, lower the poll frequency to save Redis bandwidth.

### `worker_timeout` (default: 30.0)
* **What it does:** Grace period before a worker is considered down if it stops sending heartbeats.
* **Specific Use Case:** If your workers handle heavily CPU-bound blocking tasks (e.g., Video Encoding) that might freeze the async event loop slightly, increase `worker_timeout` to `60.0` to avoid accidental evictions from the ring.

---

## 2. Worker Configuration

The `Worker` consumes workloads and manages local state.

```python
worker = Worker.from_redis(
    worker_id="worker-node-1",
    redis_url="redis://localhost:6379",
    heartbeat_interval=5.0,
    weight=1
)
```

### `heartbeat_interval` (default: 5.0)
* **What it does:** How frequently the worker pings the registry to say "I am alive".
* **Specific Use Case:** Needs to be comfortably lower than the router's `worker_timeout` (usually 1/3 or 1/4 the time). If the network is flaky, consider shortening it to `2.0`.

### `weight` (default: 1)
* **What it does:** A multiplier for the worker's presence on the hash ring. A worker with weight=2 receives twice as many routing keys as a worker with weight=1.
* **Use Case - Heterogeneous Hardware:** If you have some workers running on high-end instances (32 cores) and some on standard instances (8 cores), initialize the powerful workers with `weight=4`. The hash ring will proportionally direct more tasks to the powerful machines while maintaining consistent affinity.

---

## 3. Cache Configuration

The `AffinityCache` is the core reason you are using this middleware. It stores the localized state.

```python
from affinity_router import Worker, AffinityCache

cache = AffinityCache(max_size=10000, default_ttl=300.0)

worker = Worker.from_redis(
    worker_id="w-1", 
    cache=cache
)
```

### `max_size` (default: 10,000)
* **What it does:** The maximum number of distinct keys the LRU cache will hold in memory before evicting the oldest.
* **Use Case - Memory-Intensive Payloads (e.g., Large JSON Documents):** Lower `max_size` based on your available RAM. If a document takes 1MB, a `max_size` of 1,000 consumes ~1GB RAM.
* **Use Case - Lightweight Counters/State (e.g., rate limit ints):** Increase `max_size` to `1_000_000` safely, allowing the worker to keep thousands of user states extremely hot.

### `default_ttl` (default: 300.0 seconds)
* **What it does:** Time to live for a cached item.
* **Specific Use Case:** For a real-time multiplayer match that lasts exactly 10 minutes, set `ttl=600.0`. For highly ephemeral data like typing cursors in a collaborative editor, set `ttl=10.0` to actively free up memory.

---

## 4. State Backend Configuration (Redis)

Manages idempotency, ensuring tasks are processed exactly once.

```python
from affinity_router.state.redis import RedisStateBackend

state = RedisStateBackend(
    redis_client,
    acquire_ttl=300.0,
    result_ttl=86400.0
)
```

### `acquire_ttl` (default: 300.0)
* **What it does:** How long a worker "locks" a task before another worker is allowed to retry it (protects against worker crashes mid-task).
* **Use Case - Long Running Tasks:** If your tasks (e.g., AI model inference) take 10 minutes to finish, `acquire_ttl` MUST be greater than `600.0`, otherwise another worker might steal it while the first is still computing.

### `result_ttl` (default: 86400.0 / 24 hours)
* **What it does:** How long the success/failure result of a task is kept in Redis to prevent replay attacks or duplicate events.
* **Use Case - High Throughput Idempotency:** If you process billions of tasks, keeping results for 24 hours will exhaust Redis memory. Reduce `result_ttl` to `3600.0` (1 hour) if duplicate events are only expected within tight timeframes.

---

## 5. Transport Backend Configuration (Redis Streams)

Handles the physical data pipe between Router and Workers.

```python
from affinity_router.transport.redis import RedisStreamTransport

transport = RedisStreamTransport(redis_client, block_ms=5000)
```

### `block_ms` (default: 5000)
* **What it does:** The timeout in milliseconds for the `XREADGROUP` blocking call before the async loop yields.
* **Specific Use Case:** A lower value (`1000`) makes the worker spin slightly faster during idle times but increases Redis CPU usage. A higher value (`10000`) is great for idle systems, optimizing Redis load.

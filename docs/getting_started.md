# Getting Started with AffinityRouter

AffinityRouter is an intelligent, dependency-injected task routing middleware. By ensuring that tasks with the same `routing_key` consistently hit the same worker, it allows your workers to maintain hot local caches, drastically reducing external database lookups and latency.

## Why use AffinityRouter?

In a standard round-robin or queue-based worker system, subsequent requests for the same entity (e.g., a user profile, a document, a gaming match) can land on any worker. This means every worker must constantly read from the database, leading to slow processing times and high database load.

**AffinityRouter solves this** by using a **Consistent Hash Ring**. It ensures that all tasks for `user:123` always land on Worker A, and all tasks for `user:456` always land on Worker B.

## General Use Cases

Because AffinityRouter excels at data locality, it is the perfect middleware for stateful abstractions over stateless infrastructure.

### 1. Collaborative Real-time Applications
Think of Google Docs or Figma. Multiple users are modifying the same document concurrently.
* **The Problem:** If typing events go to random servers, keeping the document state synchronized is painfully hard and requires distributed locks.
* **The Solution:** Use `routing_key="doc:9988"`. All edits for document `9988` go to the exact same worker. The worker keeps the document state in memory (via `AffinityCache`), processes all edits linearly without locks, and flushes the result to the database asynchronously.

### 2. Multiplayer Game Servers
Players are sending highly frequent movement and action commands.
* **The Problem:** Querying a database or standard Redis instance for every player movement adds too much latency.
* **The Solution:** Use `routing_key="match:555"`. All actions for a given match go to the same server handling that match. The match state lives in RAM, enabling instant validations and broadcasts.

### 3. Tenant-based Rate Limiting & Aggregation
You have an API and want to rate-limit incoming requests per tenant, or count analytics events before saving them.
* **The Problem:** Making a Redis `INCR` call for every single API request adds network overhead.
* **The Solution:** Use `routing_key="tenant:apple"`. The worker keeps a local counter in memory. Every 10 seconds, it pushes the aggregated sum to the database.

### 4. Cache-heavy Read APIs (e.g., Social Feeds)
An endpoint that generates a personalized feed.
* **The Problem:** Fetching thousands of posts and ranking them is expensive. If the cache is cold, the user waits.
* **The Solution:** Use `routing_key="feed:user:777"`. The worker generating the feed keeps the user's graph and recent posts in its local `AffinityCache`. Successive requests are served instantly from memory.

## Quick Start

### 1. Install via UV

```bash
uv add affinity-router
```

### 2. Start the Router (API / Web tier)
The Router is responsible for taking requests and pushing them to the correct worker.

```python
from affinity_router import Router

router = Router.from_redis("redis://localhost:6379")
await router.start()

# Later, in your web endpoint:
task = await router.submit(
    routing_key="match:123",
    task_name="player_move",
    payload={"player": "p1", "x": 10, "y": 20}
)
```

### 3. Start a Worker (Background tier)
The Worker receives tasks and processes them using local memory.

```python
from affinity_router import Worker

worker = Worker.from_redis("worker-node-1", "redis://localhost:6379")

@worker.task("player_move")
async def handle_move(task):
    match_id = task.routing_key
    # Process move instantly using local state!
    return True

await worker.start()
```

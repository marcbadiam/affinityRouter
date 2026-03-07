# AffinityRouter Architecture

AffinityRouter enforces a strict application of Dependency Inversion. Standard abstractions govern the cross-node protocol, and implementations are cleanly decoupled. 

## Component Flow

1. **Submission:** 
   The User sends a payload with a specific `routing_key` to the **Router**.
2. **Ring Lookup:**
   The `Router` hashes the `routing_key` against its `ConsistentHashRing`. The ring maps the hash to a specific active `Worker ID`.
3. **Transport:**
   The `Router` serializes the `Task` and writes it to the `TransportBackend` (e.g., a Redis Stream specific to the target Worker).
4. **Consumption:**
   The `Worker` consumes tasks from its Stream.
5. **Idempotency Check:**
   The `Worker` checks the `StateBackend` (`try_acquire`).
   * If already processed: Ack the message and skip.
   * If acquired: Proceed to execution.
6. **Execution & State:**
   The `Worker` resolves the `@worker.task` handler and runs the business logic. Handlers interact heavily with the `AffinityCache` to minimize database trips. 
7. **Resolution:**
   Results or Exceptions are saved back into the `StateBackend`. The message is acknowledged via the `TransportBackend`.

## Architectural Domains

### `core/`
* **`hash_ring.py`**: A pure-Python implementation of a Consistent Hash Ring. It avoids modulo mapping; when nodes are removed or added, keys are only shifted at the boundary rather than invalidated globally.
* **`types.py`**: Defining `Task`, `TaskResult`, `WorkerInfo` using strict frozen dataclasses.

### `registry/`
* Exposes `ServiceRegistry`. Workers `register()` and `heartbeat()`. The Router polls `get_active_workers()` to build its ring topology.
* **Redis Impl:** Uses `ZSET` keeping the heartbeat timestamp as the score.

### `transport/`
* Exposes `TransportBackend`. Router pushes using `send_task()`. Workers read by iterating over `receive_tasks()`.
* **Redis Impl:** Uses Redis Streams (`XADD`, `XREADGROUP`, `XACK`). This enables consumer groups to achieve at-least-once delivery with persistent durability.

### `state/`
* Exposes `StateBackend`. Exists exclusively to stop duplicate deliveries and enforce exactly-once execution logic across distributed nodes.
* **Redis Impl:** Achieves atomicity through `SET NX EX` (Set if not exists with an expiration TTL).

### `router/` and `worker/`
* The high-level orchestrators bridging all the interfaces together. The code here contains no Redis logic, only calling methods on the injected `ABCs`.

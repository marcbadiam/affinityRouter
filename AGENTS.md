# AGENTS.md — AffinityRouter

## Build & Run

```bash
# Install dependencies (uses uv, not pip)
uv sync

# Run full test suite
uv run pytest

# Run a single test file
uv run pytest tests/unit/test_hash_ring.py

# Run a single test by name
uv run pytest -k "test_same_key_same_node"

# Run a single test class
uv run pytest tests/unit/test_router.py::TestRouterSubmit

# Run only unit or integration tests
uv run pytest tests/unit/
uv run pytest tests/integration/

# Lint
uv run ruff check src/ tests/

# Lint with auto-fix
uv run ruff check --fix src/ tests/

# Format
uv run ruff format src/ tests/

# Type check
uv run mypy src/
```

## Project Layout

```
src/affinity_router/          # Package root (src layout)
  core/                       # Pure-Python types, hash ring, exceptions
  transport/                  # Task delivery (base.py = ABC, redis.py = impl)
  registry/                   # Worker discovery (base.py = ABC, redis.py = impl)
  state/                      # Idempotency state (base.py = ABC, redis.py = impl)
  router/                     # Router with consistent hashing
  worker/                     # Worker with @task decorator and LRU cache
tests/
  unit/                       # Fast tests, no external deps (use AsyncMock)
  integration/                # Redis adapter tests (use fakeredis)
```

Each domain folder follows the same pattern: `base.py` (ABC interface) + `redis.py` (concrete implementation). Interfaces are the lowest dependency layer — no circular imports.

## Code Style

### Formatting
- **Line length**: 88 characters (Black-compatible)
- **Quotes**: Double quotes
- **Indent**: 4 spaces
- **Trailing commas**: Yes, on multi-line structures

### Imports

Every source file starts with `from __future__ import annotations`. Imports are organized in three blocks separated by blank lines:

1. Standard library
2. Third-party
3. Local (`affinity_router.*` — always absolute, never relative)

Use `TYPE_CHECKING` blocks for imports only needed by type annotations. This is enforced by ruff's `TCH` rules. Runtime imports stay outside the block.

```python
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from redis.asyncio import Redis  # only if used at runtime

if TYPE_CHECKING:
    from affinity_router.core.types import Task
    from affinity_router.registry.base import ServiceRegistry
```

`__all__` is required in every `__init__.py` and must be **alphabetically sorted** (ruff RUF022).

### Typing
- **Strict mypy** is enforced (`strict = true`, `disallow_untyped_defs = true`)
- All functions must have full parameter and return type annotations
- Use `X | None` instead of `Optional[X]` (ruff UP007 is ignored, but codebase uses `|` syntax)
- Use built-in generics: `list[str]`, `dict[str, Any]`, `set[int]` (not `List`, `Dict`, `Set`)
- Type aliases: `TaskHandler = Callable[..., Any]` — keep `Callable` as a runtime import

### Naming
- **Functions/variables**: `snake_case`
- **Classes**: `PascalCase`
- **Private attributes/methods**: Single leading underscore (`_worker_id`, `_sync_loop`)
- **Module-level constants**: `UPPER_SNAKE_CASE` or `_PREFIXED` if module-private
- **Modules**: `snake_case`, singular (`worker.py` not `workers.py`)

### Classes

- **Data containers**: Use `@dataclass(frozen=True, slots=True)` — immutable by default
- **Interfaces**: Use `ABC` + `@abstractmethod`. Suffix with `Backend` or descriptive role name
- **Behavior classes**: Regular classes. Store deps as `_private` attributes in `__init__`
- **Constructors**: Always annotate `-> None`

### Docstrings

Google-style. Module, class, and public method docstrings are expected.

```python
def add_node(self, node_id: str, weight: int = 1) -> None:
    """Add a physical node (or update its weight) on the ring.

    Creates ``replicas * weight`` virtual node positions.

    Args:
        node_id: Unique identifier for the worker node.
        weight: Multiplier for virtual node count.

    Raises:
        ValueError: If weight is less than 1.
    """
```

### Error Handling

All custom exceptions inherit from `AffinityRouterError`. The hierarchy:

```
AffinityRouterError
├── WorkerNotFoundError
├── TaskAlreadyProcessedError
├── TaskHandlerNotFoundError
├── TransportError
├── RegistryError
└── StateBackendError
```

Always use exception chaining when wrapping external errors:

```python
try:
    await self._redis.xadd(stream, fields)
except Exception as exc:
    raise TransportError(f"Failed to send task to {stream!r}") from exc
```

### Async Patterns

- All I/O interfaces are `async`. Sync user functions are wrapped with `asyncio.to_thread()`
- Background loops use `asyncio.create_task()` with descriptive `name=` arguments
- Cleanup: cancel tasks then `await` them with `contextlib.suppress(asyncio.CancelledError)`
- Track background tasks in a `set[asyncio.Task[Any]]` with `add_done_callback(tasks.discard)`

## Testing

- **Framework**: pytest + pytest-asyncio with `asyncio_mode = "auto"` (no `@pytest.mark.asyncio` needed)
- **Async tests**: Just write `async def test_...` — the mode handles the rest
- **Mocks**: Use `unittest.mock.AsyncMock` for async interfaces, factory helpers like `_make_registry()`
- **Integration tests**: Use `fakeredis.aioredis.FakeRedis` (shared fixture in `conftest.py`)
- **Organization**: Tests grouped in classes (`class TestConsistentHashRing:`)
- **Assertions**: Plain `assert` statements, no `self.assertEqual`

```python
class TestRouterSubmit:
    async def test_submit_routes_to_correct_worker(self):
        router = Router(transport=_make_transport(), registry=_make_registry([...]))
        await router.start()
        task = await router.submit(routing_key="ticket:123", task_name="solve")
        transport.send_task.assert_called_once()
        await router.stop()
```

## Key Design Decisions

- **Hash ring**: Implemented from scratch with `hashlib` + `bisect` (no external libs)
- **Dependency Inversion**: Interfaces (ABCs) are the lowest layer. Router/Worker depend on abstractions, never on Redis directly
- **No relative imports**: Always `from affinity_router.x.y import Z`
- **Redis is the only adapter** — but the architecture supports swapping in any backend

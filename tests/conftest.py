"""Shared test fixtures."""

import fakeredis.aioredis
import pytest


@pytest.fixture
async def redis():
    """Provide a fresh async fakeredis instance per test."""
    server = fakeredis.FakeServer()
    r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)
    yield r
    await r.aclose()

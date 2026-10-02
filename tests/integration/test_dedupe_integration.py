"""Dedupe lease integration tests against a real Redis server (localhost:6379)."""

import asyncio
import time
import uuid

import pytest
import pytest_asyncio
import redis.asyncio as redis
from conftest import integration, redis_available

from microdcs import MQTTConfig
from microdcs.common import CloudEvent
from microdcs.mqtt import MQTTHandler
from microdcs.redis import CloudEventDedupeDAO, DedupeState, RedisKeySchema


@integration
@redis_available
class TestDedupeLeaseIntegration:
    @pytest_asyncio.fixture
    async def env(self):
        pool = redis.ConnectionPool.from_url("redis://localhost:6379")
        client = redis.Redis(connection_pool=pool)
        schema = RedisKeySchema(prefix=f"microdcs-test-{uuid.uuid4().hex[:8]}")
        yield client, pool, schema
        async for key in client.scan_iter(f"{schema.prefix}:*"):
            await client.delete(key)
        await client.aclose()
        await pool.aclose()

    @pytest.mark.asyncio
    async def test_claim_busy_then_done(self, env):
        client, _, schema = env
        dao = CloudEventDedupeDAO(client, schema, ttl=60, lease=30)
        assert await dao.claim("src", "e1", "a") is DedupeState.CLAIMED
        assert await dao.claim("src", "e1", "b") is DedupeState.BUSY
        assert await dao.claim("src", "e1", "a") is DedupeState.CLAIMED
        await dao.mark_done("src", "e1")
        assert await dao.claim("src", "e1", "b") is DedupeState.DONE

    @pytest.mark.asyncio
    async def test_lease_of_dead_worker_expires_and_is_reclaimed(self, env):
        """A worker that claims and never finishes does not make the event a duplicate."""
        client, _, schema = env
        dao = CloudEventDedupeDAO(client, schema, ttl=60, lease=1)
        assert await dao.claim("src", "e2", "dead-worker") is DedupeState.CLAIMED
        assert await dao.lease_remaining_ms("src", "e2") > 0
        await asyncio.sleep(1.2)
        assert await dao.claim("src", "e2", "survivor") is DedupeState.CLAIMED

    @pytest.mark.asyncio
    async def test_handler_waits_for_lease_then_processes(self, env):
        client, pool, schema = env
        handler = MQTTHandler(MQTTConfig(dedupe_lease_seconds=1), pool, schema)
        ce = CloudEvent(source="src", id="e3")
        # simulate a worker that died right after claiming
        dead = CloudEventDedupeDAO(client, schema, lease=1)
        assert await dead.claim("src", "e3", "dead-worker") is DedupeState.CLAIMED

        started = time.monotonic()
        assert await handler._claim_message(ce) is True
        assert 0.5 < time.monotonic() - started < 3

    @pytest.mark.asyncio
    async def test_handler_treats_finished_message_as_duplicate(self, env):
        client, pool, schema = env
        handler = MQTTHandler(MQTTConfig(), pool, schema)
        ce = CloudEvent(source="src", id="e4")
        assert await handler._claim_message(ce) is True
        await handler._cloudevent_dedupe_dao.mark_done("src", "e4")
        assert await handler._claim_message(ce) is False

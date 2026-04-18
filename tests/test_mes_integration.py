"""MES integration tests — publisher → retained topics → MES resync flow.

Requires:
  - MQTT broker (Mosquitto) on localhost:1883
  - Redis Stack (JSON + Search modules) on localhost:6379

Start Redis Stack instead of vanilla Redis for these tests::

    docker run --rm -it --name redis -p 6379:6379 redis/redis-stack-server:latest
"""

import asyncio
import uuid

import aiomqtt
import pytest
import redis.asyncio as redis
from conftest import integration, mqtt_available, redis_available

from microdcs import MQTTConfig, ProcessingConfig, PublisherConfig
from microdcs.models.machinery_jobs import (
    ISA95JobOrderAndStateDataType,
    ISA95JobOrderDataType,
    ISA95JobResponseDataType,
    ISA95ParameterDataType,
    ISA95StateDataType,
    LocalizedText,
)
from microdcs.models.machinery_jobs_ext import StateIndex
from microdcs.publishers.machinery_jobs import JobOrderPublisher
from microdcs.redis import JobOrderAndStateDAO, JobResponseDAO, RedisKeySchema

MQTT_CONFIG = MQTTConfig()


# ---------------------------------------------------------------------------
# MES Resync Client — reference implementation
# ---------------------------------------------------------------------------


class MESResyncClient:
    """Reference implementation of the MES reconnect resync protocol.

    Demonstrates how an MES system would:

    1. Read the state-index retained topic to discover current job state.
    2. Detect sequence gaps to identify missed transitions.
    3. Read per-job retained topics for full order and result data.
    4. Track last-seen seq per scope for subsequent reconnects.

    See docs/machinery-jobs-mes-publishing.md § "MES Integration Reference"
    for the full protocol description.
    """

    def __init__(self, hostname: str, port: int, topic_prefix: str) -> None:
        self._hostname = hostname
        self._port = port
        self._prefix = topic_prefix
        self._last_seen_seq: dict[str, int] = {}

    async def read_state_index(self, scope: str) -> StateIndex | None:
        """Subscribe to the state-index retained topic and return the payload."""
        topic = f"{self._prefix}/{scope}/state-index"
        payload = await self._read_retained(topic)
        if payload is None:
            return None
        return StateIndex.from_json(payload)

    async def read_job_order(
        self, scope: str, job_order_id: str
    ) -> ISA95JobOrderAndStateDataType | None:
        """Read the retained order/{id} topic."""
        topic = f"{self._prefix}/{scope}/order/{job_order_id}"
        payload = await self._read_retained(topic)
        if payload is None:
            return None
        return ISA95JobOrderAndStateDataType.from_json(payload)

    async def read_job_result(
        self, scope: str, job_order_id: str
    ) -> ISA95JobResponseDataType | None:
        """Read the retained result/{id} topic."""
        topic = f"{self._prefix}/{scope}/result/{job_order_id}"
        payload = await self._read_retained(topic)
        if payload is None:
            return None
        return ISA95JobResponseDataType.from_json(payload)

    async def resync(self, scope: str) -> tuple[StateIndex | None, bool]:
        """Execute the resync protocol.

        Returns ``(state_index, gap_detected)``.  After calling this method,
        iterate ``state_index.jobs`` for entries with ``has_result=True`` and
        call :meth:`read_job_result` to fetch the full response payload.
        """
        state_index = await self.read_state_index(scope)
        if state_index is None:
            return None, False
        last_seq = self._last_seen_seq.get(scope, 0)
        gap_detected = state_index.seq > last_seq and last_seq > 0
        self._last_seen_seq[scope] = state_index.seq
        return state_index, gap_detected

    async def _read_retained(self, topic: str, timeout: float = 5.0) -> bytes | None:
        async with aiomqtt.Client(hostname=self._hostname, port=self._port) as client:
            await client.subscribe(topic, qos=1)
            try:
                async with asyncio.timeout(timeout):
                    async for message in client.messages:
                        return bytes(message.payload) if message.payload else None
            except TimeoutError:
                return None
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SESSION_ID = uuid.uuid4().hex[:8]


def _make_state(text: str, number: int) -> ISA95StateDataType:
    return ISA95StateDataType(
        state_text=LocalizedText(text=text, locale="en"),
        state_number=number,
    )


def _make_job_and_state(
    job_order_id: str,
    states: list[tuple[str, int]],
) -> ISA95JobOrderAndStateDataType:
    return ISA95JobOrderAndStateDataType(
        job_order=ISA95JobOrderDataType(
            job_order_id=job_order_id,
            description=[LocalizedText(text=f"Test job {job_order_id}", locale="en")],
        ),
        state=[_make_state(text, num) for text, num in states],
    )


def _make_response(
    job_order_id: str,
    response_id: str | None = None,
) -> ISA95JobResponseDataType:
    return ISA95JobResponseDataType(
        job_response_id=response_id or f"JR-{job_order_id}",
        job_order_id=job_order_id,
        job_response_data=[
            ISA95ParameterDataType(id="test_param", value="42"),
        ],
    )


async def _delete_retained(topic: str) -> None:
    """Zero-byte retained publish to clean up after tests."""
    async with aiomqtt.Client(
        hostname=MQTT_CONFIG.hostname, port=MQTT_CONFIG.port
    ) as client:
        await client.publish(topic, payload=b"", qos=1, retain=True)


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
class TestMESIntegration:
    """End-to-end tests: Redis → JobOrderPublisher → retained MQTT → MES client."""

    async def _setup_env(
        self,
    ) -> tuple[
        redis.Redis,
        redis.ConnectionPool,
        RedisKeySchema,
        JobOrderAndStateDAO,
        JobResponseDAO,
        str,
        str,
    ]:
        """Create isolated Redis + topic state for a single test."""
        test_id = uuid.uuid4().hex[:8]
        key_prefix = f"mestest-{_SESSION_ID}-{test_id}"
        topic_prefix = f"test/mes/{_SESSION_ID}/{test_id}"

        pool = redis.ConnectionPool.from_url("redis://localhost:6379")
        client = redis.Redis(connection_pool=pool)
        key_schema = RedisKeySchema(prefix=key_prefix)
        joborder_dao = JobOrderAndStateDAO(client, key_schema)
        jobresponse_dao = JobResponseDAO(client, key_schema)
        await jobresponse_dao.initialize()

        return (
            client,
            pool,
            key_schema,
            joborder_dao,
            jobresponse_dao,
            key_prefix,
            topic_prefix,
        )

    async def _cleanup(
        self,
        client: redis.Redis,
        pool: redis.ConnectionPool,
        key_prefix: str,
        topic_prefix: str,
        scopes: list[str],
        job_ids: list[str],
    ) -> None:
        """Remove all test keys from Redis and retained topics from MQTT."""
        # Redis cleanup
        cursor = "0"
        while cursor:
            cursor, keys = await client.scan(
                cursor=cursor,  # pyright: ignore[reportArgumentType]
                match=f"{key_prefix}:*",
                count=500,
            )
            if keys:
                await client.delete(*keys)

        # MQTT retained topic cleanup
        for scope in scopes:
            await _delete_retained(f"{topic_prefix}/{scope}/state-index")
            for jid in job_ids:
                await _delete_retained(f"{topic_prefix}/{scope}/order/{jid}")
                await _delete_retained(f"{topic_prefix}/{scope}/result/{jid}")

        await client.aclose()
        await pool.aclose()

    async def _start_publisher(
        self,
        pool: redis.ConnectionPool,
        key_schema: RedisKeySchema,
        topic_prefix: str,
    ) -> tuple[JobOrderPublisher, asyncio.Task]:
        """Create and start a JobOrderPublisher, wait for MQTT connection."""
        config = MQTTConfig()
        publisher_config = PublisherConfig(stream_block_ms=100)
        processing_config = ProcessingConfig(
            topic_prefixes={f"machinery-jobs:{topic_prefix}"}
        )
        publisher = JobOrderPublisher(
            config,
            publisher_config,
            processing_config,
            "machinery-jobs",
            pool,
            key_schema,
        )
        task = asyncio.create_task(publisher.task())
        await asyncio.wait_for(publisher._connected.wait(), timeout=10.0)
        return publisher, task

    async def _stop_publisher(
        self, publisher: JobOrderPublisher, task: asyncio.Task
    ) -> None:
        publisher._shutdown_event.set()
        await asyncio.wait_for(task, timeout=10.0)

    async def test_initial_state_index_published_on_startup(self):
        """Publisher publishes state-index for all active scopes on startup."""
        scope = "machine-init-test"
        job_id = "JO-INIT-001"
        (
            client,
            pool,
            key_schema,
            jo_dao,
            jr_dao,
            key_prefix,
            topic_prefix,
        ) = await self._setup_env()
        publisher = None
        task = None

        try:
            # Seed a job in Running state
            job = _make_job_and_state(job_id, [("Running", 3)])
            await jo_dao.save(job, scope, "Store")

            # Start publisher — it publishes initial state-index for active scopes
            publisher, task = await self._start_publisher(
                pool, key_schema, topic_prefix
            )

            # Wait for publisher to finish startup (initial publish + one XREAD cycle)
            await asyncio.sleep(0.5)

            # MES client reads the retained state-index
            mes = MESResyncClient(MQTT_CONFIG.hostname, MQTT_CONFIG.port, topic_prefix)
            state_index = await mes.read_state_index(scope)

            assert state_index is not None
            assert state_index.scope == scope
            assert state_index.seq >= 1
            assert len(state_index.jobs) == 1
            assert state_index.jobs[0].job_order_id == job_id
            assert state_index.jobs[0].has_result is False
        finally:
            if publisher and task:
                await self._stop_publisher(publisher, task)
            await self._cleanup(
                client, pool, key_prefix, topic_prefix, [scope], [job_id]
            )

    async def test_store_publishes_retained_order(self):
        """Store stream entry causes publisher to create retained order/{id} topic."""
        scope = "machine-store-test"
        job_id = "JO-STORE-001"
        (
            client,
            pool,
            key_schema,
            jo_dao,
            jr_dao,
            key_prefix,
            topic_prefix,
        ) = await self._setup_env()
        publisher = None
        task = None

        try:
            # Start publisher first (no data yet)
            publisher, task = await self._start_publisher(
                pool, key_schema, topic_prefix
            )

            # Store a job — DAO writes to stream, publisher picks it up
            job = _make_job_and_state(job_id, [("NotAllowedToStart", 1), ("Ready", 11)])
            await jo_dao.save(job, scope, "Store")

            # Wait for publisher XREAD cycle
            await asyncio.sleep(0.5)

            # MES client reads retained order topic
            mes = MESResyncClient(MQTT_CONFIG.hostname, MQTT_CONFIG.port, topic_prefix)
            order = await mes.read_job_order(scope, job_id)
            assert order is not None
            assert order.job_order is not None
            assert order.job_order.job_order_id == job_id

            # State index should also be updated
            state_index = await mes.read_state_index(scope)
            assert state_index is not None
            assert any(j.job_order_id == job_id for j in state_index.jobs)
        finally:
            if publisher and task:
                await self._stop_publisher(publisher, task)
            await self._cleanup(
                client, pool, key_prefix, topic_prefix, [scope], [job_id]
            )

    async def test_result_published_for_clearable_job(self):
        """ResultUpdate for a job in Ended state publishes retained result/{id}."""
        scope = "machine-result-test"
        job_id = "JO-RESULT-001"
        (
            client,
            pool,
            key_schema,
            jo_dao,
            jr_dao,
            key_prefix,
            topic_prefix,
        ) = await self._setup_env()
        publisher = None
        task = None

        try:
            # Seed a job in Ended state (clearable)
            job = _make_job_and_state(job_id, [("Ended", 5), ("Completed", 51)])
            await jo_dao.save(job, scope, "Store")

            # Save a response for this job
            response = _make_response(job_id)
            await jr_dao.save(response, scope)

            # Start publisher — processes both stream entries
            publisher, task = await self._start_publisher(
                pool, key_schema, topic_prefix
            )
            await asyncio.sleep(0.5)

            # MES client reads retained result topic
            mes = MESResyncClient(MQTT_CONFIG.hostname, MQTT_CONFIG.port, topic_prefix)
            result = await mes.read_job_result(scope, job_id)
            assert result is not None
            assert result.job_order_id == job_id

            # State-index should show has_result=True
            state_index = await mes.read_state_index(scope)
            assert state_index is not None
            ended_job = next(
                (j for j in state_index.jobs if j.job_order_id == job_id), None
            )
            assert ended_job is not None
            assert ended_job.has_result is True
        finally:
            if publisher and task:
                await self._stop_publisher(publisher, task)
            await self._cleanup(
                client, pool, key_prefix, topic_prefix, [scope], [job_id]
            )

    async def test_clear_deletes_retained_topics(self):
        """Clear stream entry removes order/{id} and result/{id} retained topics."""
        scope = "machine-clear-test"
        job_id = "JO-CLEAR-001"
        (
            client,
            pool,
            key_schema,
            jo_dao,
            jr_dao,
            key_prefix,
            topic_prefix,
        ) = await self._setup_env()
        publisher = None
        task = None

        try:
            # Seed a job in Ended state with response
            job = _make_job_and_state(job_id, [("Ended", 5), ("Completed", 51)])
            await jo_dao.save(job, scope, "Store")
            response = _make_response(job_id)
            await jr_dao.save(response, scope)

            # Start publisher — publishes initial topics
            publisher, task = await self._start_publisher(
                pool, key_schema, topic_prefix
            )
            await asyncio.sleep(0.5)

            # Verify topics exist before Clear
            mes = MESResyncClient(MQTT_CONFIG.hostname, MQTT_CONFIG.port, topic_prefix)
            assert await mes.read_job_order(scope, job_id) is not None

            # Transition to EndState and trigger Clear stream entry
            job_cleared = _make_job_and_state(job_id, [("EndState", 7)])
            await jo_dao.save(job_cleared, scope, "Clear")

            # Wait for publisher to process Clear
            await asyncio.sleep(0.5)

            # Retained topics should be deleted (read returns None)
            order_after = await mes.read_job_order(scope, job_id)
            assert order_after is None

            result_after = await mes.read_job_result(scope, job_id)
            assert result_after is None
        finally:
            if publisher and task:
                await self._stop_publisher(publisher, task)
            await self._cleanup(
                client, pool, key_prefix, topic_prefix, [scope], [job_id]
            )

    async def test_resync_detects_sequence_gap(self):
        """MES resync client detects seq gap after missed transitions."""
        scope = "machine-resync-test"
        job_ids = ["JO-RESYNC-001", "JO-RESYNC-002", "JO-RESYNC-003"]
        (
            client,
            pool,
            key_schema,
            jo_dao,
            jr_dao,
            key_prefix,
            topic_prefix,
        ) = await self._setup_env()
        publisher = None
        task = None

        try:
            # Seed first job
            job1 = _make_job_and_state(job_ids[0], [("Running", 3)])
            await jo_dao.save(job1, scope, "Store")

            # Start publisher
            publisher, task = await self._start_publisher(
                pool, key_schema, topic_prefix
            )
            await asyncio.sleep(0.5)

            # MES "connects" for the first time — records initial seq
            mes = MESResyncClient(MQTT_CONFIG.hostname, MQTT_CONFIG.port, topic_prefix)
            state_index, gap = await mes.resync(scope)
            assert state_index is not None
            initial_seq = state_index.seq
            assert gap is False  # First connect, no previous seq

            # MES "disconnects" — more jobs arrive during outage
            job2 = _make_job_and_state(job_ids[1], [("Running", 3)])
            await jo_dao.save(job2, scope, "Store")
            job3 = _make_job_and_state(job_ids[2], [("Ended", 5), ("Completed", 51)])
            await jo_dao.save(job3, scope, "Store")
            response3 = _make_response(job_ids[2])
            await jr_dao.save(response3, scope)

            # Wait for publisher to process all entries
            await asyncio.sleep(1.0)

            # MES "reconnects" — detects gap
            state_index, gap = await mes.resync(scope)
            assert state_index is not None
            assert gap is True
            assert state_index.seq > initial_seq
            assert len(state_index.jobs) == 3

            # MES fetches result for clearable job
            clearable = [j for j in state_index.jobs if j.has_result]
            assert len(clearable) == 1
            assert clearable[0].job_order_id == job_ids[2]
            result = await mes.read_job_result(scope, job_ids[2])
            assert result is not None
            assert result.job_order_id == job_ids[2]
        finally:
            if publisher and task:
                await self._stop_publisher(publisher, task)
            await self._cleanup(
                client, pool, key_prefix, topic_prefix, [scope], job_ids
            )

    async def test_multi_job_cadence(self):
        """Multiple jobs stored in rapid succession are all published correctly."""
        scope = "machine-cadence-test"
        job_count = 10
        job_ids = [f"JO-CADENCE-{i:03d}" for i in range(job_count)]
        (
            client,
            pool,
            key_schema,
            jo_dao,
            jr_dao,
            key_prefix,
            topic_prefix,
        ) = await self._setup_env()
        publisher = None
        task = None

        try:
            publisher, task = await self._start_publisher(
                pool, key_schema, topic_prefix
            )

            # Store jobs in rapid succession
            for jid in job_ids:
                job = _make_job_and_state(jid, [("Running", 3)])
                await jo_dao.save(job, scope, "Store")

            # Wait for publisher to process all entries
            await asyncio.sleep(2.0)

            # MES reads state-index — all jobs should be listed
            mes = MESResyncClient(MQTT_CONFIG.hostname, MQTT_CONFIG.port, topic_prefix)
            state_index = await mes.read_state_index(scope)
            assert state_index is not None
            published_ids = {j.job_order_id for j in state_index.jobs}
            assert published_ids == set(job_ids)

            # Each job should have a retained order topic
            for jid in job_ids:
                order = await mes.read_job_order(scope, jid)
                assert order is not None, f"Missing retained order for {jid}"
        finally:
            if publisher and task:
                await self._stop_publisher(publisher, task)
            await self._cleanup(
                client, pool, key_prefix, topic_prefix, [scope], job_ids
            )

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from microdcs import MQTTConfig, ProcessingConfig, PublisherConfig
from microdcs.models.machinery_jobs import (
    ISA95JobOrderAndStateDataType,
    ISA95JobOrderDataType,
    ISA95JobResponseDataType,
    ISA95StateDataType,
    LocalizedText,
)
from microdcs.models.machinery_jobs_ext import StateIndex, StateIndexEntry
from microdcs.publishers.machinery_jobs import (
    CLEARABLE_STATE_PREFIXES,
    JobOrderPublisher,
)
from microdcs.redis import RedisKeySchema


def _make_processing_config() -> MagicMock:
    config = MagicMock(spec=ProcessingConfig)
    config.get_topic_prefix_for_identifier.return_value = "app/jobs"
    return config


def _make_publisher(
    redis_mock: AsyncMock | None = None,
) -> tuple["JobOrderPublisher", AsyncMock]:
    """Create a JobOrderPublisher with mocked Redis and MQTT."""
    mock_redis = redis_mock or AsyncMock()
    publisher_config = PublisherConfig()
    processing_config = _make_processing_config()

    with patch(
        "microdcs.publishers.machinery_jobs.redis.Redis", return_value=mock_redis
    ):
        publisher = JobOrderPublisher(
            MQTTConfig(),
            publisher_config,
            processing_config,
            "machinery-jobs",
            MagicMock(),  # redis_connection_pool
            RedisKeySchema(prefix="test"),
        )
    # Replace DAOs with mocks (they were created with the patched Redis)
    publisher._joborder_dao = AsyncMock()
    publisher._jobresponse_dao = AsyncMock()
    # Mock MQTT publish methods so we don't need a real client
    publisher.publish_retained = AsyncMock()
    publisher.delete_retained = AsyncMock()
    return publisher, mock_redis


class TestJobOrderPublisherInit:
    def test_stores_config(self):
        publisher, _ = _make_publisher()
        assert isinstance(publisher._publisher_config, PublisherConfig)
        assert publisher._topic_prefix == "app/jobs"
        assert publisher._cursors == {}
        assert publisher._active_scopes == set()

    def test_raises_when_no_topic_prefix(self):
        config = MagicMock(spec=ProcessingConfig)
        config.get_topic_prefix_for_identifier.return_value = None
        with pytest.raises(ValueError, match="No topic prefix"):
            with patch(
                "microdcs.publishers.machinery_jobs.redis.Redis",
                return_value=AsyncMock(),
            ):
                JobOrderPublisher(
                    MQTTConfig(),
                    PublisherConfig(),
                    config,
                    "machinery-jobs",
                    MagicMock(),
                    RedisKeySchema(prefix="test"),
                )

    def test_is_instance_of_mqtt_publisher(self):
        from microdcs.mqtt import MQTTPublisher

        publisher, _ = _make_publisher()
        assert isinstance(publisher, MQTTPublisher)


class TestReconstructStateName:
    def test_simple_state(self):
        state = [
            ISA95StateDataType(
                state_text=LocalizedText(text="Running", locale="en"), state_number=3
            )
        ]
        assert JobOrderPublisher._reconstruct_state_name(state) == "Running"

    def test_compound_state(self):
        state = [
            ISA95StateDataType(
                state_text=LocalizedText(text="Ended", locale="en"), state_number=5
            ),
            ISA95StateDataType(
                state_text=LocalizedText(text="Completed", locale="en"), state_number=51
            ),
        ]
        assert JobOrderPublisher._reconstruct_state_name(state) == "Ended_Completed"

    def test_empty_list(self):
        assert JobOrderPublisher._reconstruct_state_name([]) is None

    def test_none_text(self):
        state = [ISA95StateDataType(state_text=None, state_number=0)]
        assert JobOrderPublisher._reconstruct_state_name(state) is None


class TestLoadAndSaveCursors:
    @pytest.mark.asyncio
    async def test_load_cursors_decodes_bytes(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.hgetall.return_value = {
            b"scope-1": b"1234-0",
            b"_global": b"5678-0",
        }
        await publisher._load_cursors()
        assert publisher._cursors == {"scope-1": "1234-0", "_global": "5678-0"}

    @pytest.mark.asyncio
    async def test_load_cursors_empty(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.hgetall.return_value = {}
        await publisher._load_cursors()
        assert publisher._cursors == {}

    @pytest.mark.asyncio
    async def test_save_cursors_writes_to_redis(self):
        publisher, mock_redis = _make_publisher()
        publisher._cursors = {"scope-1": "1234-0", "_global": "5678-0"}
        await publisher._save_cursors()
        mock_redis.hset.assert_awaited_once()
        call_kwargs = mock_redis.hset.call_args
        assert call_kwargs.kwargs["mapping"] == publisher._cursors

    @pytest.mark.asyncio
    async def test_save_cursors_skips_empty(self):
        publisher, mock_redis = _make_publisher()
        publisher._cursors = {}
        await publisher._save_cursors()
        mock_redis.hset.assert_not_awaited()


def _make_job(
    job_order_id: str,
    state_text: str,
    state_number: int,
    sub_state: tuple[str, int] | None = None,
) -> ISA95JobOrderAndStateDataType:
    states = [
        ISA95StateDataType(
            state_text=LocalizedText(text=state_text, locale="en"),
            state_number=state_number,
        )
    ]
    if sub_state:
        states.append(
            ISA95StateDataType(
                state_text=LocalizedText(text=sub_state[0], locale="en"),
                state_number=sub_state[1],
            )
        )
    return ISA95JobOrderAndStateDataType(
        job_order=ISA95JobOrderDataType(job_order_id=job_order_id),
        state=states,
    )


class TestPublishStateIndex:
    @pytest.mark.asyncio
    async def test_publishes_state_index_with_jobs(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.incr.return_value = 42

        job1 = _make_job("jo-1", "Running", 3)
        job2 = _make_job("jo-2", "Ended", 5, ("Completed", 51))
        publisher._joborder_dao.list.return_value = ["jo-1", "jo-2"]
        publisher._joborder_dao.retrieve.side_effect = [job1, job2]
        publisher._jobresponse_dao.retrieve_by_job_order_id.side_effect = [
            None,
            MagicMock(),
        ]

        await publisher._publish_state_index("machine-1")

        publisher.publish_retained.assert_awaited_once()
        call_args = publisher.publish_retained.call_args
        assert call_args.args[0] == "app/jobs/machine-1/state-index"
        # Deserialize and verify payload
        payload = StateIndex.from_json(call_args.args[1])
        assert payload.seq == 42
        assert payload.scope == "machine-1"
        assert len(payload.jobs) == 2
        assert payload.jobs[0].job_order_id == "jo-1"
        assert payload.jobs[0].has_result is False
        assert payload.jobs[1].job_order_id == "jo-2"
        assert payload.jobs[1].has_result is True

    @pytest.mark.asyncio
    async def test_excludes_end_state_jobs(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.incr.return_value = 1

        job_active = _make_job("jo-1", "Running", 3)
        job_end = _make_job("jo-2", "EndState", 7)
        publisher._joborder_dao.list.return_value = ["jo-1", "jo-2"]
        publisher._joborder_dao.retrieve.side_effect = [job_active, job_end]
        publisher._jobresponse_dao.retrieve_by_job_order_id.return_value = None

        await publisher._publish_state_index("s1")

        payload = StateIndex.from_json(publisher.publish_retained.call_args.args[1])
        assert len(payload.jobs) == 1
        assert payload.jobs[0].job_order_id == "jo-1"

    @pytest.mark.asyncio
    async def test_increments_pubseq(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.incr.return_value = 99
        publisher._joborder_dao.list.return_value = []

        await publisher._publish_state_index("s1")

        key_schema = publisher._key_schema
        mock_redis.incr.assert_awaited_once_with(key_schema.pub_seq("s1"))
        payload = StateIndex.from_json(publisher.publish_retained.call_args.args[1])
        assert payload.seq == 99


class TestPublishJobOrder:
    @pytest.mark.asyncio
    async def test_publishes_job_to_correct_topic(self):
        publisher, _ = _make_publisher()
        job = _make_job("jo-1", "Running", 3)
        publisher._joborder_dao.retrieve.return_value = job

        await publisher._publish_job_order("machine-1", "jo-1")

        publisher.publish_retained.assert_awaited_once()
        topic = publisher.publish_retained.call_args.args[0]
        assert topic == "app/jobs/machine-1/order/jo-1"

    @pytest.mark.asyncio
    async def test_skips_when_job_not_found(self):
        publisher, _ = _make_publisher()
        publisher._joborder_dao.retrieve.return_value = None

        await publisher._publish_job_order("s1", "missing")

        publisher.publish_retained.assert_not_awaited()


class TestPublishJobResult:
    @pytest.mark.asyncio
    async def test_publishes_result_for_clearable_state(self):
        publisher, _ = _make_publisher()
        job = _make_job("jo-1", "Ended", 5, ("Completed", 51))
        response = MagicMock(spec=ISA95JobResponseDataType)
        response.to_jsonb.return_value = b'{"result": "data"}'
        publisher._joborder_dao.retrieve.return_value = job
        publisher._jobresponse_dao.retrieve_by_job_order_id.return_value = response

        await publisher._publish_job_result("s1", "jo-1")

        publisher.publish_retained.assert_awaited_once()
        assert publisher.publish_retained.call_args.args[0] == "app/jobs/s1/result/jo-1"

    @pytest.mark.asyncio
    async def test_publishes_result_for_aborted_state(self):
        publisher, _ = _make_publisher()
        job = _make_job("jo-1", "Aborted", 6)
        response = MagicMock(spec=ISA95JobResponseDataType)
        response.to_jsonb.return_value = b'{"result": "data"}'
        publisher._joborder_dao.retrieve.return_value = job
        publisher._jobresponse_dao.retrieve_by_job_order_id.return_value = response

        await publisher._publish_job_result("s1", "jo-1")

        publisher.publish_retained.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_non_clearable_state(self):
        publisher, _ = _make_publisher()
        job = _make_job("jo-1", "Running", 3)
        publisher._joborder_dao.retrieve.return_value = job

        await publisher._publish_job_result("s1", "jo-1")

        publisher.publish_retained.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_skips_when_no_response(self):
        publisher, _ = _make_publisher()
        job = _make_job("jo-1", "Ended", 5, ("Completed", 51))
        publisher._joborder_dao.retrieve.return_value = job
        publisher._jobresponse_dao.retrieve_by_job_order_id.return_value = None

        await publisher._publish_job_result("s1", "jo-1")

        publisher.publish_retained.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_skips_when_job_not_found(self):
        publisher, _ = _make_publisher()
        publisher._joborder_dao.retrieve.return_value = None

        await publisher._publish_job_result("s1", "missing")

        publisher.publish_retained.assert_not_awaited()


class TestDeleteJobTopics:
    @pytest.mark.asyncio
    async def test_deletes_order_and_result_topics(self):
        publisher, _ = _make_publisher()

        await publisher._delete_job_topics("machine-1", "jo-1")

        assert publisher.delete_retained.await_count == 2
        calls = [c.args[0] for c in publisher.delete_retained.call_args_list]
        assert "app/jobs/machine-1/order/jo-1" in calls
        assert "app/jobs/machine-1/result/jo-1" in calls


class TestDispatch:
    @pytest.mark.asyncio
    async def test_store_publishes_order_and_state_index(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.incr.return_value = 1
        publisher._joborder_dao.list.return_value = []
        job = _make_job("jo-1", "NotAllowedToStart", 1, ("Ready", 11))
        publisher._joborder_dao.retrieve.return_value = job

        await publisher._dispatch("Store", "s1", "jo-1")

        # publish_retained called twice: once for order, once for state-index
        assert publisher.publish_retained.await_count == 2
        topics = [c.args[0] for c in publisher.publish_retained.call_args_list]
        assert "app/jobs/s1/order/jo-1" in topics
        assert "app/jobs/s1/state-index" in topics

    @pytest.mark.asyncio
    async def test_store_and_start_publishes_order_and_state_index(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.incr.return_value = 1
        publisher._joborder_dao.list.return_value = []
        job = _make_job("jo-1", "AllowedToStart", 2, ("Ready", 21))
        publisher._joborder_dao.retrieve.return_value = job

        await publisher._dispatch("StoreAndStart", "s1", "jo-1")

        assert publisher.publish_retained.await_count == 2

    @pytest.mark.asyncio
    async def test_update_republishes_order(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.incr.return_value = 1
        publisher._joborder_dao.list.return_value = []
        job = _make_job("jo-1", "Running", 3)
        publisher._joborder_dao.retrieve.return_value = job

        await publisher._dispatch("Update", "s1", "jo-1")

        topics = [c.args[0] for c in publisher.publish_retained.call_args_list]
        assert "app/jobs/s1/order/jo-1" in topics

    @pytest.mark.asyncio
    async def test_clear_deletes_topics(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.incr.return_value = 1
        publisher._joborder_dao.list.return_value = []

        await publisher._dispatch("Clear", "s1", "jo-1")

        assert publisher.delete_retained.await_count == 2

    @pytest.mark.asyncio
    async def test_result_update_for_clearable_publishes_result(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.incr.return_value = 1
        publisher._joborder_dao.list.return_value = []
        job = _make_job("jo-1", "Ended", 5, ("Completed", 51))
        publisher._joborder_dao.retrieve.return_value = job
        response = MagicMock(spec=ISA95JobResponseDataType)
        response.to_jsonb.return_value = b'{"r": 1}'
        publisher._jobresponse_dao.retrieve_by_job_order_id.return_value = response

        await publisher._dispatch("ResultUpdate", "s1", "jo-1")

        topics = [c.args[0] for c in publisher.publish_retained.call_args_list]
        assert "app/jobs/s1/result/jo-1" in topics

    @pytest.mark.asyncio
    async def test_transition_only_updates_state_index(self):
        """Non-special transitions (e.g. Start, Pause) only update state-index."""
        publisher, mock_redis = _make_publisher()
        mock_redis.incr.return_value = 1
        publisher._joborder_dao.list.return_value = []

        await publisher._dispatch("Start", "s1", "jo-1")

        assert publisher.publish_retained.await_count == 1
        assert publisher.publish_retained.call_args.args[0] == "app/jobs/s1/state-index"
        publisher.delete_retained.assert_not_awaited()


class TestRun:
    @pytest.mark.asyncio
    async def test_startup_publishes_initial_state_indexes(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.hgetall.return_value = {}
        mock_redis.smembers.return_value = {b"scope-a", b"scope-b"}
        mock_redis.incr.return_value = 1
        publisher._joborder_dao.list.return_value = []

        # xread returns empty then shutdown
        call_count = 0

        async def fake_xread(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 1:
                publisher._shutdown_event.set()
            return []

        mock_redis.xread = AsyncMock(side_effect=fake_xread)

        await publisher._run()

        # State index published for each scope during startup
        assert publisher.publish_retained.await_count == 2
        topics = {c.args[0] for c in publisher.publish_retained.call_args_list}
        assert "app/jobs/scope-a/state-index" in topics
        assert "app/jobs/scope-b/state-index" in topics

    @pytest.mark.asyncio
    async def test_xread_dispatches_and_saves_cursors(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.hgetall.return_value = {}
        mock_redis.smembers.return_value = {b"scope-1"}
        mock_redis.incr.return_value = 1
        publisher._joborder_dao.list.return_value = []
        key_schema = publisher._key_schema

        # First xread returns a Store entry, second triggers shutdown
        scope_stream = key_schema.job_change_stream("scope-1")
        call_count = 0

        async def fake_xread(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return []  # initial startup
            if call_count == 2:
                return [
                    (
                        scope_stream.encode(),
                        [
                            (
                                b"1-0",
                                {
                                    b"change_type": b"Start",
                                    b"job_order_id": b"jo-1",
                                    b"scope": b"scope-1",
                                    b"ts": b"2026-04-18T00:00:00Z",
                                },
                            )
                        ],
                    )
                ]
            publisher._shutdown_event.set()
            return []

        mock_redis.xread = AsyncMock(side_effect=fake_xread)
        job = _make_job("jo-1", "Running", 3)
        publisher._joborder_dao.retrieve.return_value = job

        await publisher._run()

        # Cursors saved (includes the scope cursor from the stream entry)
        assert publisher._cursors.get("scope-1") == "1-0"
        assert mock_redis.hset.await_count >= 1

    @pytest.mark.asyncio
    async def test_discovers_new_scope_from_global_stream(self):
        publisher, mock_redis = _make_publisher()
        mock_redis.hgetall.return_value = {}
        mock_redis.smembers.return_value = set()  # no initial scopes
        mock_redis.incr.return_value = 1
        publisher._joborder_dao.list.return_value = []
        key_schema = publisher._key_schema
        global_stream = key_schema.job_change_stream_global()

        call_count = 0

        async def fake_xread(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [
                    (
                        global_stream.encode(),
                        [
                            (
                                b"1-0",
                                {
                                    b"change_type": b"Store",
                                    b"job_order_id": b"jo-1",
                                    b"scope": b"new-scope",
                                    b"ts": b"2026-04-18T00:00:00Z",
                                },
                            )
                        ],
                    )
                ]
            publisher._shutdown_event.set()
            return []

        mock_redis.xread = AsyncMock(side_effect=fake_xread)

        await publisher._run()

        assert "new-scope" in publisher._active_scopes
        assert publisher._cursors["_global"] == "1-0"


class TestClearableStatePrefixes:
    def test_ended_is_clearable(self):
        assert "Ended_Completed".startswith(CLEARABLE_STATE_PREFIXES)

    def test_aborted_is_clearable(self):
        assert "Aborted".startswith(CLEARABLE_STATE_PREFIXES)

    def test_running_is_not_clearable(self):
        assert not "Running".startswith(CLEARABLE_STATE_PREFIXES)

    def test_end_state_is_not_clearable(self):
        assert not "EndState".startswith(CLEARABLE_STATE_PREFIXES)

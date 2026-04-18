from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from microdcs.models.machinery_jobs import ISA95StateDataType, LocalizedText
from microdcs.models.machinery_jobs_ext import StateIndex, StateIndexEntry
from microdcs.mqtt import MQTTPublisher

# ---------------------------------------------------------------------------
# StateIndexEntry
# ---------------------------------------------------------------------------


class TestStateIndexEntry:
    def test_defaults(self):
        entry = StateIndexEntry(job_order_id="jo-1")
        assert entry.job_order_id == "jo-1"
        assert entry.state == []
        assert entry.has_result is False

    def test_with_state_and_result(self):
        state = [
            ISA95StateDataType(
                state_text=LocalizedText(text="Running", locale="en"),
                state_number=3,
            )
        ]
        entry = StateIndexEntry(job_order_id="jo-2", state=state, has_result=True)
        assert entry.has_result is True
        assert len(entry.state) == 1
        assert entry.state[0].state_text is not None
        assert entry.state[0].state_text.text == "Running"

    def test_json_roundtrip(self):
        state = [
            ISA95StateDataType(
                state_text=LocalizedText(text="Ended", locale="en"),
                state_number=5,
            )
        ]
        entry = StateIndexEntry(job_order_id="jo-1", state=state, has_result=True)
        data = entry.to_jsonb()
        restored = StateIndexEntry.from_json(data)
        assert restored.job_order_id == "jo-1"
        assert restored.has_result is True
        assert len(restored.state) == 1

    def test_msgpack_roundtrip(self):
        entry = StateIndexEntry(job_order_id="jo-1", has_result=False)
        data = entry.to_msgpack()
        restored = StateIndexEntry.from_msgpack(data)
        assert restored.job_order_id == "jo-1"
        assert restored.has_result is False


# ---------------------------------------------------------------------------
# StateIndex
# ---------------------------------------------------------------------------


class TestStateIndex:
    def test_creation(self):
        idx = StateIndex(
            seq=42,
            scope="machine-1",
            published_at="2026-04-11T14:23:01Z",
        )
        assert idx.seq == 42
        assert idx.scope == "machine-1"
        assert idx.jobs == []

    def test_with_jobs(self):
        jobs = [
            StateIndexEntry(job_order_id="jo-1", has_result=False),
            StateIndexEntry(job_order_id="jo-2", has_result=True),
        ]
        idx = StateIndex(
            seq=1,
            scope="scope-a",
            published_at="2026-04-11T14:23:01Z",
            jobs=jobs,
        )
        assert len(idx.jobs) == 2
        assert idx.jobs[1].has_result is True

    def test_json_roundtrip(self):
        state = [
            ISA95StateDataType(
                state_text=LocalizedText(text="Running", locale="en"),
                state_number=3,
            )
        ]
        jobs = [
            StateIndexEntry(job_order_id="jo-1", state=state, has_result=False),
        ]
        idx = StateIndex(
            seq=103,
            scope="machine-42",
            published_at="2026-04-11T14:23:01Z",
            jobs=jobs,
        )
        data = idx.to_jsonb()
        restored = StateIndex.from_json(data)
        assert restored.seq == 103
        assert restored.scope == "machine-42"
        assert len(restored.jobs) == 1
        assert restored.jobs[0].job_order_id == "jo-1"

    def test_msgpack_roundtrip(self):
        idx = StateIndex(
            seq=1,
            scope="s",
            published_at="2026-04-11T00:00:00Z",
            jobs=[StateIndexEntry(job_order_id="jo-1")],
        )
        data = idx.to_msgpack()
        restored = StateIndex.from_msgpack(data)
        assert restored.seq == 1
        assert len(restored.jobs) == 1

    def test_json_payload_structure(self):
        """Verify the JSON output has the expected field names."""
        idx = StateIndex(
            seq=1,
            scope="m",
            published_at="2026-04-11T00:00:00Z",
            jobs=[
                StateIndexEntry(
                    job_order_id="jo-1",
                    state=[
                        ISA95StateDataType(
                            state_text=LocalizedText(text="Ended", locale="en"),
                            state_number=5,
                        )
                    ],
                    has_result=True,
                ),
            ],
        )
        d = idx.to_dict()
        assert "seq" in d
        assert "scope" in d
        assert "published_at" in d
        assert "jobs" in d
        assert d["jobs"][0]["job_order_id"] == "jo-1"
        assert d["jobs"][0]["has_result"] is True


# ---------------------------------------------------------------------------
# MQTTPublisher
# ---------------------------------------------------------------------------


class TestMQTTPublisher:
    @pytest.mark.asyncio
    async def test_publish_retained_calls_client_publish(self):
        mock_client = AsyncMock()
        publisher = MQTTPublisher(MagicMock())
        publisher._client = mock_client

        await publisher.publish_retained("test/topic", b"payload", ttl=3600)

        mock_client.publish.assert_awaited_once()
        call_kwargs = mock_client.publish.call_args
        assert call_kwargs.args[0] == "test/topic"
        assert call_kwargs.args[1] == b"payload"
        assert call_kwargs.kwargs["qos"] == 1
        assert call_kwargs.kwargs["retain"] is True
        props = call_kwargs.kwargs["properties"]
        assert props.MessageExpiryInterval == 3600

    @pytest.mark.asyncio
    async def test_delete_retained_publishes_zero_bytes(self):
        mock_client = AsyncMock()
        publisher = MQTTPublisher(MagicMock())
        publisher._client = mock_client

        await publisher.delete_retained("test/topic")

        mock_client.publish.assert_awaited_once()
        call_kwargs = mock_client.publish.call_args
        assert call_kwargs.args[0] == "test/topic"
        assert call_kwargs.args[1] == b""
        assert call_kwargs.kwargs["qos"] == 1
        assert call_kwargs.kwargs["retain"] is True

    @pytest.mark.asyncio
    async def test_publish_retained_raises_when_not_connected(self):
        publisher = MQTTPublisher(MagicMock())
        with pytest.raises(AssertionError, match="not connected"):
            await publisher.publish_retained("t", b"p", ttl=1)

    @pytest.mark.asyncio
    async def test_delete_retained_raises_when_not_connected(self):
        publisher = MQTTPublisher(MagicMock())
        with pytest.raises(AssertionError, match="not connected"):
            await publisher.delete_retained("t")

    @pytest.mark.asyncio
    async def test_task_connects_and_waits_for_shutdown(self):
        """task() connects, waits for shutdown, then disconnects."""
        mock_aiomqtt_client = AsyncMock()
        mock_aiomqtt_client.__aenter__ = AsyncMock(return_value=mock_aiomqtt_client)
        mock_aiomqtt_client.__aexit__ = AsyncMock(return_value=None)

        shutdown_event = AsyncMock()
        shutdown_event.is_set.return_value = False
        shutdown_event.wait = AsyncMock()

        with patch(
            "microdcs.mqtt.create_mqtt_client", return_value=mock_aiomqtt_client
        ):
            publisher = MQTTPublisher(MagicMock())
            publisher._shutdown_event = shutdown_event
            await publisher.task()

        shutdown_event.wait.assert_awaited()
        assert publisher._client is None

    def test_init_stores_config(self):
        config = MagicMock()
        publisher = MQTTPublisher(config)
        assert publisher._config is config
        assert publisher._client is None
        assert not publisher._connected.is_set()

    @pytest.mark.asyncio
    async def test_connected_event_set_during_task(self):
        """_connected is set while connected and cleared after disconnect."""
        mock_aiomqtt_client = AsyncMock()
        mock_aiomqtt_client.__aenter__ = AsyncMock(return_value=mock_aiomqtt_client)
        mock_aiomqtt_client.__aexit__ = AsyncMock(return_value=None)

        connected_during_run = False

        class TestPublisher(MQTTPublisher):
            async def _run(self_inner) -> None:
                nonlocal connected_during_run
                connected_during_run = self_inner._connected.is_set()

        with patch(
            "microdcs.mqtt.create_mqtt_client", return_value=mock_aiomqtt_client
        ):
            publisher = TestPublisher(MagicMock())
            await publisher.task()

        assert connected_during_run is True
        assert not publisher._connected.is_set()

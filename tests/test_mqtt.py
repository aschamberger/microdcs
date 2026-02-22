"""Tests for app/mqtt.py — MQTT protocol handler, cloud event processor, QoS enum."""

import asyncio
import uuid
from asyncio import Queue
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app import MQTTConfig, ProcessingConfig
from app.common import (
    CloudEvent,
    CloudEventProcessor,
    Direction,
    MessageIntent,
    ProcessorBinding,
    processor_config,
)
from app.dataclass import DataClassConfig, DataClassMixin
from app.mqtt import (
    MQTTHandler,
    MQTTProcessorBinding,
    OTELInstrumentedMQTTHandler,
    QoS,
)
from app.redis import RedisKeySchema

# ---------------------------------------------------------------------------
# Test fixtures / helpers
# ---------------------------------------------------------------------------


@dataclass
class SamplePayload(DataClassMixin):
    value: str = "hello"

    class Config(DataClassConfig):
        cloudevent_type: str = "com.test.sample.v1"
        cloudevent_dataschema: str = "https://example.com/sample-v1"


@dataclass
class PlainPayload(DataClassMixin):
    """Dataclass WITHOUT DataClassConfig — tests the 'no Config' branch."""

    value: str = "plain"


@processor_config(binding=ProcessorBinding.SOUTHBOUND)
class ConcreteProcessor(CloudEventProcessor):
    """Minimal concrete subclass for testing."""

    async def process_event(self, cloudevent):
        return await self.callback_incoming(cloudevent)

    async def process_response_event(self, cloudevent):
        return None

    async def handle_expiration(self, cloudevent, timeout):
        return None


def _make_processor(**kwargs) -> ConcreteProcessor:
    cfg = ProcessingConfig()
    kwargs.pop("queue_size", None)
    proc = ConcreteProcessor(
        instance_id="test-id",
        runtime_config=cfg,
        config_identifier="test",
        **kwargs,
    )
    return proc


def _make_handler() -> MQTTHandler:
    config = MQTTConfig()
    pool = MagicMock()
    key_schema = RedisKeySchema()
    with patch("app.mqtt.redis.Redis"):
        handler = MQTTHandler(config, pool, key_schema)
    return handler


def _make_binding(
    handler: MQTTHandler,
    processor: ConcreteProcessor | None = None,
    topics: set[str] | None = None,
    response_topic: str = "test/responses/test-id",
    queue_size: int = 1,
    topic_prefix: str = "test",
    publish_intents: set[MessageIntent] | None = None,
) -> MQTTProcessorBinding:
    """Register a processor via the binding system and return the binding."""
    if processor is None:
        processor = _make_processor()
    if publish_intents is None:
        publish_intents = processor.publish_intents()
    outgoing_queue: Queue[tuple[CloudEvent, MessageIntent]] = Queue(queue_size)
    binding = MQTTProcessorBinding(
        processor=processor,
        topics=topics or {"test/events/#"},
        response_topic=response_topic,
        outgoing_queue=outgoing_queue,
        topic_prefix=topic_prefix,
        publish_intents=publish_intents,
    )
    handler._bindings.append(binding)
    return binding


def _make_mqtt_message(
    topic: str = "test/events/foo",
    payload: bytes = b'{"value":"hi"}',
    qos: int = 1,
    mid: int = 42,
    retain: bool = False,
    properties: object | None = None,
    match_topics: set[str] | None = None,
) -> MagicMock:
    """Build a mock aiomqtt.Message.

    Args:
        match_topics: If given, ``topic.matches`` returns True only for patterns
            in this set.  If *None*, it matches everything (legacy behaviour).
    """
    msg = MagicMock()
    msg.topic = MagicMock()
    msg.topic.__str__ = lambda self: topic
    if match_topics is not None:
        msg.topic.matches = lambda pattern: pattern in match_topics
    else:
        msg.topic.matches = lambda pattern: True
    msg.payload = payload
    msg.qos = qos
    msg.mid = mid
    msg.retain = retain
    msg.properties = properties
    return msg


# ===================================================================
# QoS enum
# ===================================================================


class TestQoS:
    def test_at_most_once(self):
        assert QoS.AT_MOST_ONCE == 0

    def test_at_least_once(self):
        assert QoS.AT_LEAST_ONCE == 1

    def test_exactly_once(self):
        assert QoS.EXACTLY_ONCE == 2


# ===================================================================
# register_mqtt_processor and MQTTProcessorBinding
# ===================================================================


class TestRegisterMQTTProcessor:
    def test_derives_subscribe_topics_from_intents(self):
        handler = _make_handler()
        cfg = ProcessingConfig(
            topic_prefixes={"greetings:test/greetings"},
            response_topics={"greetings:test/responses"},
        )
        proc = ConcreteProcessor(
            instance_id="test-id",
            runtime_config=cfg,
            config_identifier="greetings",
        )
        handler.register_mqtt_processor(proc)
        assert len(handler._bindings) == 1
        binding = handler._bindings[0]
        # Southbound subscribes to data, events, metadata
        assert "test/greetings/data" in binding.topics
        assert "test/greetings/events" in binding.topics
        assert "test/greetings/metadata" in binding.topics
        assert binding.response_topic.endswith("/test-id")
        # Southbound publishes commands
        assert binding.topic_prefix == "test/greetings"

    def test_shared_subscription(self):
        handler = _make_handler()
        cfg = ProcessingConfig(
            topic_prefixes={"greetings:test/greetings"},
            response_topics={"greetings:test/responses"},
            shared_subscription_name="mygroup",
        )
        proc = ConcreteProcessor(
            instance_id="test-id",
            runtime_config=cfg,
            config_identifier="greetings",
        )
        handler.register_mqtt_processor(proc)
        binding = handler._bindings[0]
        for topic in binding.topics:
            assert topic.startswith("$share/mygroup/")
        # Response topic does NOT get shared subscription prefix
        assert not binding.response_topic.startswith("$share/")

    def test_sets_publish_handler_on_processor(self):
        handler = _make_handler()
        cfg = ProcessingConfig(
            topic_prefixes={"greetings:test/greetings"},
            response_topics={"greetings:test/responses"},
        )
        proc = ConcreteProcessor(
            instance_id="test-id",
            runtime_config=cfg,
            config_identifier="greetings",
        )
        handler.register_mqtt_processor(proc)
        assert len(proc._publish_handlers) == 1

    def test_missing_processor_config_raises(self):
        handler = _make_handler()

        # Undecorated processor
        class BareProcessor(CloudEventProcessor):
            async def process_event(self, cloudevent):
                return None

            async def process_response_event(self, cloudevent):
                return None

            async def handle_expiration(self, cloudevent, timeout):
                return None

        proc = BareProcessor(
            instance_id="bare",
            runtime_config=ProcessingConfig(),
            config_identifier="bare",
        )
        with pytest.raises(ValueError, match="must be decorated"):
            handler.register_mqtt_processor(proc)

    def test_missing_topic_prefix_raises(self):
        handler = _make_handler()
        cfg = ProcessingConfig(
            topic_prefixes={"other:test/prefix"},
        )
        proc = ConcreteProcessor(
            instance_id="test-id",
            runtime_config=cfg,
            config_identifier="greetings",
        )
        with pytest.raises(ValueError, match="No topic prefix found"):
            handler.register_mqtt_processor(proc)


class TestEnrichResponseTransport:
    def test_adds_topic_from_request(self):
        handler = _make_handler()
        proc = _make_processor()
        outgoing_queue: Queue[tuple[CloudEvent, MessageIntent]] = Queue()
        binding = MQTTProcessorBinding(
            processor=proc,
            topics={"test/events/#"},
            response_topic="test/responses/test-id",
            outgoing_queue=outgoing_queue,
            topic_prefix="test",
            publish_intents=proc.publish_intents(),
        )
        request = CloudEvent(
            transportmetadata={"mqtt_response_topic": "resp/topic"},
        )
        response = CloudEvent()
        result = handler._enrich_response_transport(response, request, binding)
        assert result is True
        assert response.transportmetadata is not None
        assert response.transportmetadata["mqtt_topic"] == "resp/topic"
        assert (
            response.transportmetadata["mqtt_response_topic"]
            == "test/responses/test-id"
        )
        assert response.transportmetadata["mqtt_retain"] is False

    def test_keeps_existing_topic(self):
        handler = _make_handler()
        proc = _make_processor()
        outgoing_queue: Queue[tuple[CloudEvent, MessageIntent]] = Queue()
        binding = MQTTProcessorBinding(
            processor=proc,
            topics=set(),
            response_topic="",
            outgoing_queue=outgoing_queue,
            topic_prefix="test",
            publish_intents=proc.publish_intents(),
        )
        request = CloudEvent(
            transportmetadata={"mqtt_response_topic": "other"},
        )
        response = CloudEvent(
            transportmetadata={"mqtt_topic": "already/set"},
        )
        result = handler._enrich_response_transport(response, request, binding)
        assert result is True
        assert response.transportmetadata is not None
        assert response.transportmetadata["mqtt_topic"] == "already/set"

    def test_no_response_topic_returns_false(self):
        handler = _make_handler()
        proc = _make_processor()
        outgoing_queue: Queue[tuple[CloudEvent, MessageIntent]] = Queue()
        binding = MQTTProcessorBinding(
            processor=proc,
            topics=set(),
            response_topic="",
            outgoing_queue=outgoing_queue,
            topic_prefix="test",
            publish_intents=proc.publish_intents(),
        )
        request = CloudEvent(transportmetadata=None)
        response = CloudEvent()
        result = handler._enrich_response_transport(response, request, binding)
        assert result is False


# ===================================================================
# CloudEventProcessor message_callback / type_callback (base class)
# ===================================================================


class TestCloudEventProcessorCallbacks:
    @pytest.mark.asyncio
    async def test_message_callback_no_registered_type(self):
        proc = _make_processor()
        ce = CloudEvent(type="com.unknown", data=b"x")
        assert await proc.callback_incoming(ce) is None

    @pytest.mark.asyncio
    async def test_message_callback_unserialize_error(self):
        proc = _make_processor()

        async def handler(req):
            return req

        proc.register_callback(SamplePayload, handler, direction=Direction.INCOMING)
        ce = CloudEvent(
            type="com.test.sample.v1",
            data=b"bad",
            datacontenttype="application/xml",
        )
        assert await proc.callback_incoming(ce) is None

    @pytest.mark.asyncio
    async def test_message_callback_handler_returns_none(self):
        proc = _make_processor()

        async def handler(req):
            return None

        proc.register_callback(SamplePayload, handler, direction=Direction.INCOMING)
        ce = CloudEvent(
            type="com.test.sample.v1",
            data=SamplePayload().to_jsonb(),
            datacontenttype="application/json",
        )
        assert await proc.callback_incoming(ce) is None

    @pytest.mark.asyncio
    async def test_message_callback_single_response(self):
        proc = _make_processor()

        async def handler(req):
            return SamplePayload(value="echo")

        proc.register_callback(SamplePayload, handler, direction=Direction.INCOMING)
        ce = CloudEvent(
            type="com.test.sample.v1",
            data=SamplePayload().to_jsonb(),
            datacontenttype="application/json; charset=utf-8",
            correlationid="corr-1",
        )
        result = await proc.callback_incoming(ce)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].correlationid == "corr-1"
        assert result[0].causationid == ce.id

    @pytest.mark.asyncio
    async def test_message_callback_list_response(self):
        proc = _make_processor()

        async def handler(req):
            return [SamplePayload(value="a"), SamplePayload(value="b")]

        proc.register_callback(SamplePayload, handler, direction=Direction.INCOMING)
        ce = CloudEvent(
            type="com.test.sample.v1",
            data=SamplePayload().to_jsonb(),
            datacontenttype="application/json",
        )
        result = await proc.callback_incoming(ce)
        assert isinstance(result, list)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_message_callback_response_no_config(self):
        proc = _make_processor()

        async def handler(req):
            return PlainPayload(value="no-config")

        proc.register_callback(SamplePayload, handler, direction=Direction.INCOMING)
        ce = CloudEvent(
            type="com.test.sample.v1",
            data=SamplePayload().to_jsonb(),
            datacontenttype="application/json",
        )
        result = await proc.callback_incoming(ce)
        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_message_callback_serialize_error(self):
        proc = _make_processor()

        async def handler(req):
            return SamplePayload(value="x")

        proc.register_callback(SamplePayload, handler, direction=Direction.INCOMING)
        ce = CloudEvent(
            type="com.test.sample.v1",
            data=SamplePayload().to_jsonb(),
            datacontenttype="application/json",
        )
        with patch.object(
            proc,
            "create_event",
            return_value=CloudEvent(datacontenttype="application/xml"),
        ):
            result = await proc.callback_incoming(ce)
        assert result is None

    # --- type_callback tests ---

    @pytest.mark.asyncio
    async def test_type_callback_no_registered_type(self):
        proc = _make_processor()
        result = await proc.callback_outgoing(
            SamplePayload, MessageIntent.EVENT, "out/topic"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_type_callback_handler_returns_none(self):
        proc = _make_processor()

        async def handler(**kwargs):
            return None

        proc.register_callback(SamplePayload, handler, direction=Direction.OUTGOING)
        result = await proc.callback_outgoing(
            SamplePayload, MessageIntent.EVENT, "out/topic"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_type_callback_response_no_config(self):
        proc = _make_processor()

        async def handler(**kwargs):
            return PlainPayload(value="no-config")

        proc.register_callback(SamplePayload, handler, direction=Direction.OUTGOING)
        result = await proc.callback_outgoing(
            SamplePayload, MessageIntent.EVENT, "out/topic"
        )
        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_type_callback_serialize_error(self):
        proc = _make_processor()

        async def handler(**kwargs):
            return SamplePayload(value="x")

        proc.register_callback(SamplePayload, handler, direction=Direction.OUTGOING)
        with patch.object(
            proc,
            "create_event",
            return_value=CloudEvent(datacontenttype="application/xml"),
        ):
            result = await proc.callback_outgoing(
                SamplePayload, MessageIntent.EVENT, "out/topic"
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_type_callback_success_publishes_event(self):
        proc = _make_processor()
        published: list[tuple[CloudEvent, MessageIntent]] = []
        proc.register_publish_handler(lambda ce, intent: published.append((ce, intent)))  # pyright: ignore[reportArgumentType]

        async def handler(**kwargs):
            return SamplePayload(value="outbound")

        proc.register_callback(SamplePayload, handler, direction=Direction.OUTGOING)
        result = await proc.callback_outgoing(
            SamplePayload, MessageIntent.EVENT, "out/topic"
        )
        assert isinstance(result, list)
        assert len(result) == 1
        # The event should also have been sent to the publish handler
        assert len(published) == 1

    @pytest.mark.asyncio
    async def test_type_callback_list_response(self):
        proc = _make_processor()
        published: list[tuple[CloudEvent, MessageIntent | None]] = []
        proc.register_publish_handler(lambda ce, intent: published.append((ce, intent)))

        async def handler(**kwargs):
            return [SamplePayload(value="a"), SamplePayload(value="b")]

        proc.register_callback(SamplePayload, handler, direction=Direction.OUTGOING)
        result = await proc.callback_outgoing(
            SamplePayload, MessageIntent.EVENT, "out/topic"
        )
        assert isinstance(result, list)
        assert len(result) == 2
        assert len(published) == 2


# ===================================================================
# MQTTHandler
# ===================================================================


class TestMQTTHandler:
    def test_init(self):
        handler = _make_handler()
        assert handler._runtime_config is not None
        assert handler._redis_key_schema is not None
        assert handler._cloudevent_dedupe_dao is not None
        assert handler._expiration_timeout_tasks == {}
        assert handler._bindings == []

    def test_client_no_sat_no_tls(self):
        handler = _make_handler()
        with patch("app.mqtt.aiomqtt.Client") as mock_client_cls:
            mock_client_cls.return_value = MagicMock()
            handler._client()
            mock_client_cls.assert_called_once()

    def test_client_with_sat_and_tls(self):
        handler = _make_handler()
        handler._runtime_config.sat_token_path = MagicMock()
        handler._runtime_config.sat_token_path.exists.return_value = True
        handler._runtime_config.tls_cert_path = MagicMock()
        handler._runtime_config.tls_cert_path.exists.return_value = True
        handler._runtime_config.tls_cert_path.__str__ = lambda self: "/fake/cert"  # type: ignore[assignment]

        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=MagicMock(read=lambda: "token"))
        mock_file.__exit__ = MagicMock(return_value=False)

        with (
            patch("app.mqtt.aiomqtt.Client") as mock_client_cls,
            patch("builtins.open", return_value=mock_file),
        ):
            mock_client_cls.return_value = MagicMock()
            handler._client()
            call_kwargs = mock_client_cls.call_args[1]
            assert call_kwargs["properties"] is not None
            assert call_kwargs["tls_params"] is not None

    # --- _publish_message ---

    @pytest.mark.asyncio
    async def test_publish_message_no_topic(self):
        handler = _make_handler()
        client = AsyncMock()
        ce = CloudEvent(transportmetadata=None)
        result = await handler._publish_message(client, ce)
        assert result is None
        client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_publish_message_no_topic_in_metadata(self):
        handler = _make_handler()
        client = AsyncMock()
        ce = CloudEvent(transportmetadata={})
        result = await handler._publish_message(client, ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_publish_message_basic(self):
        handler = _make_handler()
        client = AsyncMock()
        ce = CloudEvent(
            transportmetadata={"mqtt_topic": "out/topic"},
            datacontenttype="application/json; charset=utf-8",
        )
        await handler._publish_message(client, ce)
        client.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_publish_message_binary_content(self):
        handler = _make_handler()
        client = AsyncMock()
        ce = CloudEvent(
            transportmetadata={"mqtt_topic": "out/topic"},
            datacontenttype="application/octet-stream",
        )
        await handler._publish_message(client, ce)
        client.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_publish_message_with_response_topic_uses_qos1(self):
        handler = _make_handler()
        client = AsyncMock()
        ce = CloudEvent(
            transportmetadata={
                "mqtt_topic": "out/topic",
                "mqtt_response_topic": "resp/topic",
            },
            correlationid=str(uuid.uuid4()),
        )
        await handler._publish_message(client, ce)
        call_kwargs = client.publish.call_args[1]
        assert call_kwargs["qos"] == QoS.AT_LEAST_ONCE

    @pytest.mark.asyncio
    async def test_publish_message_with_expiry_schedules_task(self):
        handler = _make_handler()
        client = AsyncMock()
        proc = _make_processor()

        ce = CloudEvent(
            transportmetadata={"mqtt_topic": "out/topic"},
            expiryinterval=30,
        )
        await handler._publish_message(client, ce, processor=proc)
        assert ce.id in handler._expiration_timeout_tasks
        # Clean up task
        handler._expiration_timeout_tasks[ce.id].cancel()
        try:
            await handler._expiration_timeout_tasks[ce.id]
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_publish_message_with_retain(self):
        handler = _make_handler()
        client = AsyncMock()
        ce = CloudEvent(
            transportmetadata={"mqtt_topic": "out/topic", "mqtt_retain": True},
        )
        await handler._publish_message(client, ce)
        call_kwargs = client.publish.call_args[1]
        assert call_kwargs["retain"] is True

    # --- is_duplicate_message ---

    @pytest.mark.asyncio
    async def test_is_duplicate_message(self):
        handler = _make_handler()
        handler._cloudevent_dedupe_dao.is_duplicate = AsyncMock(return_value=True)
        ce = CloudEvent(source="src", id="id-1")
        assert await handler.is_duplicate_message(ce) is True

    @pytest.mark.asyncio
    async def test_is_not_duplicate_message(self):
        handler = _make_handler()
        handler._cloudevent_dedupe_dao.is_duplicate = AsyncMock(return_value=False)
        ce = CloudEvent(source="src", id="id-2")
        assert await handler.is_duplicate_message(ce) is False

    # --- cloudevent_from_message ---

    def test_cloudevent_from_message_basic(self):
        handler = _make_handler()
        msg = _make_mqtt_message(properties=None)
        ce = handler.cloudevent_from_message(msg)
        assert ce.data == msg.payload
        assert ce.transportmetadata is not None
        assert ce.transportmetadata["mqtt_message_id"] == 42

    def test_cloudevent_from_message_with_properties(self):
        handler = _make_handler()
        corr_uuid = uuid.uuid4()
        props = MagicMock()
        props.MessageExpiryInterval = 120
        props.ContentType = "application/json"
        props.ResponseTopic = "resp/topic"
        props.CorrelationData = corr_uuid.bytes
        props.UserProperty = [
            ("type", "com.test.sample.v1"),
            ("source", "test-source"),
        ]
        msg = _make_mqtt_message(properties=props)
        ce = handler.cloudevent_from_message(msg)
        assert ce.expiryinterval == 120
        assert ce.datacontenttype == "application/json"
        assert ce.transportmetadata is not None
        assert ce.transportmetadata["mqtt_response_topic"] == "resp/topic"
        assert ce.correlationid == str(corr_uuid)
        assert ce.type == "com.test.sample.v1"
        assert ce.source == "test-source"

    def test_cloudevent_from_message_custom_metadata(self):
        handler = _make_handler()
        props = MagicMock()
        props.UserProperty = [("customkey", "customval")]
        # Remove attributes we don't need for this test
        del props.MessageExpiryInterval
        del props.ContentType
        del props.ResponseTopic
        del props.CorrelationData
        msg = _make_mqtt_message(properties=props)
        ce = handler.cloudevent_from_message(msg)
        assert ce.custommetadata is not None
        assert ce.custommetadata.get("customkey") == "customval"

    # --- _process_message ---

    @pytest.mark.asyncio
    async def test_process_message_duplicate(self):
        handler = _make_handler()
        client = AsyncMock()
        proc = _make_processor()
        _make_binding(handler, proc)

        msg = _make_mqtt_message()
        handler._cloudevent_dedupe_dao.is_duplicate = AsyncMock(return_value=True)

        ok, sub = await handler._process_message(client, msg)
        assert ok is False

    @pytest.mark.asyncio
    async def test_process_message_non_duplicate_no_match(self):
        handler = _make_handler()
        client = AsyncMock()
        client._client = MagicMock()
        client._client.ack = MagicMock()

        handler._cloudevent_dedupe_dao.is_duplicate = AsyncMock(return_value=False)
        # No bindings registered → empty subscription
        msg = _make_mqtt_message()
        ok, sub = await handler._process_message(client, msg)
        assert ok is True

    @pytest.mark.asyncio
    async def test_process_message_cancels_expiration(self):
        handler = _make_handler()
        client = AsyncMock()
        client._client = MagicMock()
        client._client.ack = MagicMock()

        handler._cloudevent_dedupe_dao.is_duplicate = AsyncMock(return_value=False)

        # Simulate an existing expiration task for this event
        ce_id = str(uuid.uuid4())
        mock_task = MagicMock()
        handler._expiration_timeout_tasks[ce_id] = mock_task

        msg = _make_mqtt_message()
        # Override cloudevent_from_message to return CE with matching id
        ce = CloudEvent(id=ce_id, data=msg.payload)
        ce.transportmetadata = {"mqtt_topic": "t", "mqtt_qos": 1, "mqtt_retain": False}
        with patch.object(handler, "cloudevent_from_message", return_value=ce):
            await handler._process_message(client, msg)
        mock_task.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_dispatches_to_processor(self):
        handler = _make_handler()
        client = AsyncMock()
        client._client = MagicMock()
        client._client.ack = MagicMock()

        handler._cloudevent_dedupe_dao.is_duplicate = AsyncMock(return_value=False)

        proc = _make_processor()
        proc.process_event = AsyncMock(return_value=None)
        binding = _make_binding(
            handler,
            proc,
            topics={"test/events/#"},
            response_topic="never/matches",
        )

        # Only match event topics, not the response topic
        msg = _make_mqtt_message(
            topic="test/events/foo",
            match_topics=binding.topics,
        )
        await handler._process_message(client, msg)
        proc.process_event.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_process_message_response_topic_match(self):
        handler = _make_handler()
        client = AsyncMock()
        client._client = MagicMock()
        client._client.ack = MagicMock()

        handler._cloudevent_dedupe_dao.is_duplicate = AsyncMock(return_value=False)

        proc = _make_processor()
        proc.process_response_event = AsyncMock(return_value=None)
        _make_binding(
            handler,
            proc,
            topics=set(),
            response_topic="test/events/foo",
        )

        msg = _make_mqtt_message(
            topic="test/events/foo",
            match_topics={"test/events/foo"},
        )
        await handler._process_message(client, msg)
        proc.process_response_event.assert_awaited()

    @pytest.mark.asyncio
    async def test_process_message_publishes_list_response(self):
        handler = _make_handler()
        client = AsyncMock()
        client._client = MagicMock()
        client._client.ack = MagicMock()

        handler._cloudevent_dedupe_dao.is_duplicate = AsyncMock(return_value=False)

        proc = _make_processor()
        resp1 = CloudEvent(transportmetadata={"mqtt_topic": "out"})
        resp2 = CloudEvent(transportmetadata={"mqtt_topic": "out"})
        proc.process_event = AsyncMock(return_value=[resp1, resp2])
        binding = _make_binding(
            handler,
            proc,
            topics={"test/events/#"},
            response_topic="never/matches",
        )

        with patch.object(
            handler, "_publish_message", new_callable=AsyncMock
        ) as mock_pub:
            msg = _make_mqtt_message(match_topics=binding.topics)
            await handler._process_message(client, msg)
            assert mock_pub.await_count == 2

    @pytest.mark.asyncio
    async def test_process_message_publishes_single_response(self):
        handler = _make_handler()
        client = AsyncMock()
        client._client = MagicMock()
        client._client.ack = MagicMock()

        handler._cloudevent_dedupe_dao.is_duplicate = AsyncMock(return_value=False)

        proc = _make_processor()
        single_resp = CloudEvent(transportmetadata={"mqtt_topic": "out"})
        proc.process_event = AsyncMock(return_value=single_resp)
        binding = _make_binding(
            handler,
            proc,
            topics={"test/events/#"},
            response_topic="never/matches",
        )

        with patch.object(
            handler, "_publish_message", new_callable=AsyncMock
        ) as mock_pub:
            msg = _make_mqtt_message(match_topics=binding.topics)
            await handler._process_message(client, msg)
            mock_pub.assert_awaited_once()

    # --- _outgoing_message_publisher ---

    @pytest.mark.asyncio
    async def test_outgoing_message_publisher(self):
        handler = _make_handler()
        client = AsyncMock()
        proc = _make_processor()
        binding = _make_binding(handler, proc)

        ce = CloudEvent(transportmetadata={"mqtt_topic": "out"})
        await binding.outgoing_queue.put((ce, MessageIntent.EVENT))

        async def stop_after_one(*args, **kwargs):
            raise asyncio.CancelledError()

        with patch.object(
            handler,
            "_publish_message",
            new_callable=AsyncMock,
            side_effect=stop_after_one,
        ):
            with pytest.raises(asyncio.CancelledError):
                await handler._outgoing_message_publisher(client, binding)

    @pytest.mark.asyncio
    async def test_outgoing_message_publisher_uses_publish_topic_pattern_fallback(
        self,
    ):
        """When no mqtt_topic or mqtt_intent is set, the first publish_topic_pattern is used."""
        handler = _make_handler()
        client = AsyncMock()
        proc = _make_processor()
        binding = _make_binding(handler, proc, response_topic="test/responses/test-id")

        ce = CloudEvent()  # no transportmetadata / mqtt_topic
        await binding.outgoing_queue.put((ce, MessageIntent.COMMAND))

        published: list[CloudEvent] = []

        async def capture_and_stop(client, message, processor):
            published.append(message)
            raise asyncio.CancelledError()

        with patch.object(
            handler,
            "_publish_message",
            new_callable=AsyncMock,
            side_effect=capture_and_stop,
        ):
            with pytest.raises(asyncio.CancelledError):
                await handler._outgoing_message_publisher(client, binding)

        assert len(published) == 1
        assert published[0].transportmetadata is not None
        # Southbound processor → publish_intents={COMMAND} → "test/commands"
        assert published[0].transportmetadata["mqtt_topic"] == "test/commands"
        assert (
            published[0].transportmetadata["mqtt_response_topic"]
            == "test/responses/test-id"
        )

    @pytest.mark.asyncio
    async def test_outgoing_message_publisher_uses_intent_for_topic(self):
        """When intent is provided, the matching publish_topic_pattern is used."""
        handler = _make_handler()
        client = AsyncMock()
        proc = _make_processor()
        binding = _make_binding(handler, proc, response_topic="test/responses/test-id")

        ce = CloudEvent()
        await binding.outgoing_queue.put((ce, MessageIntent.COMMAND))

        published: list[CloudEvent] = []

        async def capture_and_stop(client, message, processor):
            published.append(message)
            raise asyncio.CancelledError()

        with patch.object(
            handler,
            "_publish_message",
            new_callable=AsyncMock,
            side_effect=capture_and_stop,
        ):
            with pytest.raises(asyncio.CancelledError):
                await handler._outgoing_message_publisher(client, binding)

        assert len(published) == 1
        assert published[0].transportmetadata is not None
        assert published[0].transportmetadata["mqtt_topic"] == "test/commands"

    # --- task ---

    @pytest.mark.asyncio
    async def test_task_redis_failure(self):
        import redis as _redis

        handler = _make_handler()
        handler._redis_client.ping = AsyncMock(side_effect=_redis.RedisError("down"))

        mock_client = MagicMock()
        with (
            patch.object(handler, "_client", return_value=mock_client),
            pytest.raises(_redis.RedisError),
        ):
            await handler.task()

    @pytest.mark.asyncio
    async def test_task_cancelled(self):
        handler = _make_handler()
        handler._redis_client.ping = AsyncMock()
        handler._redis_client.aclose = AsyncMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.subscribe = AsyncMock()
        mock_client.messages = MagicMock()

        # Make TaskGroup raise CancelledError immediately
        with (
            patch.object(handler, "_client", return_value=mock_client),
            patch("app.mqtt.asyncio.TaskGroup") as mock_tg_cls,
        ):

            def _close_coro(coro):
                """Close the coroutine so it doesn't trigger RuntimeWarning."""
                coro.close()

            mock_tg = MagicMock()
            mock_tg.__aenter__ = AsyncMock(return_value=mock_tg)
            mock_tg.__aexit__ = AsyncMock(side_effect=asyncio.CancelledError())
            mock_tg.create_task = MagicMock(side_effect=_close_coro)
            mock_tg_cls.return_value = mock_tg

            with pytest.raises(asyncio.CancelledError):
                await handler.task()

            handler._redis_client.aclose.assert_awaited_once()


# ===================================================================
# OTELInstrumentedMQTTHandler
# ===================================================================


class TestOTELInstrumentedMQTTHandler:
    def _make_otel_handler(self) -> OTELInstrumentedMQTTHandler:
        config = MQTTConfig()
        pool = MagicMock()
        key_schema = RedisKeySchema()
        with patch("app.mqtt.redis.Redis"):
            return OTELInstrumentedMQTTHandler(config, pool, key_schema)

    def test_init_sets_tracer_meter_metrics(self):
        handler = self._make_otel_handler()
        assert handler._tracer is not None
        assert handler._meter is not None
        assert "process_counter" in handler._metrics
        assert "process_duration" in handler._metrics

    def test_record_metrics_success(self):
        handler = self._make_otel_handler()
        handler._metrics["process_counter"] = MagicMock()
        handler._metrics["process_duration"] = MagicMock()
        handler.record_metrics(0.5, error=False, base_attributes={"key": "val"})
        handler._metrics["process_counter"].add.assert_called_once()
        handler._metrics["process_duration"].record.assert_called_once()
        attrs = handler._metrics["process_counter"].add.call_args[0][1]
        assert attrs["status"] == "success"

    def test_record_metrics_error(self):
        handler = self._make_otel_handler()
        handler._metrics["process_counter"] = MagicMock()
        handler._metrics["process_duration"] = MagicMock()
        handler.record_metrics(1.0, error=True)
        attrs = handler._metrics["process_counter"].add.call_args[0][1]
        assert attrs["status"] == "error"

    @pytest.mark.asyncio
    async def test_process_message_with_tracing(self):
        handler = self._make_otel_handler()
        client = AsyncMock()
        client._client = MagicMock()
        client._client.ack = MagicMock()

        handler._cloudevent_dedupe_dao.is_duplicate = AsyncMock(return_value=False)
        handler._metrics["process_counter"] = MagicMock()
        handler._metrics["process_duration"] = MagicMock()

        msg = _make_mqtt_message()
        props = MagicMock()
        props.UserProperty = [("traceparent", "00-abc-def-01")]
        del props.MessageExpiryInterval
        del props.ContentType
        del props.ResponseTopic
        del props.CorrelationData
        msg.properties = props

        error, sub = await handler._process_message(client, msg)
        handler._metrics["process_counter"].add.assert_called()

    @pytest.mark.asyncio
    async def test_process_message_without_user_properties(self):
        handler = self._make_otel_handler()
        client = AsyncMock()
        client._client = MagicMock()
        client._client.ack = MagicMock()

        handler._cloudevent_dedupe_dao.is_duplicate = AsyncMock(return_value=False)
        handler._metrics["process_counter"] = MagicMock()
        handler._metrics["process_duration"] = MagicMock()

        msg = _make_mqtt_message(properties=None)
        error, sub = await handler._process_message(client, msg)
        handler._metrics["process_counter"].add.assert_called()


# ===================================================================
# CloudEvent tests (originally in test_mqtt.py)
# ===================================================================


class TestCloudEvent:
    def test_cloudevent_defaults(self):
        ce = CloudEvent()
        assert ce.id is not None
        assert ce.recordedtime is not None
        assert ce.specversion == "1.0"
        assert ce.correlationid is not None

    def test_cloudevent_to_user_properties(self):
        ce = CloudEvent(type="com.example.test", source="test-source")
        props = ce.to_dict(context={"remove_data": True, "make_str_values": True})
        assert props["type"] == "com.example.test"
        assert props["source"] == "test-source"
        assert "id" in props
        assert "specversion" in props

    def test_custom_user_properties(self):
        ce = CloudEvent(type="type", custommetadata={"custom": "value"})
        props = ce.to_dict(context={"remove_data": True, "make_str_values": True})
        assert props["custom"] == "value"
        assert props["type"] == "type"

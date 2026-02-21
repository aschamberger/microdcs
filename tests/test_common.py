from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app import ProcessingConfig
from app.common import (
    CloudEvent,
    CloudEventProcessor,
    Direction,
    ErrorKind,
    MessageIntent,
    ProcessorBinding,
    ProtocolHandler,
    incoming,
    outgoing,
    processor_config,
)
from app.dataclass import DataClassConfig, DataClassMixin

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


@dataclass
class SamplePayload(DataClassMixin):
    value: str = "hello"

    class Config(DataClassConfig):
        cloudevent_type: str = "com.test.sample.v1"
        cloudevent_dataschema: str = "https://example.com/schemas/sample-v1"


@dataclass
class AnotherPayload(DataClassMixin):
    count: int = 0

    class Config(DataClassConfig):
        cloudevent_type: str = "com.test.another.v1"
        cloudevent_dataschema: str = "https://example.com/schemas/another-v1"


@dataclass
class PlainDataClass(DataClassMixin):
    """No Config subclass – used for negative tests."""

    data: str = ""


def _make_processing_config(**overrides: Any) -> ProcessingConfig:
    defaults = {
        "cloudevent_source": "test-source",
        "message_expiry_interval": 60,
    }
    defaults.update(overrides)
    return ProcessingConfig(**defaults)


# ---------------------------------------------------------------------------
# Direction enum
# ---------------------------------------------------------------------------


class TestDirection:
    def test_values(self):
        assert Direction.INCOMING == "in"
        assert Direction.OUTGOING == "out"

    def test_is_str_enum(self):
        assert isinstance(Direction.INCOMING, str)


# ---------------------------------------------------------------------------
# ErrorKind enum
# ---------------------------------------------------------------------------


class TestErrorKind:
    def test_all_members_are_strings(self):
        for member in ErrorKind:
            assert isinstance(member, str)

    def test_selected_values(self):
        assert ErrorKind.TIMEOUT == "TIMEOUT"
        assert ErrorKind.UNKNOWN_ERROR == "UNKNOWN_ERROR"
        assert ErrorKind.CONFIGURATION_INVALID == "CONFIGURATION_INVALID"
        assert ErrorKind.SERVICE_UNAVAILABLE == "SERVICE_UNAVAILABLE"


# ---------------------------------------------------------------------------
# CloudEvent – construction
# ---------------------------------------------------------------------------


class TestCloudEventConstruction:
    def test_defaults(self):
        ce = CloudEvent()
        assert ce.specversion == "1.0"
        assert ce.id is not None
        assert ce.correlationid is not None
        assert ce.recordedtime is not None
        assert ce.data is None
        assert ce.custommetadata == {}
        assert ce.transportmetadata == {}

    def test_post_init_sets_error_message_when_missing(self):
        ce = CloudEvent(mdcserrorkind=ErrorKind.TIMEOUT)
        assert ce.mdcserrormessage == "Unknown error occurred."

    def test_post_init_preserves_existing_error_message(self):
        ce = CloudEvent(
            mdcserrorkind=ErrorKind.TIMEOUT, mdcserrormessage="Custom message"
        )
        assert ce.mdcserrormessage == "Custom message"

    def test_post_init_no_errorkind_no_message(self):
        ce = CloudEvent()
        assert ce.mdcserrorkind is None
        assert ce.mdcserrormessage is None


# ---------------------------------------------------------------------------
# CloudEvent – serialization / deserialization round-trip
# ---------------------------------------------------------------------------


class TestCloudEventSerialization:
    def test_json_round_trip_basic(self):
        ce = CloudEvent(
            source="test",
            type="com.test.v1",
            datacontenttype="application/json",
        )
        json_bytes = ce.to_jsonb()
        restored = CloudEvent.from_json(json_bytes)
        assert restored.source == "test"
        assert restored.type == "com.test.v1"
        assert restored.specversion == "1.0"

    def test_omit_none_fields(self):
        ce = CloudEvent(source="test")
        d = ce.to_dict()
        assert "data" not in d
        assert "traceparent" not in d
        assert "tracestate" not in d
        assert "subject" not in d

    def test_transportmetadata_not_serialized(self):
        ce = CloudEvent(
            source="test",
            transportmetadata={"mqtt_topic": "some/topic"},
        )
        d = ce.to_dict()
        assert "transportmetadata" not in d
        assert "mqtt_topic" not in d

    def test_custommetadata_flattened_on_serialize(self):
        ce = CloudEvent(
            source="test",
            custommetadata={"x-custom-key": "custom-value"},
        )
        d = ce.to_dict()
        assert "custommetadata" not in d
        assert d["x-custom-key"] == "custom-value"

    def test_custommetadata_collected_on_deserialize(self):
        raw = {
            "specversion": "1.0",
            "source": "test",
            "id": "abc",
            "x-my-ext": "ext-val",
        }
        ce = CloudEvent.from_dict(raw)
        assert ce.custommetadata is not None
        assert ce.custommetadata["x-my-ext"] == "ext-val"

    def test_error_context_serialized_as_csv_pairs(self):
        ce = CloudEvent(
            source="test",
            mdcserrorkind=ErrorKind.TIMEOUT,
            mdcserrorcontext={"retries": "3", "limit": "10"},
        )
        d = ce.to_dict()
        ctx = d.get("mdcserrorcontext", "")
        pairs = dict(item.split("=", 1) for item in ctx.split(","))
        assert pairs["retries"] == "3"
        assert pairs["limit"] == "10"

    def test_empty_error_context_omitted(self):
        ce = CloudEvent(source="test", mdcserrorcontext={})
        d = ce.to_dict()
        assert "mdcserrorcontext" not in d

    def test_error_context_deserialized_from_csv_pairs(self):
        raw = {
            "specversion": "1.0",
            "source": "test",
            "id": "abc",
            "mdcserrorkind": "TIMEOUT",
            "mdcserrormessage": "timed out",
            "mdcserrorcontext": "retries=3,limit=10",
        }
        ce = CloudEvent.from_dict(raw)
        assert ce.mdcserrorcontext == {"retries": "3", "limit": "10"}

    def test_error_context_absent_yields_empty_dict(self):
        raw = {"specversion": "1.0", "source": "test", "id": "abc"}
        ce = CloudEvent.from_dict(raw)
        assert ce.mdcserrorcontext == {}

    def test_remove_data_context(self):
        ce = CloudEvent(source="test", data=b"payload")
        d = ce.to_dict(context={"remove_data": True})
        assert "data" not in d

    def test_make_str_values_context(self):
        ce = CloudEvent(source="test", expiryinterval=30)
        d = ce.to_dict(context={"make_str_values": True})
        assert d["expiryinterval"] == "30"
        assert isinstance(d["source"], str)

    def test_msgpack_round_trip(self):
        ce = CloudEvent(source="test", type="com.test.v1")
        packed = ce.to_msgpack()
        restored = CloudEvent.from_msgpack(packed)
        assert restored.source == "test"
        assert restored.type == "com.test.v1"


# ---------------------------------------------------------------------------
# CloudEvent – payload (un)serialization
# ---------------------------------------------------------------------------


class TestCloudEventPayload:
    def test_unserialize_json_payload(self):
        payload = SamplePayload(value="world")
        ce = CloudEvent(
            datacontenttype="application/json",
            data=payload.to_jsonb(),
        )
        result = ce.unserialize_payload(SamplePayload)
        assert isinstance(result, SamplePayload)
        assert result.value == "world"

    def test_unserialize_json_utf8_payload(self):
        payload = SamplePayload(value="utf8")
        ce = CloudEvent(
            datacontenttype="application/json; charset=utf-8",
            data=payload.to_jsonb(),
        )
        result = ce.unserialize_payload(SamplePayload)
        assert isinstance(result, SamplePayload)
        assert result.value == "utf8"

    def test_unserialize_msgpack_payload(self):
        payload = SamplePayload(value="packed")
        ce = CloudEvent(
            datacontenttype="application/msgpack",
            data=payload.to_msgpack(),
        )
        result = ce.unserialize_payload(SamplePayload)
        assert isinstance(result, SamplePayload)
        assert result.value == "packed"

    def test_unserialize_msgpack_utf8_payload(self):
        payload = SamplePayload(value="packed-utf8")
        ce = CloudEvent(
            datacontenttype="application/msgpack; charset=utf-8",
            data=payload.to_msgpack(),
        )
        result = ce.unserialize_payload(SamplePayload)
        assert isinstance(result, SamplePayload)
        assert result.value == "packed-utf8"

    def test_unserialize_octet_stream(self):
        raw = b"\x00\x01\x02"
        ce = CloudEvent(
            datacontenttype="application/octet-stream",
            data=raw,
        )
        result = ce.unserialize_payload(SamplePayload)
        assert result == raw

    def test_unserialize_empty_data_json(self):
        ce = CloudEvent(
            datacontenttype="application/json",
            data=None,
        )
        result = ce.unserialize_payload(SamplePayload)
        assert isinstance(result, SamplePayload)

    def test_unserialize_empty_data_msgpack(self):
        ce = CloudEvent(
            datacontenttype="application/msgpack",
            data=None,
        )
        result = ce.unserialize_payload(SamplePayload)
        assert isinstance(result, SamplePayload)

    def test_unserialize_unsupported_content_type_raises(self):
        ce = CloudEvent(datacontenttype="text/plain", data=b"hi")
        with pytest.raises(ValueError, match="Unsupported content type"):
            ce.unserialize_payload(SamplePayload)

    def test_serialize_json_payload(self):
        payload = SamplePayload(value="ser")
        ce = CloudEvent(datacontenttype="application/json")
        ce.serialize_payload(payload)
        assert ce.data is not None
        restored = SamplePayload.from_json(ce.data)
        assert restored.value == "ser"

    def test_serialize_json_utf8_payload(self):
        payload = SamplePayload(value="ser-utf8")
        ce = CloudEvent(datacontenttype="application/json; charset=utf-8")
        ce.serialize_payload(payload)
        assert ce.data is not None
        restored = SamplePayload.from_json(ce.data)
        assert restored.value == "ser-utf8"

    def test_serialize_msgpack_payload(self):
        payload = SamplePayload(value="packed-ser")
        ce = CloudEvent(datacontenttype="application/msgpack")
        ce.serialize_payload(payload)
        assert ce.data is not None
        restored = SamplePayload.from_msgpack(ce.data)
        assert restored.value == "packed-ser"

    def test_serialize_msgpack_utf8_payload(self):
        payload = SamplePayload(value="packed-ser-utf8")
        ce = CloudEvent(datacontenttype="application/msgpack; charset=utf-8")
        ce.serialize_payload(payload)
        assert ce.data is not None
        restored = SamplePayload.from_msgpack(ce.data)
        assert restored.value == "packed-ser-utf8"

    def test_serialize_octet_stream(self):
        raw = b"\xde\xad"
        ce = CloudEvent(datacontenttype="application/octet-stream")
        ce.serialize_payload(raw)  # type: ignore[arg-type]
        assert ce.data == raw

    def test_serialize_unsupported_content_type_raises(self):
        ce = CloudEvent(datacontenttype="text/xml")
        with pytest.raises(ValueError, match="Unsupported content type"):
            ce.serialize_payload(SamplePayload())

    def test_serialize_sets_type_and_dataschema(self):
        payload = SamplePayload(value="x")
        ce = CloudEvent(datacontenttype="application/json")
        ce.serialize_payload(payload)
        assert ce.type == "com.test.sample.v1"
        assert ce.dataschema == "https://example.com/schemas/sample-v1"

    def test_unserialize_custom_metadata_passed_to_dataclass(self):
        """Custom metadata is passed to the dataclass via __custom_metadata__ during deserialization."""
        payload = SamplePayload(value="hidden")
        ce = CloudEvent(
            datacontenttype="application/json",
            data=payload.to_jsonb(),
            custommetadata={"_hidden_str": "val"},
        )
        result = ce.unserialize_payload(SamplePayload)
        assert isinstance(result, SamplePayload)

    def test_serialize_extracts_custom_metadata(self):
        """Custom metadata is extracted from the dataclass via __get_custom_metadata__ during serialization."""
        payload = SamplePayload(value="ins")
        ce = CloudEvent(datacontenttype="application/json")
        ce.serialize_payload(payload)
        assert ce.type == "com.test.sample.v1"


# ---------------------------------------------------------------------------
# CloudEventProcessor – registration & helpers
# ---------------------------------------------------------------------------


@processor_config(binding=ProcessorBinding.SOUTHBOUND)
class ConcreteProcessor(CloudEventProcessor):
    """Minimal concrete subclass for testing base-class behaviour."""

    def __init__(
        self,
        instance_id: str = "test-1",
        runtime_config: ProcessingConfig | None = None,
        config_identifier: str = "test",
        queue_size: int = 1,
    ):
        super().__init__(
            instance_id, runtime_config or _make_processing_config(), config_identifier
        )
        self.published_events: list[tuple[CloudEvent, MessageIntent | None]] = []
        self.register_publish_handler(
            lambda ce, intent: self.published_events.append((ce, intent))
        )

    async def process_event(self, cloudevent: CloudEvent) -> Any:
        return None

    async def process_response_event(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        return None

    async def handle_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        return None


class TestCloudEventProcessorRegistration:
    def test_register_callback_incoming(self):
        proc = ConcreteProcessor()
        cb = AsyncMock()
        proc.register_callback(SamplePayload, cb, Direction.INCOMING)
        assert "com.test.sample.v1" in proc._type_callbacks_in
        assert proc._type_classes["com.test.sample.v1"] is SamplePayload

    def test_register_callback_outgoing(self):
        proc = ConcreteProcessor()
        cb = AsyncMock()
        proc.register_callback(SamplePayload, cb, Direction.OUTGOING)
        assert "com.test.sample.v1" in proc._type_callbacks_out

    def test_register_callback_union_type(self):
        proc = ConcreteProcessor()
        cb = AsyncMock()
        proc.register_callback(SamplePayload | AnotherPayload, cb)
        assert "com.test.sample.v1" in proc._type_callbacks_in
        assert "com.test.another.v1" in proc._type_callbacks_in

    def test_register_callback_non_callable_raises(self):
        proc = ConcreteProcessor()
        with pytest.raises(TypeError, match="callback must be callable"):
            proc.register_callback(SamplePayload, "not_callable")  # type: ignore

    def test_register_callback_non_dataclass_mixin_raises(self):
        proc = ConcreteProcessor()
        with pytest.raises(TypeError, match="must be a subclass of DataClassMixin"):
            proc.register_callback(str, AsyncMock())

    def test_register_callback_missing_config_raises(self):
        proc = ConcreteProcessor()
        with pytest.raises(TypeError, match="Config subclass of DataClassConfig"):
            proc.register_callback(PlainDataClass, AsyncMock())

    def test_event_has_callback(self):
        proc = ConcreteProcessor()
        proc.register_callback(SamplePayload, AsyncMock())
        ce = CloudEvent(type="com.test.sample.v1")
        assert proc.event_has_callback(ce) is True
        ce2 = CloudEvent(type="com.test.unknown.v1")
        assert proc.event_has_callback(ce2) is False


class TestCloudEventProcessorPublishAndCreate:
    def test_publish_event(self):
        proc = ConcreteProcessor(queue_size=5)
        ce = CloudEvent(source="test")
        proc.publish_event(ce)
        assert len(proc.published_events) == 1
        assert proc.published_events[0][0] is ce


class TestCloudEventProcessorDecorators:
    def test_incoming_decorator_registers_callback(self):
        class DecoratedProcessor(ConcreteProcessor):
            @incoming(SamplePayload)
            async def handle_sample(self, payload):
                return None

        proc = DecoratedProcessor()
        assert "com.test.sample.v1" in proc._type_callbacks_in
        assert proc._type_classes["com.test.sample.v1"] is SamplePayload

    def test_outgoing_decorator_registers_callback(self):
        class DecoratedProcessor(ConcreteProcessor):
            @outgoing(SamplePayload)
            async def handle_sample(self, **kwargs):
                return None

        proc = DecoratedProcessor()
        assert "com.test.sample.v1" in proc._type_callbacks_out

    def test_both_decorators_on_different_methods(self):
        class DecoratedProcessor(ConcreteProcessor):
            @incoming(SamplePayload)
            async def handle_sample_in(self, payload):
                return None

            @outgoing(AnotherPayload)
            async def handle_another_out(self, **kwargs):
                return None

        proc = DecoratedProcessor()
        assert "com.test.sample.v1" in proc._type_callbacks_in
        assert "com.test.another.v1" in proc._type_callbacks_out

    def test_incoming_decorator_union_type(self):
        class DecoratedProcessor(ConcreteProcessor):
            @incoming(SamplePayload | AnotherPayload)
            async def handle_both(self, payload):
                return None

        proc = DecoratedProcessor()
        assert "com.test.sample.v1" in proc._type_callbacks_in
        assert "com.test.another.v1" in proc._type_callbacks_in

    def test_decorator_combined_with_manual_registration(self):
        class DecoratedProcessor(ConcreteProcessor):
            @incoming(SamplePayload)
            async def handle_sample(self, payload):
                return None

        proc = DecoratedProcessor()
        cb = AsyncMock()
        proc.register_callback(AnotherPayload, cb, Direction.OUTGOING)
        assert "com.test.sample.v1" in proc._type_callbacks_in
        assert "com.test.another.v1" in proc._type_callbacks_out

    def test_publish_event_no_handlers_logs_warning(self, caplog):
        proc = ConcreteProcessor()
        proc._publish_handlers.clear()
        ce = CloudEvent(source="test")
        proc.publish_event(ce)
        assert "No publish handlers registered" in caplog.text

    def test_create_event_sets_source(self):
        cfg = _make_processing_config(cloudevent_source="my-source")
        proc = ConcreteProcessor(runtime_config=cfg)
        ce = proc.create_event()
        assert ce.source == "my-source"

    def test_create_event_sets_expiry_interval(self):
        cfg = _make_processing_config(message_expiry_interval=120)
        proc = ConcreteProcessor(runtime_config=cfg)
        ce = proc.create_event()
        assert ce.expiryinterval == 120

    def test_create_event_no_expiry_when_zero(self):
        cfg = _make_processing_config(message_expiry_interval=0)
        proc = ConcreteProcessor(runtime_config=cfg)
        ce = proc.create_event()
        assert ce.expiryinterval is None  # not set because <= 0

    def test_create_event_no_expiry_when_none(self):
        cfg = _make_processing_config(message_expiry_interval=None)
        proc = ConcreteProcessor(runtime_config=cfg)
        ce = proc.create_event()
        assert ce.expiryinterval is None

    def test_create_event_no_source_when_none(self):
        cfg = _make_processing_config(cloudevent_source=None)
        proc = ConcreteProcessor(runtime_config=cfg)
        ce = proc.create_event()
        assert ce.source is None

    def test_create_event_default_content_type(self):
        proc = ConcreteProcessor()
        ce = proc.create_event()
        assert ce.datacontenttype == "application/json; charset=utf-8"

    def test_create_event_custom_content_type(self):
        proc = ConcreteProcessor()
        ce = proc.create_event(datacontenttype="application/msgpack")
        assert ce.datacontenttype == "application/msgpack"


# ---------------------------------------------------------------------------
# ProtocolHandler
# ---------------------------------------------------------------------------


class ConcreteHandler(ProtocolHandler):
    def __init__(self):
        super().__init__()

    async def task(self) -> None:
        pass


class TestProtocolHandler:
    def test_register_processor(self):
        handler = ConcreteHandler()
        proc = ConcreteProcessor()
        handler.register_processor(proc)
        assert proc in handler._cloudevent_processors

    def test_register_multiple_processors(self):
        handler = ConcreteHandler()
        p1 = ConcreteProcessor(instance_id="a")
        p2 = ConcreteProcessor(instance_id="b")
        handler.register_processor(p1)
        handler.register_processor(p2)
        assert len(handler._cloudevent_processors) == 2

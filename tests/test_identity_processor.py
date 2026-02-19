from unittest.mock import AsyncMock, patch

import pytest

from app import ProcessingConfig
from app.common import CloudEvent
from app.identity_processor import (
    IdentityCloudEventDelegate,
    IdentityMessagePackCloudEventProcessor,
    IdentityMQTTCloudEventProcessor,
)
from app.models.greetings import Bye, Hello, HiddenObject

# ===================================================================
# Helpers
# ===================================================================


def _processing_config() -> ProcessingConfig:
    """Return a ProcessingConfig with topics that match 'identity:…'."""
    cfg = ProcessingConfig()
    cfg.topics = {
        "identity:app/events/#",
        "identity:app/invoke/#",
    }
    cfg.response_topics = {
        "identity:app/errors/delivery",
    }
    return cfg


def _make_mqtt_processor() -> IdentityMQTTCloudEventProcessor:
    proc = IdentityMQTTCloudEventProcessor("test-id", _processing_config())
    # Reset shared class-level dicts to isolate tests
    proc._type_classes = dict(proc._type_classes)
    proc._type_callbacks_in = dict(proc._type_callbacks_in)
    proc._type_callbacks_out = dict(proc._type_callbacks_out)
    proc._hidden_field_processors = dict(proc._hidden_field_processors)
    return proc


def _make_msgpack_processor() -> IdentityMessagePackCloudEventProcessor:
    proc = IdentityMessagePackCloudEventProcessor("test-id", _processing_config())
    proc._type_classes = dict(proc._type_classes)
    proc._type_callbacks_in = dict(proc._type_callbacks_in)
    proc._type_callbacks_out = dict(proc._type_callbacks_out)
    proc._hidden_field_processors = dict(proc._hidden_field_processors)
    return proc


HELLO_CE_TYPE = "com.github.aschamberger.microdcs.identity.hello.v1"
BYE_CE_TYPE = "com.github.aschamberger.microdcs.identity.bye.v1"


# ===================================================================
# Data classes
# ===================================================================


class TestHello:
    def test_defaults(self):
        h = Hello(name="World")
        assert h.name == "World"
        assert h._hidden_str is None
        assert h._hidden_obj is None

    def test_with_hidden_fields(self):
        obj = HiddenObject(field="val")
        h = Hello(name="Test", _hidden_str="sec", _hidden_obj=obj)
        assert h._hidden_str == "sec"
        assert h._hidden_obj is obj

    def test_validation_min_length(self):
        with pytest.raises(ValueError):
            Hello(name="AB")

    def test_validation_max_length(self):
        with pytest.raises(ValueError):
            Hello(name="A" * 21)

    def test_config_cloudevent_type(self):
        assert Hello.Config.cloudevent_type == HELLO_CE_TYPE

    def test_config_aliases(self):
        assert Hello.Config.aliases == {"name": "Name"}


class TestBye:
    def test_defaults(self):
        b = Bye(name="Bob")
        assert b.name == "Bob"

    def test_validation_min_length(self):
        with pytest.raises(ValueError):
            Bye(name="AB")

    def test_config_cloudevent_type(self):
        assert Bye.Config.cloudevent_type == BYE_CE_TYPE


class TestHiddenObject:
    def test_round_trip(self):
        obj = HiddenObject(field="value")
        json_str = obj.to_json()
        restored = HiddenObject.from_json(json_str)
        assert restored.field == "value"


# ===================================================================
# IdentityCloudEventDelegate
# ===================================================================


class TestIdentityCloudEventDelegate:
    @pytest.mark.asyncio
    async def test_handle_hello(self):
        hello = Hello(name="World")
        results = await IdentityCloudEventDelegate.handle_hello(hello)
        assert isinstance(results, list)
        assert len(results) == 2
        assert results[0].name == "World"
        assert results[1].name == "Alice"

    @pytest.mark.asyncio
    async def test_handle_hello_preserves_hidden_fields(self):
        obj = HiddenObject(field="x")
        hello = Hello(name="Test", _hidden_str="s", _hidden_obj=obj)
        results = await IdentityCloudEventDelegate.handle_hello(hello)
        assert isinstance(results, list)
        assert results[0]._hidden_str == "s"
        assert results[0]._hidden_obj is obj

    @pytest.mark.asyncio
    async def test_handle_bye(self):
        results = await IdentityCloudEventDelegate.handle_bye()
        assert isinstance(results, list)
        assert len(results) == 2
        assert results[0].name == "Bob"
        assert results[1].name == "Alice"

    # --- extract_hidden_fields ---

    def test_extract_hidden_str(self):
        hello = Hello(name="Test")
        meta = {"x-hidden-str": "secret"}
        IdentityCloudEventDelegate.extract_hidden_fields(hello, meta)
        assert hello._hidden_str == "secret"

    def test_extract_hidden_obj(self):
        hello = Hello(name="Test")
        obj = HiddenObject(field="val")
        meta = {"x-hidden-obj": obj.to_json()}
        IdentityCloudEventDelegate.extract_hidden_fields(hello, meta)
        assert isinstance(hello._hidden_obj, HiddenObject)
        assert hello._hidden_obj.field == "val"

    def test_extract_hidden_none_values(self):
        hello = Hello(name="Test")
        IdentityCloudEventDelegate.extract_hidden_fields(hello, {})
        assert hello._hidden_str is None
        assert hello._hidden_obj is None

    # --- insert_hidden_fields ---

    def test_insert_hidden_str(self):
        hello = Hello(name="Test", _hidden_str="sec")
        meta: dict[str, str] = {}
        IdentityCloudEventDelegate.insert_hidden_fields(hello, meta)
        assert meta["x-hidden-str"] == "sec"

    def test_insert_hidden_obj(self):
        obj = HiddenObject(field="v")
        hello = Hello(name="Test", _hidden_obj=obj)
        meta: dict[str, str] = {}
        IdentityCloudEventDelegate.insert_hidden_fields(hello, meta)
        assert "x-hidden-obj" in meta

    def test_insert_hidden_none_values(self):
        hello = Hello(name="Test")
        meta: dict[str, str] = {}
        IdentityCloudEventDelegate.insert_hidden_fields(hello, meta)
        assert "x-hidden-str" not in meta
        assert "x-hidden-obj" not in meta

    def test_insert_hidden_fields_none_custommetadata(self):
        """When custommetadata is None the method creates a local dict (no crash)."""
        hello = Hello(name="Test", _hidden_str="s")
        IdentityCloudEventDelegate.insert_hidden_fields(hello, None)  # type: ignore[arg-type]


# ===================================================================
# IdentityMQTTCloudEventProcessor
# ===================================================================


class TestIdentityMQTTCloudEventProcessor:
    def test_init_registers_callbacks(self):
        proc = _make_mqtt_processor()
        assert HELLO_CE_TYPE in proc._type_callbacks_in
        assert BYE_CE_TYPE in proc._type_callbacks_out
        # hidden field processor registered with wildcard
        assert any("identity" in k for k in proc._hidden_field_processors)

    def test_init_parses_topics(self):
        proc = _make_mqtt_processor()
        assert len(proc._topics) > 0
        assert hasattr(proc, "_response_topic")

    # --- process_event ---

    @pytest.mark.asyncio
    async def test_process_event_no_transport_metadata(self):
        proc = _make_mqtt_processor()
        ce = CloudEvent(type=HELLO_CE_TYPE, transportmetadata=None)
        result = await proc.process_event(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_process_event_no_topic_in_metadata(self):
        proc = _make_mqtt_processor()
        ce = CloudEvent(type=HELLO_CE_TYPE, transportmetadata={})
        result = await proc.process_event(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_process_event_raw_no_response_topic_metadata_none(self):
        """Unknown type, transportmetadata is None → cannot echo."""
        proc = _make_mqtt_processor()
        ce = CloudEvent(
            type="com.unknown",
            data=b"raw",
            transportmetadata=None,
        )
        # process_event checks transport metadata first → None → returns None
        result = await proc.process_event(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_process_event_raw_no_response_topic(self):
        """Unknown type with topic but no response_topic → warning, returns None."""
        proc = _make_mqtt_processor()
        ce = CloudEvent(
            type="com.unknown",
            data=b"raw",
            transportmetadata={"mqtt_topic": "some/topic"},
        )
        result = await proc.process_event(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_process_event_raw_echo(self):
        """Unknown type with response_topic → echoed as raw identity response."""
        proc = _make_mqtt_processor()
        ce = CloudEvent(
            type="com.unknown",
            data=b"raw-data",
            datacontenttype="application/octet-stream",
            correlationid="corr-1",
            transportmetadata={
                "mqtt_topic": "some/topic",
                "mqtt_response_topic": "resp/topic",
            },
        )
        result = await proc.process_event(ce)
        assert isinstance(result, CloudEvent)
        assert result.data == b"raw-data"
        assert result.type == "com.github.aschamberger.microdcs.identity.raw.v1"
        assert result.datacontenttype == "application/octet-stream"
        assert result.correlationid == "corr-1"
        assert result.causationid == ce.id
        assert result.transportmetadata is not None
        assert result.transportmetadata["mqtt_topic"] == "resp/topic"

    @pytest.mark.asyncio
    async def test_process_event_with_callback(self):
        """Known type triggers message_callback."""
        proc = _make_mqtt_processor()
        hello = Hello(name="World")
        ce = CloudEvent(
            type=HELLO_CE_TYPE,
            data=hello.to_jsonb(),
            datacontenttype="application/json; charset=utf-8",
            transportmetadata={
                "mqtt_topic": "app/events/identity",
                "mqtt_response_topic": "app/errors/delivery/test-id",
            },
        )
        result = await proc.process_event(ce)
        # handle_hello returns a list of 2 CloudEvents
        assert isinstance(result, list)
        assert len(result) == 2

    # --- process_response_event ---

    @pytest.mark.asyncio
    async def test_process_response_event_no_transport_metadata(self):
        proc = _make_mqtt_processor()
        ce = CloudEvent(transportmetadata=None)
        result = await proc.process_response_event(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_process_response_event_no_topic(self):
        proc = _make_mqtt_processor()
        ce = CloudEvent(transportmetadata={})
        result = await proc.process_response_event(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_process_response_event_returns_none(self):
        proc = _make_mqtt_processor()
        ce = CloudEvent(
            transportmetadata={"mqtt_topic": "some/topic"},
        )
        result = await proc.process_response_event(ce)
        assert result is None

    # --- send_event ---

    @pytest.mark.asyncio
    async def test_send_event(self):
        proc = _make_mqtt_processor()
        with patch.object(
            proc, "type_callback", new_callable=AsyncMock
        ) as mock_type_cb:
            with patch("app.identity_processor.asyncio.sleep", new_callable=AsyncMock):
                await proc.send_event()
            mock_type_cb.assert_awaited_once_with(Bye, "app/identity/bye")

    # --- handle_expiration ---

    @pytest.mark.asyncio
    async def test_handle_expiration(self):
        proc = _make_mqtt_processor()
        ce = CloudEvent()
        with patch("app.identity_processor.asyncio.sleep", new_callable=AsyncMock):
            result = await proc.handle_expiration(ce, 10)
        assert result is None


# ===================================================================
# IdentityMessagePackCloudEventProcessor
# ===================================================================


class TestIdentityMessagePackCloudEventProcessor:
    def test_init_registers_callbacks(self):
        proc = _make_msgpack_processor()
        assert HELLO_CE_TYPE in proc._type_callbacks_in
        assert BYE_CE_TYPE in proc._type_callbacks_out
        assert any("identity" in k for k in proc._hidden_field_processors)

    # --- process_event ---

    @pytest.mark.asyncio
    async def test_process_event_raw_echo(self):
        """Unknown type → echoed as raw identity response."""
        proc = _make_msgpack_processor()
        ce = CloudEvent(
            type="com.unknown",
            data=b"raw-data",
            datacontenttype="application/octet-stream",
            correlationid="corr-1",
        )
        result = await proc.process_event(ce)
        assert isinstance(result, CloudEvent)
        assert result.data == b"raw-data"
        assert result.type == "com.github.aschamberger.microdcs.identity.raw.v1"
        assert result.correlationid == "corr-1"
        assert result.causationid == ce.id

    @pytest.mark.asyncio
    async def test_process_event_with_callback(self):
        """Known type triggers message_callback."""
        proc = _make_msgpack_processor()
        hello = Hello(name="World")
        ce = CloudEvent(
            type=HELLO_CE_TYPE,
            data=hello.to_jsonb(),
            datacontenttype="application/json; charset=utf-8",
        )
        result = await proc.process_event(ce)
        assert isinstance(result, list)
        assert len(result) == 2

    # --- NotImplementedError methods ---

    @pytest.mark.asyncio
    async def test_process_response_event_raises(self):
        proc = _make_msgpack_processor()
        with pytest.raises(NotImplementedError):
            await proc.process_response_event(CloudEvent())

    @pytest.mark.asyncio
    async def test_send_event_raises(self):
        proc = _make_msgpack_processor()
        with pytest.raises(NotImplementedError):
            await proc.send_event()

    @pytest.mark.asyncio
    async def test_handle_expiration_raises(self):
        proc = _make_msgpack_processor()
        with pytest.raises(NotImplementedError):
            await proc.handle_expiration(CloudEvent(), 10)

from unittest.mock import AsyncMock, patch

import pytest

from app import ProcessingConfig
from app.common import CloudEvent, MessageIntent
from app.models.greetings import Bye, Hello, HiddenObject
from app.processors.greetings import GreetingsCloudEventProcessor

# ===================================================================
# Helpers
# ===================================================================


def _processing_config() -> ProcessingConfig:
    """Return a ProcessingConfig suitable for greetings processor."""
    return ProcessingConfig()


def _make_processor() -> GreetingsCloudEventProcessor:
    return GreetingsCloudEventProcessor("test-id", _processing_config())


HELLO_CE_TYPE = "com.github.aschamberger.microdcs.greetings.hello.v1"
BYE_CE_TYPE = "com.github.aschamberger.microdcs.greetings.bye.v1"


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
# GreetingsCloudEventProcessor — handler methods
# ===================================================================


class TestGreetingsCloudEventProcessorHandlers:
    @pytest.mark.asyncio
    async def test_handle_hello(self):
        proc = _make_processor()
        hello = Hello(name="World")
        results = await proc.handle_hello(hello)
        assert isinstance(results, list)
        assert len(results) == 2
        assert results[0].name == "World"
        assert results[1].name == "Alice"

    @pytest.mark.asyncio
    async def test_handle_hello_preserves_hidden_fields(self):
        proc = _make_processor()
        obj = HiddenObject(field="x")
        hello = Hello(name="Test", _hidden_str="s", _hidden_obj=obj)
        results = await proc.handle_hello(hello)
        assert isinstance(results, list)
        assert results[0]._hidden_str == "s"
        assert results[0]._hidden_obj is obj

    @pytest.mark.asyncio
    async def test_handle_bye(self):
        proc = _make_processor()
        results = await proc.handle_bye()
        assert isinstance(results, list)
        assert len(results) == 2
        assert results[0].name == "Bob"
        assert results[1].name == "Alice"

    # --- hidden fields via __custom_metadata__ / __get_custom_metadata__ ---

    def test_extract_hidden_str(self):
        """Hidden str is populated from __custom_metadata__ during construction."""
        hello = Hello(name="Test", __custom_metadata__={"x-hidden-str": "secret"})
        assert hello._hidden_str == "secret"

    def test_extract_hidden_obj(self):
        """Hidden obj is populated from __custom_metadata__ during construction."""
        obj = HiddenObject(field="val")
        hello = Hello(name="Test", __custom_metadata__={"x-hidden-obj": obj.to_json()})
        assert isinstance(hello._hidden_obj, HiddenObject)
        assert hello._hidden_obj.field == "val"

    def test_extract_hidden_none_values(self):
        hello = Hello(name="Test", __custom_metadata__={})
        assert hello._hidden_str is None
        assert hello._hidden_obj is None

    # --- __get_custom_metadata__ (insert hidden fields) ---

    def test_insert_hidden_str(self):
        hello = Hello(name="Test", _hidden_str="sec")
        meta = hello.__get_custom_metadata__()
        assert meta["x-hidden-str"] == "sec"

    def test_insert_hidden_obj(self):
        obj = HiddenObject(field="v")
        hello = Hello(name="Test", _hidden_obj=obj)
        meta = hello.__get_custom_metadata__()
        assert "x-hidden-obj" in meta

    def test_insert_hidden_none_values(self):
        hello = Hello(name="Test")
        meta = hello.__get_custom_metadata__()
        assert "x-hidden-str" not in meta
        assert "x-hidden-obj" not in meta

    def test_insert_hidden_fields_none_custommetadata(self):
        """__get_custom_metadata__ returns empty dict when no hidden fields are set."""
        hello = Hello(name="Test")
        meta = hello.__get_custom_metadata__()
        assert isinstance(meta, dict)


# ===================================================================
# GreetingsCloudEventProcessor — process_event / response / send
# ===================================================================


class TestGreetingsCloudEventProcessor:
    def test_init_registers_callbacks(self):
        proc = _make_processor()
        assert HELLO_CE_TYPE in proc._type_callbacks_in
        assert BYE_CE_TYPE in proc._type_callbacks_out

    # --- process_event ---

    @pytest.mark.asyncio
    async def test_process_event_raw_echo(self):
        """Unknown type → echoed as raw greetings response."""
        proc = _make_processor()
        ce = CloudEvent(
            type="com.unknown",
            data=b"raw-data",
            datacontenttype="application/octet-stream",
            correlationid="corr-1",
        )
        result = await proc.process_event(ce)
        assert isinstance(result, CloudEvent)
        assert result.data == b"raw-data"
        assert result.type == "com.github.aschamberger.microdcs.greetings.raw.v1"
        assert result.datacontenttype == "application/octet-stream"
        assert result.correlationid == "corr-1"
        assert result.causationid == ce.id

    @pytest.mark.asyncio
    async def test_process_event_with_callback(self):
        """Known type triggers message_callback."""
        proc = _make_processor()
        hello = Hello(name="World")
        ce = CloudEvent(
            type=HELLO_CE_TYPE,
            data=hello.to_jsonb(),
            datacontenttype="application/json; charset=utf-8",
        )
        result = await proc.process_event(ce)
        # handle_hello returns a list of 2 CloudEvents
        assert isinstance(result, list)
        assert len(result) == 2

    # --- process_response_event ---

    @pytest.mark.asyncio
    async def test_process_response_event_returns_none(self):
        proc = _make_processor()
        ce = CloudEvent()
        result = await proc.process_response_event(ce)
        assert result is None

    # --- send_event ---

    @pytest.mark.asyncio
    async def test_send_event(self):
        proc = _make_processor()
        with patch.object(
            proc, "callback_outgoing", new_callable=AsyncMock
        ) as mock_type_cb:
            with patch(
                "app.processors.greetings.asyncio.sleep", new_callable=AsyncMock
            ):
                await proc.send_event()
            mock_type_cb.assert_awaited_once_with(Bye, intent=MessageIntent.COMMAND)

    # --- handle_expiration ---

    @pytest.mark.asyncio
    async def test_handle_expiration(self):
        proc = _make_processor()
        ce = CloudEvent()
        with patch("app.processors.greetings.asyncio.sleep", new_callable=AsyncMock):
            result = await proc.handle_expiration(ce, 10)
        assert result is None

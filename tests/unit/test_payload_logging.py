"""Payload content must stay out of the logs unless APP_LOGGING_LOG_PAYLOADS is set."""

import logging
import os
from dataclasses import dataclass
from unittest.mock import patch

import pytest
from conftest import SamplePayload, make_concrete_processor

from microdcs import LoggingConfig, RuntimeConfig, _set_payload_logging, loggable
from microdcs.common import CloudEvent, Direction
from microdcs.dataclass import DataClassConfig, DataClassMixin

SECRET = "TOP-SECRET-VALUE"


@dataclass
class CountPayload(DataClassMixin):
    count: int = 0

    class Config(DataClassConfig):
        type_id: str = "com.test.count.v1"
        type_schema: str = "https://example.com/schemas/count-v1"


@pytest.fixture(autouse=True)
def reset_payload_logging():
    _set_payload_logging(False)
    yield
    _set_payload_logging(False)


def _event(type_id: str, data: bytes) -> CloudEvent:
    return CloudEvent(
        type=type_id, data=data, datacontenttype="application/json", source="s"
    )


async def _echo_handler(request, **kwargs):
    return SamplePayload(value=request.value)


class TestLoggable:
    def test_hides_content_by_default(self):
        assert SECRET not in str(loggable({"k": SECRET}))
        assert "content hidden" in loggable({"k": SECRET})

    def test_shows_content_when_enabled(self):
        _set_payload_logging(True)
        assert loggable({"k": SECRET}) == {"k": SECRET}

    def test_flag_set_from_logging_config(self):
        with patch.object(LoggingConfig, "set_logging_config"):
            LoggingConfig(log_payloads=True)
            assert loggable("x") == "x"
            LoggingConfig()
            assert loggable("x") != "x"

    def test_flag_set_from_environment(self):
        with (
            patch.dict(os.environ, {"APP_LOGGING_LOG_PAYLOADS": "true"}),
            patch.object(LoggingConfig, "set_logging_config"),
        ):
            RuntimeConfig()
        assert loggable("x") == "x"


class TestCloudEventRepr:
    def test_repr_excludes_data(self):
        assert SECRET not in repr(CloudEvent(data=SECRET.encode()))


class TestCallbackLogging:
    @pytest.mark.asyncio
    async def test_request_and_response_hidden_by_default(self, caplog):
        proc = make_concrete_processor()
        proc.register_callback(
            SamplePayload, _echo_handler, direction=Direction.INCOMING
        )
        event = _event("com.test.sample.v1", f'{{"value": "{SECRET}"}}'.encode())
        with caplog.at_level(logging.DEBUG, logger="app.common"):
            await proc.callback_incoming(event)
        assert "Request before callback" in caplog.text
        assert SECRET not in caplog.text

    @pytest.mark.asyncio
    async def test_request_logged_when_enabled(self, caplog):
        _set_payload_logging(True)
        proc = make_concrete_processor()
        proc.register_callback(
            SamplePayload, _echo_handler, direction=Direction.INCOMING
        )
        event = _event("com.test.sample.v1", f'{{"value": "{SECRET}"}}'.encode())
        with caplog.at_level(logging.DEBUG, logger="app.common"):
            await proc.callback_incoming(event)
        assert SECRET in caplog.text

    @pytest.mark.asyncio
    async def test_invalid_field_value_not_echoed_in_error(self, caplog):
        proc = make_concrete_processor()

        async def handler(request, **kwargs):
            return None

        proc.register_callback(CountPayload, handler, direction=Direction.INCOMING)
        event = _event("com.test.count.v1", f'{{"count": "{SECRET}"}}'.encode())
        with caplog.at_level(logging.ERROR, logger="app.common"):
            result = await proc.callback_incoming(event)
        assert result is None
        assert 'Field "count"' in caplog.text
        assert SECRET not in caplog.text

    @pytest.mark.asyncio
    async def test_invalid_field_value_echoed_when_enabled(self, caplog):
        _set_payload_logging(True)
        proc = make_concrete_processor()

        async def handler(request, **kwargs):
            return None

        proc.register_callback(CountPayload, handler, direction=Direction.INCOMING)
        event = _event("com.test.count.v1", f'{{"count": "{SECRET}"}}'.encode())
        with caplog.at_level(logging.ERROR, logger="app.common"):
            await proc.callback_incoming(event)
        assert SECRET in caplog.text

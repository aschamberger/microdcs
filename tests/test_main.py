import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.microdcs import MicroDCS


def _close_coroutine_arg(coro, *_args, **_kwargs):
    """Side-effect for create_task that closes the coroutine argument."""
    if asyncio.iscoroutine(coro):
        coro.close()


def _setup_task_group_mock(mock_tg_cls: MagicMock) -> AsyncMock:
    mock_tg = AsyncMock()
    mock_tg.create_task = MagicMock(side_effect=_close_coroutine_arg)
    mock_tg_cls.return_value.__aenter__ = AsyncMock(return_value=mock_tg)
    mock_tg_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    return mock_tg


def _setup_handler_mock(handler_cls: MagicMock) -> MagicMock:
    handler = MagicMock()
    handler.task = AsyncMock()
    handler_cls.return_value = handler
    return handler


def _create_microdcs(otel_enabled: bool) -> MicroDCS:
    """Create a MicroDCS instance with mocked configuration."""
    with patch.object(MicroDCS, "__init__", lambda self: None):
        dcs = MicroDCS()
    dcs.runtime_config = MagicMock()
    dcs.runtime_config.processing.otel_instrumentation_enabled = otel_enabled
    dcs.redis_connection_pool = AsyncMock()
    dcs.redis_key_schema = MagicMock()
    dcs._mqtt_processors = []
    dcs._msgpack_processors = []
    return dcs


class TestMain:
    """Tests for MicroDCS.main() coroutine."""

    @pytest.mark.asyncio
    async def test_main_runs_with_otel_disabled(self):
        """main() wires up handlers and starts tasks (OTEL off)."""
        dcs = _create_microdcs(otel_enabled=False)

        mock_mqtt_proc = MagicMock()
        mock_mp_proc = MagicMock()
        dcs._mqtt_processors.append((mock_mqtt_proc, "greetings"))
        dcs._msgpack_processors.append(mock_mp_proc)

        with (
            patch("app.microdcs.MQTTHandler") as mock_mqtt_cls,
            patch("app.microdcs.MessagePackHandler") as mock_mp_cls,
            patch("app.microdcs.SystemEventTaskGroup") as mock_tg_cls,
        ):
            mock_tg = _setup_task_group_mock(mock_tg_cls)
            mock_mqtt_handler = _setup_handler_mock(mock_mqtt_cls)
            mock_mp_handler = _setup_handler_mock(mock_mp_cls)

            await dcs.main()

            # Non-OTEL handlers were used
            mock_mqtt_cls.assert_called_once()
            mock_mp_cls.assert_called_once()

            # MQTT processor registered via register_mqtt_processor
            mock_mqtt_handler.register_mqtt_processor.assert_called_once_with(
                mock_mqtt_proc, "greetings", dcs.runtime_config.processing
            )
            # MessagePack processor registered via register_processor
            mock_mp_handler.register_processor.assert_called_once_with(mock_mp_proc)

            # Tasks created: mqtt_handler.task, msgpack_handler.task
            assert mock_tg.create_task.call_count == 2

            # Connection pool closed
            dcs.redis_connection_pool.aclose.assert_awaited_once()  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_main_runs_with_otel_enabled(self):
        """main() uses OTEL-instrumented handlers when flag is set."""
        dcs = _create_microdcs(otel_enabled=True)

        mock_mqtt_proc = MagicMock()
        mock_mp_proc = MagicMock()
        dcs._mqtt_processors.append((mock_mqtt_proc, "greetings"))
        dcs._msgpack_processors.append(mock_mp_proc)

        with (
            patch("app.microdcs.OTELInstrumentedMQTTHandler") as mock_otel_mqtt_cls,
            patch(
                "app.microdcs.OTELInstrumentedMessagePackHandler"
            ) as mock_otel_mp_cls,
            patch("app.microdcs.SystemEventTaskGroup") as mock_tg_cls,
        ):
            mock_tg = _setup_task_group_mock(mock_tg_cls)
            _setup_handler_mock(mock_otel_mqtt_cls)
            _setup_handler_mock(mock_otel_mp_cls)

            await dcs.main()

            # OTEL-instrumented handlers were used
            mock_otel_mqtt_cls.assert_called_once()
            mock_otel_mp_cls.assert_called_once()

            # Tasks created
            assert mock_tg.create_task.call_count == 2

            dcs.redis_connection_pool.aclose.assert_awaited_once()  # type: ignore[union-attr]

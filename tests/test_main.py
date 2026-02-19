import asyncio
import importlib
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _close_coroutine(coro, **_kwargs):
    """Close a coroutine so it doesn't trigger 'was never awaited' warnings."""
    coro.close()


def _close_coroutine_arg(coro, *_args, **_kwargs):
    """Side-effect for create_task that closes the coroutine argument."""
    if asyncio.iscoroutine(coro):
        coro.close()


def _import_main_module():
    """Import (or re-import) app.__main__ with asyncio.run patched out
    so the module-level asyncio.run() call is a no-op."""
    with patch(
        "asyncio.run", side_effect=_close_coroutine
    ):  # prevent real execution at import time
        if "app.__main__" in sys.modules:
            return importlib.reload(sys.modules["app.__main__"])
        return importlib.import_module("app.__main__")


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


def _setup_processor_mock(
    proc_cls: MagicMock, *, has_send_event: bool = False
) -> MagicMock:
    proc = MagicMock()
    if has_send_event:
        proc.send_event = AsyncMock()
    proc_cls.return_value = proc
    return proc


class TestMain:
    """Tests for the main() coroutine in app.__main__."""

    @pytest.mark.asyncio
    async def test_main_runs_with_otel_disabled(self):
        """main() wires up handlers and starts tasks (OTEL off)."""
        mod = _import_main_module()

        with (
            patch.dict(
                "os.environ",
                {"APP_PROCESSING_OTEL_INSTRUMENTATION_ENABLED": "false"},
                clear=False,
            ),
            patch.object(mod, "MQTTHandler") as mock_mqtt_cls,
            patch.object(mod, "MessagePackHandler") as mock_mp_cls,
            patch.object(mod, "GreetingsMQTTCloudEventProcessor") as mock_mqtt_proc_cls,
            patch.object(
                mod, "GreetingsMessagePackCloudEventProcessor"
            ) as mock_mp_proc_cls,
            patch.object(mod, "redis") as mock_redis_mod,
            patch.object(mod, "SystemEventTaskGroup") as mock_tg_cls,
        ):
            mock_tg = _setup_task_group_mock(mock_tg_cls)
            mock_mqtt_handler = _setup_handler_mock(mock_mqtt_cls)
            mock_mp_handler = _setup_handler_mock(mock_mp_cls)
            mock_mqtt_proc = _setup_processor_mock(
                mock_mqtt_proc_cls, has_send_event=True
            )
            mock_mp_proc = _setup_processor_mock(mock_mp_proc_cls)

            mock_pool = AsyncMock()
            mock_redis_mod.ConnectionPool.return_value = mock_pool

            await mod.main()

            # Non-OTEL handlers were used
            mock_mqtt_cls.assert_called_once()
            mock_mp_cls.assert_called_once()

            # Processors registered
            mock_mqtt_handler.register_processor.assert_called_once_with(mock_mqtt_proc)
            mock_mp_handler.register_processor.assert_called_once_with(mock_mp_proc)

            # Tasks created: mqtt_handler.task, msgpack_handler.task, mqtt_ip.send_event
            assert mock_tg.create_task.call_count == 3

            # Connection pool closed
            mock_pool.aclose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_main_runs_with_otel_enabled(self):
        """main() uses OTEL-instrumented handlers when flag is set."""
        mod = _import_main_module()

        with (
            patch.dict(
                "os.environ",
                {"APP_PROCESSING_OTEL_INSTRUMENTATION_ENABLED": "true"},
                clear=False,
            ),
            patch.object(mod, "OTELInstrumentedMQTTHandler") as mock_otel_mqtt_cls,
            patch.object(mod, "OTELInstrumentedMessagePackHandler") as mock_otel_mp_cls,
            patch.object(mod, "GreetingsMQTTCloudEventProcessor") as mock_mqtt_proc_cls,
            patch.object(
                mod, "GreetingsMessagePackCloudEventProcessor"
            ) as mock_mp_proc_cls,
            patch.object(mod, "redis") as mock_redis_mod,
            patch.object(mod, "SystemEventTaskGroup") as mock_tg_cls,
        ):
            mock_tg = _setup_task_group_mock(mock_tg_cls)
            _setup_handler_mock(mock_otel_mqtt_cls)
            _setup_handler_mock(mock_otel_mp_cls)
            _setup_processor_mock(mock_mqtt_proc_cls, has_send_event=True)
            _setup_processor_mock(mock_mp_proc_cls)

            mock_pool = AsyncMock()
            mock_redis_mod.ConnectionPool.return_value = mock_pool

            await mod.main()

            # OTEL-instrumented handlers were used
            mock_otel_mqtt_cls.assert_called_once()
            mock_otel_mp_cls.assert_called_once()

            # Tasks created
            assert mock_tg.create_task.call_count == 3

            mock_pool.aclose.assert_awaited_once()

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core import MicroDCS


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


def _create_microdcs(otel_enabled: bool) -> MicroDCS:
    """Create a MicroDCS instance with mocked configuration."""
    with patch.object(MicroDCS, "__init__", lambda self: None):
        dcs = MicroDCS()
    dcs.runtime_config = MagicMock()
    dcs.runtime_config.processing.otel_instrumentation_enabled = otel_enabled
    dcs.redis_connection_pool = AsyncMock()
    dcs.redis_key_schema = MagicMock()
    dcs._protocol_handlers = {}
    dcs._handler_bindings = {}
    dcs._processors = set()
    dcs._additional_tasks = set()
    return dcs


def _register_mock_handler_and_binding(dcs: MicroDCS):
    """Register a mock handler pair and binding on the MicroDCS instance."""
    mock_handler = MagicMock()
    mock_handler.task = AsyncMock()
    mock_handler.register_binding = MagicMock()

    mock_otel_handler = MagicMock()
    mock_otel_handler.task = AsyncMock()
    mock_otel_handler.register_binding = MagicMock()

    handler_cls = type(mock_handler)
    dcs._protocol_handlers[handler_cls] = (mock_handler, mock_otel_handler)

    mock_proc = MagicMock()
    mock_proc.initialize = AsyncMock()
    mock_proc.post_start = AsyncMock()

    mock_binding = MagicMock()
    mock_binding.processor = mock_proc

    dcs._handler_bindings[handler_cls] = {mock_binding}
    dcs._processors = {mock_proc}

    return mock_handler, mock_otel_handler, mock_binding, mock_proc


class TestMain:
    """Tests for MicroDCS.main() coroutine."""

    @pytest.mark.asyncio
    async def test_main_runs_with_otel_disabled(self):
        """main() uses non-OTEL handler when flag is off."""
        dcs = _create_microdcs(otel_enabled=False)
        mock_handler, mock_otel_handler, mock_binding, mock_proc = (
            _register_mock_handler_and_binding(dcs)
        )

        with patch("app.core.SystemEventTaskGroup") as mock_tg_cls:
            mock_tg = _setup_task_group_mock(mock_tg_cls)

            await dcs.main()

            # Non-OTEL handler was used
            mock_handler.register_binding.assert_called_once_with(mock_binding)
            mock_otel_handler.register_binding.assert_not_called()

            # Task created for handler
            assert mock_tg.create_task.call_count >= 1

            # Processor lifecycle
            mock_proc.initialize.assert_awaited_once()
            mock_proc.post_start.assert_awaited_once()

            # Connection pool closed
            dcs.redis_connection_pool.aclose.assert_awaited_once()  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_main_runs_with_otel_enabled(self):
        """main() uses OTEL-instrumented handler when flag is set."""
        dcs = _create_microdcs(otel_enabled=True)
        mock_handler, mock_otel_handler, mock_binding, mock_proc = (
            _register_mock_handler_and_binding(dcs)
        )

        with patch("app.core.SystemEventTaskGroup") as mock_tg_cls:
            mock_tg = _setup_task_group_mock(mock_tg_cls)

            await dcs.main()

            # OTEL handler was used
            mock_otel_handler.register_binding.assert_called_once_with(mock_binding)
            mock_handler.register_binding.assert_not_called()

            # Task created
            assert mock_tg.create_task.call_count >= 1

            dcs.redis_connection_pool.aclose.assert_awaited_once()  # type: ignore[union-attr]

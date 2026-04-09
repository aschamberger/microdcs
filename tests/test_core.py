import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis.asyncio as redis

from microdcs import RedisConfig
from microdcs.core import MicroDCS


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
    dcs.runtime_config.validate = AsyncMock()
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
    mock_proc.shutdown = AsyncMock()

    mock_binding = MagicMock()
    mock_binding.processor = mock_proc

    dcs._handler_bindings[handler_cls] = {mock_binding}
    dcs._processors = {mock_proc}

    return mock_handler, mock_otel_handler, mock_binding, mock_proc


def _create_mock_additional_task() -> MagicMock:
    """Create a mock AdditionalTask instance."""
    mock_task = MagicMock()
    mock_task.task = AsyncMock()
    mock_task.register_shutdown_event = MagicMock()
    return mock_task


class TestMain:
    """Tests for MicroDCS.main() coroutine."""

    @pytest.mark.asyncio
    async def test_main_runs_with_otel_disabled(self):
        """main() uses non-OTEL handler when flag is off."""
        dcs = _create_microdcs(otel_enabled=False)
        mock_handler, mock_otel_handler, mock_binding, mock_proc = (
            _register_mock_handler_and_binding(dcs)
        )

        with patch("microdcs.core.SystemEventTaskGroup") as mock_tg_cls:
            mock_tg = _setup_task_group_mock(mock_tg_cls)

            await dcs.main()

            dcs.runtime_config.validate.assert_awaited_once()  # type: ignore[union-attr]

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

        with patch("microdcs.core.SystemEventTaskGroup") as mock_tg_cls:
            mock_tg = _setup_task_group_mock(mock_tg_cls)

            await dcs.main()

            dcs.runtime_config.validate.assert_awaited_once()  # type: ignore[union-attr]

            # OTEL handler was used
            mock_otel_handler.register_binding.assert_called_once_with(mock_binding)
            mock_handler.register_binding.assert_not_called()

            # Task created
            assert mock_tg.create_task.call_count >= 1

            dcs.redis_connection_pool.aclose.assert_awaited_once()  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_main_runs_additional_tasks_with_shutdown_event(self):
        """main() registers shutdown event and creates task for additional tasks."""
        dcs = _create_microdcs(otel_enabled=False)
        _register_mock_handler_and_binding(dcs)
        mock_task = _create_mock_additional_task()
        dcs._additional_tasks.add(mock_task)

        with patch("microdcs.core.SystemEventTaskGroup") as mock_tg_cls:
            mock_tg = _setup_task_group_mock(mock_tg_cls)

            await dcs.main()

            mock_task.register_shutdown_event.assert_called_once_with(
                mock_tg.shutdown_event
            )
            assert mock_tg.create_task.call_count >= 2  # handler + additional task


class TestStartupFailureRecovery:
    """Tests for failures during MicroDCS startup."""

    @pytest.mark.asyncio
    async def test_main_raises_when_validate_fails(self):
        """main() propagates RuntimeConfig.validate() errors."""
        dcs = _create_microdcs(otel_enabled=False)
        _register_mock_handler_and_binding(dcs)
        dcs.runtime_config.validate = AsyncMock(side_effect=ValueError("bad config"))

        with pytest.raises(ValueError, match="bad config"):
            await dcs.main()

        # Redis pool should NOT be closed — we never reached that point
        dcs.redis_connection_pool.aclose.assert_not_awaited()  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_main_raises_when_processor_initialize_fails(self):
        """main() propagates processor.initialize() errors before task group starts."""
        dcs = _create_microdcs(otel_enabled=False)
        _, _, _, mock_proc = _register_mock_handler_and_binding(dcs)
        mock_proc.initialize = AsyncMock(side_effect=RuntimeError("init boom"))

        with pytest.raises(RuntimeError, match="init boom"):
            await dcs.main()

        dcs.redis_connection_pool.aclose.assert_not_awaited()  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_main_raises_when_no_bindings_for_handler(self):
        """main() raises ValueError when handler has no registered bindings."""
        dcs = _create_microdcs(otel_enabled=False)
        mock_handler = MagicMock()
        mock_handler.task = AsyncMock()
        mock_otel_handler = MagicMock()
        mock_otel_handler.task = AsyncMock()
        handler_cls = type(mock_handler)
        dcs._protocol_handlers[handler_cls] = (mock_handler, mock_otel_handler)
        # Intentionally no bindings registered for this handler

        with patch("microdcs.core.SystemEventTaskGroup") as mock_tg_cls:
            _setup_task_group_mock(mock_tg_cls)

            with pytest.raises(ValueError, match="No ProtocolBinding found"):
                await dcs.main()


class TestRegistration:
    """Tests for handler and binding registration."""

    def test_register_binding_before_handler_raises(self):
        """register_protocol_binding() raises if handler not yet registered."""
        dcs = _create_microdcs(otel_enabled=False)
        mock_binding = MagicMock()
        mock_binding.get_protocol_handler.return_value = type(MagicMock())

        with pytest.raises(ValueError, match="not registered"):
            dcs.register_protocol_binding(mock_binding)

    def test_register_protocol_handler_stores_handler_pair(self):
        """register_protocol_handler() stores handler and instrumented handler."""
        dcs = _create_microdcs(otel_enabled=False)
        mock_handler = MagicMock()
        mock_otel_handler = MagicMock()

        dcs.register_protocol_handler(mock_handler, mock_otel_handler)

        handler_cls = type(mock_handler)
        assert handler_cls in dcs._protocol_handlers
        assert dcs._protocol_handlers[handler_cls] == (mock_handler, mock_otel_handler)

    def test_register_binding_adds_processor(self):
        """register_protocol_binding() adds the binding's processor to the set."""
        dcs = _create_microdcs(otel_enabled=False)
        mock_handler = MagicMock()
        mock_otel_handler = MagicMock()
        dcs.register_protocol_handler(mock_handler, mock_otel_handler)

        mock_proc = MagicMock()
        mock_binding = MagicMock()
        mock_binding.get_protocol_handler.return_value = type(mock_handler)
        mock_binding.processor = mock_proc

        dcs.register_protocol_binding(mock_binding)

        assert mock_proc in dcs._processors
        assert mock_binding in dcs._handler_bindings[type(mock_handler)]

    def test_add_additional_task(self):
        """add_additional_task() adds the task to the set."""
        dcs = _create_microdcs(otel_enabled=False)
        mock_task = _create_mock_additional_task()

        dcs.add_additional_task(mock_task)

        assert mock_task in dcs._additional_tasks


class TestGracefulShutdown:
    """Tests for graceful shutdown and cleanup."""

    @pytest.mark.asyncio
    async def test_redis_pool_closed_after_task_group_exits(self):
        """Redis connection pool is closed even after normal task group exit."""
        dcs = _create_microdcs(otel_enabled=False)
        _register_mock_handler_and_binding(dcs)

        with patch("microdcs.core.SystemEventTaskGroup") as mock_tg_cls:
            _setup_task_group_mock(mock_tg_cls)
            await dcs.main()

        dcs.redis_connection_pool.aclose.assert_awaited_once()  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_redis_pool_not_closed_when_task_group_raises(self):
        """Redis pool cleanup is skipped if the task group raises."""
        dcs = _create_microdcs(otel_enabled=False)
        _register_mock_handler_and_binding(dcs)

        with patch("microdcs.core.SystemEventTaskGroup") as mock_tg_cls:
            mock_tg_cls.return_value.__aenter__ = AsyncMock(
                return_value=MagicMock(
                    create_task=MagicMock(side_effect=_close_coroutine_arg),
                    shutdown_event=MagicMock(),
                )
            )
            mock_tg_cls.return_value.__aexit__ = AsyncMock(
                side_effect=ExceptionGroup("boom", [RuntimeError("handler died")])
            )

            with pytest.raises(ExceptionGroup):
                await dcs.main()

        # Redis pool won't be closed because exception propagated from task group
        dcs.redis_connection_pool.aclose.assert_not_awaited()  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_processor_post_start_called_after_tasks_created(self):
        """Processor post_start() is called after all tasks are created."""
        dcs = _create_microdcs(otel_enabled=False)
        _, _, _, mock_proc = _register_mock_handler_and_binding(dcs)

        call_order: list[str] = []
        mock_proc.initialize = AsyncMock(
            side_effect=lambda: call_order.append("initialize")
        )
        mock_proc.post_start = AsyncMock(
            side_effect=lambda: call_order.append("post_start")
        )

        with patch("microdcs.core.SystemEventTaskGroup") as mock_tg_cls:
            mock_tg = _setup_task_group_mock(mock_tg_cls)
            mock_tg.create_task = MagicMock(
                side_effect=lambda coro: (
                    call_order.append("create_task"),
                    _close_coroutine_arg(coro),
                )
            )
            await dcs.main()

        assert call_order == ["initialize", "create_task", "post_start"]

    @pytest.mark.asyncio
    async def test_processor_shutdown_called_after_task_group_exits(self):
        """Processor shutdown() is called after the task group exits."""
        dcs = _create_microdcs(otel_enabled=False)
        _, _, _, mock_proc = _register_mock_handler_and_binding(dcs)

        with patch("microdcs.core.SystemEventTaskGroup") as mock_tg_cls:
            _setup_task_group_mock(mock_tg_cls)
            await dcs.main()

        mock_proc.shutdown.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_processor_shutdown_called_before_redis_close(self):
        """Processor shutdown() runs before Redis connection pool is closed."""
        dcs = _create_microdcs(otel_enabled=False)
        _, _, _, mock_proc = _register_mock_handler_and_binding(dcs)

        call_order: list[str] = []
        mock_proc.shutdown = AsyncMock(
            side_effect=lambda: call_order.append("shutdown")
        )
        dcs.redis_connection_pool.aclose = AsyncMock(
            side_effect=lambda: call_order.append("redis_close")
        )

        with patch("microdcs.core.SystemEventTaskGroup") as mock_tg_cls:
            _setup_task_group_mock(mock_tg_cls)
            await dcs.main()

        assert call_order == ["shutdown", "redis_close"]


class TestHandlerCrashPropagation:
    """Tests for handler crash behaviour in the task group."""

    @pytest.mark.asyncio
    async def test_handler_exception_propagates_as_exception_group(self):
        """A real SystemEventTaskGroup propagates handler exceptions."""
        dcs = _create_microdcs(otel_enabled=False)
        _, _, _, mock_proc = _register_mock_handler_and_binding(dcs)

        # Use a real task group but make the handler task fail immediately
        async def failing_handler():
            raise RuntimeError("handler crashed")

        handler, otel_handler, binding, _ = _register_mock_handler_and_binding(dcs)
        handler.task = failing_handler
        # Replace first handler entry with this one
        handler_cls = type(handler)
        dcs._protocol_handlers = {handler_cls: (handler, otel_handler)}
        dcs._handler_bindings = {handler_cls: {binding}}

        with pytest.raises(ExceptionGroup) as exc_info:
            await dcs.main()

        # Unwrap to find the RuntimeError
        assert any(
            isinstance(e, RuntimeError) and "handler crashed" in str(e)
            for e in exc_info.value.exceptions
        )

    @pytest.mark.asyncio
    async def test_multiple_handlers_one_crashes(self):
        """When one handler crashes, the task group cancels the other."""
        dcs = _create_microdcs(otel_enabled=False)

        completed = asyncio.Event()

        async def failing_handler():
            raise RuntimeError("boom")

        async def long_running_handler():
            try:
                await asyncio.sleep(999)
            except asyncio.CancelledError:
                completed.set()
                raise

        # Register two handler types
        handler1 = MagicMock()
        handler1.task = failing_handler
        otel1 = MagicMock()
        otel1.task = AsyncMock()
        cls1 = type(handler1)

        handler2 = MagicMock()
        handler2.task = long_running_handler
        otel2 = MagicMock()
        otel2.task = AsyncMock()
        cls2 = type(handler2)

        dcs._protocol_handlers = {
            cls1: (handler1, otel1),
            cls2: (handler2, otel2),
        }

        proc = MagicMock()
        proc.initialize = AsyncMock()
        proc.post_start = AsyncMock()

        binding1 = MagicMock()
        binding1.processor = proc
        binding2 = MagicMock()
        binding2.processor = proc

        dcs._handler_bindings = {cls1: {binding1}, cls2: {binding2}}
        dcs._processors = {proc}

        with pytest.raises(ExceptionGroup):
            await dcs.main()

        # The long-running handler was cancelled by the task group
        assert completed.is_set()


class TestRedisConnectionPool:
    """Tests for Redis connection pool configuration in MicroDCS.__init__."""

    def _make_dcs_with_redis_config(self, **redis_overrides) -> MicroDCS:
        redis_cfg = RedisConfig(**redis_overrides)
        with patch("microdcs.core.RuntimeConfig") as mock_rc:
            mock_rc.return_value.redis = redis_cfg
            mock_rc.return_value.redis.key_prefix = redis_cfg.key_prefix
            dcs = MicroDCS()
        return dcs

    def test_default_no_auth_no_ssl(self):
        dcs = self._make_dcs_with_redis_config()
        pool = dcs.redis_connection_pool
        assert pool.connection_kwargs.get("username") is None
        assert pool.connection_kwargs.get("password") is None
        assert pool.connection_class is not redis.SSLConnection

    def test_username_password_passed(self):
        dcs = self._make_dcs_with_redis_config(username="myuser", password="secret")
        pool = dcs.redis_connection_pool
        assert pool.connection_kwargs["username"] == "myuser"
        assert pool.connection_kwargs["password"] == "secret"

    def test_ssl_enabled(self):
        dcs = self._make_dcs_with_redis_config(ssl=True)
        pool = dcs.redis_connection_pool
        assert pool.connection_class is redis.SSLConnection

    def test_ssl_with_ca_certs(self):
        dcs = self._make_dcs_with_redis_config(
            ssl=True, ssl_ca_certs=Path("/fake/ca.crt")
        )
        pool = dcs.redis_connection_pool
        assert pool.connection_class is redis.SSLConnection
        assert pool.connection_kwargs["ssl_ca_certs"] == "/fake/ca.crt"

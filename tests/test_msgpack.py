"""Tests for app/msgpack.py — MessagePack RPC protocol handler and client."""

import asyncio
import inspect
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import msgpack
import pytest

from app import MessagePackConfig, ProcessingConfig
from app.common import (
    CloudEvent,
    CloudEventProcessor,
    Direction,
    ProcessorBinding,
    processor_config,
)
from app.dataclass import DataClassConfig, DataClassMixin
from app.msgpack import (
    MessagePackHandler,
    MessagePackRpcClient,
    OTELInstrumentedMessagePackHandler,
    RpcDispatcher,
    RpcMessageType,
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
    """A dataclass WITHOUT DataClassConfig — used to test the 'no Config' path."""

    value: str = "plain"


@processor_config(binding=ProcessorBinding.SOUTHBOUND)
class ConcreteProcessor(CloudEventProcessor):
    """Minimal concrete subclass so we can test the ABC methods."""

    async def process_event(self, cloudevent):
        return await self.callback_incoming(cloudevent)

    async def process_response_event(self, cloudevent):
        return None

    async def handle_expiration(self, cloudevent, timeout):
        return None


def _make_processor(**kwargs) -> ConcreteProcessor:
    cfg = ProcessingConfig()
    return ConcreteProcessor(
        instance_id="test-id",
        runtime_config=cfg,
        config_identifier="test",
        **kwargs,
    )


def _make_handler(**kwargs) -> MessagePackHandler:
    config = MessagePackConfig()
    pool = MagicMock()
    key_schema = RedisKeySchema()
    with patch("app.msgpack.redis.Redis"):
        handler = MessagePackHandler(config, pool, key_schema, **kwargs)
    return handler


# ===================================================================
# RpcMessageType
# ===================================================================


class TestRpcMessageType:
    def test_request_value(self):
        assert RpcMessageType.REQUEST == 0

    def test_response_value(self):
        assert RpcMessageType.RESPONSE == 1

    def test_notification_value(self):
        assert RpcMessageType.NOTIFICATION == 2


# ===================================================================
# RpcDispatcher
# ===================================================================


class TestRpcDispatcher:
    @pytest.mark.asyncio
    async def test_dispatch_sync_function(self):
        d = RpcDispatcher()
        d.register("add", lambda a, b: a + b)
        assert await d.dispatch("add", [2, 3]) == 5

    @pytest.mark.asyncio
    async def test_dispatch_async_function(self):
        d = RpcDispatcher()

        async def async_mul(a, b):
            return a * b

        d.register("mul", async_mul)
        assert await d.dispatch("mul", [4, 5]) == 20

    @pytest.mark.asyncio
    async def test_dispatch_unknown_method_raises(self):
        d = RpcDispatcher()
        with pytest.raises(ValueError, match="Method 'nope' not found"):
            await d.dispatch("nope", [])

    def test_register_overwrites(self):
        d = RpcDispatcher()
        d.register("f", lambda: 1)
        d.register("f", lambda: 2)
        assert d._methods["f"]() == 2

    @pytest.mark.asyncio
    async def test_dispatch_checks_coroutine(self):
        """Verify the iscoroutinefunction branch."""
        d = RpcDispatcher()

        def sync_fn():
            return "sync"

        async def async_fn():
            return "async"

        d.register("s", sync_fn)
        d.register("a", async_fn)
        assert inspect.iscoroutinefunction(d._methods["a"])
        assert not inspect.iscoroutinefunction(d._methods["s"])
        assert await d.dispatch("s", []) == "sync"
        assert await d.dispatch("a", []) == "async"


# ===================================================================
# CloudEventProcessor (base class — message_callback via msgpack)
# ===================================================================


class TestCloudEventProcessorCallbacks:
    @pytest.mark.asyncio
    async def test_message_callback_no_registered_type(self):
        proc = _make_processor()
        ce = CloudEvent(type="com.unknown.type", data=b'{"value":"x"}')
        result = await proc.callback_incoming(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_message_callback_unserialize_error(self):
        proc = _make_processor()

        async def handler(req):
            return req

        proc.register_callback(SamplePayload, handler, direction=Direction.INCOMING)
        # data with unsupported content type triggers ValueError
        ce = CloudEvent(
            type="com.test.sample.v1",
            data=b"bad",
            datacontenttype="application/xml",
        )
        result = await proc.callback_incoming(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_message_callback_returns_none_from_handler(self):
        proc = _make_processor()

        async def handler(req):
            return None

        proc.register_callback(SamplePayload, handler, direction=Direction.INCOMING)
        ce = CloudEvent(
            type="com.test.sample.v1",
            data=SamplePayload(value="hi").to_jsonb(),
            datacontenttype="application/json",
        )
        result = await proc.callback_incoming(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_message_callback_single_response(self):
        proc = _make_processor()

        async def handler(req):
            return SamplePayload(value="echo")

        proc.register_callback(SamplePayload, handler, direction=Direction.INCOMING)
        ce = CloudEvent(
            type="com.test.sample.v1",
            data=SamplePayload(value="in").to_jsonb(),
            datacontenttype="application/json; charset=utf-8",
            correlationid="corr-1",
        )
        result = await proc.callback_incoming(ce)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].causationid == ce.id
        assert result[0].correlationid == "corr-1"

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
        """Response with no Config class is skipped."""
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
        # Patch create_event to return a CE with unsupported content type
        with patch.object(
            proc,
            "create_event",
            return_value=CloudEvent(datacontenttype="application/xml"),
        ):
            result = await proc.callback_incoming(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_message_callback_msgpack_content_type(self):
        proc = _make_processor()

        async def handler(req):
            return SamplePayload(value="packed")

        proc.register_callback(SamplePayload, handler, direction=Direction.INCOMING)
        ce = CloudEvent(
            type="com.test.sample.v1",
            data=SamplePayload().to_msgpack(),
            datacontenttype="application/msgpack",
        )
        result = await proc.callback_incoming(ce)
        assert isinstance(result, list)
        assert len(result) == 1


# ===================================================================
# MessagePackHandler
# ===================================================================


class TestMessagePackHandler:
    def test_init_registers_publish_and_heartbeat(self):
        handler = _make_handler()
        assert "publish" in handler._dispatcher._methods
        assert "heartbeat" in handler._dispatcher._methods

    def test_register_method(self):
        handler = _make_handler()
        handler.register_method("custom", lambda: 42)
        assert "custom" in handler._dispatcher._methods

    @pytest.mark.asyncio
    async def test_heartbeat(self):
        handler = _make_handler()
        result = await handler.heartbeat("2025-01-01")
        assert result is None

    @pytest.mark.asyncio
    async def test_publish_no_processors(self):
        handler = _make_handler()
        result = await handler.publish({"type": "test"})
        assert result == []

    @pytest.mark.asyncio
    async def test_publish_with_processor_list_response(self):
        handler = _make_handler()
        proc = _make_processor()

        async def fake_process(
            cloudevent: CloudEvent,
        ) -> list[CloudEvent] | CloudEvent | None:
            return [
                CloudEvent(type="resp1"),
                CloudEvent(type="resp2"),
            ]

        proc.process_event = fake_process
        handler.register_processor(proc)

        result = await handler.publish({"type": "com.test.sample.v1"})
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_publish_with_processor_single_response(self):
        handler = _make_handler()
        proc = _make_processor()

        async def fake_process(
            cloudevent: CloudEvent,
        ) -> list[CloudEvent] | CloudEvent | None:
            return CloudEvent(type="single")

        proc.process_event = fake_process
        handler.register_processor(proc)

        result = await handler.publish({"type": "com.test.sample.v1"})
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_publish_with_processor_none_response(self):
        handler = _make_handler()
        proc = _make_processor()

        async def fake_process(
            cloudevent: CloudEvent,
        ) -> list[CloudEvent] | CloudEvent | None:
            return None

        proc.process_event = fake_process
        handler.register_processor(proc)

        result = await handler.publish({"type": "com.test.sample.v1"})
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_publish_with_transport_metadata(self):
        handler = _make_handler()
        proc = _make_processor()

        async def fake_process(
            cloudevent: CloudEvent,
        ) -> list[CloudEvent] | CloudEvent | None:
            assert cloudevent.transportmetadata == {"key": "val"}
            return None

        proc.process_event = fake_process
        handler.register_processor(proc)

        await handler.publish({"type": "t"}, transportmetadata={"key": "val"})

    @pytest.mark.asyncio
    async def test_server_creates_asyncio_server(self):
        handler = _make_handler()
        # TLS cert does not exist in test env, so ssl_context should be None
        with patch("asyncio.start_server", new_callable=AsyncMock) as mock_start:
            mock_start.return_value = MagicMock()
            server = await handler._server()
            mock_start.assert_awaited_once()
            assert server is not None

    @pytest.mark.asyncio
    async def test_server_with_tls(self):
        handler = _make_handler()
        handler._runtime_config.tls_cert_path = MagicMock()
        handler._runtime_config.tls_cert_path.exists.return_value = True
        handler._runtime_config.tls_cert_path.__str__ = lambda self: "/fake/cert"  # type: ignore[assignment]
        with (
            patch("asyncio.start_server", new_callable=AsyncMock) as mock_start,
            patch("ssl.create_default_context") as mock_ssl,
        ):
            mock_start.return_value = MagicMock()
            await handler._server()
            mock_ssl.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_response_success(self):
        handler = _make_handler()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()

        await handler._send_response(writer, lock, 1, None, "ok")
        writer.write.assert_called_once()
        writer.drain.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_send_response_write_error(self):
        handler = _make_handler()
        writer = MagicMock()
        writer.write = MagicMock(side_effect=BrokenPipeError("broken"))
        lock = asyncio.Lock()

        # Should not raise
        await handler._send_response(writer, lock, 1, None, "ok")

    @pytest.mark.asyncio
    async def test_handle_rpc_task_request_success(self):
        handler = _make_handler()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()

        await handler._handle_rpc_task(
            writer, lock, semaphore, RpcMessageType.REQUEST, 42, "heartbeat", ["ts"]
        )
        # Semaphore should be released
        assert not semaphore.locked()

    @pytest.mark.asyncio
    async def test_handle_rpc_task_request_error(self):
        handler = _make_handler()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()

        await handler._handle_rpc_task(
            writer,
            lock,
            semaphore,
            RpcMessageType.REQUEST,
            99,
            "unknown_method",
            [],
        )
        # Semaphore released even on error
        assert not semaphore.locked()
        # Response should contain an error string
        data = writer.write.call_args[0][0]
        response = msgpack.unpackb(data)
        assert response[0] == RpcMessageType.RESPONSE
        assert response[1] == 99
        assert response[2] is not None  # error string
        assert response[3] is None  # result

    @pytest.mark.asyncio
    async def test_handle_rpc_task_notification_no_response(self):
        handler = _make_handler()
        writer = MagicMock()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()

        await handler._handle_rpc_task(
            writer,
            lock,
            semaphore,
            RpcMessageType.NOTIFICATION,
            None,
            "heartbeat",
            ["ts"],
        )
        # No response sent for notifications
        writer.write.assert_not_called()
        assert not semaphore.locked()

    @pytest.mark.asyncio
    async def test_handle_rpc_task_cancelled(self):
        handler = _make_handler()

        async def cancel_dispatch(method_name: str, params: list) -> None:  # type: ignore[type-arg]
            raise asyncio.CancelledError()

        handler._dispatcher.dispatch = cancel_dispatch  # type: ignore[assignment]
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()

        with pytest.raises(asyncio.CancelledError):
            await handler._handle_rpc_task(
                writer,
                lock,
                semaphore,
                RpcMessageType.REQUEST,
                10,
                "slow",
                [],
            )
        # Semaphore still released (finally block)
        assert not semaphore.locked()

    @pytest.mark.asyncio
    async def test_handle_client_clean_disconnect(self):
        handler = _make_handler()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        # reader.read returns empty bytes → clean disconnect
        reader.read = AsyncMock(return_value=b"")
        await handler._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_processes_request(self):
        handler = _make_handler()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()

        # First read returns a request, second read returns empty (disconnect)
        request = msgpack.packb(
            [RpcMessageType.REQUEST, 1, "heartbeat", ["2025-01-01"]]
        )
        reader.read = AsyncMock(side_effect=[request, b""])
        await handler._handle_client(reader, writer)

        # Client was properly closed (task may be cancelled before writing)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_processes_notification(self):
        handler = _make_handler()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        notification = msgpack.packb([RpcMessageType.NOTIFICATION, "heartbeat", ["ts"]])
        reader.read = AsyncMock(side_effect=[notification, b""])
        await handler._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_malformed_message(self):
        handler = _make_handler()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        # A malformed message (wrong type value) followed by disconnect
        malformed = msgpack.packb([99, "bad"])
        reader.read = AsyncMock(side_effect=[malformed, b""])
        await handler._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_non_list_message(self):
        handler = _make_handler()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        # A non-list message
        non_list = msgpack.packb("just a string")
        reader.read = AsyncMock(side_effect=[non_list, b""])
        await handler._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_read_exception(self):
        handler = _make_handler()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        reader.read = AsyncMock(side_effect=ConnectionResetError("reset"))
        await handler._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_partial_data_releases_semaphore(self):
        """Data that doesn't complete a message should release the semaphore."""
        handler = _make_handler()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        # Incomplete msgpack data then disconnect
        reader.read = AsyncMock(side_effect=[b"\x93", b""])
        await handler._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_batch_messages(self):
        """Multiple messages in a single read."""
        handler = _make_handler()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()

        req1 = msgpack.packb([RpcMessageType.REQUEST, 1, "heartbeat", ["ts1"]])
        req2 = msgpack.packb([RpcMessageType.REQUEST, 2, "heartbeat", ["ts2"]])
        assert isinstance(req1, bytes) and isinstance(req2, bytes)
        batch = req1 + req2
        reader.read = AsyncMock(side_effect=[batch, b""])
        await handler._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_task_redis_ping_success(self):
        handler = _make_handler()
        mock_server = AsyncMock()
        mock_server.serve_forever = AsyncMock(side_effect=asyncio.CancelledError())

        handler._redis_client.ping = AsyncMock()
        handler._redis_client.aclose = AsyncMock()

        with pytest.raises(asyncio.CancelledError):
            with patch.object(handler, "_server", return_value=mock_server):
                await handler.task()

        handler._redis_client.aclose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_task_redis_ping_failure(self):
        import redis as _redis

        handler = _make_handler()
        handler._redis_client.ping = AsyncMock(side_effect=_redis.RedisError("down"))

        mock_server = AsyncMock()

        with pytest.raises(_redis.RedisError):
            with patch.object(handler, "_server", return_value=mock_server):
                await handler.task()


# ===================================================================
# OTELInstrumentedMessagePackHandler
# ===================================================================


class TestOTELInstrumentedMessagePackHandler:
    def _make_otel_handler(self) -> OTELInstrumentedMessagePackHandler:
        config = MessagePackConfig()
        pool = MagicMock()
        key_schema = RedisKeySchema()
        with patch("app.msgpack.redis.Redis"):
            return OTELInstrumentedMessagePackHandler(config, pool, key_schema)

    def test_init_sets_tracer_and_meter(self):
        handler = self._make_otel_handler()
        assert handler._tracer is not None
        assert handler._meter is not None
        assert "call_counter" in handler._metrics
        assert "call_duration" in handler._metrics

    def test_record_metrics_success(self):
        handler = self._make_otel_handler()
        handler._metrics["call_counter"] = MagicMock()
        handler._metrics["call_duration"] = MagicMock()
        handler.record_metrics(0.5, error=False, base_attributes={"rpc.method": "x"})
        handler._metrics["call_counter"].add.assert_called_once()
        handler._metrics["call_duration"].record.assert_called_once()

    def test_record_metrics_error(self):
        handler = self._make_otel_handler()
        handler._metrics["call_counter"] = MagicMock()
        handler._metrics["call_duration"] = MagicMock()
        handler.record_metrics(1.0, error=True)
        attrs = handler._metrics["call_counter"].add.call_args[0][1]
        assert attrs["status"] == "error"

    @pytest.mark.asyncio
    async def test_handle_rpc_task_calls_parent(self):
        handler = self._make_otel_handler()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()

        handler._metrics["call_counter"] = MagicMock()
        handler._metrics["call_duration"] = MagicMock()

        await handler._handle_rpc_task(
            writer,
            lock,
            semaphore,
            RpcMessageType.REQUEST,
            1,
            "heartbeat",
            ["ts"],
        )
        assert not semaphore.locked()
        handler._metrics["call_counter"].add.assert_called()

    @pytest.mark.asyncio
    async def test_handle_rpc_task_with_context_extraction(self):
        """First param is dict → extract OTEL context."""
        handler = self._make_otel_handler()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()

        handler._metrics["call_counter"] = MagicMock()
        handler._metrics["call_duration"] = MagicMock()

        # params[0] is dict → triggers context extraction
        await handler._handle_rpc_task(
            writer,
            lock,
            semaphore,
            RpcMessageType.REQUEST,
            2,
            "heartbeat",
            [{"traceparent": "00-abc-def-01"}, "ts"],
        )
        assert not semaphore.locked()

    @pytest.mark.asyncio
    async def test_handle_rpc_task_error_path(self):
        handler = self._make_otel_handler()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()

        handler._metrics["call_counter"] = MagicMock()
        handler._metrics["call_duration"] = MagicMock()

        # Parent's _handle_rpc_task swallows dispatch errors internally,
        # so we force an exception to escape via the parent method.
        with patch.object(
            MessagePackHandler,
            "_handle_rpc_task",
            new_callable=AsyncMock,
            side_effect=RuntimeError("boom"),
        ):
            await handler._handle_rpc_task(
                writer,
                lock,
                semaphore,
                RpcMessageType.REQUEST,
                3,
                "no_such_method",
                [],
            )
        # Metrics should record error=True
        counter_call_attrs = handler._metrics["call_counter"].add.call_args[0][1]
        assert counter_call_attrs["status"] == "error"


# ===================================================================
# MessagePackRpcClient
# ===================================================================


class TestMessagePackRpcClient:
    @pytest.mark.asyncio
    async def test_connect_and_close(self):
        client = MessagePackRpcClient(host="127.0.0.1", port=9999)
        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_reader.read = AsyncMock(return_value=b"")

        with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
            await client.connect()
            assert client.writer is not None

        await client.close()
        assert client.writer is None

    @pytest.mark.asyncio
    async def test_connect_idempotent(self):
        client = MessagePackRpcClient()
        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_reader.read = AsyncMock(return_value=b"")

        with patch(
            "asyncio.open_connection", return_value=(mock_reader, mock_writer)
        ) as mock_open:
            await client.connect()
            await client.connect()  # second call is no-op
            mock_open.assert_called_once()
        await client.close()

    @pytest.mark.asyncio
    async def test_close_without_connect(self):
        client = MessagePackRpcClient()
        # Should not raise
        await client.close()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_reader.read = AsyncMock(return_value=b"")

        with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
            async with MessagePackRpcClient() as client:
                assert client.writer is not None
        assert client.writer is None

    @pytest.mark.asyncio
    async def test_send_not_connected_raises(self):
        client = MessagePackRpcClient()
        with pytest.raises(RuntimeError, match="Client is not connected"):
            await client._send([0, 1, "test", []])

    @pytest.mark.asyncio
    async def test_notify(self):
        client = MessagePackRpcClient()
        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_writer.write = MagicMock()
        mock_writer.drain = AsyncMock()
        mock_reader.read = AsyncMock(return_value=b"")

        with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
            await client.connect()

        await client.notify("heartbeat", 123)
        mock_writer.write.assert_called()
        data = mock_writer.write.call_args[0][0]
        msg = msgpack.unpackb(data)
        assert msg[0] == RpcMessageType.NOTIFICATION
        assert msg[1] == "heartbeat"

        await client.close()

    @pytest.mark.asyncio
    async def test_call_with_response(self):
        client = MessagePackRpcClient()
        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_writer.write = MagicMock()
        mock_writer.drain = AsyncMock()

        # Simulate reader loop: returns response then empty
        response_bytes = msgpack.packb([RpcMessageType.RESPONSE, 1, None, "result_ok"])
        mock_reader.read = AsyncMock(side_effect=[response_bytes, b""])

        with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
            await client.connect()

        result = await client.call("publish", {"data": "test"})
        assert result == "result_ok"

        await client.close()

    @pytest.mark.asyncio
    async def test_call_with_error_response(self):
        client = MessagePackRpcClient()
        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_writer.write = MagicMock()
        mock_writer.drain = AsyncMock()

        response_bytes = msgpack.packb(
            [RpcMessageType.RESPONSE, 1, "something broke", None]
        )
        mock_reader.read = AsyncMock(side_effect=[response_bytes, b""])

        with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
            await client.connect()

        with pytest.raises(RuntimeError, match="RPC Error"):
            await client.call("bad_method")

        await client.close()

    @pytest.mark.asyncio
    async def test_call_send_failure_cleans_up(self):
        client = MessagePackRpcClient()
        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_writer.write = MagicMock(side_effect=OSError("pipe"))
        mock_writer.drain = AsyncMock()
        mock_reader.read = AsyncMock(return_value=b"")

        with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
            await client.connect()

        with pytest.raises(OSError):
            await client.call("method")

        # Pending request should be cleaned up
        assert len(client._pending_requests) == 0
        await client.close()

    @pytest.mark.asyncio
    async def test_reader_loop_not_connected_raises(self):
        client = MessagePackRpcClient()
        with pytest.raises(RuntimeError, match="Client is not connected"):
            await client._reader_loop()

    @pytest.mark.asyncio
    async def test_reader_loop_handles_cancelled(self):
        client = MessagePackRpcClient()
        mock_reader = AsyncMock()
        client.reader = mock_reader
        mock_reader.read = AsyncMock(side_effect=asyncio.CancelledError())
        # Should not raise
        await client._reader_loop()

    @pytest.mark.asyncio
    async def test_reader_loop_handles_exception(self):
        client = MessagePackRpcClient()
        mock_reader = AsyncMock()
        client.reader = mock_reader
        mock_reader.read = AsyncMock(side_effect=ConnectionResetError("reset"))
        # Should not raise (prints error)
        await client._reader_loop()

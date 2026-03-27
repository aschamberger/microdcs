"""Tests for app/msgpack.py — MessagePack RPC protocol handler and client."""

import asyncio
import inspect
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import msgpack
import pytest

from microdcs import MessagePackConfig, ProcessingConfig
from microdcs.common import (
    CloudEvent,
    CloudEventProcessor,
    Direction,
    MessageIntent,
    ProcessorBinding,
    processor_config,
)
from microdcs.dataclass import DataClassConfig, DataClassMixin
from microdcs.msgpack import (
    MessagePackHandler,
    MessagePackProtocolBinding,
    MessagePackRpcClient,
    MessagePackRpcServer,
    OTELInstrumentedMessagePackHandler,
    RpcMessageType,
)
from microdcs.redis import RedisKeySchema

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

    async def process_cloudevent(self, cloudevent):
        return await self.callback_incoming(cloudevent)

    async def process_response_cloudevent(self, cloudevent):
        return None

    async def handle_cloudevent_expiration(self, cloudevent, timeout):
        return None

    async def trigger_outgoing_event(self, **kwargs):
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
    with patch("microdcs.msgpack.redis.Redis"):
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
# MessagePackHandler._dispatch_method (formerly RpcDispatcher)
# ===================================================================


class TestDispatchMethod:
    @pytest.mark.asyncio
    async def test_dispatch_sync_function(self):
        handler = _make_handler()
        handler._methods["add"] = lambda a, b: a + b
        assert (
            await handler._dispatch_method("add", [2, 3], RpcMessageType.REQUEST, 0)
            == 5
        )

    @pytest.mark.asyncio
    async def test_dispatch_async_function(self):
        handler = _make_handler()

        async def async_mul(a, b):
            return a * b

        handler._methods["mul"] = async_mul
        assert (
            await handler._dispatch_method("mul", [4, 5], RpcMessageType.REQUEST, 0)
            == 20
        )

    @pytest.mark.asyncio
    async def test_dispatch_unknown_method_raises(self):
        handler = _make_handler()
        with pytest.raises(ValueError, match="Method 'nope' not found"):
            await handler._dispatch_method("nope", [], RpcMessageType.REQUEST, 0)

    def test_register_overwrites(self):
        handler = _make_handler()
        handler._methods["f"] = lambda: 1
        handler._methods["f"] = lambda: 2
        assert handler._methods["f"]() == 2

    @pytest.mark.asyncio
    async def test_dispatch_checks_coroutine(self):
        """Verify the iscoroutinefunction branch."""
        handler = _make_handler()

        def sync_fn():
            return "sync"

        async def async_fn():
            return "async"

        handler._methods["s"] = sync_fn
        handler._methods["a"] = async_fn
        assert inspect.iscoroutinefunction(handler._methods["a"])
        assert not inspect.iscoroutinefunction(handler._methods["s"])
        assert (
            await handler._dispatch_method("s", [], RpcMessageType.REQUEST, 0) == "sync"
        )
        assert (
            await handler._dispatch_method("a", [], RpcMessageType.REQUEST, 0)
            == "async"
        )


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
        assert "publish" in handler._methods
        assert "heartbeat" in handler._methods

    def test_register_method(self):
        handler = _make_handler()
        handler._methods["custom"] = lambda: 42
        assert "custom" in handler._methods

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

        proc.process_cloudevent = fake_process
        handler._cloudevent_processors.append(proc)

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

        proc.process_cloudevent = fake_process
        handler._cloudevent_processors.append(proc)

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

        proc.process_cloudevent = fake_process
        handler._cloudevent_processors.append(proc)

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

        proc.process_cloudevent = fake_process
        handler._cloudevent_processors.append(proc)

        await handler.publish({"type": "t"}, transportmetadata={"key": "val"})

    def test_server_creates_rpc_server(self):
        handler = _make_handler()
        server = handler._server()
        assert isinstance(server, MessagePackRpcServer)

    def test_server_with_tls(self):
        handler = _make_handler()
        handler._runtime_config.tls_cert_path = MagicMock()
        handler._runtime_config.tls_cert_path.exists.return_value = True
        handler._runtime_config.tls_cert_path.__str__ = lambda self: "/fake/cert"  # type: ignore[assignment]
        with patch("ssl.create_default_context") as mock_ssl:
            server = handler._server()
            mock_ssl.assert_called_once()
            assert server._ssl_context is not None

    @pytest.mark.asyncio
    async def test_send_response_success(self):
        handler = _make_handler()
        server = handler._server()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()

        await server._send_response(writer, lock, 1, None, "ok")
        writer.write.assert_called_once()
        writer.drain.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_send_response_write_error(self):
        handler = _make_handler()
        server = handler._server()
        writer = MagicMock()
        writer.write = MagicMock(side_effect=BrokenPipeError("broken"))
        lock = asyncio.Lock()

        # Should not raise
        await server._send_response(writer, lock, 1, None, "ok")

    @pytest.mark.asyncio
    async def test_handle_rpc_task_request_success(self):
        handler = _make_handler()
        server = handler._server()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()

        await server._handle_rpc_task(
            writer, lock, semaphore, RpcMessageType.REQUEST, 42, "heartbeat", ["ts"]
        )
        # Semaphore should be released
        assert not semaphore.locked()

    @pytest.mark.asyncio
    async def test_handle_rpc_task_request_error(self):
        handler = _make_handler()
        server = handler._server()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()

        await server._handle_rpc_task(
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
        server = handler._server()
        writer = MagicMock()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()

        await server._handle_rpc_task(
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
        server = handler._server()

        async def cancel_dispatch(method_name, params, msg_type, msg_id):
            raise asyncio.CancelledError()

        server._dispatcher = cancel_dispatch
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()

        with pytest.raises(asyncio.CancelledError):
            await server._handle_rpc_task(
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
        server = handler._server()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        # reader.read returns empty bytes → clean disconnect
        reader.read = AsyncMock(return_value=b"")
        await server._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_processes_request(self):
        handler = _make_handler()
        server = handler._server()
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
        await server._handle_client(reader, writer)

        # Client was properly closed (task may be cancelled before writing)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_processes_notification(self):
        handler = _make_handler()
        server = handler._server()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        notification = msgpack.packb([RpcMessageType.NOTIFICATION, "heartbeat", ["ts"]])
        reader.read = AsyncMock(side_effect=[notification, b""])
        await server._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_malformed_message(self):
        handler = _make_handler()
        server = handler._server()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        # A malformed message (wrong type value) followed by disconnect
        malformed = msgpack.packb([99, "bad"])
        reader.read = AsyncMock(side_effect=[malformed, b""])
        await server._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_non_list_message(self):
        handler = _make_handler()
        server = handler._server()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        # A non-list message
        non_list = msgpack.packb("just a string")
        reader.read = AsyncMock(side_effect=[non_list, b""])
        await server._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_read_exception(self):
        handler = _make_handler()
        server = handler._server()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        reader.read = AsyncMock(side_effect=ConnectionResetError("reset"))
        await server._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_partial_data_releases_semaphore(self):
        """Data that doesn't complete a message should release the semaphore."""
        handler = _make_handler()
        server = handler._server()
        reader = AsyncMock()
        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("127.0.0.1", 9999))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        # Incomplete msgpack data then disconnect
        reader.read = AsyncMock(side_effect=[b"\x93", b""])
        await server._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_batch_messages(self):
        """Multiple messages in a single read."""
        handler = _make_handler()
        server = handler._server()
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
        await server._handle_client(reader, writer)
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_task_redis_ping_success(self):
        handler = _make_handler()
        mock_server = AsyncMock()

        handler._redis_client.ping = AsyncMock()

        # Pre-set shutdown event so the handler exits via the graceful
        # shutdown branch after one iteration (serve_forever gets cancelled).
        handler._shutdown_event.set()

        with patch.object(handler, "_server", return_value=mock_server):
            await handler.task()

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
        with patch("microdcs.msgpack.redis.Redis"):
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
    async def test_dispatch_method_calls_parent_and_records_metrics(self):
        handler = self._make_otel_handler()
        handler._metrics["call_counter"] = MagicMock()
        handler._metrics["call_duration"] = MagicMock()

        await handler._dispatch_method("heartbeat", ["ts"], RpcMessageType.REQUEST, 1)
        handler._metrics["call_counter"].add.assert_called()

    @pytest.mark.asyncio
    async def test_dispatch_method_with_context_extraction(self):
        """First param is dict → extract OTEL context."""
        handler = self._make_otel_handler()
        handler._metrics["call_counter"] = MagicMock()
        handler._metrics["call_duration"] = MagicMock()

        # params[0] is dict → triggers context extraction
        await handler._dispatch_method(
            "heartbeat",
            [{"traceparent": "00-abc-def-01"}, "ts"],
            RpcMessageType.REQUEST,
            2,
        )
        handler._metrics["call_counter"].add.assert_called()

    @pytest.mark.asyncio
    async def test_dispatch_method_error_path(self):
        handler = self._make_otel_handler()
        handler._metrics["call_counter"] = MagicMock()
        handler._metrics["call_duration"] = MagicMock()

        # Parent's _dispatch_method raises ValueError for unknown method;
        # OTEL handler catches Exception and records error=True
        await handler._dispatch_method("no_such_method", [], RpcMessageType.REQUEST, 3)
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


# ===================================================================
# MessagePackRpcServer — client tracking & send_notification
# ===================================================================


class TestMessagePackRpcServerClientTracking:
    def _make_server(self) -> MessagePackRpcServer:
        return MessagePackRpcServer(
            dispatcher=AsyncMock(),
            hostname="localhost",
            port=8888,
        )

    def test_clients_dict_initialized_empty(self):
        server = self._make_server()
        assert server._clients == {}

    @pytest.mark.asyncio
    async def test_handle_client_registers_and_unregisters(self):
        """Client is added to _clients on connect and removed on disconnect."""
        server = self._make_server()
        reader = AsyncMock()
        writer = MagicMock()
        addr = ("127.0.0.1", 5555)
        writer.get_extra_info = MagicMock(return_value=addr)
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        reader.read = AsyncMock(return_value=b"")
        await server._handle_client(reader, writer)

        # After disconnect, client should be removed
        assert addr not in server._clients
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_client_registers_during_connection(self):
        """Client is present in _clients while processing messages."""
        server = self._make_server()
        reader = AsyncMock()
        writer = MagicMock()
        addr = ("127.0.0.1", 6666)
        writer.get_extra_info = MagicMock(return_value=addr)
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()

        # Verify client is registered immediately after _handle_client starts.
        # Use a side_effect on the first read to check _clients while connected.
        registered_during_read = False

        async def read_side_effect(size):
            nonlocal registered_during_read
            registered_during_read = addr in server._clients
            return b""

        reader.read = AsyncMock(side_effect=read_side_effect)
        await server._handle_client(reader, writer)

        assert registered_during_read is True
        assert addr not in server._clients

    @pytest.mark.asyncio
    async def test_send_notification_to_connected_clients(self):
        """send_notification broadcasts a NOTIFICATION frame to all clients."""
        server = self._make_server()
        writer1 = MagicMock()
        writer1.write = MagicMock()
        writer1.drain = AsyncMock()
        lock1 = asyncio.Lock()

        writer2 = MagicMock()
        writer2.write = MagicMock()
        writer2.drain = AsyncMock()
        lock2 = asyncio.Lock()

        server._clients[("127.0.0.1", 1111)] = (writer1, lock1)
        server._clients[("127.0.0.1", 2222)] = (writer2, lock2)

        await server.send_notification("cloudevent", [{"type": "test"}, "events"])

        # Both writers should have received the notification
        for w in (writer1, writer2):
            w.write.assert_called_once()
            w.drain.assert_awaited_once()
            data = w.write.call_args[0][0]
            msg = msgpack.unpackb(data)
            assert msg[0] == RpcMessageType.NOTIFICATION
            assert msg[1] == "cloudevent"
            assert msg[2] == [{"type": "test"}, "events"]

    @pytest.mark.asyncio
    async def test_send_notification_no_clients(self):
        """send_notification with no connected clients does nothing."""
        server = self._make_server()
        # Should not raise
        await server.send_notification("cloudevent", [{"type": "test"}, "events"])

    @pytest.mark.asyncio
    async def test_send_notification_handles_write_error(self):
        """A broken client doesn't prevent notification to other clients."""
        server = self._make_server()
        bad_writer = MagicMock()
        bad_writer.write = MagicMock(side_effect=BrokenPipeError("broken"))
        bad_writer.drain = AsyncMock()
        bad_lock = asyncio.Lock()

        good_writer = MagicMock()
        good_writer.write = MagicMock()
        good_writer.drain = AsyncMock()
        good_lock = asyncio.Lock()

        server._clients[("127.0.0.1", 1111)] = (bad_writer, bad_lock)
        server._clients[("127.0.0.1", 2222)] = (good_writer, good_lock)

        # Should not raise
        await server.send_notification("cloudevent", [{"type": "test"}, "events"])

        # Good writer still received the notification
        good_writer.write.assert_called_once()
        good_writer.drain.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_send_notification_uses_lock(self):
        """send_notification acquires the per-client write lock."""
        server = self._make_server()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        lock = asyncio.Lock()

        server._clients[("127.0.0.1", 1111)] = (writer, lock)

        # Acquire the lock externally; send_notification must wait for it
        acquired = False

        async def hold_lock_and_send():
            nonlocal acquired
            async with lock:
                acquired = True
                # Release happens when we exit
            # Now send_notification can proceed

        await hold_lock_and_send()

        await server.send_notification("test_method", ["param"])
        assert acquired
        writer.write.assert_called_once()


# ===================================================================
# MessagePackHandler — outgoing publisher
# ===================================================================


class TestMessagePackHandlerOutgoing:
    @pytest.mark.asyncio
    async def test_outgoing_message_publisher_sends_notification(self):
        """_outgoing_message_publisher reads from queue and sends notification."""
        handler = _make_handler()
        server = handler._server()
        server.send_notification = AsyncMock()

        processor = _make_processor()
        config = MessagePackConfig()
        binding = MessagePackProtocolBinding(processor, ProcessingConfig(), config)

        ce = CloudEvent(type="com.test.sample.v1")
        intent = MessageIntent.EVENT
        await binding.outgoing_queue.put((ce, intent))

        # Run the publisher; it should pick up the item and call send_notification
        publisher_task = asyncio.create_task(
            handler._outgoing_message_publisher(server, binding)
        )
        # Wait for the queue to drain
        await binding.outgoing_queue.join()
        publisher_task.cancel()
        try:
            await publisher_task
        except asyncio.CancelledError:
            pass

        server.send_notification.assert_called_once()
        call_args = server.send_notification.call_args
        assert call_args[0][0] == "cloudevent"
        params = call_args[0][1]
        assert params[1] == "events"  # intent.value

    @pytest.mark.asyncio
    async def test_outgoing_message_publisher_calls_task_done(self):
        """task_done is called after processing each item."""
        handler = _make_handler()
        server = handler._server()
        server.send_notification = AsyncMock()

        processor = _make_processor()
        config = MessagePackConfig()
        binding = MessagePackProtocolBinding(processor, ProcessingConfig(), config)

        # Put two items
        ce1 = CloudEvent(type="com.test.sample.v1")
        ce2 = CloudEvent(type="com.test.sample.v1")
        await binding.outgoing_queue.put((ce1, MessageIntent.EVENT))
        await binding.outgoing_queue.put((ce2, MessageIntent.DATA))

        publisher_task = asyncio.create_task(
            handler._outgoing_message_publisher(server, binding)
        )
        await binding.outgoing_queue.join()
        publisher_task.cancel()
        try:
            await publisher_task
        except asyncio.CancelledError:
            pass

        assert server.send_notification.call_count == 2

    @pytest.mark.asyncio
    async def test_task_starts_publisher_tasks(self):
        """task() creates publisher tasks for each binding."""
        handler = _make_handler()
        processor = _make_processor()
        config = MessagePackConfig()
        binding = MessagePackProtocolBinding(processor, ProcessingConfig(), config)
        handler.register_binding(binding)

        handler._redis_client.ping = AsyncMock()
        handler._shutdown_event.set()

        mock_server = AsyncMock()
        with patch.object(handler, "_server", return_value=mock_server):
            await handler.task()

    @pytest.mark.asyncio
    async def test_task_drains_outgoing_queue_on_shutdown(self):
        """On shutdown, outgoing queues are drained before publishers are cancelled."""
        handler = _make_handler()
        processor = _make_processor()
        config = MessagePackConfig()
        binding = MessagePackProtocolBinding(processor, ProcessingConfig(), config)
        handler.register_binding(binding)

        handler._redis_client.ping = AsyncMock()

        # Put an item in the outgoing queue before shutdown
        ce = CloudEvent(type="com.test.sample.v1")
        await binding.outgoing_queue.put((ce, MessageIntent.EVENT))

        mock_server = AsyncMock()

        async def serve_forever_then_shutdown():
            # Signal shutdown so the handler exits
            handler._shutdown_event.set()
            # Give the handler loop time to process
            await asyncio.sleep(0)

        mock_server.serve_forever = serve_forever_then_shutdown
        mock_server.send_notification = AsyncMock()

        with patch.object(handler, "_server", return_value=mock_server):
            await handler.task()

        # The queue should be empty after draining
        assert binding.outgoing_queue.empty()


# ===================================================================
# MessagePackProtocolBinding
# ===================================================================


class TestMessagePackProtocolBinding:
    def test_init_registers_publish_handler(self):
        """Constructor registers publish_handler on the processor."""
        processor = _make_processor()
        config = MessagePackConfig()
        binding = MessagePackProtocolBinding(processor, ProcessingConfig(), config)
        assert binding.publish_handler in processor._publish_handlers

    def test_init_with_outgoing_filter(self):
        """outgoing_ce_type_filter is passed to the base class."""
        processor = _make_processor()
        config = MessagePackConfig()
        filters = {"com.test.sample.v1", "com.test.other.*"}
        binding = MessagePackProtocolBinding(
            processor, ProcessingConfig(), config, filters
        )
        assert binding.outgoing_ce_type_filter == filters

    def test_init_default_empty_filter(self):
        """Default filter is an empty set."""
        processor = _make_processor()
        config = MessagePackConfig()
        binding = MessagePackProtocolBinding(processor, ProcessingConfig(), config)
        assert binding.outgoing_ce_type_filter == set()

    def test_publish_handler_enqueues_event(self):
        """publish_handler puts events on the outgoing_queue."""
        processor = _make_processor()
        config = MessagePackConfig()
        binding = MessagePackProtocolBinding(processor, ProcessingConfig(), config)
        ce = CloudEvent(type="com.test.sample.v1")
        binding.publish_handler(ce, MessageIntent.EVENT)
        assert not binding.outgoing_queue.empty()
        item = binding.outgoing_queue.get_nowait()
        assert item[0] is ce
        assert item[1] == MessageIntent.EVENT

    def test_publish_handler_with_filter_matching(self):
        """Events matching the filter are enqueued."""
        processor = _make_processor()
        config = MessagePackConfig()
        binding = MessagePackProtocolBinding(
            processor, ProcessingConfig(), config, {"com.test.sample.v1"}
        )
        ce = CloudEvent(type="com.test.sample.v1")
        binding.publish_handler(ce, MessageIntent.EVENT)
        assert not binding.outgoing_queue.empty()

    def test_publish_handler_with_filter_not_matching(self):
        """Events not matching the filter are not enqueued."""
        processor = _make_processor()
        config = MessagePackConfig()
        binding = MessagePackProtocolBinding(
            processor, ProcessingConfig(), config, {"com.test.other.v1"}
        )
        ce = CloudEvent(type="com.test.sample.v1")
        binding.publish_handler(ce, MessageIntent.EVENT)
        assert binding.outgoing_queue.empty()

    def test_publish_handler_with_wildcard_filter(self):
        """Wildcard filter matches event types."""
        processor = _make_processor()
        config = MessagePackConfig()
        binding = MessagePackProtocolBinding(
            processor, ProcessingConfig(), config, {"com.test.*"}
        )
        ce = CloudEvent(type="com.test.sample.v1")
        binding.publish_handler(ce, MessageIntent.EVENT)
        assert not binding.outgoing_queue.empty()

    def test_publish_event_routes_to_binding(self):
        """processor.publish_event() routes through binding's publish_handler."""
        processor = _make_processor()
        config = MessagePackConfig()
        MessagePackProtocolBinding(processor, ProcessingConfig(), config)
        ce = CloudEvent(type="com.test.sample.v1")
        processor.publish_event(ce, MessageIntent.DATA)
        # The processor's publish_event should have routed through the binding

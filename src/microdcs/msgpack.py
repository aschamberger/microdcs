import asyncio
import errno
import inspect
import itertools
import logging
import random
import ssl
import time
from enum import IntEnum
from typing import Any, Callable

import msgpack
import redis.asyncio as redis
from opentelemetry import metrics, trace
from opentelemetry.propagate import extract, inject
from opentelemetry.semconv._incubating.attributes import (
    network_attributes,
    rpc_attributes,
    server_attributes,
)
from opentelemetry.semconv.attributes import error_attributes

from microdcs import MessagePackConfig, ProcessingConfig
from microdcs.common import (
    CloudEvent,
    CloudEventProcessor,
    ProtocolBinding,
    ProtocolHandler,
)
from microdcs.redis import RedisKeySchema

logger = logging.getLogger("handler.msgpack")


class RpcMessageType(IntEnum):
    REQUEST = 0  # [0, msgid, method, params]
    RESPONSE = 1  # [1, msgid, error, result]
    NOTIFICATION = 2  # [2, method, params]


class MessagePackHandler(ProtocolHandler["MessagePackProtocolBinding"]):
    def __init__(
        self,
        runtime_config: MessagePackConfig,
        redis_connection_pool: redis.ConnectionPool,
        redis_key_schema: RedisKeySchema,
    ):
        super().__init__()
        self._runtime_config: MessagePackConfig = runtime_config
        self._redis_client: redis.Redis = redis.Redis(
            connection_pool=redis_connection_pool
        )
        self._redis_key_schema: RedisKeySchema = redis_key_schema
        self._methods: dict[str, Callable] = {
            "publish": self.publish,
            "heartbeat": self.heartbeat,
        }

    async def publish(
        self,
        cloudevent_dict: dict[str, Any],
        transportmetadata: dict[str, str] | None = None,
    ):
        logger.debug(
            "Publishing message %s with transport metadata: %s",
            cloudevent_dict,
            transportmetadata,
        )

        # deserialize CloudEvent and add transport metadata
        cloudevent = CloudEvent.from_dict(cloudevent_dict)
        cloudevent.transportmetadata = transportmetadata

        # Process the event through registered CloudEvent processors
        responses: list[dict[str, Any]] = []
        for processor in self._cloudevent_processors:
            processor_response = await processor.process_cloudevent(cloudevent)
            if processor_response:
                if isinstance(processor_response, list):
                    for response in processor_response:
                        responses.append(response.to_dict())
                else:
                    responses.append(processor_response.to_dict())

        return responses

    async def heartbeat(self, timestamp):
        logger.debug("Heartbeat: %s", timestamp)
        # No return value needed for notification

    async def _dispatch_method(
        self,
        method_name: str,
        params: list,
        msg_type: RpcMessageType,
        msg_id: int,
        peer_addr: tuple | None = None,
    ):
        """Finds the method and executes it."""
        if method_name not in self._methods:
            raise ValueError(f"Method '{method_name}' not found")

        func = self._methods[method_name]

        # Check if the function is a coroutine (async)
        if inspect.iscoroutinefunction(func):
            return await func(*params)
        else:
            return func(*params)

    def _server(self) -> MessagePackRpcServer:
        ssl_context = None
        if self._runtime_config.tls_cert_path.exists():
            ssl_context = ssl.create_default_context(
                ssl.Purpose.CLIENT_AUTH,
                cafile=str(self._runtime_config.tls_cert_path),
            )
            if self._runtime_config.tls_client_auth:
                ssl_context.verify_mode = ssl.CERT_REQUIRED
        return MessagePackRpcServer(
            dispatcher=self._dispatch_method,
            hostname=self._runtime_config.hostname,
            port=self._runtime_config.port,
            ssl_context=ssl_context,
            max_queued_connections=self._runtime_config.max_queued_connections,
            max_concurrent_requests=self._runtime_config.max_concurrent_requests,
            max_buffer_size=self._runtime_config.max_buffer_size,
        )

    async def _outgoing_message_publisher(
        self, server: MessagePackRpcServer, binding: "MessagePackProtocolBinding"
    ) -> None:
        while True:
            cloudevent, intent = await binding.outgoing_queue.get()
            notification_params = [cloudevent.to_dict(), intent.value]
            await server.send_notification("cloudevent", notification_params)
            binding.outgoing_queue.task_done()

    async def task(self) -> None:
        logger.info("Starting MessagePack handler task")
        # redis is required for message deduplication and expiration handling,
        # so we check the connection before starting the server
        try:
            await self._redis_client.ping()  # pyright: ignore[reportGeneralTypeIssues]
        except redis.RedisError as e:
            logger.error(f"Error connecting to Redis: {e}")
            raise
        server = self._server()
        try:
            async with server:
                # Start publisher tasks for outgoing events
                publisher_tasks: list[asyncio.Task] = []
                for binding in self._bindings:
                    publisher_tasks.append(
                        asyncio.create_task(
                            self._outgoing_message_publisher(server, binding)
                        )
                    )
                # Wait for either serve_forever to end or shutdown event
                serve_task = asyncio.create_task(server.serve_forever())
                shutdown_task = asyncio.create_task(self._shutdown_event.wait())

                done, _ = await asyncio.wait(
                    {serve_task, shutdown_task} | set(publisher_tasks),
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if shutdown_task in done:
                    # Graceful shutdown: close server to stop accepting
                    # new connections; __aexit__ handles wait_closed()
                    # to let in-flight RPCs drain
                    logger.info("Graceful shutdown: closing MessagePack server")
                    serve_task.cancel()
                    try:
                        await serve_task
                    except asyncio.CancelledError:
                        pass

                    # Drain outgoing queues before cancelling publishers
                    for binding in self._bindings:
                        if not binding.outgoing_queue.empty():
                            logger.info(
                                "Draining outgoing queue (%d items)",
                                binding.outgoing_queue.qsize(),
                            )
                            await binding.outgoing_queue.join()

                    # Cancel publishers after queues are drained
                    for task in publisher_tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*publisher_tasks, return_exceptions=True)
                    logger.info("All publishers completed")
                else:
                    # serve_forever or a publisher ended unexpectedly
                    shutdown_task.cancel()
                    serve_task.cancel()
                    for task in publisher_tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(
                        serve_task,
                        *publisher_tasks,
                        return_exceptions=True,
                    )
                    for task in done:
                        if not task.cancelled() and task.exception() is not None:
                            raise task.exception()  # pyright: ignore[reportGeneralTypeIssues]
        except asyncio.CancelledError:
            logger.info("MessagePack handler task cancelled; shutting down")
            raise
        logger.info("MessagePack handler shutdown complete")


class OTELInstrumentedMessagePackHandler(MessagePackHandler):
    def __init__(
        self,
        runtime_config: MessagePackConfig,
        redis_connection_pool: redis.ConnectionPool,
        redis_key_schema: RedisKeySchema,
    ):
        super().__init__(
            runtime_config,
            redis_connection_pool,
            redis_key_schema,
        )

        self._tracer: trace.Tracer = trace.get_tracer(__name__)
        self._meter: metrics.Meter = metrics.get_meter(__name__)
        self._server_duration = self._meter.create_histogram(
            "rpc.server.call.duration",
            unit="s",
            description="Duration of inbound MessagePack RPC calls in seconds",
            explicit_bucket_boundaries_advisory=[
                0.005,
                0.01,
                0.025,
                0.05,
                0.075,
                0.1,
                0.25,
                0.5,
                0.75,
                1,
                2.5,
                5,
                7.5,
                10,
            ],
        )

    async def _dispatch_method(
        self,
        method_name: str,
        params: list,
        msg_type: RpcMessageType,
        msg_id: int,
        peer_addr: tuple | None = None,
    ):
        # start timing
        start = time.monotonic()
        # extract context from MessagePack message properties
        context = None
        if len(params) > 0 and isinstance(params[0], dict):
            context = extract(params[0])
        # determine if method is recognized; unknown methods map to "_OTHER" per spec
        recognized = method_name in self._methods
        span_method = method_name if recognized else "_OTHER"
        # define base attributes for both trace and metrics
        base_attributes: dict[str, Any] = {
            rpc_attributes.RPC_SYSTEM: "messagepack",
            rpc_attributes.RPC_METHOD: span_method,
            network_attributes.NETWORK_TRANSPORT: "tcp",
            server_attributes.SERVER_ADDRESS: self._runtime_config.hostname,
            server_attributes.SERVER_PORT: self._runtime_config.port,
        }
        if not recognized:
            base_attributes[rpc_attributes.RPC_METHOD_ORIGINAL] = method_name
        if peer_addr is not None:
            base_attributes[network_attributes.NETWORK_PEER_ADDRESS] = str(peer_addr[0])
            base_attributes[network_attributes.NETWORK_PEER_PORT] = peer_addr[1]
        # start trace span (SERVER kind per spec) and call parent method
        error_type: str | None = None
        result = None
        with self._tracer.start_as_current_span(
            span_method,
            kind=trace.SpanKind.SERVER,
            context=context,
        ) as span:
            span.set_attributes(base_attributes)
            try:
                result = await super()._dispatch_method(
                    method_name, params, msg_type, msg_id, peer_addr
                )
            except Exception as exc:
                error_type = type(exc).__qualname__
                span.record_exception(exc)
                span.set_status(trace.Status(trace.StatusCode.ERROR))
                span.set_attribute(error_attributes.ERROR_TYPE, error_type)
                raise
            finally:
                duration = time.monotonic() - start
                if error_type is not None:
                    self._server_duration.record(
                        duration,
                        base_attributes | {error_attributes.ERROR_TYPE: error_type},
                    )
                else:
                    self._server_duration.record(duration, base_attributes)
        return result

    async def _outgoing_message_publisher(
        self, server: MessagePackRpcServer, binding: "MessagePackProtocolBinding"
    ) -> None:
        while True:
            cloudevent, intent = await binding.outgoing_queue.get()
            # inject current span context so receivers can link to the originating trace
            with self._tracer.start_as_current_span(
                "cloudevent",
                kind=trace.SpanKind.PRODUCER,
            ):
                trace_headers: dict[str, str] = {}
                inject(trace_headers)
                if trace_headers:
                    if cloudevent.custommetadata is None:
                        cloudevent.custommetadata = {}
                    cloudevent.custommetadata.update(trace_headers)
                notification_params = [cloudevent.to_dict(), intent.value]
                await server.send_notification("cloudevent", notification_params)
            binding.outgoing_queue.task_done()


class MessagePackProtocolBinding(ProtocolBinding["MessagePackHandler"]):
    def __init__(
        self,
        processor: CloudEventProcessor,
        processing_config: ProcessingConfig,
        msgpack_config: MessagePackConfig,
        outgoing_ce_type_filter: set[str] = set(),
    ):
        super().__init__(
            processor,
            processing_config,
            msgpack_config.binding_outgoing_queue_size,
            outgoing_ce_type_filter,
        )
        self._msgpack_config = msgpack_config
        # Only RPC type NOTIFICATIONs are supported for outgoing messages,
        # since REQUEST/RESPONSE semantics don't make sense for outgoing events
        processor.register_publish_handler(self.publish_handler)


class MessagePackRpcServer:
    def __init__(
        self,
        dispatcher: Callable[[str, list, RpcMessageType, int, tuple | None], Any],
        hostname: str = "localhost",
        port: int = 8888,
        ssl_context: ssl.SSLContext | None = None,
        max_queued_connections: int = 100,
        max_concurrent_requests: int = 10,
        max_buffer_size: int = 8 * 1024 * 1024,
        reconnect: bool = True,
    ):
        self._host = hostname
        self._port = port
        self._ssl_context = ssl_context
        self._max_queued_connections = max_queued_connections
        self._max_concurrent_requests = max_concurrent_requests
        self._max_buffer_size = max_buffer_size
        self._reconnect = reconnect
        self._backoff: int = 1
        self._server: asyncio.Server | None = None
        self._methods: dict[str, Callable] = {}
        self._dispatcher = dispatcher
        # Track connected clients: maps peername → (writer, lock)
        self._clients: dict[Any, tuple[asyncio.StreamWriter, asyncio.Lock]] = {}

    async def __aenter__(self) -> MessagePackRpcServer:
        """Starts the server, retrying on EADDRINUSE if reconnect=True.

        Interrupted by CancelledError (force-cancel from SystemEventTaskGroup
        grace-period expiry) if shutdown occurs during a backoff sleep.
        """
        max_backoff = 60  # seconds
        while True:
            try:
                self._server = await asyncio.start_server(
                    self._handle_client,
                    self._host,
                    self._port,
                    ssl=self._ssl_context,
                    backlog=self._max_queued_connections,
                )
                logger.info(
                    "MessagePack-RPC Server running on %s:%d",
                    self._host,
                    self._port,
                )
                return self
            except OSError as e:
                if e.errno in (errno.EADDRNOTAVAIL, errno.EACCES):
                    logger.error(f"FATAL CONFIG ERROR: {e.strerror} (errno {e.errno})")
                    raise
                if self._reconnect and e.errno == errno.EADDRINUSE:
                    sleep_time = self._backoff + random.uniform(0, 0.1 * self._backoff)
                    logger.warning(
                        f"Port {self._port} busy. Retrying in {sleep_time:.2f}s..."
                    )
                    await asyncio.sleep(sleep_time)
                    self._backoff = min(self._backoff * 2, max_backoff)
                    continue
                raise

    async def __aexit__(self, exc_type, exc, tb):
        """Ensures the server closes cleanly when the block is exited."""
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        logger.info("MessagePack-RPC Server shut down cleanly.")

    async def serve_forever(self):
        """Wrapper for the server's internal loop."""
        if self._server:
            await self._server.serve_forever()

    async def send_notification(self, method: str, params: list) -> None:
        """Send a NOTIFICATION frame to all connected clients."""
        frame = msgpack.packb([RpcMessageType.NOTIFICATION, method, params])
        if frame is None:
            return
        for addr, (writer, lock) in list(self._clients.items()):
            try:
                async with lock:
                    writer.write(frame)
                    await writer.drain()
            except Exception as e:
                logger.error("Failed to send notification to %s: %s", addr, e)

    async def _send_response(self, writer, lock, msg_id, error, result):
        """Sends the response safely using the Write Lock."""
        response = [RpcMessageType.RESPONSE, msg_id, error, result]
        try:
            serialized_data = msgpack.packb(response)
            async with lock:
                writer.write(serialized_data)
                await writer.drain()
        except Exception as e:
            # If writing fails (e.g., pipe broken), we just log it.
            # The main loop will handle the disconnect.
            logger.error("Failed to write response: %s", e)

    async def _handle_rpc_task(
        self,
        writer,
        lock,
        semaphore,
        msg_type,
        msg_id,
        method,
        params,
        peer_addr: tuple | None = None,
    ):
        """
        Executes business logic.
        CRITICAL: Wraps everything in try/finally to ensure the semaphore is ALWAYS released.
        """
        logger.debug("Handling RPC task: %s, %s, %s", msg_type, msg_id, method)
        try:
            error = None
            result = None

            try:
                # Execute business logic
                result = await self._dispatcher(
                    method, params, msg_type, msg_id, peer_addr
                )
            except asyncio.CancelledError:
                # If the server cancels us (client disconnect), we stop immediately.
                logger.info("Task cancelled for %s", method)
                raise
            except Exception as e:
                error = str(e)
                logger.error("RPC Error executing '%s': %s", method, e)
            # Send Response (only for Requests, not Notifications)
            if msg_type == RpcMessageType.REQUEST:
                await self._send_response(writer, lock, msg_id, error, result)

        finally:
            # --- RELEASE BACKPRESSURE ---
            # Whether we succeeded, failed, or were cancelled, we MUST give back the slot.
            semaphore.release()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logger.info("Connected: %s", addr)

        # 1. Thread-safe Write Lock
        socket_lock = asyncio.Lock()

        # Register client for outgoing notifications
        self._clients[addr] = (writer, socket_lock)

        # 2. Backpressure Control
        semaphore = asyncio.Semaphore(self._max_concurrent_requests)

        # 3. Active Task Tracking (for cleanup)
        active_tasks = set()

        unpacker = msgpack.Unpacker(
            raw=False,
            max_buffer_size=self._max_buffer_size,
        )

        try:
            while True:
                # A. Pre-check: Do we have room for at least ONE request?
                # If we are full, this blocks, stopping us from reading the socket.
                await semaphore.acquire()

                try:
                    data = await reader.read(4096)
                except Exception:
                    # Read failed? Release the token we just grabbed.
                    semaphore.release()
                    raise

                if not data:
                    # Clean disconnect? Release the token.
                    semaphore.release()
                    break

                unpacker.feed(data)

                # B. Process Batch
                # We currently hold 1 semaphore token (from step A).
                first_msg_processed = False

                for msg in unpacker:
                    if not isinstance(msg, list):
                        continue

                    # For the first message, we use the token we acquired in step A.
                    # For subsequent messages in this batch, we must acquire NEW tokens.
                    if first_msg_processed:
                        await semaphore.acquire()

                    first_msg_processed = True

                    # Parse Header
                    msg_type = msg[0]
                    msg_id, method, params = None, None, []

                    if msg_type == RpcMessageType.REQUEST and len(msg) == 4:
                        msg_id, method, params = msg[1], msg[2], msg[3]
                    elif msg_type == RpcMessageType.NOTIFICATION and len(msg) == 3:
                        method, params = msg[1], msg[2]
                    else:
                        # Malformed? Release the token immediately and skip
                        semaphore.release()
                        continue

                    # Spawn the Task
                    task = asyncio.create_task(
                        self._handle_rpc_task(
                            writer,
                            socket_lock,
                            semaphore,
                            msg_type,
                            msg_id,
                            method,
                            params,
                            addr,
                        )
                    )

                    # Track it
                    active_tasks.add(task)
                    # Automatically remove from set when done
                    task.add_done_callback(active_tasks.discard)

                # Corner Case: We read data, but it didn't complete a full message yet.
                # We are holding a token that wasn't used. Give it back.
                if not first_msg_processed:
                    semaphore.release()

        except Exception as e:
            logger.error("Connection error: %s", e)

        finally:
            logger.info(
                "Disconnecting %s. Cleaning up %d pending tasks...",
                addr,
                len(active_tasks),
            )

            # --- CLEANUP PHASE ---
            # 1. Cancel all running tasks
            for task in active_tasks:
                task.cancel()

            # 2. Wait for them to finish cancelling (they will release semaphores)
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)

            logger.info("Cleanup complete for %s", addr)
            self._clients.pop(addr, None)
            writer.close()
            await writer.wait_closed()


class MessagePackRpcClient:
    def __init__(
        self,
        host="localhost",
        port=8888,
        max_buffer_size: int = 8 * 1024 * 1024,
    ):
        self._host = host
        self._port = port
        self._max_buffer_size = max_buffer_size
        self.reader = None
        self.writer = None
        self._id_counter = itertools.count(1)
        self._pending_requests = {}
        self._listen_task = None
        self._lock = asyncio.Lock()

    async def connect(self):
        if self.writer:
            return  # Already connected
        self.reader, self.writer = await asyncio.open_connection(self._host, self._port)
        self._listen_task = asyncio.create_task(self._reader_loop())
        print(f"Connected to {self._host}:{self._port}")
        return self

    async def close(self):
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.writer = None  # Reset to None so we know it's closed

        print("Disconnected.")

    # --- MAGIC METHODS FOR 'async with' ---
    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def _send(self, payload):
        if self.writer is None:
            raise RuntimeError(
                "Client is not connected. Did you forget 'await client.connect()'?"
            )

        data = msgpack.packb(payload)
        if data is not None:
            # print(f"Sending: {data}")
            async with self._lock:
                self.writer.write(data)
                await self.writer.drain()

    async def call(self, method: str, *args):
        msg_id = next(self._id_counter)
        future = asyncio.get_running_loop().create_future()
        self._pending_requests[msg_id] = future

        # [0, msgid, method, params]
        payload = [RpcMessageType.REQUEST, msg_id, method, list(args)]

        try:
            await self._send(payload)
            return await future
        except Exception:
            # If send fails, cleanup the future so we don't leak memory
            self._pending_requests.pop(msg_id, None)
            raise
        finally:
            self._pending_requests.pop(msg_id, None)

    async def notify(self, method: str, *args):
        # [2, method, params]
        payload = [RpcMessageType.NOTIFICATION, method, list(args)]
        await self._send(payload)

    async def _reader_loop(self):
        if self.reader is None:
            raise RuntimeError(
                "Client is not connected. Did you forget 'await client.connect()'?"
            )

        unpacker = msgpack.Unpacker(
            raw=False,
            max_buffer_size=self._max_buffer_size,
        )
        try:
            while True:
                data = await self.reader.read(4096)
                if not data:
                    break
                unpacker.feed(data)

                for msg in unpacker:
                    if (
                        isinstance(msg, list)
                        and len(msg) == 4
                        and msg[0] == RpcMessageType.RESPONSE
                    ):
                        _, msg_id, error, result = msg
                        future = self._pending_requests.get(msg_id)

                        if future and not future.done():
                            if error:
                                future.set_exception(
                                    RuntimeError(f"RPC Error: {error}")
                                )
                            else:
                                future.set_result(result)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Reader Error: {e}")

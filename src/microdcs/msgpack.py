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
from opentelemetry.propagate import extract
from opentelemetry.semconv._incubating.attributes import (
    network_attributes,
    rpc_attributes,
    server_attributes,
)

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
        self, method_name: str, params: list, msg_type: RpcMessageType, msg_id: int
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
                cafile=str(self._runtime_config.tls_cert_path)
            )
        return MessagePackRpcServer(
            dispatcher=self._dispatch_method,
            hostname=self._runtime_config.hostname,
            port=self._runtime_config.port,
            ssl_context=ssl_context,
            max_queued_connections=self._runtime_config.max_queued_connections,
            max_concurrent_requests=self._runtime_config.max_concurrent_requests,
        )

    async def task(self) -> None:
        logger.info("Starting MessagePack handler task")
        server = self._server()
        backoff = 1  # seconds
        max_backoff = 60  # seconds
        while True:
            try:
                # redis is required for message deduplication and expiration handling,
                # so we check the connection before starting the server
                try:
                    await self._redis_client.ping()  # pyright: ignore[reportGeneralTypeIssues]
                except redis.RedisError as e:
                    logger.error(f"Error connecting to Redis: {e}")
                    raise
                async with server:
                    backoff = 1  # Reset backoff after successful start
                    # Wait for either serve_forever to end or shutdown event
                    serve_task = asyncio.create_task(server.serve_forever())
                    shutdown_task = asyncio.create_task(self._shutdown_event.wait())

                    done, _ = await asyncio.wait(
                        {serve_task, shutdown_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if shutdown_task in done:
                        # Graceful shutdown: close server to stop accepting
                        # new connections; __aexit__ handles wait_closed()
                        # to let in-flight RPCs drain
                        logger.info(
                            "Graceful shutdown: closing MessagePack server"
                        )
                        serve_task.cancel()
                        try:
                            await serve_task
                        except asyncio.CancelledError:
                            pass
                        break  # exit retry loop
                    else:
                        # serve_forever ended unexpectedly
                        shutdown_task.cancel()
                        if not serve_task.cancelled() and serve_task.exception() is not None:
                            raise serve_task.exception()  # pyright: ignore[reportGeneralTypeIssues]

            except OSError as e:
                # 1. Config Check: Check for fatal errors
                if e.errno in (errno.EADDRNOTAVAIL, errno.EACCES):
                    logger.error(f"FATAL CONFIG ERROR: {e.strerror} (errno {e.errno})")
                    raise  # Break out of the loop and the app

                # 2. Retry Logic: Handle 'Address in use'
                if e.errno == errno.EADDRINUSE:
                    sleep_time = backoff + random.uniform(0, 0.1 * backoff)
                    logger.warning(
                        f"Port {self._runtime_config.port} busy. Retrying in {sleep_time:.2f}s..."
                    )
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(), timeout=sleep_time
                        )
                        logger.info("Shutdown during reconnect backoff; exiting")
                        break
                    except asyncio.TimeoutError:
                        backoff = min(backoff * 2, max_backoff)
                        continue

                raise  # Any other OSError we didn't account for

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
        self._metrics: dict[str, metrics.Instrument] = {
            "call_counter": self._meter.create_counter(
                "rpc.server.call.count",
                description="Count of MessagePack RPC calls processed",
            ),
            "call_duration": self._meter.create_histogram(
                "rpc.server.call.duration",
                description="Duration of MessagePack RPC calls in milliseconds",
            ),
        }

    def record_metrics(
        self, duration: float, error: bool = False, base_attributes: dict[str, str] = {}
    ) -> None:
        attributes = base_attributes | {"status": "error" if error else "success"}
        self._metrics["call_counter"].add(1, attributes)  # pyright: ignore[reportAttributeAccessIssue]
        self._metrics["call_duration"].record(duration, attributes)  # pyright: ignore[reportAttributeAccessIssue]

    async def _dispatch_method(
        self, method_name: str, params: list, msg_type: RpcMessageType, msg_id: int
    ):
        # start timing
        processing_start_time = time.time()
        # extract context from MessagePack message properties
        context = None
        if len(params) > 0 and isinstance(params[0], dict):
            context = extract(params[0])
        # define base attributes for both trace and metrics
        base_attributes = {
            rpc_attributes.RPC_SYSTEM: "messagepack",
            rpc_attributes.RPC_SERVICE: "micro-dcs",
            rpc_attributes.RPC_METHOD: method_name,
            network_attributes.NETWORK_TRANSPORT: "tcp",
            server_attributes.SERVER_ADDRESS: self._runtime_config.hostname,
            server_attributes.SERVER_PORT: self._runtime_config.port,
        }
        # start trace span and call parent method
        with self._tracer.start_as_current_span(
            "{rpc.method}",
            kind=trace.SpanKind.CONSUMER,
            context=context,
        ) as span:
            span.set_attributes(base_attributes)

        error = False
        try:
            await super()._dispatch_method(
                method_name, params, RpcMessageType.REQUEST, 0
            )
        except Exception:
            span.set_status(
                trace.Status(
                    trace.StatusCode.ERROR,
                    "Error processing MessagePack message",
                )
            )
            error = True
        processing_duration = time.time() - processing_start_time
        self.record_metrics(processing_duration, error, base_attributes)


class MessagePackProtocolBinding(ProtocolBinding["MessagePackHandler"]):
    def __init__(
        self,
        processor: CloudEventProcessor,
        processing_config: ProcessingConfig,
        msgpack_config: MessagePackConfig,
    ):
        super().__init__(
            processor, processing_config, msgpack_config.binding_outgoing_queue_size
        )
        self._msgpack_config = msgpack_config
        # not currently used, but can be implemented in the future for
        # outgoing message publication and filtering, only RPC type NOTIFICATIONs
        # are supported for outgoing messages, since REQUEST/RESPONSE semantics
        # don't make sense for outgoing events
        # processor.register_publish_handler(self.publish_handler)


class MessagePackRpcServer:
    def __init__(
        self,
        dispatcher: Callable[[str, list, RpcMessageType, int], Any],
        hostname: str = "localhost",
        port: int = 8888,
        ssl_context: ssl.SSLContext | None = None,
        max_queued_connections: int = 100,
        max_concurrent_requests: int = 10,
    ):
        self._host = hostname
        self._port = port
        self._ssl_context = ssl_context
        self._max_queued_connections = max_queued_connections
        self._max_concurrent_requests = max_concurrent_requests
        self._server: asyncio.Server | None = None
        self._methods: dict[str, Callable] = {}
        self._dispatcher = dispatcher

    async def __aenter__(self) -> MessagePackRpcServer:
        """Starts the server when entering the 'async with' block."""
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
        self, writer, lock, semaphore, msg_type, msg_id, method, params
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
                result = await self._dispatcher(method, params, msg_type, msg_id)
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

        # 2. Backpressure Control
        semaphore = asyncio.Semaphore(self._max_concurrent_requests)

        # 3. Active Task Tracking (for cleanup)
        active_tasks = set()

        unpacker = msgpack.Unpacker(raw=False)

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
            writer.close()
            await writer.wait_closed()


class MessagePackRpcClient:
    def __init__(self, host="localhost", port=8888):
        self._host = host
        self._port = port
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

        unpacker = msgpack.Unpacker(raw=False)
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

import asyncio
import inspect
import logging
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

from app import MessagePackConfig
from app.common import CloudEvent, CloudEventProcessor, ProtocolHandler
from app.dataclass import DataClassMixin, type_has_config_class
from app.redis import RedisKeySchema

logger = logging.getLogger("handler.msgpack")


class RpcMessageType(IntEnum):
    REQUEST = 0  # [0, msgid, method, params]
    RESPONSE = 1  # [1, msgid, error, result]
    NOTIFICATION = 2  # [2, method, params]


class MessagePackCloudEventProcessor(CloudEventProcessor):
    def __init__(
        self,
        instance_id: str,
        runtime_config: Any,
        topic_identifier: str | None = None,
        queue_size: int = 1,
    ):
        self._instance_id = instance_id
        self._runtime_config = runtime_config

    async def message_callback(
        self, request_cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        if request_cloudevent.type not in self._type_callbacks_in:
            logger.error(
                "No callback registered for cloud event type: %s",
                request_cloudevent.type,
            )
            return None

        payload_type = self._type_classes[request_cloudevent.type]
        callback: Callable[..., Any] = self._type_callbacks_in[request_cloudevent.type]
        try:
            request: DataClassMixin | bytes = request_cloudevent.unserialize_payload(
                payload_type,
                self._hidden_field_processors,
            )
        except ValueError as e:
            logger.error(e)
            return None
        logger.debug("Request before callback: %s", request)

        responses: list[DataClassMixin] | DataClassMixin | None = await callback(
            request
        )

        if responses is None:
            return None

        if not isinstance(responses, list):
            responses = [responses]

        response_cloudevents: list[CloudEvent] = []
        for response in responses:
            logger.debug("Response from callback: %s", response)
            if not type_has_config_class(type(response)):
                logger.warning("Response has no Config class")
                continue

            response_cloudevent = self.create_event()
            response_cloudevent.correlationid = request_cloudevent.correlationid
            response_cloudevent.causationid = request_cloudevent.id
            try:
                response_cloudevent.serialize_payload(
                    response,
                    self._hidden_field_processors,
                )
            except ValueError as e:
                logger.exception(
                    "Error serializing payload for message type %s: %s",
                    response_cloudevent.type,
                    e,
                )
                return None

            response_cloudevents.append(response_cloudevent)
        return response_cloudevents


class RpcDispatcher:
    def __init__(self):
        self._methods: dict[str, Callable] = {}

    def register(self, name: str, func: Callable):
        """Register a function to handle RPC calls."""
        self._methods[name] = func

    async def dispatch(self, method_name: str, params: list):
        """Finds the method and executes it."""
        if method_name not in self._methods:
            raise ValueError(f"Method '{method_name}' not found")

        func = self._methods[method_name]

        # Check if the function is a coroutine (async)
        if inspect.iscoroutinefunction(func):
            return await func(*params)
        else:
            return func(*params)


class MessagePackHandler(ProtocolHandler):
    _runtime_config: MessagePackConfig
    _redis_client: redis.Redis
    _redis_key_schema: RedisKeySchema
    _dispatcher: RpcDispatcher

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
            if isinstance(processor, MessagePackCloudEventProcessor):
                processor_response = await processor.process_event(cloudevent)
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

    def __init__(
        self,
        runtime_config: MessagePackConfig,
        redis_connection_pool: redis.ConnectionPool,
        redis_key_schema: RedisKeySchema,
        dispatcher: RpcDispatcher = RpcDispatcher(),
    ):
        self._runtime_config = runtime_config
        self._redis_client = redis.Redis(connection_pool=redis_connection_pool)
        try:
            self._redis_client.ping()
        except redis.RedisError as e:
            logger.error(f"Error connecting to Redis: {e}")
            raise
        self._redis_key_schema = redis_key_schema
        self._dispatcher = dispatcher
        self.register_method("publish", self.publish)
        self.register_method("heartbeat", self.heartbeat)

    async def _server(self) -> asyncio.Server:
        ssl_context = None
        if self._runtime_config.tls_cert_path.exists():
            ssl_context = ssl.create_default_context(
                cafile=str(self._runtime_config.tls_cert_path)
            )
        return await asyncio.start_server(
            self._handle_client,
            self._runtime_config.hostname,
            self._runtime_config.port,
            ssl=ssl_context,
            backlog=self._runtime_config.max_queued_connections,
        )

    def register_method(self, name, func):
        self._dispatcher.register(name, func)

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
                result = await self._dispatcher.dispatch(method, params)
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
        semaphore = asyncio.Semaphore(self._runtime_config.max_concurrent_requests)

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

    async def task(self) -> None:
        logger.info("Starting MessagePack handler task")
        logger.info(
            "MessagePack-RPC Server running on %s:%d",
            self._runtime_config.hostname,
            self._runtime_config.port,
        )
        server = await self._server()
        try:
            async with server:
                await server.serve_forever()
        except asyncio.CancelledError:
            logger.info("MessagePack handler task cancelled; shutting down")
            await self._redis_client.aclose()
            raise


class OTELInstrumentedMessagePackHandler(MessagePackHandler):
    _tracer: trace.Tracer
    _meter: metrics.Meter
    _metrics: dict[str, metrics.Instrument]

    def __init__(
        self,
        runtime_config: MessagePackConfig,
        redis_connection_pool: redis.ConnectionPool,
        redis_key_schema: RedisKeySchema,
        rpc_dispatcher: RpcDispatcher = RpcDispatcher(),
    ):
        super().__init__(
            runtime_config, redis_connection_pool, redis_key_schema, rpc_dispatcher
        )

        self._tracer = trace.get_tracer(__name__)
        self._meter = metrics.get_meter(__name__)
        self._metrics = {
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

    async def _handle_rpc_task(
        self, writer, lock, semaphore, msg_type, msg_id, method, params
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
            rpc_attributes.RPC_METHOD: method,
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
            await super()._handle_rpc_task(
                writer, lock, semaphore, msg_type, msg_id, method, params
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

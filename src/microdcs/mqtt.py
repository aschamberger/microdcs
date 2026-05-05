import asyncio
import dataclasses
import enum
import functools
import logging
import re
import ssl
import time
import uuid

import aiomqtt
import mqtt5
import redis.asyncio as redis
from opentelemetry import metrics, trace
from opentelemetry.propagate import extract, inject
from opentelemetry.semconv._incubating.attributes import messaging_attributes
from opentelemetry.semconv._incubating.metrics import messaging_metrics
from opentelemetry.semconv.attributes import error_attributes, server_attributes

from microdcs import MQTTConfig, ProcessingConfig
from microdcs.common import (
    AdditionalTask,
    CloudEvent,
    CloudEventProcessor,
    MessageIntent,
    ProtocolBinding,
    ProtocolHandler,
)
from microdcs.redis import CloudEventDedupeDAO, RedisKeySchema

logger = logging.getLogger("handler.mqtt")
publisher_logger = logging.getLogger("publisher.mqtt")


class QoS(enum.IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


def create_mqtt_client(
    config: MQTTConfig,
    client_identifier: str | None = None,
    *,
    clean_start: bool = False,
    reconnect: bool = True,
) -> aiomqtt.Client:
    """Create an ``aiomqtt.Client`` from a :class:`MQTTConfig`.

    Shared between :class:`MQTTHandler` (subscriber) and
    :class:`MQTTPublisher` (retained-publish writer) to avoid duplicating
    connection setup.

    *client_identifier* overrides ``config.identifier`` when provided, which
    allows callers (e.g. :class:`MQTTPublisher`) to use a different MQTT
    client ID than the handler without changing the shared config object.

    *clean_start* and *reconnect* default to the long-lived handler settings
    (persistent session, auto-reconnect).  Pass ``clean_start=True,
    reconnect=False`` for short-lived clients such as integration-test helpers.
    """
    authentication_method: str | None = None
    authentication_data: bytes | None = None
    if config.sat_token_path.exists():
        with open(config.sat_token_path, "rb") as f:
            authentication_method = "K8S-SAT"
            authentication_data = f.read()
    ssl_context: ssl.SSLContext | None = None
    if config.tls_cert_path.exists():
        ssl_context = ssl.create_default_context(cafile=str(config.tls_cert_path))
    return aiomqtt.Client(
        hostname=config.hostname,
        port=config.port,
        identifier=client_identifier
        if client_identifier is not None
        else config.identifier,
        authentication_method=authentication_method,
        authentication_data=authentication_data,
        ssl_context=ssl_context,
        session_expiry_interval=config.session_expiry_interval,
        clean_start=clean_start,
        reconnect=reconnect,
    )


@functools.lru_cache(maxsize=256)
def _compile_topic_pattern(pattern: str) -> re.Pattern[str]:
    """Compile an MQTT subscription pattern to a regex.

    Patterns are fixed at binding setup time, so the cache hit rate is
    effectively 100% after warm-up.
    """
    escaped = re.escape(pattern)
    regex = escaped.replace(r"\+", "[^/]*").replace(r"/\#", "(|/.*)")
    return re.compile(regex)


def _topic_matches(pattern: str, topic: str) -> bool:
    """Check if an MQTT topic matches a subscription pattern.

    Supports '+' (single-level wildcard) and '#' (multi-level wildcard).
    Shared subscription prefixes (``$share/<group>/``) in *pattern* are
    stripped before matching, because the broker delivers messages with the
    original topic (without the prefix).
    """
    if pattern.startswith("$share/"):
        pattern = pattern.split("/", 2)[2]
    if pattern == "#":
        return True
    return bool(_compile_topic_pattern(pattern).fullmatch(topic))


class MQTTHandler(ProtocolHandler["MQTTProtocolBinding"]):
    def __init__(
        self,
        runtime_config: MQTTConfig,
        redis_connection_pool: redis.ConnectionPool,
        redis_key_schema: RedisKeySchema,
    ):
        super().__init__()
        self._runtime_config: MQTTConfig = runtime_config
        self._redis_client: redis.Redis = redis.Redis(
            connection_pool=redis_connection_pool
        )
        self._redis_key_schema: RedisKeySchema = redis_key_schema
        self._cloudevent_dedupe_dao: CloudEventDedupeDAO = CloudEventDedupeDAO(
            self._redis_client,
            redis_key_schema,
            ttl=self._runtime_config.dedupe_ttl_seconds,
        )
        self._expiration_timeout_tasks: dict[str, asyncio.Task[object]] = {}

    def _client(self) -> aiomqtt.Client:
        client = create_mqtt_client(
            self._runtime_config,
            client_identifier=self._runtime_config.identifier + "-proc",
        )
        return client

    async def _publish_message(
        self,
        client: aiomqtt.Client,
        cloudevent: CloudEvent,
        processor: CloudEventProcessor | None = None,
    ) -> None:
        if (
            cloudevent.transportmetadata is None
            or cloudevent.transportmetadata.get("mqtt_topic") is None
        ):
            logger.error("No topic specified for publishing message")
            return None
        logger.debug(
            "Publishing message to topic %s",
            cloudevent.transportmetadata.get("mqtt_topic"),
        )
        qos: mqtt5.QoS = mqtt5.QoS.AT_MOST_ONCE
        message_expiry_interval: int | None = None
        if cloudevent.expiryinterval is not None:
            message_expiry_interval = int(cloudevent.expiryinterval)
        content_type: str | None = None
        if cloudevent.datacontenttype is not None:
            content_type = cloudevent.datacontenttype
        response_topic: str | None = None
        if (
            cloudevent.transportmetadata is not None
            and cloudevent.transportmetadata.get("mqtt_response_topic") is not None
        ):
            response_topic = cloudevent.transportmetadata.get("mqtt_response_topic")
            qos = mqtt5.QoS.AT_LEAST_ONCE
        _correlation_data_id = (
            cloudevent.causationid
            if cloudevent.causationid is not None
            else cloudevent.id
        )
        correlation_data: bytes | None = None
        if _correlation_data_id is not None:
            correlation_data = uuid.UUID(_correlation_data_id).bytes
        # Convert dictionary to list of tuples for user properties
        user_properties: list[tuple[str, str]] = list(
            cloudevent.to_dict(
                context={"remove_data": True, "make_str_values": True}
            ).items()
        )
        await client.publish(
            cloudevent.transportmetadata.get("mqtt_topic", ""),
            cloudevent.data or b"",
            qos=qos,
            packet_id=next(client.packet_ids)
            if qos != mqtt5.QoS.AT_MOST_ONCE
            else None,
            retain=cloudevent.transportmetadata.get("mqtt_retain", False)
            if cloudevent.transportmetadata
            else False,
            message_expiry_interval=message_expiry_interval,
            content_type=content_type,
            response_topic=response_topic,
            correlation_data=correlation_data,
            user_properties=user_properties,
        )
        # schedule expiration handling if applicable
        if (
            processor is not None
            and cloudevent.id is not None
            and cloudevent.expiryinterval is not None
            and int(cloudevent.expiryinterval) > 0
            and cloudevent.transportmetadata is not None
            and cloudevent.transportmetadata.get("mqtt_response_topic") is not None
        ):
            _interval = int(cloudevent.expiryinterval)

            async def _expiration_task(
                _ce=cloudevent, _interval=_interval, _proc=processor
            ) -> list[CloudEvent] | CloudEvent | None:
                await asyncio.sleep(_interval)
                return await _proc.handle_cloudevent_expiration(_ce, _interval)

            _expiration_key = (
                cloudevent.causationid
                if cloudevent.causationid is not None
                else cloudevent.id
            )
            self._expiration_timeout_tasks[_expiration_key] = asyncio.create_task(
                _expiration_task()
            )
            self._expiration_timeout_tasks[_expiration_key].add_done_callback(
                lambda _task, _id=_expiration_key: (
                    logger.error(
                        "Expiration task for event %s failed: %s",
                        _id,
                        _task.exception(),
                    )
                    if not _task.cancelled() and _task.exception() is not None
                    else None,
                    self._expiration_timeout_tasks.pop(_id, None),
                )
            )

    async def _is_duplicate_message(self, cloudevent: CloudEvent) -> bool:
        logger.debug(
            "Checking for duplicate message with source %s ID %s",
            cloudevent.source,
            cloudevent.id,
        )
        return await self._cloudevent_dedupe_dao.is_duplicate(
            str(cloudevent.source), str(cloudevent.id)
        )

    def _cloudevent_from_message(self, message: mqtt5.PublishPacket) -> CloudEvent:
        # Construct CloudEvent from MQTT message
        cloudevent = CloudEvent(data=message.payload)
        # Populate transport metadata
        cloudevent.transportmetadata = {
            "mqtt_message_id": message.packet_id,
            "mqtt_topic": message.topic,
            "mqtt_qos": QoS(int(message.qos)),
            "mqtt_retain": message.retain,
        }
        # Populate from MQTT 5 properties (now direct attributes on PublishPacket)
        if message.message_expiry_interval is not None:
            cloudevent.expiryinterval = message.message_expiry_interval
        if message.content_type is not None:
            cloudevent.datacontenttype = str(message.content_type)
        if message.response_topic is not None:
            cloudevent.transportmetadata["mqtt_response_topic"] = str(
                message.response_topic
            )
        if message.correlation_data is not None:
            cloudevent.transportmetadata["mqtt_correlation_data"] = str(
                uuid.UUID(bytes=message.correlation_data)
            )
        if message.user_properties is not None:
            # Convert list of tuples to dictionary
            cloudevent.custommetadata = dict(message.user_properties)
        # Populate CloudEvent attributes from user properties if present
        for field in dataclasses.fields(CloudEvent):
            if (
                cloudevent.custommetadata is not None
                and field.name in cloudevent.custommetadata
            ):
                setattr(
                    cloudevent,
                    field.name,
                    cloudevent.custommetadata[field.name],
                )
                del cloudevent.custommetadata[field.name]
        return cloudevent

    async def _process_message(
        self, client: aiomqtt.Client, message: mqtt5.PublishPacket
    ) -> tuple[bool, str]:
        # extract CloudEvent from MQTT message
        cloudevent = self._cloudevent_from_message(message)
        # check for duplicate message IDs due to QoS 1 (at-least-once delivery)
        if await self._is_duplicate_message(cloudevent):
            logger.info(
                "Duplicate message received on topic %s with message ID %s",
                message.topic,
                message.packet_id,
            )
            for binding in self._bindings:
                if _topic_matches(binding.response_topic, message.topic):
                    return False, binding.response_topic
                for topic in binding.topics:
                    if _topic_matches(topic, message.topic):
                        return False, topic
            return False, ""
        else:
            logger.debug("Received message on topic %s", message.topic)

        # cancel expiration timeout task if applicable
        # responses carry causationid = original request id, matching the expiration task key
        if (
            cloudevent.causationid is not None
            and cloudevent.causationid in self._expiration_timeout_tasks
        ):
            self._expiration_timeout_tasks[cloudevent.causationid].cancel()

        # Dispatch message to registered processors
        # It is assumed that each message is processed by only one processor
        # If multiple processors match the topic, all will be invoked sequentially
        subscription: list[str] = []
        for binding in self._bindings:
            if _topic_matches(binding.response_topic, message.topic):
                subscription.append(binding.response_topic)
                processor_response = (
                    await binding.processor.process_response_cloudevent(cloudevent)
                )
                if isinstance(processor_response, list):
                    for response in processor_response:
                        await self._publish_message(client, response, binding.processor)
                elif isinstance(processor_response, CloudEvent):
                    await self._publish_message(
                        client, processor_response, binding.processor
                    )
                elif processor_response is None:
                    continue
            for topic in binding.topics:
                if _topic_matches(topic, message.topic):
                    subscription.append(topic)
                    processor_response = await binding.processor.process_cloudevent(
                        cloudevent
                    )
                    if isinstance(processor_response, list):
                        for response in processor_response:
                            binding.enrich_response_transportmetadata(
                                response, cloudevent
                            )
                            await self._publish_message(
                                client, response, binding.processor
                            )
                    elif isinstance(processor_response, CloudEvent):
                        binding.enrich_response_transportmetadata(
                            processor_response, cloudevent
                        )
                        await self._publish_message(
                            client, processor_response, binding.processor
                        )
                    elif processor_response is None:
                        continue

        # Acknowledge QoS 1 messages using the native puback method
        if message.packet_id is not None:
            await client.puback(message.packet_id)

        return True, ", ".join(subscription)

    async def _process_messages(self, client: aiomqtt.Client) -> None:
        logger.info("Starting MQTT message processing")
        async for message in client.messages():
            if isinstance(message, aiomqtt.PubRelPacket):
                continue  # skip QoS 2 pubrel packets (not used in this handler)
            # Shield message processing from cancellation so that in-flight
            # messages are fully processed, ACKed, and expiration tasks set up.
            processing = asyncio.create_task(self._process_message(client, message))
            try:
                await asyncio.shield(processing)
            except asyncio.CancelledError:
                # Worker cancelled, but finish processing the current message
                await processing
                return

    async def _outgoing_message_publisher(
        self, client: aiomqtt.Client, binding: MQTTProtocolBinding
    ) -> None:
        while True:
            cloudevent, intent = await binding.outgoing_queue.get()
            # Enrich with MQTT transport metadata from the binding
            binding.enrich_publish_transportmetadata(intent, cloudevent)
            # Retry publish if the connection is temporarily down
            while True:
                try:
                    await self._publish_message(client, cloudevent, binding.processor)
                    break
                except (
                    aiomqtt.ConnectError,
                    aiomqtt.ProtocolError,
                    aiomqtt.NegativeAckError,
                ):
                    logger.warning(
                        "Publish failed while reconnecting; waiting for connection"
                    )
                    await client.connected()
            binding.outgoing_queue.task_done()

    async def _cancel_and_wait(self, tasks: list[asyncio.Task]) -> None:
        """Cancel tasks and wait for them to finish."""
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def task(self) -> None:
        logger.info("Starting MQTT handler task")
        logger.info(
            "Connecting to MQTT Server running on %s:%d",
            self._runtime_config.hostname,
            self._runtime_config.port,
        )
        # redis is required for message deduplication and expiration handling,
        # so we check the connection before starting the MQTT client
        try:
            await self._redis_client.ping()  # pyright: ignore[reportGeneralTypeIssues]
        except redis.RedisError as e:
            logger.error(f"Error connecting to Redis: {e}")
            raise
        client: aiomqtt.Client = self._client()
        try:
            async with client:
                for binding in self._bindings:
                    for topic in binding.topics:
                        await client.subscribe(aiomqtt.TopicFilter(topic))
                        logger.info("Subscribed to topic: %s", topic)
                    await client.subscribe(aiomqtt.TopicFilter(binding.response_topic))
                    logger.info(
                        "Subscribed to response topic: %s",
                        binding.response_topic,
                    )
                # Start worker and publisher tasks (manual management
                # instead of TaskGroup for controlled graceful shutdown)
                worker_tasks: list[asyncio.Task] = []
                publisher_tasks: list[asyncio.Task] = []
                logger.info(
                    "Starting %d message worker tasks",
                    self._runtime_config.message_workers,
                )
                for _ in range(self._runtime_config.message_workers):
                    worker_tasks.append(
                        asyncio.create_task(self._process_messages(client))
                    )
                for binding in self._bindings:
                    publisher_tasks.append(
                        asyncio.create_task(
                            self._outgoing_message_publisher(client, binding)
                        )
                    )
                all_tasks = worker_tasks + publisher_tasks

                try:
                    # Wait for shutdown event or an unexpected task failure
                    shutdown_waiter = asyncio.create_task(self._shutdown_event.wait())
                    done, _ = await asyncio.wait(
                        set(all_tasks) | {shutdown_waiter},
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if shutdown_waiter in done:
                        # === Graceful shutdown sequence ===
                        # Phase 1: Unsubscribe – stop receiving new messages
                        logger.info("Graceful shutdown: unsubscribing from MQTT topics")
                        for binding in self._bindings:
                            for topic in binding.topics:
                                try:
                                    await client.unsubscribe(topic)
                                    logger.info("Unsubscribed from topic: %s", topic)
                                except (
                                    aiomqtt.ConnectError,
                                    aiomqtt.ProtocolError,
                                    aiomqtt.NegativeAckError,
                                ):
                                    logger.warning(
                                        "Failed to unsubscribe from topic: %s",
                                        topic,
                                    )
                            try:
                                await client.unsubscribe(binding.response_topic)
                                logger.info(
                                    "Unsubscribed from response topic: %s",
                                    binding.response_topic,
                                )
                            except (
                                aiomqtt.ConnectError,
                                aiomqtt.ProtocolError,
                                aiomqtt.NegativeAckError,
                            ):
                                logger.warning(
                                    "Failed to unsubscribe from response topic: %s",
                                    binding.response_topic,
                                )

                        # Phase 2: Cancel workers – shielded processing
                        # ensures in-flight messages finish (process, ack,
                        # create expiration tasks, enqueue responses)
                        await self._cancel_and_wait(worker_tasks)
                        logger.info("All message workers completed")

                        # Phase 3: Drain outgoing queues – send remaining
                        # outgoing events that were enqueued during processing
                        for binding in self._bindings:
                            if not binding.outgoing_queue.empty():
                                logger.info(
                                    "Draining outgoing queue (%d items)",
                                    binding.outgoing_queue.qsize(),
                                )
                                await binding.outgoing_queue.join()

                        # Phase 4: Cancel publishers – queues are drained
                        await self._cancel_and_wait(publisher_tasks)
                        logger.info("All publishers completed")

                        # Phase 5: Wait for expiration timeout tasks
                        pending = [
                            task
                            for task in self._expiration_timeout_tasks.values()
                            if not task.done()
                        ]
                        if pending:
                            logger.info(
                                "Waiting for %d expiration timeout task(s)",
                                len(pending),
                            )
                            await asyncio.gather(*pending, return_exceptions=True)

                        logger.info("MQTT graceful shutdown complete")
                    else:
                        # A worker/publisher died unexpectedly
                        shutdown_waiter.cancel()
                        await self._cancel_and_wait(all_tasks)
                        for task in done:
                            if not task.cancelled() and task.exception() is not None:
                                raise task.exception()  # pyright: ignore[reportGeneralTypeIssues]

                except asyncio.CancelledError:
                    # Force shutdown (grace period exceeded) –
                    # cancel everything including expiration tasks
                    await self._cancel_and_wait(all_tasks)
                    for task in list(self._expiration_timeout_tasks.values()):
                        if not task.done():
                            task.cancel()
                    raise

        except asyncio.CancelledError:
            logger.info("MQTT handler task cancelled; shutting down")
            for task in list(self._expiration_timeout_tasks.values()):
                if not task.done():
                    task.cancel()
            raise

        logger.info("MQTT handler shutdown complete")


class OTELInstrumentedMQTTHandler(MQTTHandler):
    def __init__(
        self,
        runtime_config: MQTTConfig,
        redis_connection_pool: redis.ConnectionPool,
        redis_key_schema: "RedisKeySchema",
    ):
        super().__init__(runtime_config, redis_connection_pool, redis_key_schema)

        self._tracer: trace.Tracer = trace.get_tracer(__name__)
        self._meter: metrics.Meter = metrics.get_meter(__name__)
        self._consumed_messages = (
            messaging_metrics.create_messaging_client_consumed_messages(self._meter)
        )
        self._process_duration = messaging_metrics.create_messaging_process_duration(
            self._meter
        )
        self._operation_duration = (
            messaging_metrics.create_messaging_client_operation_duration(self._meter)
        )
        self._sent_messages = messaging_metrics.create_messaging_client_sent_messages(
            self._meter
        )

    async def _process_message(
        self, client: aiomqtt.Client, message: mqtt5.PublishPacket
    ) -> tuple[bool, str]:
        # extract creation context from MQTT user properties for trace linking
        links: list[trace.Link] = []
        if message.user_properties is not None:
            user_props = dict(message.user_properties)
            if user_props:
                creation_ctx = extract(user_props)
                creation_span_ctx = trace.get_current_span(
                    creation_ctx
                ).get_span_context()
                if creation_span_ctx.is_valid:
                    links = [trace.Link(creation_span_ctx)]

        # span attributes (high-cardinality destination name kept on span only)
        span_attrs: dict[str, Any] = {
            messaging_attributes.MESSAGING_SYSTEM: "mqtt",
            messaging_attributes.MESSAGING_OPERATION_TYPE: messaging_attributes.MessagingOperationTypeValues.PROCESS.value,
            messaging_attributes.MESSAGING_OPERATION_NAME: "process",
            messaging_attributes.MESSAGING_DESTINATION_NAME: message.topic,
            messaging_attributes.MESSAGING_CLIENT_ID: self._runtime_config.identifier,
            messaging_attributes.MESSAGING_MESSAGE_ID: str(message.packet_id),
            server_attributes.SERVER_ADDRESS: self._runtime_config.hostname,
            server_attributes.SERVER_PORT: self._runtime_config.port,
        }
        if isinstance(message.payload, (bytes, bytearray)):
            span_attrs[messaging_attributes.MESSAGING_MESSAGE_BODY_SIZE] = len(
                message.payload
            )

        # metric attributes (no high-cardinality destination name)
        metric_attrs: dict[str, Any] = {
            messaging_attributes.MESSAGING_SYSTEM: "mqtt",
            messaging_attributes.MESSAGING_OPERATION_TYPE: messaging_attributes.MessagingOperationTypeValues.PROCESS.value,
            messaging_attributes.MESSAGING_OPERATION_NAME: "process",
            server_attributes.SERVER_ADDRESS: self._runtime_config.hostname,
            server_attributes.SERVER_PORT: self._runtime_config.port,
        }

        # count delivery before processing (tracks messages dispatched to application)
        self._consumed_messages.add(1, metric_attrs)

        start = time.monotonic()
        error_type: str | None = None
        ok: bool = False
        subscription: str = ""

        with self._tracer.start_as_current_span(
            "process",
            kind=trace.SpanKind.CONSUMER,
            attributes=span_attrs,
            links=links,
        ) as span:
            try:
                ok, subscription = await super()._process_message(client, message)
                span.set_attribute(
                    messaging_attributes.MESSAGING_DESTINATION_SUBSCRIPTION_NAME,
                    subscription,
                )
            except Exception as exc:
                error_type = type(exc).__qualname__
                span.record_exception(exc)
                span.set_status(trace.Status(trace.StatusCode.ERROR))
                raise
            finally:
                duration = time.monotonic() - start
                if error_type is not None:
                    self._process_duration.record(
                        duration,
                        metric_attrs | {error_attributes.ERROR_TYPE: error_type},
                    )
                else:
                    self._process_duration.record(duration, metric_attrs)

        return ok, subscription

    async def _publish_message(
        self,
        client: aiomqtt.Client,
        cloudevent: CloudEvent,
        processor: CloudEventProcessor | None = None,
    ) -> None:
        topic = (
            cloudevent.transportmetadata.get("mqtt_topic")
            if cloudevent.transportmetadata is not None
            else None
        )
        if topic is None:
            await super()._publish_message(client, cloudevent, processor)
            return

        span_attrs: dict[str, Any] = {
            messaging_attributes.MESSAGING_SYSTEM: "mqtt",
            messaging_attributes.MESSAGING_OPERATION_TYPE: messaging_attributes.MessagingOperationTypeValues.SEND.value,
            messaging_attributes.MESSAGING_OPERATION_NAME: "publish",
            messaging_attributes.MESSAGING_DESTINATION_NAME: str(topic),
            messaging_attributes.MESSAGING_CLIENT_ID: self._runtime_config.identifier,
            server_attributes.SERVER_ADDRESS: self._runtime_config.hostname,
            server_attributes.SERVER_PORT: self._runtime_config.port,
        }
        if isinstance(cloudevent.data, (bytes, bytearray)):
            span_attrs[messaging_attributes.MESSAGING_MESSAGE_BODY_SIZE] = len(
                cloudevent.data
            )

        metric_attrs: dict[str, Any] = {
            messaging_attributes.MESSAGING_SYSTEM: "mqtt",
            messaging_attributes.MESSAGING_OPERATION_TYPE: messaging_attributes.MessagingOperationTypeValues.SEND.value,
            messaging_attributes.MESSAGING_OPERATION_NAME: "publish",
            server_attributes.SERVER_ADDRESS: self._runtime_config.hostname,
            server_attributes.SERVER_PORT: self._runtime_config.port,
        }

        start = time.monotonic()
        error_type: str | None = None

        with self._tracer.start_as_current_span(
            "publish",
            kind=trace.SpanKind.PRODUCER,
            attributes=span_attrs,
        ):
            # inject current span context into custommetadata so it propagates as MQTT
            # UserProperty — CloudEvent.__post_serialize__ flattens custommetadata keys
            trace_headers: dict[str, str] = {}
            inject(trace_headers)
            if trace_headers:
                if cloudevent.custommetadata is None:
                    cloudevent.custommetadata = {}
                cloudevent.custommetadata.update(trace_headers)

            try:
                await super()._publish_message(client, cloudevent, processor)
            except Exception as exc:
                error_type = type(exc).__qualname__
                trace.get_current_span().record_exception(exc)
                trace.get_current_span().set_status(
                    trace.Status(trace.StatusCode.ERROR)
                )
                raise
            finally:
                duration = time.monotonic() - start
                if error_type is not None:
                    err_attrs = metric_attrs | {error_attributes.ERROR_TYPE: error_type}
                    self._operation_duration.record(duration, err_attrs)
                    self._sent_messages.add(1, err_attrs)
                else:
                    self._operation_duration.record(duration, metric_attrs)
                    self._sent_messages.add(1, metric_attrs)


class MQTTProtocolBinding(ProtocolBinding["MQTTHandler"]):
    def __init__(
        self,
        processor: CloudEventProcessor,
        processing_config: ProcessingConfig,
        mqtt_config: MQTTConfig,
        mqtt_path_from_subject: bool = True,
        outgoing_ce_type_filter: set[str] = set(),
    ):
        super().__init__(
            processor,
            processing_config,
            mqtt_config.binding_outgoing_queue_size,
            outgoing_ce_type_filter,
        )
        self._mqtt_config = mqtt_config
        self.mqtt_path_from_subject = mqtt_path_from_subject

        # Resolve topic prefix/wildcard levels for this processor
        self.topic_prefix = processor._runtime_config.get_topic_prefix_for_identifier(
            processor._config_identifier
        )
        if self.topic_prefix is None:
            raise ValueError(
                f"No topic prefix found for identifier '{processor._config_identifier}' "
                f"in APP_PROCESSING_TOPIC_PREFIX"
            )
        topic_wildcard_levels = (
            processor._runtime_config.get_wildcard_levels_for_identifier(
                processor._config_identifier
            )
        )
        self.topic_discriminator: str | None = (
            processor._runtime_config.get_discriminator_for_identifier(
                processor._config_identifier
            )
        )

        # Build subscribe topics from prefix + subscribe intents
        self.topics: set[str] = set()
        wildcard_string = "/+"
        for intent in processor.subscribe_intents():
            for i in range(0, topic_wildcard_levels + 1):
                if self.topic_discriminator:
                    topic = f"{self.topic_prefix}{wildcard_string * i}/{self.topic_discriminator}/{intent.value}"
                else:
                    topic = f"{self.topic_prefix}{wildcard_string * i}/{intent.value}"
                if processor._runtime_config.shared_subscription_name:
                    topic = f"$share/{processor._runtime_config.shared_subscription_name}/{topic}"
                self.topics.add(topic)

        processing_config.check_topic_discriminator_uniqueness()

        # Resolve response topic for this processor
        response_topic_base = (
            processor._runtime_config.get_response_topic_for_identifier(
                processor._config_identifier
            )
        )
        self.response_topic = f"{response_topic_base}/{processor._instance_id}"

        processor.register_publish_handler(self.publish_handler)

        self.publish_intents = processor.publish_intents()

        logger.info(
            "Registered %s processor '%s' | subscribes: %s | publishes: %s | response: %s",
            processor.binding.value,
            processor._instance_id,
            self.topics,
            self.publish_intents,
            self.response_topic,
        )

    def enrich_publish_transportmetadata(
        self, intent: MessageIntent | None, cloudevent: CloudEvent
    ) -> None:

        if cloudevent.transportmetadata is None:
            cloudevent.transportmetadata = {}

        # Resolve publish topic from intent + publish_topic_patterns
        if "mqtt_topic" not in cloudevent.transportmetadata:
            if intent is None or intent not in self.publish_intents:
                # no topic prefix raises exception already in __init__, so we can assume topic_prefix is always set here
                cloudevent.transportmetadata["mqtt_topic"] = self.topic_prefix  # type: ignore
            elif self.mqtt_path_from_subject and cloudevent.subject is not None:
                if self.topic_discriminator:
                    cloudevent.transportmetadata["mqtt_topic"] = (
                        f"{self.topic_prefix}/{cloudevent.subject.replace('.', '/')}/{self.topic_discriminator}/{intent.value}"
                    )
                else:
                    cloudevent.transportmetadata["mqtt_topic"] = (
                        f"{self.topic_prefix}/{cloudevent.subject.replace('.', '/')}/{intent.value}"
                    )
            else:
                if self.topic_discriminator:
                    cloudevent.transportmetadata["mqtt_topic"] = (
                        f"{self.topic_prefix}/{self.topic_discriminator}/{intent.value}"
                    )
                else:
                    cloudevent.transportmetadata["mqtt_topic"] = (
                        f"{self.topic_prefix}/{intent.value}"
                    )

        # Set our backchannel response topic if not already set
        if (
            "mqtt_response_topic" not in cloudevent.transportmetadata
            and intent == MessageIntent.COMMAND
            and self.response_topic
        ):
            cloudevent.transportmetadata["mqtt_response_topic"] = self.response_topic

    def enrich_response_transportmetadata(
        self,
        response: CloudEvent,
        request: CloudEvent,
    ) -> None:
        if response.transportmetadata is None:
            response.transportmetadata = {}
        # If the response already has a topic (e.g. raw echo), keep it
        if "mqtt_topic" not in response.transportmetadata:
            if request.transportmetadata and request.transportmetadata.get(
                "mqtt_response_topic"
            ):
                response.transportmetadata["mqtt_topic"] = request.transportmetadata[
                    "mqtt_response_topic"
                ]
            else:
                logger.warning("No response topic specified; cannot publish response.")
        # Set our backchannel response topic if not already set on the response
        if (
            "mqtt_response_topic" not in response.transportmetadata
            and self.response_topic
        ):
            response.transportmetadata["mqtt_response_topic"] = self.response_topic


class MQTTPublisher(AdditionalTask):
    """MQTT publisher for retained messages with TTL and zero-byte delete.

    Extends :class:`AdditionalTask` so it can be registered with
    :class:`~microdcs.core.MicroDCS` via :meth:`add_additional_task` and
    run alongside protocol handlers in the main task group.

    The :meth:`task` method manages the MQTT connection lifecycle with
    automatic reconnect.  Override :meth:`_run` in subclasses to perform
    work while connected (the default implementation waits for shutdown).
    :meth:`publish_retained` and :meth:`delete_retained` are available
    while the connection is active.
    """

    def __init__(self, config: MQTTConfig) -> None:
        super().__init__()
        self._config = config
        self._client: aiomqtt.Client | None = None
        self._connected: asyncio.Event = asyncio.Event()

    async def publish_retained(
        self,
        topic: str,
        payload: bytes | str,
        ttl: int,
    ) -> None:
        """Publish a retained message with an MQTT v5 Message Expiry Interval.

        Args:
            topic: The MQTT topic to publish to.
            payload: The message payload (bytes or UTF-8 string).
            ttl: Message Expiry Interval in seconds.
        """
        assert self._client is not None, "Client not connected — use from within task()"
        publisher_logger.debug("Publishing retained message to %s (ttl=%d)", topic, ttl)
        await self._client.publish(
            topic,
            payload.encode() if isinstance(payload, str) else payload,
            qos=aiomqtt.QoS.AT_LEAST_ONCE,
            packet_id=next(self._client.packet_ids),
            retain=True,
            message_expiry_interval=ttl,
        )

    async def delete_retained(self, topic: str) -> None:
        """Delete a retained topic by publishing a zero-byte retained message.

        Args:
            topic: The MQTT topic to clear.
        """
        assert self._client is not None, "Client not connected — use from within task()"
        publisher_logger.debug("Deleting retained topic %s", topic)
        await self._client.publish(
            topic,
            b"",
            qos=aiomqtt.QoS.AT_LEAST_ONCE,
            packet_id=next(self._client.packet_ids),
            retain=True,
        )

    async def _run(self) -> None:
        """Execute while the MQTT connection is active.

        Override in subclasses to perform work (e.g. stream processing).
        The default implementation waits for the shutdown event.  Any
        MQTT error raised here is caught by :meth:`task`
        which triggers reconnection.
        """
        await self._shutdown_event.wait()

    async def task(self) -> None:
        publisher_logger.info(
            "Starting MQTT publisher, connecting to %s:%d",
            self._config.hostname,
            self._config.port,
        )
        client = create_mqtt_client(
            self._config,
            client_identifier=self._config.identifier + "-pub",
        )
        async with client:
            self._client = client
            self._connected.set()
            publisher_logger.info("MQTT publisher connected")
            while True:
                try:
                    await self._run()
                    break  # normal exit (shutdown event)
                except (
                    aiomqtt.ConnectError,
                    aiomqtt.ProtocolError,
                    aiomqtt.NegativeAckError,
                ):
                    publisher_logger.warning(
                        "Connection lost in _run(); waiting for reconnect"
                    )
                    await client.connected()
            self._client = None
            self._connected.clear()
        publisher_logger.info("MQTT publisher shutdown complete")

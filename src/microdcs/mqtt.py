import asyncio
import dataclasses
import enum
import logging
import random
import time
import uuid
from typing import Any

import aiomqtt
import paho.mqtt.client
import redis.asyncio as redis
from opentelemetry import metrics, trace
from opentelemetry.propagate import extract
from opentelemetry.semconv._incubating.attributes import messaging_attributes
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

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


def create_mqtt_client(config: MQTTConfig, **kwargs: Any) -> aiomqtt.Client:
    """Create an ``aiomqtt.Client`` from a :class:`MQTTConfig`.

    Shared between :class:`MQTTHandler` (subscriber) and
    :class:`MQTTPublisher` (retained-publish writer) to avoid duplicating
    connection setup.  Extra *kwargs* are forwarded to the
    ``aiomqtt.Client`` constructor (e.g. ``clean_start``,
    ``max_queued_incoming_messages``).
    """
    properties = None
    if config.sat_token_path.exists():
        with open(config.sat_token_path, "rb") as f:
            sat_token = f.read()
            properties = Properties(PacketTypes.CONNECT)
            properties.AuthenticationMethod = "K8S-SAT"
            properties.AuthenticationData = sat_token
    tls_params = None
    if config.tls_cert_path.exists():
        tls_params = aiomqtt.TLSParameters(ca_certs=str(config.tls_cert_path))
    return aiomqtt.Client(
        protocol=aiomqtt.ProtocolVersion.V5,
        hostname=config.hostname,
        port=config.port,
        identifier=config.identifier,
        timeout=config.connect_timeout,
        properties=properties,
        tls_params=tls_params,
        **kwargs,
    )


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
        self._expiration_timeout_tasks: dict[str, Any] = {}

    def _client(self) -> aiomqtt.Client:
        client = create_mqtt_client(
            self._runtime_config,
            clean_start=paho.mqtt.client.MQTT_CLEAN_START_FIRST_ONLY,
            max_queued_incoming_messages=self._runtime_config.incoming_queue_size,
            max_queued_outgoing_messages=self._runtime_config.outgoing_queue_size,
        )
        # FIXME: set this as a aiomqtt client property when https://github.com/empicano/aiomqtt/pull/346 is merged
        client._client.manual_ack_set(True)  # type: ignore #
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
        qos: int = QoS.AT_MOST_ONCE
        properties = Properties(PacketTypes.PUBLISH)
        if cloudevent.expiryinterval is not None:
            properties.MessageExpiryInterval = cloudevent.expiryinterval
        if cloudevent.datacontenttype is not None:
            properties.ContentType = cloudevent.datacontenttype
            if cloudevent.datacontenttype == "application/octet-stream":
                properties.PayloadFormatIndicator = 0  # bytes
            else:
                properties.PayloadFormatIndicator = 1  # UTF-8 string
        if (
            cloudevent.transportmetadata is not None
            and cloudevent.transportmetadata.get("mqtt_response_topic") is not None
        ):
            properties.ResponseTopic = cloudevent.transportmetadata.get(
                "mqtt_response_topic"
            )
            qos = QoS.AT_LEAST_ONCE
        if cloudevent.correlationid is not None:
            properties.CorrelationData = uuid.UUID(cloudevent.correlationid).bytes  # type: ignore
        # Convert dictionary to list of tuples
        properties.UserProperty = list(
            cloudevent.to_dict(
                context={"remove_data": True, "make_str_values": True}
            ).items()
        )
        await client.publish(
            cloudevent.transportmetadata.get("mqtt_topic", ""),
            cloudevent.data,
            qos=qos,
            retain=cloudevent.transportmetadata.get("mqtt_retain", False)
            if cloudevent.transportmetadata
            else False,
            properties=properties,
            timeout=self._runtime_config.publish_timeout,
        )
        # schedule expiration handling if applicable
        if (
            processor is not None
            and cloudevent.id is not None
            and cloudevent.expiryinterval is not None
            and int(cloudevent.expiryinterval) > 0
        ):
            _interval = int(cloudevent.expiryinterval)

            async def _expiration_task(
                _ce=cloudevent, _interval=_interval, _proc=processor
            ) -> list[CloudEvent] | CloudEvent | None:
                await asyncio.sleep(_interval)
                return await _proc.handle_cloudevent_expiration(_ce, _interval)

            self._expiration_timeout_tasks[cloudevent.id] = asyncio.create_task(
                _expiration_task()
            )
            self._expiration_timeout_tasks[cloudevent.id].add_done_callback(
                lambda _task, _id=cloudevent.id: (
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

    def _cloudevent_from_message(self, message: aiomqtt.Message) -> CloudEvent:
        # Construct CloudEvent from MQTT message
        cloudevent = CloudEvent(data=message.payload)
        # Populate transport metadata
        cloudevent.transportmetadata = {
            "mqtt_message_id": message.mid,
            "mqtt_topic": str(message.topic),
            "mqtt_qos": QoS(message.qos),
            "mqtt_retain": message.retain,
        }
        # Populate from MQTT 5 properties
        if message.properties and hasattr(message.properties, "PayloadFormatIndicator"):
            pass
        if message.properties and hasattr(message.properties, "MessageExpiryInterval"):
            cloudevent.expiryinterval = message.properties.MessageExpiryInterval  # type: ignore
        if message.properties and hasattr(message.properties, "ContentType"):
            cloudevent.datacontenttype = str(message.properties.ContentType)  # type: ignore
        if message.properties and hasattr(message.properties, "ResponseTopic"):
            cloudevent.transportmetadata["mqtt_response_topic"] = str(
                message.properties.ResponseTopic  # type: ignore
            )
        if message.properties and hasattr(message.properties, "CorrelationData"):
            cloudevent.correlationid = str(
                uuid.UUID(bytes=message.properties.CorrelationData)  # type: ignore
            )
        if message.properties and hasattr(message.properties, "UserProperty"):
            # Convert list of tuples to dictionary
            cloudevent.custommetadata = dict(message.properties.UserProperty)  # type: ignore
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
        self, client: aiomqtt.Client, message: aiomqtt.Message
    ) -> tuple[bool, str]:
        # extract CloudEvent from MQTT message
        cloudevent = self._cloudevent_from_message(message)
        # check for duplicate message IDs due to QoS 1 (at-least-once delivery)
        if await self._is_duplicate_message(cloudevent):
            logger.info(
                "Duplicate message received on topic %s with message ID %d",
                message.topic,
                message.mid,
            )
            for binding in self._bindings:
                if message.topic.matches(binding.response_topic):
                    return False, binding.response_topic
                for topic in binding.topics:
                    if message.topic.matches(topic):
                        return False, topic
            return False, ""
        else:
            logger.debug("Received message on topic %s", message.topic)

        # cancel expiration timeout task if applicable
        # responses correlate back to the original request via correlationid
        if cloudevent.correlationid in self._expiration_timeout_tasks:
            self._expiration_timeout_tasks[cloudevent.correlationid].cancel()

        # Dispatch message to registered processors
        # It is assumed that each message is processed by only one processor
        # If multiple processors match the topic, all will be invoked sequentially
        subscription: list[str] = []
        for binding in self._bindings:
            if message.topic.matches(binding.response_topic):
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
                if message.topic.matches(topic):
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

        # FIXME: use the native ack method when https://github.com/empicano/aiomqtt/pull/346 is merged
        client._client.ack(message.mid, message.qos)  # type: ignore #

        return True, ", ".join(subscription)

    async def _process_messages(self, client: aiomqtt.Client) -> None:
        logger.info("Starting MQTT message processing")
        message: aiomqtt.Message
        async for message in client.messages:
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

            await self._publish_message(client, cloudevent, binding.processor)
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
        client: aiomqtt.Client = self._client()
        backoff = 1  # seconds
        max_backoff = 60  # seconds
        while True:
            try:
                # redis is required for message deduplication and expiration handling,
                # so we check the connection before starting the MQTT client
                try:
                    await self._redis_client.ping()  # pyright: ignore[reportGeneralTypeIssues]
                except redis.RedisError as e:
                    logger.error(f"Error connecting to Redis: {e}")
                    raise
                async with client:
                    backoff = 1  # reset backoff after successful connection
                    for binding in self._bindings:
                        for topic in binding.topics:
                            await client.subscribe(topic)
                            logger.info("Subscribed to topic: %s", topic)
                        await client.subscribe(binding.response_topic)
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
                        shutdown_waiter = asyncio.create_task(
                            self._shutdown_event.wait()
                        )
                        done, _ = await asyncio.wait(
                            set(all_tasks) | {shutdown_waiter},
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                        if shutdown_waiter in done:
                            # === Graceful shutdown sequence ===
                            # Phase 1: Unsubscribe – stop receiving new messages
                            logger.info(
                                "Graceful shutdown: unsubscribing from MQTT topics"
                            )
                            for binding in self._bindings:
                                for topic in binding.topics:
                                    try:
                                        await client.unsubscribe(topic)
                                        logger.info(
                                            "Unsubscribed from topic: %s", topic
                                        )
                                    except aiomqtt.MqttError:
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
                                except aiomqtt.MqttError:
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
                            break  # exit retry loop
                        else:
                            # A worker/publisher died unexpectedly
                            shutdown_waiter.cancel()
                            await self._cancel_and_wait(all_tasks)
                            for task in done:
                                if (
                                    not task.cancelled()
                                    and task.exception() is not None
                                ):
                                    raise task.exception()  # pyright: ignore[reportGeneralTypeIssues]

                    except asyncio.CancelledError:
                        # Force shutdown (grace period exceeded) –
                        # cancel everything including expiration tasks
                        await self._cancel_and_wait(all_tasks)
                        for task in list(self._expiration_timeout_tasks.values()):
                            if not task.done():
                                task.cancel()
                        raise

            except aiomqtt.MqttError:
                if self._shutdown_event.is_set():
                    logger.info("MQTT connection lost during shutdown; exiting")
                    break
                sleep_time = backoff + random.uniform(0, 0.1 * backoff)
                logger.warning(f"Connection lost. Retrying in {sleep_time:.2f}s...")
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=sleep_time
                    )
                    logger.info("Shutdown during reconnect backoff; exiting")
                    break
                except asyncio.TimeoutError:
                    backoff = min(backoff * 2, max_backoff)

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
        self._metrics: dict[str, metrics.Instrument] = {
            "process_counter": self._meter.create_counter(
                "messaging.process.counter",
                description="Count of MQTT messages processed",
            ),
            "process_duration": self._meter.create_histogram(
                "messaging.process.duration",
                description="Duration of MQTT message processing in milliseconds",
            ),
        }

    def record_metrics(
        self, duration: float, error: bool = False, base_attributes: dict[str, str] = {}
    ) -> None:
        attributes = base_attributes | {"status": "error" if error else "success"}
        self._metrics["process_counter"].add(1, attributes)  # pyright: ignore[reportAttributeAccessIssue]
        self._metrics["process_duration"].record(duration, attributes)  # pyright: ignore[reportAttributeAccessIssue]

    async def _process_message(
        self, client: aiomqtt.Client, message: aiomqtt.Message
    ) -> tuple[bool, str]:
        # start timing
        processing_start_time = time.time()
        # extract context from MQTT message properties
        context = None
        if message.properties and hasattr(message.properties, "UserProperty"):
            user_properties = dict(message.properties.UserProperty)  # type: ignore
            context = extract(user_properties) if user_properties else None
        # define base attributes for both trace and metrics
        base_attributes = {
            messaging_attributes.MESSAGING_SYSTEM: "mqtt",
            messaging_attributes.MESSAGING_OPERATION_TYPE: "process",
            messaging_attributes.MESSAGING_OPERATION_NAME: "process receive",
            messaging_attributes.MESSAGING_DESTINATION_NAME: str(message.topic),
            "messaging.mqtt.qos": str(message.qos),
            "messaging.mqtt.retain": str(message.retain),
        }
        # start trace span and call parent method
        with self._tracer.start_as_current_span(
            "process MQTT {messaging.destination.name}",
            kind=trace.SpanKind.CONSUMER,
            context=context,
        ) as span:
            span.set_attributes(base_attributes)
            span.set_attribute(
                messaging_attributes.MESSAGING_MESSAGE_ID, str(message.mid)
            )

            error, subscription = await super()._process_message(client, message)

            span.set_attribute(
                messaging_attributes.MESSAGING_DESTINATION_SUBSCRIPTION_NAME,
                subscription,
            )
            if error:
                span.set_status(
                    trace.Status(
                        trace.StatusCode.ERROR,
                        "Error processing MQTT message",
                    )
                )
            processing_duration = time.time() - processing_start_time
            self.record_metrics(processing_duration, error, base_attributes)

            return error, subscription


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
        properties = Properties(PacketTypes.PUBLISH)
        properties.MessageExpiryInterval = ttl
        publisher_logger.debug("Publishing retained message to %s (ttl=%d)", topic, ttl)
        await self._client.publish(
            topic,
            payload,
            qos=1,
            retain=True,
            properties=properties,
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
            qos=1,
            retain=True,
        )

    async def _run(self) -> None:
        """Execute while the MQTT connection is active.

        Override in subclasses to perform work (e.g. stream processing).
        The default implementation waits for the shutdown event.  Any
        :class:`aiomqtt.MqttError` raised here is caught by :meth:`task`
        which triggers reconnection.
        """
        await self._shutdown_event.wait()

    async def task(self) -> None:
        publisher_logger.info(
            "Starting MQTT publisher, connecting to %s:%d",
            self._config.hostname,
            self._config.port,
        )
        client = create_mqtt_client(self._config)
        backoff = 1  # seconds
        max_backoff = 60  # seconds
        while True:
            try:
                async with client:
                    self._client = client
                    self._connected.set()
                    publisher_logger.info("MQTT publisher connected")
                    await self._run()
                    self._client = None
                    self._connected.clear()
                    publisher_logger.info("MQTT publisher shutdown complete")
                    return
            except aiomqtt.MqttError:
                self._client = None
                self._connected.clear()
                if self._shutdown_event.is_set():
                    publisher_logger.info(
                        "MQTT connection lost during shutdown; exiting"
                    )
                    return
                sleep_time = backoff + random.uniform(0, 0.1 * backoff)
                publisher_logger.warning(
                    "Connection lost. Retrying in %.2fs...", sleep_time
                )
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=sleep_time
                    )
                    publisher_logger.info("Shutdown during reconnect backoff; exiting")
                    return
                except asyncio.TimeoutError:
                    backoff = min(backoff * 2, max_backoff)

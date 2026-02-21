import asyncio
import dataclasses
import enum
import logging
import time
import uuid
from asyncio import Queue
from typing import Any

import aiomqtt
import paho.mqtt.client
import redis.asyncio as redis
from opentelemetry import metrics, trace
from opentelemetry.propagate import extract
from opentelemetry.semconv._incubating.attributes import messaging_attributes
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from app import MQTTConfig, ProcessingConfig
from app.common import (
    CloudEvent,
    CloudEventProcessor,
    MessageIntent,
    ProcessorBinding,
    ProtocolHandler,
)
from app.redis import CloudEventDedupeDAO, RedisKeySchema

logger = logging.getLogger("handler.mqtt")


class QoS(enum.IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


class MQTTProcessorBinding:
    """Associates a protocol-agnostic CloudEventProcessor with MQTT-specific
    routing configuration (topics, response topic, outgoing queue).

    Publish topics and the response topic are stored as *patterns* that may
    contain MQTT single-level wildcards (``+``).  Use the ``resolve_*``
    helpers to replace wildcards with concrete segments derived from an
    incoming message topic.
    """

    def __init__(
        self,
        processor: CloudEventProcessor,
        topics: set[str],
        response_topic: str,
        outgoing_queue: Queue[tuple[CloudEvent, MessageIntent | None]],
        topic_prefix: str,
        publish_intents: set[MessageIntent],
    ):
        self.processor = processor
        self.topics = topics
        self.response_topic = response_topic
        self.outgoing_queue = outgoing_queue
        self.topic_prefix = topic_prefix
        self.publish_topic_patterns: dict[MessageIntent, str] = {
            intent: f"{topic_prefix}/{intent.value}" for intent in publish_intents
        }

    # -- wildcard helpers --------------------------------------------------

    @staticmethod
    def resolve_topic_wildcards(pattern: str, concrete_topic: str) -> str:
        """Replace ``+`` wildcard segments in *pattern* with the corresponding
        segments (by position) from *concrete_topic*.

        Example::

            >>> MQTTProcessorBinding.resolve_topic_wildcards(
            ...     "app/jobs/+/events", "app/jobs/woodworking/commands")
            'app/jobs/woodworking/events'
        """
        pattern_parts = pattern.split("/")
        concrete_parts = concrete_topic.split("/")
        return "/".join(
            concrete_parts[i] if part == "+" and i < len(concrete_parts) else part
            for i, part in enumerate(pattern_parts)
        )

    @staticmethod
    def has_wildcards(topic: str) -> bool:
        """Return True if *topic* contains MQTT wildcard characters."""
        return "+" in topic or "#" in topic

    def resolve_publish_topic(
        self, intent: MessageIntent, concrete_reference_topic: str
    ) -> str | None:
        """Return the concrete publish topic for *intent*, resolving any
        wildcards from *concrete_reference_topic*."""
        pattern = self.publish_topic_patterns.get(intent)
        if pattern is None:
            return None
        return self.resolve_topic_wildcards(pattern, concrete_reference_topic)

    def resolve_response_topic(self, concrete_reference_topic: str) -> str:
        """Return the concrete response topic, resolving any wildcards from
        *concrete_reference_topic*."""
        return self.resolve_topic_wildcards(
            self.response_topic, concrete_reference_topic
        )


class MQTTHandler(ProtocolHandler):
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
        self._bindings: list[MQTTProcessorBinding] = []

    def register_mqtt_processor(
        self,
        processor: CloudEventProcessor,
        topic_identifier: str,
        processing_config: ProcessingConfig,
        queue_size: int = 1,
    ) -> None:
        """Register a processor with MQTT-specific routing configuration.

        Subscribe topics are derived from the processor's ``@processor_config``
        binding direction and intents combined with the topic prefix looked up
        via *topic_identifier* in ``processing_config.topic_prefixes``.

        Response topics are still resolved from
        ``processing_config.response_topics`` unchanged.
        """
        if not hasattr(type(processor), "_processor_binding"):
            raise ValueError(
                f"{type(processor).__name__} must be decorated with @processor_config"
            )

        # Resolve topic prefix for this processor
        topic_prefix: str | None = None
        for entry in processing_config.topic_prefixes:
            name, _, prefix = entry.partition(":")
            if name.strip() == topic_identifier:
                topic_prefix = prefix.strip()
                break
        if topic_prefix is None:
            raise ValueError(
                f"No topic prefix found for identifier '{topic_identifier}' "
                f"in APP_PROCESSING_TOPIC_PREFIX"
            )

        # Build subscribe topics from prefix + subscribe intents
        topics: set[str] = set()
        for intent in processor.subscribe_intents():
            topic = f"{topic_prefix}/{intent.value}"
            if processing_config.shared_subscription_name:
                topic = f"$share/{processing_config.shared_subscription_name}/{topic}"
            topics.add(topic)

        # Resolve response topic (kept as-is from config)
        response_topic = ""
        for response_topic_str in processing_config.response_topics:
            proc_name, topic = response_topic_str.split(":", 1)
            if proc_name == topic_identifier:
                response_topic = f"{topic}/{processor._instance_id}"
                break

        outgoing_queue: Queue[tuple[CloudEvent, MessageIntent | None]] = Queue(
            queue_size
        )
        processor.register_publish_handler(
            lambda ce, intent: outgoing_queue.put_nowait((ce, intent))
        )
        binding = MQTTProcessorBinding(
            processor=processor,
            topics=topics,
            response_topic=response_topic,
            outgoing_queue=outgoing_queue,
            topic_prefix=topic_prefix,
            publish_intents=processor.publish_intents(),
        )
        self._bindings.append(binding)
        logger.info(
            "Registered %s processor '%s' | subscribes: %s | publishes: %s | response: %s",
            processor.binding.value,
            processor._instance_id,
            topics,
            set(binding.publish_topic_patterns.values()),
            response_topic,
        )

    def _enrich_response_transport(
        self,
        response: CloudEvent,
        request: CloudEvent,
        binding: MQTTProcessorBinding,
    ) -> bool:
        """Add MQTT transport metadata to a response CE.

        Wildcards in publish / response topic patterns are resolved from the
        concrete incoming request topic.

        Returns False if the response cannot be published (no target topic).
        """
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
                return False
        # Set our backchannel response topic, resolving wildcards from the
        # incoming request's concrete topic when necessary.
        if (
            "mqtt_response_topic" not in response.transportmetadata
            and binding.response_topic
        ):
            if MQTTProcessorBinding.has_wildcards(binding.response_topic):
                request_topic = (
                    request.transportmetadata.get("mqtt_topic", "")
                    if request.transportmetadata
                    else ""
                )
                response.transportmetadata["mqtt_response_topic"] = (
                    binding.resolve_response_topic(request_topic)
                )
            else:
                response.transportmetadata["mqtt_response_topic"] = (
                    binding.response_topic
                )
        if "mqtt_retain" not in response.transportmetadata:
            response.transportmetadata["mqtt_retain"] = False
        return True

    def _client(self) -> aiomqtt.Client:
        properties = None
        if self._runtime_config.sat_token_path.exists():
            with open(self._runtime_config.sat_token_path, "r") as f:
                sat_token = f.read()
                properties = Properties(PacketTypes.CONNECT)
                properties.AuthenticationMethod = "K8S-SAT"
                properties.AuthenticationData = sat_token
        tls_params = None
        if self._runtime_config.tls_cert_path.exists():
            tls_params = aiomqtt.TLSParameters(
                ca_certs=str(self._runtime_config.tls_cert_path)
            )
        client: aiomqtt.Client = aiomqtt.Client(
            protocol=aiomqtt.ProtocolVersion.V5,
            hostname=self._runtime_config.hostname,
            port=self._runtime_config.port,
            identifier=self._runtime_config.identifier,
            timeout=self._runtime_config.connect_timeout,
            clean_start=paho.mqtt.client.MQTT_CLEAN_START_FIRST_ONLY,
            max_queued_incoming_messages=self._runtime_config.incoming_queue_size,
            max_queued_outgoing_messages=self._runtime_config.outgoing_queue_size,
            properties=properties,
            tls_params=tls_params,
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
            self._expiration_timeout_tasks[cloudevent.id] = asyncio.create_task(
                processor.handle_expiration(cloudevent, int(cloudevent.expiryinterval))
            )
            self._expiration_timeout_tasks[cloudevent.id].add_done_callback(
                lambda _task, _id=cloudevent.id: self._expiration_timeout_tasks.pop(
                    _id, None
                )
            )

    async def is_duplicate_message(self, cloudevent: CloudEvent) -> bool:
        logger.debug(
            "Checking for duplicate message with source %s ID %s",
            cloudevent.source,
            cloudevent.id,
        )
        return await self._cloudevent_dedupe_dao.is_duplicate(
            str(cloudevent.source), str(cloudevent.id)
        )

    def cloudevent_from_message(self, message: aiomqtt.Message) -> CloudEvent:
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
        cloudevent = self.cloudevent_from_message(message)
        # check for duplicate message IDs due to QoS 1 (at-least-once delivery)
        if await self.is_duplicate_message(cloudevent):
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
        if cloudevent.id in self._expiration_timeout_tasks:
            self._expiration_timeout_tasks[cloudevent.id].cancel()

        # Dispatch message to registered processors
        # It is assumed that each message is processed by only one processor
        # If multiple processors match the topic, all will be invoked sequentially
        subscription: list[str] = []
        for binding in self._bindings:
            if message.topic.matches(binding.response_topic):
                subscription.append(binding.response_topic)
                processor_response = await binding.processor.process_response_event(
                    cloudevent
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
                    processor_response = await binding.processor.process_event(
                        cloudevent
                    )
                    if isinstance(processor_response, list):
                        for response in processor_response:
                            if self._enrich_response_transport(
                                response, cloudevent, binding
                            ):
                                await self._publish_message(
                                    client, response, binding.processor
                                )
                    elif isinstance(processor_response, CloudEvent):
                        if self._enrich_response_transport(
                            processor_response, cloudevent, binding
                        ):
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
            await self._process_message(client, message)

    async def _outgoing_message_publisher(
        self, client: aiomqtt.Client, binding: MQTTProcessorBinding
    ) -> None:
        while True:
            message, intent = await binding.outgoing_queue.get()
            # Enrich with MQTT transport metadata from the binding
            if message.transportmetadata is None:
                message.transportmetadata = {}

            # Resolve publish topic from intent + publish_topic_patterns
            if "mqtt_topic" not in message.transportmetadata:
                if intent is not None:
                    pattern = binding.publish_topic_patterns.get(intent)
                else:
                    # Fallback: first available publish topic pattern
                    pattern = (
                        next(iter(binding.publish_topic_patterns.values()), None)
                        if binding.publish_topic_patterns
                        else None
                    )
                if pattern:
                    if not MQTTProcessorBinding.has_wildcards(pattern):
                        message.transportmetadata["mqtt_topic"] = pattern
                    else:
                        logger.warning(
                            "Publish topic pattern '%s' contains wildcards that "
                            "cannot be resolved for proactive outgoing message; "
                            "set mqtt_topic explicitly on the CloudEvent.",
                            pattern,
                        )

            # Resolve response backchannel topic
            if (
                "mqtt_response_topic" not in message.transportmetadata
                and binding.response_topic
            ):
                if not MQTTProcessorBinding.has_wildcards(binding.response_topic):
                    message.transportmetadata["mqtt_response_topic"] = (
                        binding.response_topic
                    )
                elif message.transportmetadata.get("mqtt_topic"):
                    # Resolve wildcards from the already-resolved publish topic
                    message.transportmetadata["mqtt_response_topic"] = (
                        binding.resolve_response_topic(
                            message.transportmetadata["mqtt_topic"]
                        )
                    )

            if "mqtt_retain" not in message.transportmetadata:
                message.transportmetadata["mqtt_retain"] = False
            if not message.transportmetadata.get("mqtt_topic"):
                logger.warning(
                    "No publish topic resolved for binding '%s'; "
                    "dropping outgoing event.",
                    binding.processor._instance_id,
                )
                binding.outgoing_queue.task_done()
                continue
            await self._publish_message(client, message, binding.processor)
            binding.outgoing_queue.task_done()

    async def task(self) -> None:
        logger.info("Starting MQTT handler task")
        logger.info(
            "Connecting to MQTT Server running on %s:%d",
            self._runtime_config.hostname,
            self._runtime_config.port,
        )
        client: aiomqtt.Client = self._client()
        interval = 1  # seconds
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
                    for binding in self._bindings:
                        for topic in binding.topics:
                            await client.subscribe(topic)
                            logger.info("Subscribed to topic: %s", topic)
                        await client.subscribe(binding.response_topic)
                        logger.info(
                            "Subscribed to response topic: %s",
                            binding.response_topic,
                        )
                    async with asyncio.TaskGroup() as tg:
                        logger.info(
                            "Starting %d message worker tasks",
                            self._runtime_config.message_workers,
                        )
                        for _ in range(self._runtime_config.message_workers):
                            tg.create_task(self._process_messages(client))
                        for binding in self._bindings:
                            tg.create_task(
                                self._outgoing_message_publisher(client, binding)
                            )
            except aiomqtt.MqttError:
                logger.warning(
                    f"Connection lost; Reconnecting in {interval} seconds ..."
                )
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                logger.info("MQTT handler task cancelled; shutting down")
                await self._redis_client.aclose()
                raise


class OTELInstrumentedMQTTHandler(MQTTHandler):
    def __init__(
        self,
        runtime_config: MQTTConfig,
        redis_connection_pool: redis.ConnectionPool,
        redis_key_schema: "RedisKeySchema",
    ):
        super().__init__(runtime_config, redis_connection_pool, redis_key_schema)

        self._tracer = trace.get_tracer(__name__)
        self._meter = metrics.get_meter(__name__)
        self._metrics: dict[str, Any] = {
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

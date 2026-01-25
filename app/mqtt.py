import asyncio
import dataclasses
import enum
import hashlib
import logging
import time
import typing
import uuid
from asyncio import Queue
from typing import Any, Callable

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
    ProtocolHandler,
)
from app.dataclass import DataClassMixin, get_cloudevent_type, type_has_config_class

logger = logging.getLogger("handler.mqtt")


class QoS(enum.IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


class MQTTCloudEventProcessor(CloudEventProcessor):
    _topic_identifier: str
    _topics: set[str]
    _response_topic: str

    def __init__(
        self,
        instance_id: str,
        runtime_config: ProcessingConfig,
        topic_identifier: str,
        queue_size: int = 1,
    ):
        self._instance_id = instance_id
        self._runtime_config = runtime_config
        self._topic_identifier = topic_identifier
        self._topics = set()
        for topic_str in self._runtime_config.topics:
            processor, topic = topic_str.split(":", 1)
            if processor == self._topic_identifier:
                # add shared subscription prefix if applicable
                if self._runtime_config.shared_subscription_name:
                    topic = f"$share/{self._runtime_config.shared_subscription_name}/{topic}"
                self._topics.add(topic)
        for response_topic_str in self._runtime_config.response_topics:
            processor, topic = response_topic_str.split(":", 1)
            if processor == self._topic_identifier:
                # do not add shared subscription prefix to response topic
                # we want to receive direct responses here
                self._response_topic = f"{topic}/{self._instance_id}"
                break
        self.outgoing_queue = Queue(queue_size)

    def create_mqtt_event(
        self,
        topic: str,
        response_topic: str | None = None,
        retain: bool = False,
        datacontenttype: str | None = "application/json; charset=utf-8",
    ) -> CloudEvent:
        transportmetadata: dict[str, Any] = {
            "mqtt_topic": topic,
            "mqtt_retain": retain,
        }
        if response_topic is not None:
            transportmetadata["mqtt_response_topic"] = response_topic
        cloudevent = self.create_event(
            datacontenttype=datacontenttype,
        )
        cloudevent.transportmetadata = transportmetadata
        return cloudevent

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

        if not isinstance(request_cloudevent.transportmetadata, dict) or (
            isinstance(request_cloudevent.transportmetadata, dict)
            and request_cloudevent.transportmetadata.get("mqtt_response_topic") is None
        ):
            logger.warning("No response topic specified; cannot send response.")
            return None

        if not isinstance(responses, list):
            responses = [responses]

        response_cloudevents: list[CloudEvent] = []
        for response in responses:
            logger.debug("Response from callback: %s", response)
            if not type_has_config_class(type(response)):
                logger.warning("Response has no Config class")
                continue

            response_cloudevent = self.create_mqtt_event(
                topic=str(
                    request_cloudevent.transportmetadata.get("mqtt_response_topic")
                ),
                response_topic=self._response_topic,
            )
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

    async def type_callback(
        self, payload_type: type, topic: str, **kwargs
    ) -> list[CloudEvent] | CloudEvent | None:
        cloudevent_type = get_cloudevent_type(payload_type)
        if cloudevent_type is None or cloudevent_type not in self._type_callbacks_out:
            logger.error(
                "No callback registered for cloud event type: %s", cloudevent_type
            )
            return None

        payload_type = self._type_classes[cloudevent_type]
        callback: Callable[..., Any] = self._type_callbacks_out[cloudevent_type]
        responses: list[DataClassMixin] | DataClassMixin | None = await callback(
            **kwargs
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

            response_cloudevent = self.create_mqtt_event(
                topic=topic,
                response_topic=self._response_topic,
            )
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

            await self.outgoing_queue.put(response_cloudevent)
            response_cloudevents.append(response_cloudevent)
        return response_cloudevents


class MQTTHandler(ProtocolHandler):
    _runtime_config: MQTTConfig
    _redis: redis.Redis
    _expiration_timeout_tasks: dict[str, asyncio.Task]

    def __init__(
        self, runtime_config: MQTTConfig, redis_connection_pool: redis.ConnectionPool
    ):
        self._runtime_config = runtime_config
        self._redis = redis.Redis(connection_pool=redis_connection_pool)
        try:
            self._redis.ping()
        except redis.RedisError as e:
            logger.error(f"Error connecting to Redis: {e}")
            raise
        self._expiration_timeout_tasks = {}
        super().__init__()

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
        processor: MQTTCloudEventProcessor | None = None,
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
        # create deduplication key based on CloudEvent source and ID
        # hash it to keep the Redis key size consistent and small
        dedupe_raw = f"{cloudevent.source}:{cloudevent.id}"
        dedupe_key = f"{self._runtime_config.dedupe_key_prefix}{hashlib.md5(dedupe_raw.encode()).hexdigest()}"
        # atomic SET NX (Set if Not eXists) with expiration
        return (
            False
            if await self._redis.set(
                dedupe_key,
                "1",
                ex=self._runtime_config.dedupe_ttl_seconds,
                nx=True,
            )
            else True
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
            for processor in self._cloudevent_processors:
                if isinstance(processor, MQTTCloudEventProcessor):
                    if message.topic.matches(processor._response_topic):
                        return False, processor._response_topic
                    for topic in processor._topics:
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
        for processor in self._cloudevent_processors:
            if isinstance(processor, MQTTCloudEventProcessor):
                if message.topic.matches(processor._response_topic):
                    subscription.append(processor._response_topic)
                    processor_response = await processor.process_response_event(
                        cloudevent
                    )
                    if isinstance(processor_response, list):
                        for response in processor_response:
                            await self._publish_message(client, response, processor)
                    elif isinstance(processor_response, CloudEvent):
                        await self._publish_message(
                            client, processor_response, processor
                        )
                    elif processor_response is None:
                        continue
                for topic in processor._topics:
                    if message.topic.matches(topic):
                        subscription.append(topic)
                        processor_response = await processor.process_event(cloudevent)
                        if isinstance(processor_response, list):
                            for response in processor_response:
                                await self._publish_message(client, response, processor)
                        elif isinstance(processor_response, CloudEvent):
                            await self._publish_message(
                                client, processor_response, processor
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
        self, client: aiomqtt.Client, processor: CloudEventProcessor
    ) -> None:
        while True:
            message = await processor.outgoing_queue.get()
            await self._publish_message(
                client, message, typing.cast(MQTTCloudEventProcessor, processor)
            )
            processor.outgoing_queue.task_done()

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
                async with client:
                    for processor in self._cloudevent_processors:
                        if isinstance(processor, MQTTCloudEventProcessor):
                            for topic in processor._topics:
                                await client.subscribe(topic)
                                logger.info("Subscribed to topic: %s", topic)
                            await client.subscribe(processor._response_topic)
                            logger.info(
                                "Subscribed to response topic: %s",
                                processor._response_topic,
                            )
                    async with asyncio.TaskGroup() as tg:
                        logger.info(
                            "Starting %d message worker tasks",
                            self._runtime_config.message_workers,
                        )
                        for _ in range(self._runtime_config.message_workers):
                            tg.create_task(self._process_messages(client))
                        for processor in self._cloudevent_processors:
                            if isinstance(processor, MQTTCloudEventProcessor):
                                tg.create_task(
                                    self._outgoing_message_publisher(client, processor)
                                )
            except aiomqtt.MqttError:
                logger.warning(
                    f"Connection lost; Reconnecting in {interval} seconds ..."
                )
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                logger.info("MQTT handler task cancelled; shutting down")
                await self._redis.aclose()
                raise


class OTELInstrumentedMQTTHandler(MQTTHandler):
    _tracer: trace.Tracer
    _meter: metrics.Meter
    _metrics: dict[str, metrics.Instrument]

    def __init__(
        self, runtime_config: MQTTConfig, redis_connection_pool: redis.ConnectionPool
    ):
        super().__init__(runtime_config, redis_connection_pool)

        self._tracer = trace.get_tracer(__name__)
        self._meter = metrics.get_meter(__name__)
        self._metrics = {
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

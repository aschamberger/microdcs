import asyncio
import dataclasses
import datetime
import enum
import logging
from abc import ABC, abstractmethod
from asyncio import Queue
from dataclasses import dataclass
from typing import Any, Callable
from uuid import uuid4

import aiomqtt
import paho.mqtt.client
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from app import MQTTConfig, ProcessingConfig
from app.common import CloudEventAttributes, serialize_payload, unserialize_payload
from app.dataclass import DataClassConfig, DataClassMixin

logger = logging.getLogger("handler.mqtt")


class QoS(enum.IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


@dataclass
class MQTTProcessorMessage:
    topic: str
    payload: bytes
    qos: QoS
    retain: bool
    payload_format_indicator: int
    message_expiry_interval: int | None
    content_type: str | None
    response_topic: str | None
    correlation_data: bytes | None
    user_properties: dict[str, str] | None
    cloudevent: CloudEventAttributes

    def __init__(
        self,
        topic: str,
        *,
        payload: bytes,
        qos: QoS = QoS.AT_LEAST_ONCE,
        retain: bool = False,
        payload_format_indicator: int = 1,
        message_expiry_interval: int | None = None,
        content_type: str = "application/json; charset=utf-8",
        response_topic: str | None = None,
        correlation_data: bytes = uuid4().bytes,
        user_properties: dict[str, str] | None = None,
    ):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain
        self.payload_format_indicator = payload_format_indicator
        self.message_expiry_interval = message_expiry_interval
        self.content_type = content_type
        self.response_topic = response_topic
        self.correlation_data = correlation_data
        self.user_properties = user_properties
        self.cloudevent = CloudEventAttributes()
        self.cloudevent.id = self.correlation_data.hex().upper()
        self.cloudevent.datacontenttype = self.content_type
        self.cloudevent.time = datetime.datetime.now().isoformat() + "Z"

    def cloudevent_to_user_properties(self) -> None:
        if self.user_properties is None:
            self.user_properties = {}
        for field in dataclasses.fields(CloudEventAttributes):
            value = getattr(self.cloudevent, field.name)
            if value is not None:
                self.user_properties[field.name] = str(
                    value
                )  # user properties must be str


class MQTTMessageProcessor(ABC):
    instance_id: str
    runtime_config: ProcessingConfig
    identifier: str
    topics: set[str]
    response_topic: str
    type_classes: dict[str, type[DataClassMixin]] = {}
    type_callbacks: dict[str, Callable[..., Any]] = {}
    hidden_field_processors: dict[
        str,
        tuple[
            Callable[[DataClassMixin, dict[str, str]], None] | None,
            Callable[[DataClassMixin, dict[str, str]], None] | None,
        ],
    ] = {}

    outgoing_queue: Queue[MQTTProcessorMessage]

    def __init__(
        self,
        instance_id: str,
        runtime_config: ProcessingConfig,
        identifier: str,
        queue_size: int = 1,
    ):
        self.instance_id = instance_id
        self.runtime_config = runtime_config
        self.identifier = identifier
        self.topics = set()
        for topic_str in self.runtime_config.topics:
            processor, topic = topic_str.split(":", 1)
            if processor == self.identifier:
                # add shared subscription prefix if applicable
                if self.runtime_config.shared_subscription_name:
                    topic = (
                        f"$share/{self.runtime_config.shared_subscription_name}/{topic}"
                    )
                self.topics.add(topic)
        for response_topic_str in self.runtime_config.response_topics:
            processor, topic = response_topic_str.split(":", 1)
            if processor == self.identifier:
                # do not add shared subscription prefix to response topic
                # we want to receive direct responses here
                self.response_topic = f"{topic}/{self.instance_id}"
                break
        self.outgoing_queue = Queue(queue_size)

    def register_callback(
        self, message_dataclass: type, callback: Callable[..., Any]
    ) -> None:
        if not callable(callback):
            raise TypeError("callback must be callable")
        if not issubclass(message_dataclass, DataClassMixin):
            raise TypeError("message_dataclass must be a subclass of DataClassMixin")
        config_class = getattr(message_dataclass, "Config", None)
        if config_class is None or not issubclass(config_class, DataClassConfig):
            raise TypeError(
                "message_dataclass must have a Config subclass of DataClassConfig"
            )
        if not hasattr(config_class, "cloudevent_type"):
            raise TypeError(
                "message_dataclass must have a Config subclass with cloudevent_type attribute"
            )
        cloudevent_type = getattr(config_class, "cloudevent_type")
        self.type_classes[cloudevent_type] = message_dataclass
        self.type_callbacks[cloudevent_type] = callback

    def message_has_callback(self, message: MQTTProcessorMessage) -> bool:
        return message.cloudevent.type in self.type_callbacks

    def register_hidden_field_processor(
        self,
        cloudevent_type: str,
        extractor: Callable[..., Any] | None = None,
        inserter: Callable[..., Any] | None = None,
    ) -> None:
        if not callable(extractor):
            raise TypeError("extractor must be callable")
        if not callable(inserter):
            raise TypeError("inserter must be callable")
        self.hidden_field_processors[cloudevent_type] = (extractor, inserter)

    async def message_callback(
        self, message: MQTTProcessorMessage
    ) -> list[MQTTProcessorMessage] | MQTTProcessorMessage | None:
        if message.cloudevent.type not in self.type_callbacks:
            logger.error(
                "No callback registered for message type: %s", message.cloudevent.type
            )
            return None

        payload_type = self.type_classes[message.cloudevent.type]
        callback: Callable[..., Any] = self.type_callbacks[message.cloudevent.type]
        try:
            request: DataClassMixin | bytes = unserialize_payload(
                message.payload,
                payload_type,
                message.cloudevent.datacontenttype or "application/json; charset=utf-8",
                message.user_properties or {},
                self.hidden_field_processors,
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

        if message.response_topic is None:
            logger.warning("No response topic specified; cannot send response.")
            return None

        if not isinstance(responses, list):
            responses = [responses]

        mqtt_responses: list[MQTTProcessorMessage] = []
        for response in responses:
            logger.debug("Response from callback: %s", response)
            # Get Config class by name
            response_config = getattr(response, "Config", None)
            if response_config is None:
                logger.warning("Response has no Config class")
                return None

            try:
                response_payload, user_properties = serialize_payload(
                    response,
                    message.cloudevent.datacontenttype
                    or "application/json; charset=utf-8",
                    self.hidden_field_processors,
                )
            except ValueError as e:
                logger.exception(
                    "Error serializing payload for message type %s: %s",
                    message.cloudevent.type,
                    e,
                )
                return None

            response_message = self.create_message(
                topic=message.response_topic,
                payload=response_payload,
                cloudevent_type=response_config.cloudevent_type,
                cloudevent_dataschema=response_config.cloudevent_dataschema,
                user_properties=user_properties,
            )
            mqtt_responses.append(response_message)
        return mqtt_responses

    def create_message(
        self,
        topic: str,
        *,
        payload: bytes,
        qos: QoS = QoS.AT_LEAST_ONCE,
        retain: bool = False,
        payload_format_indicator: int = 1,
        message_expiry_interval: int | None = None,
        content_type: str = "application/json; charset=utf-8",
        response_topic: str | None = None,
        correlation_data: bytes = uuid4().bytes,
        user_properties: dict[str, str] | None = None,
        cloudevent_type: str | None = None,
        cloudevent_dataschema: str | None = None,
    ):
        message = MQTTProcessorMessage(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            payload_format_indicator=payload_format_indicator,
            message_expiry_interval=message_expiry_interval,
            content_type=content_type,
            response_topic=response_topic,
            correlation_data=correlation_data,
            user_properties=user_properties,
        )
        # set response topic if applicable
        if message.response_topic is None and self.response_topic is not None:
            message.response_topic = self.response_topic
        message.cloudevent.source = self.runtime_config.cloudevent_source
        message.cloudevent.subject = self.identifier
        if cloudevent_type is not None:
            message.cloudevent.type = cloudevent_type
        if cloudevent_dataschema is not None:
            message.cloudevent.dataschema = cloudevent_dataschema
        return message

    @abstractmethod
    async def process_message(
        self, message: MQTTProcessorMessage
    ) -> list[MQTTProcessorMessage] | MQTTProcessorMessage | None:
        pass

    @abstractmethod
    async def process_response_message(
        self, message: MQTTProcessorMessage
    ) -> list[MQTTProcessorMessage] | MQTTProcessorMessage | None:
        pass


class MQTTHandler:
    _runtime_config: MQTTConfig
    _message_processors: list[MQTTMessageProcessor] = []

    def __init__(self, runtime_config: MQTTConfig):
        self._runtime_config = runtime_config

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
            timeout=self._runtime_config.timeout,
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
        message: MQTTProcessorMessage,
    ) -> None:
        logger.debug("Publishing message to topic %s", message.topic)
        message.cloudevent_to_user_properties()
        properties = Properties(PacketTypes.PUBLISH)
        if message.message_expiry_interval is not None:
            properties.MessageExpiryInterval = message.message_expiry_interval
        elif (
            self._runtime_config.message_expiry_interval > 0
            and message.qos == QoS.AT_MOST_ONCE
        ):
            properties.MessageExpiryInterval = (
                self._runtime_config.message_expiry_interval
            )
        if message.payload_format_indicator is not None:
            # https://www.hivemq.com/blog/mqtt5-essentials-part8-payload-format-description/
            properties.PayloadFormatIndicator = message.payload_format_indicator
        if message.content_type is not None:
            properties.ContentType = message.content_type
        if message.response_topic is not None:
            properties.ResponseTopic = message.response_topic
        if message.correlation_data is not None:
            properties.CorrelationData = message.correlation_data
        if message.user_properties is not None and len(message.user_properties) > 0:
            # Convert dictionary to list of tuples
            properties.UserProperty = list(message.user_properties.items())
        await client.publish(
            message.topic,
            message.payload,
            qos=message.qos,
            retain=message.retain,
            properties=properties,
            timeout=self._runtime_config.message_publication_timeout,
        )

    async def _process_message(
        self, client: aiomqtt.Client, message: aiomqtt.Message
    ) -> None:
        logger.debug("Received message on topic %s", message.topic)
        processor_message = MQTTProcessorMessage(
            topic=str(message.topic),
            payload=message.payload,
            qos=QoS(message.qos),
            retain=message.retain,
        )
        # Populate MQTT 5 properties
        if message.properties and hasattr(message.properties, "PayloadFormatIndicator"):
            processor_message.payload_format_indicator = (
                message.properties.PayloadFormatIndicator  # type: ignore
            )
        if message.properties and hasattr(message.properties, "MessageExpiryInterval"):
            processor_message.message_expiry_interval = (
                message.properties.MessageExpiryInterval  # type: ignore
            )
        if message.properties and hasattr(message.properties, "ContentType"):
            processor_message.content_type = str(message.properties.ContentType)  # type: ignore
        if message.properties and hasattr(message.properties, "ResponseTopic"):
            processor_message.response_topic = str(message.properties.ResponseTopic)  # type: ignore
        if message.properties and hasattr(message.properties, "CorrelationData"):
            processor_message.correlation_data = message.properties.CorrelationData  # type: ignore
        if message.properties and hasattr(message.properties, "UserProperty"):
            # Convert list of tuples to dictionary
            processor_message.user_properties = dict(message.properties.UserProperty)  # type: ignore
        # Populate CloudEvent attributes from user properties if present
        for field in dataclasses.fields(CloudEventAttributes):
            if (
                processor_message.user_properties is not None
                and field.name in processor_message.user_properties
            ):
                setattr(
                    processor_message.cloudevent,
                    field.name,
                    processor_message.user_properties[field.name],
                )

        # Dispatch message to registered processors
        # It is assumed that each message is processed by only one processor
        # If multiple processors match the topic, all will be invoked sequentially
        responses: list[MQTTProcessorMessage] = []
        for processor in self._message_processors:
            if message.topic.matches(processor.response_topic):
                processor_response = await processor.process_response_message(
                    processor_message
                )
                if isinstance(processor_response, list):
                    responses.extend(processor_response)
                elif isinstance(processor_response, MQTTProcessorMessage):
                    responses.append(processor_response)
                elif processor_response is None:
                    continue
            for topic in processor.topics:
                if message.topic.matches(topic):
                    processor_response = await processor.process_message(
                        processor_message
                    )
                    if isinstance(processor_response, list):
                        responses.extend(processor_response)
                    elif isinstance(processor_response, MQTTProcessorMessage):
                        responses.append(processor_response)
                    elif processor_response is None:
                        continue

        # send responses back if any
        for response in responses:
            await self._publish_message(client, response)

        # FIXME: use the native ack method when https://github.com/empicano/aiomqtt/pull/346 is merged
        client._client.ack(message.mid, message.qos)  # type: ignore #

    async def _process_messages(self, client: aiomqtt.Client) -> None:
        logger.info("Starting MQTT message processing")
        message: aiomqtt.Message
        async for message in client.messages:
            await self._process_message(client, message)

    def register_message_processor(self, processor: MQTTMessageProcessor) -> None:
        self._message_processors.append(processor)

    async def _outgoing_message_publisher(
        self, client: aiomqtt.Client, processor: MQTTMessageProcessor
    ) -> None:
        while True:
            message = await processor.outgoing_queue.get()
            await self._publish_message(client, message)
            processor.outgoing_queue.task_done()

    async def task(self) -> None:
        logger.info("Starting MQTT handler task")
        client: aiomqtt.Client = self._client()
        interval = 1  # seconds
        while True:
            try:
                async with client:
                    for processor in self._message_processors:
                        for topic in processor.topics:
                            await client.subscribe(topic)
                        await client.subscribe(processor.response_topic)
                    async with asyncio.TaskGroup() as tg:
                        logger.info(
                            "Starting %d message worker tasks",
                            self._runtime_config.message_workers,
                        )
                        for _ in range(self._runtime_config.message_workers):
                            tg.create_task(self._process_messages(client))
                        for processor in self._message_processors:
                            tg.create_task(
                                self._outgoing_message_publisher(client, processor)
                            )
            except aiomqtt.MqttError:
                logger.warning(
                    f"Connection lost; Reconnecting in {interval} seconds ..."
                )
                await asyncio.sleep(interval)


class OTELInstrumentedMQTTHandler(MQTTHandler):
    def __init__(self, runtime_config: MQTTConfig):
        super().__init__(runtime_config)

    async def _process_message(
        self, client: aiomqtt.Client, message: aiomqtt.Message
    ) -> None:
        return await super()._process_message(client, message)

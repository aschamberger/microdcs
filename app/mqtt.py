import asyncio
import dataclasses
import datetime
import enum
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import aiomqtt
import paho.mqtt.client
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from app import MQTTConfig, ProcessingConfig

logger = logging.getLogger("handler.mqtt")


class QoS(enum.IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


@dataclass
class CloudEventAttributes:
    specversion: str = "1.0"
    """The version of the CloudEvents specification used by this event."""
    id: str | None = None
    """Populated from MQTT message correlation_data; MUST be a non-empty string; MUST be unique within the scope of the producer"""
    source: str | None = None
    """MUST be a non-empty URI-reference; An absolute URI is RECOMMENDED"""
    type: str | None = None
    """MUST be a non-empty string; SHOULD be prefixed with a reverse-DNS name. The prefixed domain dictates the organization which defines the semantics of this event type.
    """
    datacontenttype: str | None = None
    """Populated from MQTT message content_type; If present, MUST adhere to the format specified in RFC 2046"""
    dataschema: str | None = None
    """If present, MUST be a non-empty URI"""
    subject: str | None = None
    """If present, MUST be a non-empty string"""
    time: str | None = None
    """If present, MUST adhere to the format specified in RFC 3339"""


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
    user_properties: dict[str, Any] | None
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
        user_properties: dict[str, Any] | None = None,
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
        self.cloudevent.id = self.correlation_data.decode("utf-8", errors="ignore")
        self.cloudevent.datacontenttype = self.content_type
        self.cloudevent.time = datetime.datetime.now().isoformat() + "Z"

    def cloudevent_to_user_properties(self) -> None:
        if self.user_properties is None:
            self.user_properties = {}
        self.user_properties |= dataclasses.asdict(self.cloudevent)


class MQTTMessageProcessor(ABC):
    runtime_config: ProcessingConfig
    identifier: str
    topics: set[str]

    def __init__(self, runtime_config: ProcessingConfig, identifier: str):
        self.runtime_config = runtime_config
        self.identifier = identifier
        self.topics = set()
        for topic_str in runtime_config.topics:
            processor, topic = topic_str.split(":", 1)
            if processor == self.identifier:
                self.topics.add(topic)

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
        user_properties: dict[str, Any] | None = None,
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
        else:
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
        # FIXME payload type error should be gone with new release: https://github.com/empicano/aiomqtt/commit/68303021095de6c2782e01c4f1391442fb7c8246
        processor_message = MQTTProcessorMessage(
            topic=str(message.topic),
            payload=message.payload,  # type: ignore
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
                    async with asyncio.TaskGroup() as tg:
                        logger.info(
                            "Starting %d message worker tasks",
                            self._runtime_config.message_workers,
                        )
                        for _ in range(self._runtime_config.message_workers):
                            tg.create_task(self._process_messages(client))
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

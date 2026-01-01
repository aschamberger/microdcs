import asyncio
import enum
from abc import abstractmethod
from typing import Any
from uuid import uuid4

import aiomqtt
import paho.mqtt.client
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from app import MQTTConfig


class QoS(enum.IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


class MQTTProcessorMessage:
    topic: str | aiomqtt.Topic
    payload: bytes
    user_properties: dict[str, Any]
    response_topic: str | None = None
    retain: bool = False

    def __init__(
        self,
        topic: str | aiomqtt.Topic,
        payload: bytes,
        user_properties: dict[str, Any],
        response_topic: str | None = None,
        retain: bool = False,
    ):
        self.topic = topic
        self.payload = payload
        self.user_properties = user_properties
        self.response_topic = response_topic
        self.retain = retain


class MQTTMessageProcessor:
    topics: set[str]

    def __init__(self, topics: set[str]):
        self.topics = topics

    @abstractmethod
    async def process_message(
        self, message: MQTTProcessorMessage
    ) -> list[MQTTProcessorMessage] | MQTTProcessorMessage:
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
        topic: str,
        payload: bytes,
        response_topic: str | None = None,
        user_properties: dict[str, Any] | None = None,
        retain: bool = False,
    ) -> None:
        properties = Properties(PacketTypes.PUBLISH)
        properties.MessageExpiryInterval = self._runtime_config.message_expiry_interval
        # https://www.hivemq.com/blog/mqtt5-essentials-part8-payload-format-description/
        properties.PayloadFormatIndicator = 1  # UTF-8
        properties.ContentType = "application/json"
        properties.CorrelationData = uuid4().bytes
        if response_topic:
            properties.ResponseTopic = response_topic
        if user_properties:
            properties.UserProperty = list[user_properties.items()]
        await client.publish(
            topic,
            payload,
            qos=QoS.AT_LEAST_ONCE,
            retain=retain,
            properties=properties,
            timeout=self._runtime_config.message_publication_timeout,
        )

    async def _process_message(
        self, client: aiomqtt.Client, message: aiomqtt.Message
    ) -> None:
        user_properties: dict[str, Any] = {}
        if message.properties and hasattr(message.properties, "UserProperty"):
            # Convert list of tuples to dictionary
            user_properties = dict(message.properties.UserProperty)  # type: ignore
        response_topic: str | None = None
        if message.properties and hasattr(message.properties, "ResponseTopic"):
            response_topic = str(message.properties.ResponseTopic)  # type: ignore
        # FIXME type error should be gone with new release: https://github.com/empicano/aiomqtt/commit/68303021095de6c2782e01c4f1391442fb7c8246
        processor_message = MQTTProcessorMessage(
            message.topic, message.payload, user_properties, response_topic
        )  # type: ignore

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
                    else:
                        responses.append(processor_response)

        # send responses back if any
        for response in responses:
            await self._publish_message(
                client,
                topic=str(response.topic),
                payload=response.payload,
                response_topic=response.response_topic,
                user_properties=response.user_properties,
                retain=response.retain,
            )

        # FIXME: use the native ack method when https://github.com/empicano/aiomqtt/pull/346 is merged
        client._client.ack(message.mid, message.qos)  # type: ignore #

    async def _process_messages(self, client: aiomqtt.Client) -> None:
        message: aiomqtt.Message
        async for message in client.messages:
            await self._process_message(client, message)

    def register_message_processor(self, processor: MQTTMessageProcessor) -> None:
        self._message_processors.append(processor)

    async def task(self) -> None:
        client: aiomqtt.Client = self._client()
        interval = 1  # seconds
        while True:
            try:
                async with client:
                    for processor in self._message_processors:
                        for topic in processor.topics:
                            await client.subscribe(topic)
                    async with asyncio.TaskGroup() as tg:
                        for _ in range(self._runtime_config.message_workers):
                            tg.create_task(self._process_messages(client))
            except aiomqtt.MqttError:
                print(f"Connection lost; Reconnecting in {interval} seconds ...")
                await asyncio.sleep(interval)


class OTELInstrumentedMQTTHandler(MQTTHandler):
    def __init__(self, runtime_config: MQTTConfig):
        super().__init__(runtime_config)

    async def _process_message(
        self, client: aiomqtt.Client, message: aiomqtt.Message
    ) -> None:
        return await super()._process_message(client, message)

import asyncio
import dataclasses
import enum
import logging
from asyncio import Queue
from typing import Any, Callable

import aiomqtt
import paho.mqtt.client
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from app import MQTTConfig, ProcessingConfig
from app.common import (
    CloudEvent,
    CloudEventProcessor,
    ProtocolHandler,
)
from app.dataclass import DataClassMixin

logger = logging.getLogger("handler.mqtt")


class QoS(enum.IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


class MQTTMessageProcessor(CloudEventProcessor):
    runtime_config: ProcessingConfig
    topics: set[str]
    response_topic: str

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

    async def message_callback(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        if cloudevent.type not in self.type_callbacks_in:
            logger.error("No callback registered for message type: %s", cloudevent.type)
            return None

        payload_type = self.type_classes[cloudevent.type]
        callback: Callable[..., Any] = self.type_callbacks_in[cloudevent.type]
        try:
            request: DataClassMixin | bytes = cloudevent.unserialize_payload(
                payload_type,
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

        if (
            cloudevent.transportmetadata is None
            or cloudevent.transportmetadata.get("mqtt_response_topic") is None
        ):
            logger.warning("No response topic specified; cannot send response.")
            return None

        if not isinstance(responses, list):
            responses = [responses]

        mqtt_responses: list[CloudEvent] = []
        for response in responses:
            logger.debug("Response from callback: %s", response)
            # Get Config class by name
            response_config = getattr(response, "Config", None)
            if response_config is None:
                logger.warning("Response has no Config class")
                return None

            response_message = CloudEvent(
                datacontenttype="application/json; charset=utf-8",
                id=cloudevent.id,
                transportmetadata={
                    "mqtt_topic": cloudevent.transportmetadata.get(
                        "mqtt_response_topic"
                    ),
                },
            )
            if self.response_topic is not None and isinstance(
                response_message.transportmetadata, dict
            ):
                response_message.transportmetadata["mqtt_response_topic"] = (
                    self.response_topic
                )
            if self.runtime_config.cloudevent_source is not None:
                response_message.source = self.runtime_config.cloudevent_source
            if (
                self.runtime_config.message_expiry_interval is not None
                and int(self.runtime_config.message_expiry_interval) > 0
            ):
                response_message.expiryinterval = (
                    self.runtime_config.message_expiry_interval
                )
            try:
                response_message.serialize_payload(
                    response,
                    self.hidden_field_processors,
                )
            except ValueError as e:
                logger.exception(
                    "Error serializing payload for message type %s: %s",
                    cloudevent.type,
                    e,
                )
                return None

            mqtt_responses.append(response_message)
        return mqtt_responses


class MQTTHandler(ProtocolHandler):
    _runtime_config: MQTTConfig
    cloudevent_processors: list[CloudEventProcessor] = []

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
        if cloudevent.id is not None:
            properties.CorrelationData = cloudevent.id.encode("utf-8")
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

    async def _process_message(
        self, client: aiomqtt.Client, message: aiomqtt.Message
    ) -> None:
        logger.debug("Received message on topic %s", message.topic)
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
            cloudevent.id = message.properties.CorrelationData.decode("utf-8")  # type: ignore
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

        # Dispatch message to registered processors
        # It is assumed that each message is processed by only one processor
        # If multiple processors match the topic, all will be invoked sequentially
        responses: list[CloudEvent] = []
        for processor in self.cloudevent_processors:
            if isinstance(processor, MQTTMessageProcessor):
                if message.topic.matches(processor.response_topic):
                    processor_response = await processor.process_response_event(
                        cloudevent
                    )
                    if isinstance(processor_response, list):
                        responses.extend(processor_response)
                    elif isinstance(processor_response, CloudEvent):
                        responses.append(processor_response)
                    elif processor_response is None:
                        continue
                for topic in processor.topics:
                    if message.topic.matches(topic):
                        processor_response = await processor.process_event(cloudevent)
                        if isinstance(processor_response, list):
                            responses.extend(processor_response)
                        elif isinstance(processor_response, CloudEvent):
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

    async def _outgoing_message_publisher(
        self, client: aiomqtt.Client, processor: CloudEventProcessor
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
                    for processor in self.cloudevent_processors:
                        if isinstance(processor, MQTTMessageProcessor):
                            for topic in processor.topics:
                                await client.subscribe(topic)
                                logger.info("Subscribed to topic: %s", topic)
                            await client.subscribe(processor.response_topic)
                            logger.info(
                                "Subscribed to response topic: %s",
                                processor.response_topic,
                            )
                    async with asyncio.TaskGroup() as tg:
                        logger.info(
                            "Starting %d message worker tasks",
                            self._runtime_config.message_workers,
                        )
                        for _ in range(self._runtime_config.message_workers):
                            tg.create_task(self._process_messages(client))
                        for processor in self.cloudevent_processors:
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

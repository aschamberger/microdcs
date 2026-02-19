import asyncio
import logging

from app import ProcessingConfig
from app.common import CloudEvent, Direction
from app.models.greetings import Bye, Hello
from app.mqtt import MQTTCloudEventProcessor
from app.msgpack import MessagePackCloudEventProcessor

logger = logging.getLogger("processor.greetings")


class GreetingsCloudEventDelegate:
    @classmethod
    async def handle_hello(cls, hello: Hello) -> list[Hello] | Hello | None:
        logger.info("Received hello from: %s", hello.name)

        logger.debug("Processing %s %s %s", hello, hello._hidden_str, hello._hidden_obj)

        h1 = hello.response(name=hello.name)
        h2 = Hello(name="Alice")
        return [
            h1,
            h2,
        ]

    @classmethod
    async def handle_bye(cls, **kwargs) -> list[Bye] | Bye | None:
        h1 = Bye(name="Bob")
        h2 = Bye(name="Alice")
        return [
            h1,
            h2,
        ]


class GreetingsMQTTCloudEventProcessor(MQTTCloudEventProcessor):
    def __init__(self, instance_id: str, runtime_config: ProcessingConfig):
        super().__init__(instance_id, runtime_config, "greetings")
        self.register_callback(
            Hello,
            GreetingsCloudEventDelegate.handle_hello,
            direction=Direction.INCOMING,
        )
        self.register_callback(
            Bye, GreetingsCloudEventDelegate.handle_bye, direction=Direction.OUTGOING
        )

    async def process_event(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        if (
            cloudevent.transportmetadata is None
            or cloudevent.transportmetadata.get("mqtt_topic") is None
        ):
            logger.error("No topic specified for publishing message")
            return None
        logger.debug(
            "Processing response message on topic %s",
            cloudevent.transportmetadata.get("mqtt_topic"),
        )

        if not self.event_has_callback(cloudevent):
            logger.info("No handler registered for message type: %s", cloudevent.type)
            # Special case: raw greetings messages are echoed back
            # Normally, we would not do this here, this is just to demo the functionality
            if (
                cloudevent.transportmetadata is None
                or cloudevent.transportmetadata.get("mqtt_response_topic") is None
            ):
                logger.warning(
                    "No response topic specified; cannot send greetings response."
                )
                return None
            response = CloudEvent(
                data=cloudevent.data,
                type="com.github.aschamberger.microdcs.greetings.raw.v1",
                dataschema="https://aschamberger.github.io/schemas/microdcs/greetings/v1.0.0/raw",
                datacontenttype=cloudevent.datacontenttype,
                correlationid=cloudevent.correlationid,
                causationid=cloudevent.id,
                transportmetadata={
                    "mqtt_topic": cloudevent.transportmetadata.get(
                        "mqtt_response_topic"
                    ),
                },
            )
            return response

        result = await self.message_callback(cloudevent)
        return result

    async def process_response_event(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        if (
            cloudevent.transportmetadata is None
            or cloudevent.transportmetadata.get("mqtt_topic") is None
        ):
            logger.error("No topic specified for publishing message")
            return None
        logger.debug(
            "Processing response message on topic %s",
            cloudevent.transportmetadata.get("mqtt_topic"),
        )

        logger.debug("Response message: %s", cloudevent)

        # For error messages, we do not send any response here
        # however we could retry in some cases or log to an external system
        return None

    async def send_event(self) -> None:
        await asyncio.sleep(5)  # wait for system to be ready
        logger.info("Sending bye event")
        payload_type = Bye
        topic = "app/greetings/bye"
        await self.type_callback(payload_type, topic)

    async def handle_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        await asyncio.sleep(timeout)  # yield control
        logger.info("Message expired: %s", cloudevent.id)
        return None


class GreetingsMessagePackCloudEventProcessor(MessagePackCloudEventProcessor):
    def __init__(self, instance_id: str, runtime_config: ProcessingConfig):
        super().__init__(instance_id, runtime_config)
        self.register_callback(
            Hello,
            GreetingsCloudEventDelegate.handle_hello,
            direction=Direction.INCOMING,
        )
        self.register_callback(
            Bye, GreetingsCloudEventDelegate.handle_bye, direction=Direction.OUTGOING
        )

    async def process_event(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        if not self.event_has_callback(cloudevent):
            logger.info("No handler registered for message type: %s", cloudevent.type)
            # Special case: raw greetings messages are echoed back
            # Normally, we would not do this here, this is just to demo the functionality
            response = CloudEvent(
                data=cloudevent.data,
                type="com.github.aschamberger.microdcs.greetings.raw.v1",
                dataschema="https://aschamberger.github.io/schemas/microdcs/greetings/v1.0.0/raw",
                datacontenttype=cloudevent.datacontenttype,
                correlationid=cloudevent.correlationid,
                causationid=cloudevent.id,
            )
            return response

        result = await self.message_callback(cloudevent)
        return result

    async def process_response_event(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        raise NotImplementedError(
            "MessagePack greetings processor does not process response events."
        )

    async def send_event(self) -> None:
        raise NotImplementedError(
            "MessagePack greetings processor does not send events."
        )

    async def handle_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        raise NotImplementedError(
            "MessagePack greetings processor does not handle expirations."
        )

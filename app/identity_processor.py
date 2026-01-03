import logging
from dataclasses import dataclass
from typing import ClassVar

from mashumaro.config import BaseConfig
from mashumaro.mixins.orjson import DataClassORJSONMixin

from app import ProcessingConfig
from app.mqtt import MQTTMessageProcessor, MQTTProcessorMessage

logger = logging.getLogger("processor.identity")


@dataclass
class Hello(DataClassORJSONMixin):
    name: str

    class Config(BaseConfig):
        cloudevent_type: str = "com.github.aschamberger.micro-dcs.identity.hello.v1"
        cloudevent_dataschema: str = (
            "https://aschamberger.github.io/schemas/micro-dcs/identity/hello-v1"
        )
        allow_deserialization_not_by_alias = True
        serialize_by_alias = True
        omit_none = True
        aliases = {
            "name": "NameField",
        }


async def handle_hello(hello: Hello) -> list[Hello] | Hello | None:
    logger.info("Received hello from: %s", hello.name)

    return [Hello(name=hello.name), Hello(name="Alice")]


class IdentityMQTTMessageProcessor(MQTTMessageProcessor):
    def __init__(self, runtime_config: ProcessingConfig):
        super().__init__(runtime_config, "identity")
        self.register_callback(Hello, handle_hello)

    async def process_message(
        self, message: MQTTProcessorMessage
    ) -> list[MQTTProcessorMessage] | MQTTProcessorMessage | None:
        logger.info("Processing message on topic: %s", message.topic)

        if not self.message_has_callback(message):
            logger.info(
                "No handler registered for message type: %s", message.cloudevent.type
            )
            # Special case: raw identity messages are echoed back
            # Normally, we would not do this here, but for identity messages it's acceptable
            if message.response_topic is None:
                logger.warning(
                    "No response topic specified; cannot send identity response."
                )
                return None
            response = self.create_message(
                topic=message.response_topic,
                payload=message.payload,
                qos=message.qos,
                retain=message.retain,
                cloudevent_type="com.github.aschamberger.micro-dcs.identity.raw.v1",
                cloudevent_dataschema="https://aschamberger.github.io/schemas/micro-dcs/identity/raw-v1",
            )
            return response

        result = await self.message_callback(message)
        return result

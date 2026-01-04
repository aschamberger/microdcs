import logging
from dataclasses import dataclass, field

from app import ProcessingConfig
from app.dataclass import DataClassConfig, DataClassMixin, DataClassValidationMixin
from app.mqtt import MQTTMessageProcessor, MQTTProcessorMessage

logger = logging.getLogger("processor.identity")


@dataclass
class HiddenObject(DataClassMixin):
    field: str

    class Config(DataClassConfig):
        pass


@dataclass
class Hello(DataClassMixin, DataClassValidationMixin):
    _hidden_str: str | None = field(kw_only=True, default=None)
    _hidden_obj: HiddenObject | None = field(kw_only=True, default=None)
    name: str = field(metadata={"min_length": 3, "max_length": 20})

    class Config(DataClassConfig):
        cloudevent_type: str = "com.github.aschamberger.micro-dcs.identity.hello.v1"
        cloudevent_dataschema: str = (
            "https://aschamberger.github.io/schemas/micro-dcs/identity/hello-v1"
        )
        aliases = {
            "name": "NameField",
        }


class IdentityMQTTMessageProcessor(MQTTMessageProcessor):
    @classmethod
    async def handle_hello(cls, hello: Hello) -> list[Hello] | Hello | None:
        logger.info("Received hello from: %s", hello.name)

        logger.debug("Processing %s %s %s", hello, hello._hidden_str, hello._hidden_obj)

        h1 = Hello(
            name=hello.name,
            _hidden_str=hello._hidden_str,
            _hidden_obj=hello._hidden_obj,
        )
        h2 = Hello(name="Alice")
        return [
            h1,
            h2,
        ]

    @classmethod
    def extract_hidden_str(cls, message: MQTTProcessorMessage) -> str | None:
        if message.user_properties is None:
            return None
        return message.user_properties.get("x-hidden-str", None)

    @classmethod
    def insert_hidden_str(cls, message: MQTTProcessorMessage, value: str) -> None:
        if message.user_properties is None:
            message.user_properties = {}
        message.user_properties["x-hidden-str"] = value

    @classmethod
    def extract_hidden_obj(cls, message: MQTTProcessorMessage) -> HiddenObject | None:
        if message.user_properties is None:
            return None
        obj_data = message.user_properties.get("x-hidden-obj", None)
        if obj_data is None:
            return None
        return HiddenObject.from_json(obj_data)

    @classmethod
    def insert_hidden_obj(
        cls, message: MQTTProcessorMessage, value: HiddenObject
    ) -> None:
        if message.user_properties is None:
            message.user_properties = {}
        message.user_properties["x-hidden-obj"] = value.to_json()

    def __init__(self, runtime_config: ProcessingConfig):
        super().__init__(runtime_config, "identity")
        self.register_callback(Hello, __class__.handle_hello)
        self.register_hidden_field(
            "hidden_str",
            extractor=__class__.extract_hidden_str,
            inserter=__class__.insert_hidden_str,
        )
        self.register_hidden_field(
            "hidden_obj",
            extractor=__class__.extract_hidden_obj,
            inserter=__class__.insert_hidden_obj,
        )

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

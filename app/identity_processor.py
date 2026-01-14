import asyncio
import logging
from dataclasses import dataclass, field

from app import ProcessingConfig
from app.common import CloudEvent
from app.dataclass import DataClassConfig, DataClassMixin, DataClassValidationMixin
from app.mqtt import MQTTCloudEventProcessor

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


class IdentityMQTTCloudEventProcessor(MQTTCloudEventProcessor):
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
    def extract_hidden_fields(
        cls, dclass: DataClassMixin, user_properties: dict[str, str]
    ):
        if hasattr(dclass, "_hidden_str"):
            setattr(dclass, "_hidden_str", user_properties.get("x-hidden-str", None))
        if hasattr(dclass, "_hidden_obj"):
            obj_data = user_properties.get("x-hidden-obj", None)
            if obj_data is not None:
                setattr(dclass, "_hidden_obj", HiddenObject.from_json(obj_data))

    @classmethod
    def insert_hidden_fields(
        cls, dclass: DataClassMixin, user_properties: dict[str, str]
    ) -> None:
        if user_properties is None:
            user_properties = {}
        str_data = getattr(dclass, "_hidden_str", None)
        if str_data is not None:
            user_properties["x-hidden-str"] = str_data
        obj_data = getattr(dclass, "_hidden_obj", None)
        if obj_data is not None:
            user_properties["x-hidden-obj"] = obj_data.to_json()

    def __init__(self, instance_id: str, runtime_config: ProcessingConfig):
        super().__init__(instance_id, runtime_config, "identity")
        self.register_callback(Hello, __class__.handle_hello)
        self.register_hidden_field_processor(
            "com.github.aschamberger.micro-dcs.identity.*",
            extractor=__class__.extract_hidden_fields,
            inserter=__class__.insert_hidden_fields,
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
            # Special case: raw identity messages are echoed back
            # Normally, we would not do this here, but for identity messages it's acceptable
            if (
                cloudevent.transportmetadata is None
                or cloudevent.transportmetadata.get("mqtt_response_topic") is None
            ):
                logger.warning(
                    "No response topic specified; cannot send identity response."
                )
                return None
            response = CloudEvent(
                data=cloudevent.data,
                type="com.github.aschamberger.micro-dcs.identity.raw.v1",
                dataschema="https://aschamberger.github.io/schemas/micro-dcs/identity/raw-v1",
                datacontenttype=cloudevent.datacontenttype,
                id=cloudevent.id,
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

    async def handle_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        await asyncio.sleep(timeout)  # yield control
        logger.info("Message expired: %s", cloudevent.id)
        return None

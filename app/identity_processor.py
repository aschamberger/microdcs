import asyncio
import logging
from dataclasses import InitVar, dataclass, field
from typing import Any

from app import ProcessingConfig
from app.common import CloudEvent, Direction
from app.dataclass import (
    DataClassConfig,
    DataClassMixin,
    DataClassResponseMixin,
    DataClassValidationMixin,
)
from app.mqtt import MQTTCloudEventProcessor
from app.msgpack import MessagePackCloudEventProcessor

logger = logging.getLogger("processor.identity")


@dataclass
class HiddenObject(DataClassMixin):
    field: str

    class Config(DataClassConfig):
        pass


class InitDataClassMixin(DataClassMixin):
    def __post_init__(self, __request_object__: Any = None) -> None:
        super_post_init = getattr(super(), "__post_init__", None)
        if super_post_init is not None:
            super_post_init()
        # Copy hidden fields from request object only when one is provided
        if __request_object__ is not None:
            self._hidden_str = getattr(__request_object__, "_hidden_str", None)
            self._hidden_obj = getattr(__request_object__, "_hidden_obj", None)


@dataclass(kw_only=True)
class Hello(
    InitDataClassMixin, DataClassValidationMixin, DataClassResponseMixin["Hello"]
):
    __request_object__: InitVar[Hello | None] = None
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


@dataclass(kw_only=True)
class Bye(DataClassMixin, DataClassValidationMixin):
    name: str = field(metadata={"min_length": 3, "max_length": 20})

    class Config(DataClassConfig):
        cloudevent_type: str = "com.github.aschamberger.micro-dcs.identity.bye.v1"
        cloudevent_dataschema: str = (
            "https://aschamberger.github.io/schemas/micro-dcs/identity/bye-v1"
        )


class IdentityCloudEventDelegate:
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

    @classmethod
    def extract_hidden_fields(
        cls, dclass: DataClassMixin, custommetadata: dict[str, str]
    ):
        if hasattr(dclass, "_hidden_str"):
            setattr(dclass, "_hidden_str", custommetadata.get("x-hidden-str", None))
        if hasattr(dclass, "_hidden_obj"):
            obj_data = custommetadata.get("x-hidden-obj", None)
            if obj_data is not None:
                setattr(dclass, "_hidden_obj", HiddenObject.from_json(obj_data))

    @classmethod
    def insert_hidden_fields(
        cls, dclass: DataClassMixin, custommetadata: dict[str, str]
    ) -> None:
        if custommetadata is None:
            custommetadata = {}
        str_data = getattr(dclass, "_hidden_str", None)
        if str_data is not None:
            custommetadata["x-hidden-str"] = str_data
        obj_data = getattr(dclass, "_hidden_obj", None)
        if obj_data is not None:
            custommetadata["x-hidden-obj"] = obj_data.to_json()


class IdentityMQTTCloudEventProcessor(MQTTCloudEventProcessor):
    def __init__(self, instance_id: str, runtime_config: ProcessingConfig):
        super().__init__(instance_id, runtime_config, "identity")
        self.register_callback(
            Hello, IdentityCloudEventDelegate.handle_hello, direction=Direction.INCOMING
        )
        self.register_callback(
            Bye, IdentityCloudEventDelegate.handle_bye, direction=Direction.OUTGOING
        )
        self.register_hidden_field_processor(
            "com.github.aschamberger.micro-dcs.identity.*",
            extractor=IdentityCloudEventDelegate.extract_hidden_fields,
            inserter=IdentityCloudEventDelegate.insert_hidden_fields,
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
            # Normally, we would not do this here, this is just to demo the functionality
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
        topic = "app/identity/bye"
        await self.type_callback(payload_type, topic)

    async def handle_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        await asyncio.sleep(timeout)  # yield control
        logger.info("Message expired: %s", cloudevent.id)
        return None


class IdentityMessagePackCloudEventProcessor(MessagePackCloudEventProcessor):
    def __init__(self, instance_id: str, runtime_config: ProcessingConfig):
        super().__init__(instance_id, runtime_config)
        self.register_callback(
            Hello, IdentityCloudEventDelegate.handle_hello, direction=Direction.INCOMING
        )
        self.register_callback(
            Bye, IdentityCloudEventDelegate.handle_bye, direction=Direction.OUTGOING
        )
        self.register_hidden_field_processor(
            "com.github.aschamberger.micro-dcs.identity.*",
            extractor=IdentityCloudEventDelegate.extract_hidden_fields,
            inserter=IdentityCloudEventDelegate.insert_hidden_fields,
        )

    async def process_event(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        if not self.event_has_callback(cloudevent):
            logger.info("No handler registered for message type: %s", cloudevent.type)
            # Special case: raw identity messages are echoed back
            # Normally, we would not do this here, this is just to demo the functionality
            response = CloudEvent(
                data=cloudevent.data,
                type="com.github.aschamberger.micro-dcs.identity.raw.v1",
                dataschema="https://aschamberger.github.io/schemas/micro-dcs/identity/raw-v1",
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
            "MessagePack identity processor does not process response events."
        )

    async def send_event(self) -> None:
        raise NotImplementedError(
            "MessagePack identity processor does not send events."
        )

    async def handle_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        raise NotImplementedError(
            "MessagePack identity processor does not handle expirations."
        )

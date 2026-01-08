import asyncio
from typing import Any

import aiomqtt
import orjson

from app import MQTTConfig
from app.common import DeliveryError, ErrorKind
from app.mqtt import MQTTHandler, MQTTProcessorMessage


async def main():
    mqtt_config: MQTTConfig = MQTTConfig()
    mqtt_config.identifier = "test-sender"
    mh: MQTTHandler = MQTTHandler(mqtt_config)

    mqtt_client: aiomqtt.Client = mh._client()

    topic: str = "app/identity/request"
    response_topic: str = "app/identity/response"
    error_topic: str = "app/identity/errors"

    async with mqtt_client:
        # Send a raw identity message
        payload: dict[str, Any] = {
            "message": "Hello, MQTT!",
            "some_number": "42",
            "something_else": "foo",
            "list_example": [1, 2, 3],
        }
        cloudevent_type = "com.github.aschamberger.micro-dcs.identity.raw.v1"
        cloudevent_dataschema = (
            "https://aschamberger.github.io/schemas/micro-dcs/identity/raw-v1"
        )
        await publish_message(
            mh,
            mqtt_client,
            topic,
            response_topic,
            orjson.dumps(payload),
            cloudevent_type,
            cloudevent_dataschema,
        )

        # Send a Hello identity message
        payload: dict[str, Any] = {
            "NameField": "Bob",
            "addition": "42",
        }
        cloudevent_type = "com.github.aschamberger.micro-dcs.identity.hello.v1"
        cloudevent_dataschema = (
            "https://aschamberger.github.io/schemas/micro-dcs/identity/hello-v1"
        )
        user_properties = {
            "x-hidden-str": "This is a hidden string",
            "x-hidden-obj": orjson.dumps({"field": "This is a hidden object field"}),
        }
        await publish_message(
            mh,
            mqtt_client,
            topic,
            response_topic,
            orjson.dumps(payload),
            cloudevent_type,
            cloudevent_dataschema,
            user_properties,
        )

        # send a DeliveryError message
        payload: dict[str, Any] = {
            "error_kind": "DELIVERY_TIMEOUT",
            "error_message": "The message could not be delivered within the timeout period.",
            "error_context": {"abort_message_delivery_timeout": 5.0},
            "original_topic": topic,
            "original_payload": orjson.Fragment(orjson.dumps({"NameField": "Charlie"})),
        }
        error = DeliveryError(
            error_kind=ErrorKind.TIMEOUT,
            error_message="The message could not be delivered within the timeout period.",
            error_context={
                "abort_message_delivery_timeout": 5.0,
                "message_expiry_interval": 10,
            },
            original_topic=topic,
            original_payload=orjson.dumps({"NameField": "Charlie"}),
        )

        cloudevent_type = "com.github.aschamberger.micro-dcs.deliveryerror.v1"
        cloudevent_dataschema = (
            "https://aschamberger.github.io/schemas/micro-dcs/deliveryerror/v1"
        )
        await publish_message(
            mh,
            mqtt_client,
            error_topic,
            None,
            error.to_jsonb(),
            cloudevent_type,
            cloudevent_dataschema,
        )


async def publish_message(
    mh: MQTTHandler,
    mqtt_client: aiomqtt.Client,
    topic: str,
    response_topic: str | None,
    payload: bytes,
    cloudevent_type: str,
    cloudevent_dataschema: str,
    user_properties: dict[str, str] | None = None,
):
    message = MQTTProcessorMessage(
        topic=topic,
        payload=payload,
        response_topic=response_topic,
    )
    message.cloudevent.source = "https://example.com/sender"
    message.cloudevent.subject = "test"
    message.cloudevent.type = cloudevent_type
    message.cloudevent.dataschema = cloudevent_dataschema

    if user_properties is not None:
        if message.user_properties is None:
            message.user_properties = {}
        message.user_properties |= user_properties

    await mh._publish_message(mqtt_client, message)


if __name__ == "__main__":
    asyncio.run(main())

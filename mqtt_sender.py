import asyncio
from typing import Any

import aiomqtt
import orjson

from app import MQTTConfig
from app.mqtt import MQTTHandler, MQTTProcessorMessage


async def main():
    mqtt_config: MQTTConfig = MQTTConfig()
    mqtt_config.identifier = "test-sender"
    mh: MQTTHandler = MQTTHandler(mqtt_config)

    mqtt_client: aiomqtt.Client = mh._client()

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
            mh, mqtt_client, payload, cloudevent_type, cloudevent_dataschema
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
        await publish_message(
            mh, mqtt_client, payload, cloudevent_type, cloudevent_dataschema
        )


async def publish_message(
    mh: MQTTHandler,
    mqtt_client: aiomqtt.Client,
    payload: dict[str, Any],
    cloudevent_type: str,
    cloudevent_dataschema: str,
):
    topic: str = "app/identity/request"
    response_topic: str = "app/identity/response"

    message = MQTTProcessorMessage(
        topic=topic,
        payload=orjson.dumps(payload),
        response_topic=response_topic,
    )
    message.cloudevent.source = "https://example.com/sender"
    message.cloudevent.subject = "test"
    message.cloudevent.type = cloudevent_type
    message.cloudevent.dataschema = cloudevent_dataschema

    await mh._publish_message(mqtt_client, message)


asyncio.run(main())

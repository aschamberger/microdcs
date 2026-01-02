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
        topic: str = "app/identity/request"
        payload: dict[str, Any] = {
            "message": "Hello, MQTT!",
            "some_number": "42",
            "something_else": "foo",
            "list_example": [1, 2, 3],
        }
        response_topic: str = "app/identity/response"

        message = MQTTProcessorMessage(
            topic=topic,
            payload=orjson.dumps(payload),
            response_topic=response_topic,
        )
        message.cloudevent.source = "https://example.com/sender"
        message.cloudevent.subject = "test"
        message.cloudevent.type = "com.example.test"
        message.cloudevent.dataschema = "https://example.com/schemas/test"

        await mh._publish_message(mqtt_client, message)


asyncio.run(main())

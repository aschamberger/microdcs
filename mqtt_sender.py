import asyncio
from typing import Any, Callable

import aiomqtt
import orjson
import redis.asyncio as redis

from app import MQTTConfig, RedisConfig
from app.common import CloudEvent
from app.dataclass import DataClassMixin
from app.identity_processor import (
    Hello,
    HiddenObject,
    IdentityCloudEventDelegate,
    IdentityMQTTCloudEventProcessor,
)
from app.mqtt import MQTTHandler


async def main():
    redis_config: RedisConfig = RedisConfig()
    redis_connection_pool = redis.ConnectionPool(
        host=redis_config.hostname, port=redis_config.port, protocol=3
    )
    mqtt_config: MQTTConfig = MQTTConfig()
    mqtt_config.identifier = "test-sender"
    mh: MQTTHandler = MQTTHandler(mqtt_config, redis_connection_pool)

    mqtt_client: aiomqtt.Client = mh._client()

    topic: str = "app/identity/request"
    response_topic: str = "app/identity/response"
    ce_source = "https://aschamberger.github.com/micro-dcs/test-sender"

    async with mqtt_client:
        # Send a raw identity message
        payload: dict[str, Any] = {
            "message": "Hello, MQTT!",
            "some_number": "42",
            "something_else": "foo",
            "list_example": [1, 2, 3],
        }
        raw_ce = CloudEvent(
            source=ce_source,
            data=orjson.dumps(payload),
            type="com.github.aschamberger.micro-dcs.identity.raw.v1",
            dataschema="https://aschamberger.github.io/schemas/micro-dcs/identity/raw-v1",
            datacontenttype="application/json; charset=utf-8",
            transportmetadata={
                "mqtt_topic": topic,
                "mqtt_response_topic": response_topic,
            },
        )
        await mh._publish_message(mqtt_client, raw_ce)

        # Send a Hello identity message
        bob = Hello(
            name="Bob",
            _hidden_str="This is a hidden string",
            _hidden_obj=HiddenObject(field="This is a hidden object field"),
        )
        bob_ce_1 = CloudEvent(
            source=ce_source,
            datacontenttype="application/json; charset=utf-8",
            transportmetadata={
                "mqtt_topic": topic,
                "mqtt_response_topic": response_topic,
            },
        )
        hidden_field_processors: dict[
            str,
            tuple[
                Callable[[DataClassMixin, dict[str, str]], None] | None,
                Callable[[DataClassMixin, dict[str, str]], None] | None,
            ],
        ] = {
            "com.github.aschamberger.micro-dcs.identity.hello.v1": (
                IdentityCloudEventDelegate.extract_hidden_fields,
                IdentityCloudEventDelegate.insert_hidden_fields,
            ),
        }
        bob_ce_1.serialize_payload(bob, hidden_field_processors)
        await mh._publish_message(mqtt_client, bob_ce_1)

        # Send another Hello identity message with additional field in payload
        payload: dict[str, Any] = {
            "NameField": "Bob",
            "addition": "42",
        }
        bob_ce_2 = CloudEvent(
            source=ce_source,
            data=orjson.dumps(payload),
            type="com.github.aschamberger.micro-dcs.identity.hello.v1",
            dataschema="https://aschamberger.github.io/schemas/micro-dcs/identity/hello-v1",
            datacontenttype="application/json; charset=utf-8",
            custommetadata={
                "x-hidden-str": "This is a hidden string",
                "x-hidden-obj": str(
                    orjson.dumps({"field": "This is a hidden object field"}), "utf-8"
                ),
            },
            transportmetadata={
                "mqtt_topic": topic,
                "mqtt_response_topic": response_topic,
            },
        )
        await mh._publish_message(mqtt_client, bob_ce_2)


if __name__ == "__main__":
    asyncio.run(main())

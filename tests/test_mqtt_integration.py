from typing import Any

import orjson
import pytest
import pytest_asyncio
import redis.asyncio as redis

from app import MQTTConfig, RedisConfig
from app.common import CloudEvent
from app.models.greetings import Hello, HiddenObject
from app.mqtt import MQTTHandler
from app.redis import RedisKeySchema
from tests.conftest import integration, mqtt_available, redis_available

MQTT_CONFIG = MQTTConfig()
REDIS_CONFIG = RedisConfig()


TOPIC = "app/greetings/request"
RESPONSE_TOPIC = "app/greetings/response"
CE_SOURCE = "https://aschamberger.github.com/micro-dcs/test-sender"


@pytest_asyncio.fixture
async def mqtt_handler():
    redis_connection_pool = redis.ConnectionPool(
        host=REDIS_CONFIG.hostname, port=REDIS_CONFIG.port, protocol=3
    )
    redis_key_schema = RedisKeySchema()
    mqtt_config = MQTTConfig()
    mqtt_config.identifier = "test-sender"
    handler = MQTTHandler(mqtt_config, redis_connection_pool, redis_key_schema)
    yield handler
    await redis_connection_pool.aclose()


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
async def test_publish_raw_greetings_message(mqtt_handler: MQTTHandler):
    """Send a raw greetings message via MQTT."""
    mqtt_client = mqtt_handler._client()

    payload: dict[str, Any] = {
        "message": "Hello, MQTT!",
        "some_number": "42",
        "something_else": "foo",
        "list_example": [1, 2, 3],
    }
    raw_ce = CloudEvent(
        source=CE_SOURCE,
        data=orjson.dumps(payload),
        type="com.github.aschamberger.microdcs.greetings.raw.v1",
        dataschema="https://aschamberger.github.io/schemas/microdcs/greetings/v1.0.0/raw",
        datacontenttype="application/json; charset=utf-8",
        transportmetadata={
            "mqtt_topic": TOPIC,
            "mqtt_response_topic": RESPONSE_TOPIC,
        },
    )

    async with mqtt_client:
        await mqtt_handler._publish_message(mqtt_client, raw_ce)


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
async def test_publish_hello_greetings_message(mqtt_handler: MQTTHandler):
    """Send a Hello greetings message with hidden fields via MQTT."""
    mqtt_client = mqtt_handler._client()

    bob = Hello(
        name="Bob",
        _hidden_str="This is a hidden string",
        _hidden_obj=HiddenObject(field="This is a hidden object field"),
    )
    bob_ce = CloudEvent(
        source=CE_SOURCE,
        datacontenttype="application/json; charset=utf-8",
        transportmetadata={
            "mqtt_topic": TOPIC,
            "mqtt_response_topic": RESPONSE_TOPIC,
        },
    )
    bob_ce.serialize_payload(bob)

    async with mqtt_client:
        await mqtt_handler._publish_message(mqtt_client, bob_ce)


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
async def test_publish_hello_with_additional_payload_fields(
    mqtt_handler: MQTTHandler,
):
    """Send a Hello greetings message with additional fields in payload via MQTT."""
    mqtt_client = mqtt_handler._client()

    payload: dict[str, Any] = {
        "NameField": "Bob",
        "addition": "42",
    }
    bob_ce = CloudEvent(
        source=CE_SOURCE,
        data=orjson.dumps(payload),
        type="com.github.aschamberger.microdcs.greetings.hello.v1",
        dataschema="https://aschamberger.github.io/schemas/microdcs/greetings/v1.0.0/hello",
        datacontenttype="application/json; charset=utf-8",
        custommetadata={
            "x-hidden-str": "This is a hidden string",
            "x-hidden-obj": str(
                orjson.dumps({"field": "This is a hidden object field"}), "utf-8"
            ),
        },
        transportmetadata={
            "mqtt_topic": TOPIC,
            "mqtt_response_topic": RESPONSE_TOPIC,
        },
    )

    async with mqtt_client:
        await mqtt_handler._publish_message(mqtt_client, bob_ce)

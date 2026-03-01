import asyncio
from typing import Any

import aiomqtt
import orjson
import pytest
import pytest_asyncio
import redis.asyncio as redis

from app import MQTTConfig, ProcessingConfig, RedisConfig
from app.common import CloudEvent, MessageIntent
from app.models.greetings import Hello, HiddenObject
from app.models.machinery_jobs import (
    EUInformation,
    ISA95JobOrderDataType,
    ISA95MaterialDataType,
    ISA95PropertyDataType,
    LocalizedText,
    OutputInformationDataType,
    StartCall,
    StoreAndStartCall,
    StoreAndStartResponse,
    StoreCall,
    StoreResponse,
)
from app.models.machinery_jobs_ext import MethodReturnStatus
from app.mqtt import MQTTHandler
from app.processors.greetings import GreetingsCloudEventProcessor
from app.processors.machinery_jobs import MachineryJobsCloudEventProcessor
from app.redis import RedisKeySchema
from tests.conftest import app_available, integration, mqtt_available, redis_available

MQTT_CONFIG = MQTTConfig()
REDIS_CONFIG = RedisConfig()

# --------------------------------------------------------------------------
# Dynamic topic configuration — mirrors the real app setup so that tests
# survive changes to ProcessorBinding / MessageIntent / topic prefix values.
# --------------------------------------------------------------------------

GREETINGS_PREFIX = "app/greetings"
JOBS_PREFIX = "app/jobs"
SCOPE = "woodworking"  # used as CE subject in some tests

PROCESSING_CONFIG = ProcessingConfig(
    topic_prefixes={
        f"greetings:{GREETINGS_PREFIX}",
        f"machinery_jobs:{JOBS_PREFIX}",
    },
    response_topics={
        f"greetings:{GREETINGS_PREFIX}/responses",
        f"machinery_jobs:{JOBS_PREFIX}/responses",
    },
)

# Derive the actual publish/subscribe topics from the processor config
# so tests don't break when intents or topic structure change.
_greetings_subscribe_intents = GreetingsCloudEventProcessor._subscribe_intents
_greetings_publish_intents = GreetingsCloudEventProcessor._publish_intents
_jobs_subscribe_intents = MachineryJobsCloudEventProcessor._subscribe_intents
_jobs_publish_intents = MachineryJobsCloudEventProcessor._publish_intents

# Greetings is southbound: subscribes to data/events/meta, publishes to commands
GREETINGS_SUBSCRIBE_TOPICS = {
    f"{GREETINGS_PREFIX}/{intent.value}" for intent in _greetings_subscribe_intents
}
# Topic the test sender publishes greetings events to (one of the subscribe intents)
GREETINGS_EVENT_TOPIC = f"{GREETINGS_PREFIX}/{MessageIntent.EVENT.value}"

# Jobs is northbound: subscribes to commands, publishes to data/events/meta
JOBS_SUBSCRIBE_TOPICS = {
    f"{JOBS_PREFIX}/{SCOPE.replace('.', '/')}/{intent.value}"
    for intent in _jobs_subscribe_intents
}
# Topic the test sender publishes job commands to (the subscribe intent)
JOBS_COMMAND_TOPIC = (
    f"{JOBS_PREFIX}/{SCOPE.replace('.', '/')}/{MessageIntent.COMMAND.value}"
)

CE_SOURCE = "https://aschamberger.github.com/microdcs/test-sender"
# Test-only response topics (not derived from the app, just for the test sender)
GREETINGS_RESPONSE_TOPIC = "tests/greetings/response"
JOBS_RESPONSE_TOPIC = "tests/jobs/response"


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


# Default timeout (seconds) for waiting on response messages from the app.
RESPONSE_TIMEOUT = 5.0


async def _publish_and_collect_responses(
    mqtt_handler: MQTTHandler,
    cloudevent: CloudEvent,
    *,
    expected_responses: int = 1,
    timeout: float = RESPONSE_TIMEOUT,
) -> list[aiomqtt.Message]:
    """Subscribe to the response topic, publish *cloudevent*, and collect responses.

    Returns the list of :class:`aiomqtt.Message` objects received on the
    response topic within *timeout* seconds.  The helper waits until
    *expected_responses* messages arrive or the timeout expires — whichever
    comes first.
    """
    response_topic = cloudevent.transportmetadata["mqtt_response_topic"]  # type: ignore[index]
    mqtt_client = mqtt_handler._client()
    collected: list[aiomqtt.Message] = []

    async with mqtt_client:
        await mqtt_client.subscribe(response_topic)
        await mqtt_handler._publish_message(mqtt_client, cloudevent)

        try:
            async with asyncio.timeout(timeout):
                async for message in mqtt_client.messages:
                    collected.append(message)
                    if len(collected) >= expected_responses:
                        break
        except TimeoutError:
            pass  # return whatever was collected so far

    return collected


def _assert_cloudevent_type(message: aiomqtt.Message, expected_type: str) -> None:
    """Assert that an MQTT v5 message carries the expected CloudEvent ``type``
    in its UserProperty list."""
    user_props = {}
    if message.properties and hasattr(message.properties, "UserProperty"):
        user_props = dict(message.properties.UserProperty)  # type: ignore[arg-type]
    assert user_props.get("type") == expected_type, (
        f"Expected CE type '{expected_type}', got '{user_props.get('type')}'"
    )


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
@app_available
async def test_publish_raw_greetings_message(mqtt_handler: MQTTHandler):
    """Send a raw greetings message and verify the echo response."""
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
            "mqtt_topic": GREETINGS_EVENT_TOPIC,
            "mqtt_response_topic": GREETINGS_RESPONSE_TOPIC,
        },
    )

    responses = await _publish_and_collect_responses(
        mqtt_handler, raw_ce, expected_responses=1
    )

    assert len(responses) == 1, f"Expected 1 echo response, got {len(responses)}"
    response_payload = orjson.loads(responses[0].payload)
    assert response_payload == payload


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
@app_available
async def test_publish_hello_greetings_message(mqtt_handler: MQTTHandler):
    """Send a Hello greetings message and verify two Hello responses."""
    bob = Hello(
        name="Bob",
        _hidden_str="This is a hidden string",
        _hidden_obj=HiddenObject(field="This is a hidden object field"),
    )
    bob_ce = CloudEvent(
        source=CE_SOURCE,
        datacontenttype="application/json; charset=utf-8",
        transportmetadata={
            "mqtt_topic": GREETINGS_EVENT_TOPIC,
            "mqtt_response_topic": GREETINGS_RESPONSE_TOPIC,
        },
    )
    bob_ce.serialize_payload(bob)

    # handle_hello returns [response(name="Bob"), Hello(name="Alice")]
    responses = await _publish_and_collect_responses(
        mqtt_handler, bob_ce, expected_responses=2
    )

    assert len(responses) == 2, f"Expected 2 Hello responses, got {len(responses)}"
    names = {orjson.loads(r.payload).get("Name") for r in responses}
    assert names == {"Bob", "Alice"}


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
@app_available
async def test_publish_hello_with_additional_payload_fields(
    mqtt_handler: MQTTHandler,
):
    """Send a Hello greetings message with additional fields and verify responses."""
    payload: dict[str, Any] = {
        "Name": "Bob",
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
            "mqtt_topic": GREETINGS_EVENT_TOPIC,
            "mqtt_response_topic": GREETINGS_RESPONSE_TOPIC,
        },
    )

    # handle_hello returns [response(name="Bob"), Hello(name="Alice")]
    responses = await _publish_and_collect_responses(
        mqtt_handler, bob_ce, expected_responses=2
    )

    assert len(responses) == 2, f"Expected 2 Hello responses, got {len(responses)}"
    names = {orjson.loads(r.payload).get("Name") for r in responses}
    assert names == {"Bob", "Alice"}


# ===================================================================
# OPC UA Machinery Jobs B.3 example – Woodworking job order
# ===================================================================


def _make_b3_woodworking_job_order(
    job_order_id: str = "12345",
) -> ISA95JobOrderDataType:
    """Create a job order matching the OPC UA Machinery Jobs B.3 example.

    A woodworking machine breaks a tree trunk into a shelf floor,
    four table legs, and a bag of spruce chips.
    https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/B.3
    """
    tree_input = ISA95MaterialDataType(
        material_use="Material consumed",
        quantity="1",
        engineering_units=EUInformation(
            display_name=LocalizedText(text="pcs", locale="en"),
        ),
        properties=[
            ISA95PropertyDataType(
                id="Identification",
                value=OutputInformationDataType(
                    item_number="TreeTrunk",
                ).to_dict(),
            ),
            ISA95PropertyDataType(id="Param_771", value="200"),
            ISA95PropertyDataType(id="Location", value="Cutting_line_input_1"),
        ],
    )

    shelf_floor_output = ISA95MaterialDataType(
        material_use="Material produced",
        quantity="1",
        engineering_units=EUInformation(
            display_name=LocalizedText(text="pcs", locale="en"),
        ),
        properties=[
            ISA95PropertyDataType(
                id="Identification",
                value=OutputInformationDataType(
                    item_number="Shelf_Floor_0010",
                    order_number="Order_Forest_Utilize_01",
                    lot_number="Forest_Spruce_ShelfFloor_1245",
                    serial_number="FSSF_1234568",
                ).to_dict(),
            ),
            ISA95PropertyDataType(id="Param_1234", value="2500"),
            ISA95PropertyDataType(id="Param_1235", value="1500"),
            ISA95PropertyDataType(id="Param_1236", value="15.8"),
            ISA95PropertyDataType(id="Location", value="Cutting_line_output_1"),
        ],
    )

    table_legs_output = ISA95MaterialDataType(
        material_use="Material produced",
        quantity="4",
        engineering_units=EUInformation(
            display_name=LocalizedText(text="pcs", locale="en"),
        ),
        properties=[
            ISA95PropertyDataType(
                id="Identification",
                value=OutputInformationDataType(
                    item_number="Table_Leg_012",
                    order_number="Order_Forest_Utilize_01",
                    lot_number="Forest_Spruce_TableLeg_124",
                ).to_dict(),
            ),
            ISA95PropertyDataType(id="Param_1234", value="125"),
            ISA95PropertyDataType(id="Param_1235", value="855"),
            ISA95PropertyDataType(id="Param_1236", value="125"),
            ISA95PropertyDataType(id="Location", value="Cutting_line_output_2"),
        ],
    )

    chips_output = ISA95MaterialDataType(
        material_use="Material produced",
        quantity="1.75",
        engineering_units=EUInformation(
            display_name=LocalizedText(text="m^3", locale="en"),
        ),
        properties=[
            ISA95PropertyDataType(
                id="Identification",
                value=OutputInformationDataType(
                    item_number="SpruceChips_012",
                ).to_dict(),
            ),
            ISA95PropertyDataType(id="Param_333", value="Spruce"),
            ISA95PropertyDataType(id="Location", value="BagFiller_output_1"),
        ],
    )

    return ISA95JobOrderDataType(
        job_order_id=job_order_id,
        description=[
            LocalizedText(text="Order_Forest_Utilize_01", locale="en"),
        ],
        start_time="2023-01-27T10:17:00Z",
        end_time="2023-01-27T10:19:00Z",
        material_requirements=[
            tree_input,
            shelf_floor_output,
            table_legs_output,
            chips_output,
        ],
    )


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
@app_available
async def test_publish_store_job_order_message(mqtt_handler: MQTTHandler):
    """Send a StoreCall and verify a successful StoreResponse."""
    job_order = _make_b3_woodworking_job_order(job_order_id="store-test-1")
    store_call = StoreCall(
        job_order=job_order,
        comment=[LocalizedText(text="Store woodworking job", locale="en")],
    )
    ce = CloudEvent(
        source=CE_SOURCE,
        datacontenttype="application/json; charset=utf-8",
        subject=SCOPE,
        transportmetadata={
            "mqtt_topic": JOBS_COMMAND_TOPIC,
            "mqtt_response_topic": JOBS_RESPONSE_TOPIC,
        },
    )
    ce.serialize_payload(store_call)

    responses = await _publish_and_collect_responses(
        mqtt_handler, ce, expected_responses=1
    )

    assert len(responses) == 1, f"Expected 1 StoreResponse, got {len(responses)}"
    response_data = orjson.loads(responses[0].payload)
    assert response_data["ReturnStatus"] == MethodReturnStatus.NO_ERROR
    _assert_cloudevent_type(responses[0], StoreResponse.Config.cloudevent_type)


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
@app_available
async def test_publish_store_and_start_job_order_message(mqtt_handler: MQTTHandler):
    """Send a StoreAndStartCall and verify a successful StoreAndStartResponse."""
    job_order = _make_b3_woodworking_job_order(job_order_id="store-and-start-1")
    store_and_start_call = StoreAndStartCall(
        job_order=job_order,
        comment=[
            LocalizedText(text="Store and start woodworking job", locale="en"),
        ],
    )
    ce = CloudEvent(
        source=CE_SOURCE,
        datacontenttype="application/json; charset=utf-8",
        subject=SCOPE,
        transportmetadata={
            "mqtt_topic": JOBS_COMMAND_TOPIC,
            "mqtt_response_topic": JOBS_RESPONSE_TOPIC,
        },
    )
    ce.serialize_payload(store_and_start_call)

    responses = await _publish_and_collect_responses(
        mqtt_handler, ce, expected_responses=1
    )

    assert len(responses) == 1, (
        f"Expected 1 StoreAndStartResponse, got {len(responses)}"
    )
    response_data = orjson.loads(responses[0].payload)
    assert response_data["ReturnStatus"] == MethodReturnStatus.NO_ERROR
    _assert_cloudevent_type(responses[0], StoreAndStartResponse.Config.cloudevent_type)


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
@app_available
async def test_publish_start_job_order_message(mqtt_handler: MQTTHandler):
    """Store a job order then start it, verifying both responses."""
    # First, store the job so it exists in Redis
    job_order = _make_b3_woodworking_job_order(job_order_id="start-test-1")
    store_call = StoreCall(
        job_order=job_order,
        comment=[LocalizedText(text="Store before start", locale="en")],
    )
    store_ce = CloudEvent(
        source=CE_SOURCE,
        datacontenttype="application/json; charset=utf-8",
        subject=SCOPE,
        transportmetadata={
            "mqtt_topic": JOBS_COMMAND_TOPIC,
            "mqtt_response_topic": JOBS_RESPONSE_TOPIC,
        },
    )
    store_ce.serialize_payload(store_call)
    store_responses = await _publish_and_collect_responses(
        mqtt_handler, store_ce, expected_responses=1
    )
    assert len(store_responses) == 1
    assert (
        orjson.loads(store_responses[0].payload)["ReturnStatus"]
        == MethodReturnStatus.NO_ERROR
    )

    # Now send the StartCall referencing the stored job order
    start_call = StartCall(
        job_order_id="start-test-1",
        comment=[
            LocalizedText(text="Start the woodworking job", locale="en"),
        ],
    )
    ce = CloudEvent(
        source=CE_SOURCE,
        datacontenttype="application/json; charset=utf-8",
        subject=SCOPE,
        transportmetadata={
            "mqtt_topic": JOBS_COMMAND_TOPIC,
            "mqtt_response_topic": JOBS_RESPONSE_TOPIC,
        },
    )
    ce.serialize_payload(start_call)

    responses = await _publish_and_collect_responses(
        mqtt_handler, ce, expected_responses=1
    )

    assert len(responses) == 1, f"Expected 1 StartResponse, got {len(responses)}"
    response_data = orjson.loads(responses[0].payload)
    assert response_data["ReturnStatus"] == MethodReturnStatus.NO_ERROR


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
@app_available
async def test_publish_store_job_order_raw_json(mqtt_handler: MQTTHandler):
    """Send a StoreCall as raw JSON and verify a successful StoreResponse."""
    payload: dict[str, Any] = {
        "JobOrder": {
            "JobOrderID": "raw-json-1",
            "Description": [
                {"Text": "Order_Forest_Utilize_01", "Locale": "en"},
            ],
            "StartTime": "2023-01-27T10:17:00Z",
            "EndTime": "2023-01-27T10:19:00Z",
            "MaterialRequirements": [
                {
                    "MaterialUse": "Material consumed",
                    "Quantity": "1",
                    "EngineeringUnits": {
                        "DisplayName": {"Text": "pcs", "Locale": "en"},
                    },
                    "Properties": [
                        {
                            "ID": "Identification",
                            "Value": {"ItemNumber": "TreeTrunk"},
                        },
                        {"ID": "Param_771", "Value": "200"},
                        {"ID": "Location", "Value": "Cutting_line_input_1"},
                    ],
                },
                {
                    "MaterialUse": "Material produced",
                    "Quantity": "1",
                    "EngineeringUnits": {
                        "DisplayName": {"Text": "pcs", "Locale": "en"},
                    },
                    "Properties": [
                        {
                            "ID": "Identification",
                            "Value": {
                                "ItemNumber": "Shelf_Floor_0010",
                                "OrderNumber": "Order_Forest_Utilize_01",
                                "LotNumber": "Forest_Spruce_ShelfFloor_1245",
                                "SerialNumber": "FSSF_1234568",
                            },
                        },
                        {"ID": "Param_1234", "Value": "2500"},
                        {"ID": "Param_1235", "Value": "1500"},
                        {"ID": "Param_1236", "Value": "15.8"},
                        {"ID": "Location", "Value": "Cutting_line_output_1"},
                    ],
                },
            ],
        },
        "Comment": [{"Text": "Store woodworking job (raw JSON)", "Locale": "en"}],
    }

    ce = CloudEvent(
        source=CE_SOURCE,
        data=orjson.dumps(payload),
        type=StoreCall.Config.cloudevent_type,
        dataschema=StoreCall.Config.cloudevent_dataschema,
        datacontenttype="application/json; charset=utf-8",
        subject=SCOPE,
        transportmetadata={
            "mqtt_topic": JOBS_COMMAND_TOPIC,
            "mqtt_response_topic": JOBS_RESPONSE_TOPIC,
        },
    )

    responses = await _publish_and_collect_responses(
        mqtt_handler, ce, expected_responses=1
    )

    assert len(responses) == 1, f"Expected 1 StoreResponse, got {len(responses)}"
    response_data = orjson.loads(responses[0].payload)
    assert response_data["ReturnStatus"] == MethodReturnStatus.NO_ERROR
    _assert_cloudevent_type(responses[0], StoreResponse.Config.cloudevent_type)

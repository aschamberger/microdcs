from typing import Any

import orjson
import pytest
import pytest_asyncio
import redis.asyncio as redis

from app import MQTTConfig, RedisConfig
from app.common import CloudEvent
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
    StoreCall,
)
from app.mqtt import MQTTHandler
from app.redis import RedisKeySchema
from tests.conftest import integration, mqtt_available, redis_available

MQTT_CONFIG = MQTTConfig()
REDIS_CONFIG = RedisConfig()


GREETINGS_TOPIC = "app/greetings/request"
GREETINGS_RESPONSE_TOPIC = "app/greetings/response"
JOBS_TOPIC = "app/jobs/woodworking/command"
JOBS_RESPONSE_TOPIC = "app/jobs/woodworking/response"
CE_SOURCE = "https://aschamberger.github.com/microdcs/test-sender"


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
            "mqtt_topic": GREETINGS_TOPIC,
            "mqtt_response_topic": GREETINGS_RESPONSE_TOPIC,
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
            "mqtt_topic": GREETINGS_TOPIC,
            "mqtt_response_topic": GREETINGS_RESPONSE_TOPIC,
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
            "mqtt_topic": GREETINGS_TOPIC,
            "mqtt_response_topic": GREETINGS_RESPONSE_TOPIC,
        },
    )

    async with mqtt_client:
        await mqtt_handler._publish_message(mqtt_client, bob_ce)


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
async def test_publish_store_job_order_message(mqtt_handler: MQTTHandler):
    """Send a StoreCall with the B.3 woodworking job order via MQTT."""
    mqtt_client = mqtt_handler._client()

    job_order = _make_b3_woodworking_job_order()
    store_call = StoreCall(
        job_order=job_order,
        comment=[LocalizedText(text="Store woodworking job", locale="en")],
    )
    ce = CloudEvent(
        source=CE_SOURCE,
        datacontenttype="application/json; charset=utf-8",
        transportmetadata={
            "mqtt_topic": JOBS_TOPIC,
            "mqtt_response_topic": JOBS_RESPONSE_TOPIC,
        },
    )
    ce.serialize_payload(store_call)

    async with mqtt_client:
        await mqtt_handler._publish_message(mqtt_client, ce)


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
async def test_publish_store_and_start_job_order_message(mqtt_handler: MQTTHandler):
    """Send a StoreAndStartCall with the B.3 woodworking job order via MQTT."""
    mqtt_client = mqtt_handler._client()

    job_order = _make_b3_woodworking_job_order()
    store_and_start_call = StoreAndStartCall(
        job_order=job_order,
        comment=[
            LocalizedText(text="Store and start woodworking job", locale="en"),
        ],
    )
    ce = CloudEvent(
        source=CE_SOURCE,
        datacontenttype="application/json; charset=utf-8",
        transportmetadata={
            "mqtt_topic": JOBS_TOPIC,
            "mqtt_response_topic": JOBS_RESPONSE_TOPIC,
        },
    )
    ce.serialize_payload(store_and_start_call)

    async with mqtt_client:
        await mqtt_handler._publish_message(mqtt_client, ce)


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
async def test_publish_start_job_order_message(mqtt_handler: MQTTHandler):
    """Send a StartCall referencing an existing job order ID via MQTT."""
    mqtt_client = mqtt_handler._client()

    start_call = StartCall(
        job_order_id="12345",
        comment=[
            LocalizedText(text="Start the woodworking job", locale="en"),
        ],
    )
    ce = CloudEvent(
        source=CE_SOURCE,
        datacontenttype="application/json; charset=utf-8",
        transportmetadata={
            "mqtt_topic": JOBS_TOPIC,
            "mqtt_response_topic": JOBS_RESPONSE_TOPIC,
        },
    )
    ce.serialize_payload(start_call)

    async with mqtt_client:
        await mqtt_handler._publish_message(mqtt_client, ce)


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
async def test_publish_store_job_order_raw_json(mqtt_handler: MQTTHandler):
    """Send a StoreCall as raw JSON payload (not via serialize_payload) via MQTT."""
    mqtt_client = mqtt_handler._client()

    payload: dict[str, Any] = {
        "JobOrder": {
            "JobOrderID": "12345",
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
        transportmetadata={
            "mqtt_topic": JOBS_TOPIC,
            "mqtt_response_topic": JOBS_RESPONSE_TOPIC,
        },
    )

    async with mqtt_client:
        await mqtt_handler._publish_message(mqtt_client, ce)

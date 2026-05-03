import socket
from dataclasses import dataclass

import pytest

from microdcs import MessagePackConfig, MQTTConfig, ProcessingConfig, RedisConfig
from microdcs.common import (
    CloudEventProcessor,
    ProcessorBinding,
    processor_config,
)
from microdcs.dataclass import DataClassConfig, DataClassMixin
from microdcs.models.greetings import Bye
from microdcs.models.machinery_jobs import (
    EUInformation,
    ISA95JobOrderDataType,
    ISA95MaterialDataType,
    ISA95PropertyDataType,
    LocalizedText,
    OutputInformationDataType,
)
from microdcs.models.machinery_jobs_ext import ConfigWorkMaster
from microdcs.models.sfc_recipe import (
    SfcActionAssociation,
    SfcActionQualifier,
    SfcInteraction,
    SfcRecipe,
    SfcStep,
    SfcTransition,
)
from microdcs.models.sfc_recipe_ext import SFC_RECIPE_DATASCHEMA

MQTT_CONFIG = MQTTConfig()
REDIS_CONFIG = RedisConfig()
MSGPACK_CONFIG = MessagePackConfig()


# ---------------------------------------------------------------------------
# Shared test dataclasses
# ---------------------------------------------------------------------------


@dataclass
class SamplePayload(DataClassMixin):
    value: str = "hello"

    class Config(DataClassConfig):
        type_id: str = "com.test.sample.v1"
        type_schema: str = "https://example.com/sample-v1"


@dataclass
class PlainPayload(DataClassMixin):
    """Dataclass WITHOUT DataClassConfig — used for negative tests."""

    value: str = "plain"


# ---------------------------------------------------------------------------
# Shared concrete processor (minimal ABC implementation)
# ---------------------------------------------------------------------------


@processor_config(binding=ProcessorBinding.SOUTHBOUND)
class ConcreteProcessor(CloudEventProcessor):
    """Minimal concrete subclass for testing."""

    async def process_cloudevent(self, cloudevent):
        return await self.callback_incoming(cloudevent)

    async def process_response_cloudevent(self, cloudevent):
        return None

    async def handle_cloudevent_expiration(self, cloudevent, timeout):
        return None

    async def trigger_outgoing_event(self, **kwargs):
        return None


def make_concrete_processor(**kwargs) -> ConcreteProcessor:
    cfg = ProcessingConfig()
    kwargs.pop("queue_size", None)
    return ConcreteProcessor(
        instance_id="test-id",
        runtime_config=cfg,
        config_identifier="test",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# OPC UA B.3 Example Data – Woodworking job order
# ---------------------------------------------------------------------------


def make_woodworking_job_order(
    job_order_id: str = "12345",
) -> ISA95JobOrderDataType:
    """Create a job order based on OPC UA Machinery Jobs B.3 example.

    A woodworking machine breaks a tree trunk into a shelf floor,
    four table legs, and a bag of chips.
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


# ---------------------------------------------------------------------------
# Service availability checks
# ---------------------------------------------------------------------------


def _is_service_available(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a TCP service is reachable."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


mqtt_available = pytest.mark.skipif(
    not _is_service_available(MQTT_CONFIG.hostname, MQTT_CONFIG.port),
    reason=f"MQTT broker not reachable at {MQTT_CONFIG.hostname}:{MQTT_CONFIG.port}",
)

redis_available = pytest.mark.skipif(
    not _is_service_available(REDIS_CONFIG.hostname, REDIS_CONFIG.port),
    reason=f"Redis server not reachable at {REDIS_CONFIG.hostname}:{REDIS_CONFIG.port}",
)

msgpack_server_available = pytest.mark.skipif(
    not _is_service_available(MSGPACK_CONFIG.hostname, MSGPACK_CONFIG.port),
    reason=f"MessagePack RPC server not reachable at {MSGPACK_CONFIG.hostname}:{MSGPACK_CONFIG.port}",
)

app_available = pytest.mark.skipif(
    not _is_service_available(MSGPACK_CONFIG.hostname, MSGPACK_CONFIG.port),
    reason=f"App not running (MessagePack RPC server not reachable at {MSGPACK_CONFIG.hostname}:{MSGPACK_CONFIG.port})",
)

integration = pytest.mark.integration


# ---------------------------------------------------------------------------
# SFC example recipe / work master
# ---------------------------------------------------------------------------


def build_example_sfc_recipe() -> SfcRecipe:
    return SfcRecipe(
        steps=[
            SfcStep(name="send_greeting", initial=True),
            SfcStep(name="done"),
        ],
        transitions=[
            SfcTransition(
                source="send_greeting",
                target="done",
                condition="true",
                priority=0,
            )
        ],
        actions=[
            SfcActionAssociation(
                name="send_bye",
                step="send_greeting",
                qualifier=SfcActionQualifier.NON_STORED,
                interaction=SfcInteraction.PUSH_COMMAND,
                type_id=Bye.Config.type_id,
                timeout_seconds=30,
                parameters={"name": "MicroDCS"},
            )
        ],
    )


def build_example_work_master() -> ConfigWorkMaster:
    recipe = build_example_sfc_recipe()
    return ConfigWorkMaster(
        id="wm-example-sfc-greetings",
        description=LocalizedText(
            text="Example SFC work master for the greetings demo processor", locale="en"
        ),
        data=recipe.to_dict(),
        dataschema=SFC_RECIPE_DATASCHEMA,
    )

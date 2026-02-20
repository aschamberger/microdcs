"""Tests for the MachineryJobsCloudEventProcessor.

Based on the OPC UA Machinery Jobs companion specification example B.3:
A woodworking job that breaks a tree down into a shelf floor, table legs, and chips.
https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/B.3
"""

import copy
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis.asyncio as redis

from app import ProcessingConfig
from app.common import CloudEvent
from app.models.machinery_jobs import (
    AbortCall,
    AbortResponse,
    CancelCall,
    CancelResponse,
    ClearCall,
    ClearResponse,
    EUInformation,
    ISA95JobOrderAndStateDataType,
    ISA95JobOrderDataType,
    ISA95MaterialDataType,
    ISA95PropertyDataType,
    ISA95StateDataType,
    LocalizedText,
    OutputInformationDataType,
    PauseCall,
    PauseResponse,
    RequestJobResponseByJobOrderIDCall,
    RequestJobResponseByJobOrderIDResponse,
    RequestJobResponseByJobOrderStateCall,
    RequestJobResponseByJobOrderStateResponse,
    ResumeCall,
    ResumeResponse,
    RevokeStartCall,
    RevokeStartResponse,
    StartCall,
    StartResponse,
    StopCall,
    StopResponse,
    StoreAndStartCall,
    StoreAndStartResponse,
    StoreCall,
    StoreResponse,
    UpdateCall,
    UpdateResponse,
)
from app.models.machinery_jobs_ext import JobOrderControlExt, MethodReturnStatus
from app.processors.machinery_jobs import MachineryJobsCloudEventProcessor
from app.redis import RedisKeySchema

# ===================================================================
# Constants
# ===================================================================

MQTT_TOPIC = "app/jobs/woodworking/request"
RESPONSE_TOPIC = "app/jobs/woodworking/response"

STORE_CE_TYPE = StoreCall.Config.cloudevent_type
STORE_AND_START_CE_TYPE = StoreAndStartCall.Config.cloudevent_type
START_CE_TYPE = StartCall.Config.cloudevent_type
STOP_CE_TYPE = StopCall.Config.cloudevent_type
PAUSE_CE_TYPE = PauseCall.Config.cloudevent_type
RESUME_CE_TYPE = ResumeCall.Config.cloudevent_type
ABORT_CE_TYPE = AbortCall.Config.cloudevent_type
CANCEL_CE_TYPE = CancelCall.Config.cloudevent_type
CLEAR_CE_TYPE = ClearCall.Config.cloudevent_type
REVOKE_START_CE_TYPE = RevokeStartCall.Config.cloudevent_type
UPDATE_CE_TYPE = UpdateCall.Config.cloudevent_type


# ===================================================================
# OPC UA B.3 Example Data – Woodworking job order
# ===================================================================


def _make_woodworking_job_order(job_order_id: str = "12345") -> ISA95JobOrderDataType:
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


# ===================================================================
# Fixtures
# ===================================================================


def _processing_config() -> ProcessingConfig:
    return ProcessingConfig(
        cloudevent_source="https://aschamberger.github.com/micro-dcs/test",
    )


@pytest.fixture
def redis_key_schema():
    return RedisKeySchema(prefix="test-machinery-jobs")


@pytest.fixture
def mock_redis_pool():
    pool = MagicMock(spec=redis.ConnectionPool)
    pool.connection_kwargs = {}
    return pool


@pytest.fixture
def processor(mock_redis_pool, redis_key_schema):
    proc = MachineryJobsCloudEventProcessor(
        instance_id="test-processor",
        runtime_config=_processing_config(),
        redis_connection_pool=mock_redis_pool,
        redis_key_schema=redis_key_schema,
    )
    return proc


def _mock_dao_save(proc: MachineryJobsCloudEventProcessor):
    proc._joborder_and_state_dao.save = AsyncMock()


def _mock_dao_retrieve(
    proc: MachineryJobsCloudEventProcessor,
    return_value: ISA95JobOrderAndStateDataType | None,
):
    # Return a deep copy each call, mirroring production deserialization which
    # creates a fresh object.  This avoids ``get_graph`` attribute conflicts
    # when HierarchicalGraphMachine.add_model is called on the same object.
    async def _retrieve(*_args, **_kwargs):
        if return_value is None:
            return None
        result = copy.deepcopy(return_value)
        # Production deserialization creates a clean object without state-machine
        # artefacts. Remove any that were left by add_model / remove_model.
        if result.job_order is not None:
            for attr in ("get_graph",):
                if hasattr(result.job_order, attr):
                    delattr(result.job_order, attr)
        return result

    proc._joborder_and_state_dao.retrieve = AsyncMock(side_effect=_retrieve)


def _mock_jobresponse_dao(proc: MachineryJobsCloudEventProcessor):
    proc._jobresponse_dao.initialize = AsyncMock()
    proc._jobresponse_dao.retrieve_by_job_order_id = AsyncMock(return_value=None)
    proc._jobresponse_dao.retrieve_by_state = AsyncMock(return_value=[])


# ===================================================================
# Data class tests
# ===================================================================


class TestISA95JobOrderDataType:
    def test_woodworking_job_order_fields(self):
        job = _make_woodworking_job_order()
        assert job.job_order_id == "12345"
        assert job.start_time == "2023-01-27T10:17:00Z"
        assert job.end_time == "2023-01-27T10:19:00Z"
        assert job.material_requirements is not None
        assert len(job.material_requirements) == 4

    def test_woodworking_job_serialization_round_trip(self):
        job = _make_woodworking_job_order()
        json_dict = job.to_dict()
        # _state is a transient attribute (starts with _) stripped by __post_serialize__
        assert "_state" not in json_dict
        restored = ISA95JobOrderDataType.from_dict(json_dict)
        assert restored.job_order_id == job.job_order_id
        assert restored._state == "InitialState"  # default from JobStateMixin
        assert restored.material_requirements is not None
        assert len(restored.material_requirements) == 4

    def test_material_requirements_content(self):
        job = _make_woodworking_job_order()
        assert job.material_requirements is not None
        tree = job.material_requirements[0]
        assert tree.material_use == "Material consumed"
        assert tree.quantity == "1"

        shelf = job.material_requirements[1]
        assert shelf.material_use == "Material produced"
        assert shelf.quantity == "1"

        legs = job.material_requirements[2]
        assert legs.quantity == "4"

        chips = job.material_requirements[3]
        assert chips.quantity == "1.75"

    def test_config_aliases(self):
        assert ISA95JobOrderDataType.Config.aliases["job_order_id"] == "JobOrderID"
        assert (
            ISA95JobOrderDataType.Config.aliases["material_requirements"]
            == "MaterialRequirements"
        )


class TestStoreCall:
    def test_config_cloudevent_type(self):
        assert StoreCall.Config.cloudevent_type == STORE_CE_TYPE

    def test_store_call_response_type(self):
        call = StoreCall(job_order=_make_woodworking_job_order())
        resp = call.response(return_status=MethodReturnStatus.NO_ERROR)
        assert isinstance(resp, StoreResponse)
        assert resp.return_status == MethodReturnStatus.NO_ERROR


class TestStoreAndStartCall:
    def test_config_cloudevent_type(self):
        assert StoreAndStartCall.Config.cloudevent_type == STORE_AND_START_CE_TYPE

    def test_store_and_start_call_response_type(self):
        call = StoreAndStartCall(job_order=_make_woodworking_job_order())
        resp = call.response(return_status=MethodReturnStatus.NO_ERROR)
        assert isinstance(resp, StoreAndStartResponse)


class TestMethodReturnStatus:
    def test_no_error(self):
        assert MethodReturnStatus.NO_ERROR == 1

    def test_unknown_job_order_id(self):
        assert MethodReturnStatus.UNKNOWN_JOB_ORDER_ID == 2

    def test_invalid_job_order_status(self):
        assert MethodReturnStatus.INVALID_JOB_ORDER_STATUS == 8

    def test_unable_to_accept(self):
        assert MethodReturnStatus.UNABLE_TO_ACCEPT_JOB_ORDER == 16

    def test_invalid_request(self):
        assert MethodReturnStatus.INVALID_REQUEST == (1 << 32)


# ===================================================================
# Processor — registration
# ===================================================================


class TestMachineryJobsProcessorRegistration:
    def test_all_incoming_callbacks_registered(self, processor):
        registered_types = set(processor._type_callbacks_in.keys())
        expected = {
            STORE_CE_TYPE,
            STORE_AND_START_CE_TYPE,
            START_CE_TYPE,
            STOP_CE_TYPE,
            PAUSE_CE_TYPE,
            RESUME_CE_TYPE,
            ABORT_CE_TYPE,
            CANCEL_CE_TYPE,
            CLEAR_CE_TYPE,
            REVOKE_START_CE_TYPE,
            UPDATE_CE_TYPE,
            RequestJobResponseByJobOrderIDCall.Config.cloudevent_type,
            RequestJobResponseByJobOrderStateCall.Config.cloudevent_type,
        }
        assert expected.issubset(registered_types)


# ===================================================================
# Processor — Store transitions
# ===================================================================


class TestProcessStore:
    @pytest.mark.asyncio
    async def test_store_success(self, processor):
        _mock_dao_save(processor)
        job_order = _make_woodworking_job_order()
        method = StoreCall(job_order=job_order)

        result = await processor.process_store(
            method, mqtt_topic=MQTT_TOPIC, correlationid="corr-1", request_id="req-1"
        )

        assert isinstance(result, StoreResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR
        processor._joborder_and_state_dao.save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_store_none_job_order(self, processor):
        method = StoreCall(job_order=None)

        result = await processor.process_store(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, StoreResponse)
        assert result.return_status == MethodReturnStatus.INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_store_job_not_acceptable(self, processor):
        _mock_dao_save(processor)
        job_order = _make_woodworking_job_order()
        method = StoreCall(job_order=job_order)

        with patch.object(
            processor, "is_job_acceptable", new_callable=AsyncMock, return_value=False
        ):
            result = await processor.process_store(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, StoreResponse)
        assert result.return_status == MethodReturnStatus.UNABLE_TO_ACCEPT_JOB_ORDER

    @pytest.mark.asyncio
    async def test_store_persist_failure(self, processor):
        processor._joborder_and_state_dao.save = AsyncMock(
            side_effect=Exception("Redis down")
        )
        job_order = _make_woodworking_job_order()
        method = StoreCall(job_order=job_order)

        result = await processor.process_store(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, StoreResponse)
        assert result.return_status == MethodReturnStatus.UNABLE_TO_ACCEPT_JOB_ORDER

    @pytest.mark.asyncio
    async def test_store_sets_not_allowed_to_start_state(self, processor):
        _mock_dao_save(processor)
        job_order = _make_woodworking_job_order()
        method = StoreCall(job_order=job_order)

        await processor.process_store(method, mqtt_topic=MQTT_TOPIC)

        saved_args = processor._joborder_and_state_dao.save.call_args
        saved_obj: ISA95JobOrderAndStateDataType = saved_args[0][0]
        assert saved_obj.state is not None
        # Store -> NotAllowedToStart (with Ready substate)
        state_texts = [s.state_text.text for s in saved_obj.state if s.state_text]
        assert "NotAllowedToStart" in state_texts
        assert "Ready" in state_texts


class TestProcessStoreAndStart:
    @pytest.mark.asyncio
    async def test_store_and_start_success(self, processor):
        _mock_dao_save(processor)
        job_order = _make_woodworking_job_order()
        method = StoreAndStartCall(job_order=job_order)

        result = await processor.process_store_and_start(
            method, mqtt_topic=MQTT_TOPIC, correlationid="corr-1", request_id="req-1"
        )

        assert isinstance(result, StoreAndStartResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_store_and_start_sets_allowed_to_start_state(self, processor):
        _mock_dao_save(processor)
        job_order = _make_woodworking_job_order()
        method = StoreAndStartCall(job_order=job_order)

        await processor.process_store_and_start(method, mqtt_topic=MQTT_TOPIC)

        saved_args = processor._joborder_and_state_dao.save.call_args
        saved_obj: ISA95JobOrderAndStateDataType = saved_args[0][0]
        assert saved_obj.state is not None
        state_texts = [s.state_text.text for s in saved_obj.state if s.state_text]
        assert "AllowedToStart" in state_texts
        assert "Ready" in state_texts

    @pytest.mark.asyncio
    async def test_store_and_start_none_job_order(self, processor):
        method = StoreAndStartCall(job_order=None)

        result = await processor.process_store_and_start(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, StoreAndStartResponse)
        assert result.return_status == MethodReturnStatus.INVALID_REQUEST


# ===================================================================
# Processor — Existing-job transitions
# ===================================================================


def _stored_job_order_and_state(
    state: str = "NotAllowedToStart_Ready",
    job_order_id: str = "12345",
) -> ISA95JobOrderAndStateDataType:
    """Create a persisted ISA95JobOrderAndStateDataType in a given state."""
    job_order = _make_woodworking_job_order(job_order_id)
    job_order._state = state
    state_data = []
    if "_" in state:
        main, sub = state.split("_")
        main_ids = JobOrderControlExt.Config.opcua_state_machine_state_ids[state].split(
            "_"
        )
        state_data = [
            ISA95StateDataType(
                state_text=LocalizedText(text=main, locale="en"),
                state_number=int(main_ids[0]),
            ),
            ISA95StateDataType(
                state_text=LocalizedText(text=sub, locale="en"),
                state_number=int(main_ids[1]),
            ),
        ]
    else:
        state_data = [
            ISA95StateDataType(
                state_text=LocalizedText(text=state, locale="en"),
                state_number=int(
                    JobOrderControlExt.Config.opcua_state_machine_state_ids[state]
                ),
            ),
        ]
    return ISA95JobOrderAndStateDataType(
        job_order=job_order,
        state=state_data,
    )


class TestProcessStart:
    @pytest.mark.asyncio
    async def test_start_from_not_allowed_to_start(self, processor):
        """Start moves job from NotAllowedToStart -> AllowedToStart."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("NotAllowedToStart_Ready")
        _mock_dao_retrieve(processor, stored)

        method = StartCall(
            job_order_id="12345",
            comment=[LocalizedText(text="Starting the woodworking job", locale="en")],
        )

        result = await processor.process_start(
            method, mqtt_topic=MQTT_TOPIC, correlationid="corr-1", request_id="req-1"
        )

        assert isinstance(result, StartResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_start_with_none_job_order_id(self, processor):
        method = StartCall(job_order_id=None)

        result = await processor.process_start(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, StartResponse)
        assert result.return_status == MethodReturnStatus.INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_start_unknown_job_order(self, processor):
        _mock_dao_retrieve(processor, None)
        method = StartCall(job_order_id="nonexistent")

        result = await processor.process_start(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, StartResponse)
        assert result.return_status == MethodReturnStatus.UNKNOWN_JOB_ORDER_ID

    @pytest.mark.asyncio
    async def test_start_invalid_state(self, processor):
        """Start from Running should fail with INVALID_JOB_ORDER_STATUS."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Running")
        _mock_dao_retrieve(processor, stored)

        method = StartCall(job_order_id="12345")

        result = await processor.process_start(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, StartResponse)
        assert result.return_status == MethodReturnStatus.INVALID_JOB_ORDER_STATUS


class TestProcessAbort:
    @pytest.mark.asyncio
    async def test_abort_from_running(self, processor):
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Running")
        _mock_dao_retrieve(processor, stored)

        method = AbortCall(
            job_order_id="12345",
            comment=[LocalizedText(text="Emergency abort", locale="en")],
        )

        result = await processor.process_abort(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, AbortResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_abort_from_not_allowed_to_start(self, processor):
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("NotAllowedToStart_Ready")
        _mock_dao_retrieve(processor, stored)

        method = AbortCall(job_order_id="12345")

        result = await processor.process_abort(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, AbortResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_abort_from_allowed_to_start(self, processor):
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("AllowedToStart_Ready")
        _mock_dao_retrieve(processor, stored)

        method = AbortCall(job_order_id="12345")

        result = await processor.process_abort(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, AbortResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_abort_from_interrupted(self, processor):
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Interrupted_Suspended")
        _mock_dao_retrieve(processor, stored)

        method = AbortCall(job_order_id="12345")

        result = await processor.process_abort(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, AbortResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR


class TestProcessPauseResume:
    @pytest.mark.asyncio
    async def test_pause_from_running(self, processor):
        """Running -> Interrupted (Pause)."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Running")
        _mock_dao_retrieve(processor, stored)

        method = PauseCall(job_order_id="12345")

        result = await processor.process_pause(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, PauseResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_resume_from_interrupted(self, processor):
        """Interrupted -> Running (Resume)."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Interrupted_Suspended")
        _mock_dao_retrieve(processor, stored)

        method = ResumeCall(job_order_id="12345")

        result = await processor.process_resume(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, ResumeResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_pause_from_not_running_fails(self, processor):
        """Pause from NotAllowedToStart should fail."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("NotAllowedToStart_Ready")
        _mock_dao_retrieve(processor, stored)

        method = PauseCall(job_order_id="12345")

        result = await processor.process_pause(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, PauseResponse)
        assert result.return_status == MethodReturnStatus.INVALID_JOB_ORDER_STATUS


class TestProcessStop:
    @pytest.mark.asyncio
    async def test_stop_from_running(self, processor):
        """Running -> Ended (Stop)."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Running")
        _mock_dao_retrieve(processor, stored)

        method = StopCall(job_order_id="12345")

        result = await processor.process_stop(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, StopResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_stop_from_interrupted(self, processor):
        """Interrupted -> Ended (Stop)."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Interrupted_Suspended")
        _mock_dao_retrieve(processor, stored)

        method = StopCall(job_order_id="12345")

        result = await processor.process_stop(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, StopResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR


class TestProcessCancel:
    @pytest.mark.asyncio
    async def test_cancel_from_not_allowed_to_start(self, processor):
        """NotAllowedToStart -> EndState (Cancel)."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("NotAllowedToStart_Ready")
        _mock_dao_retrieve(processor, stored)

        method = CancelCall(job_order_id="12345")

        result = await processor.process_cancel(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, CancelResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_cancel_from_allowed_to_start(self, processor):
        """AllowedToStart -> EndState (Cancel)."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("AllowedToStart_Ready")
        _mock_dao_retrieve(processor, stored)

        method = CancelCall(job_order_id="12345")

        result = await processor.process_cancel(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, CancelResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_cancel_from_running_fails(self, processor):
        """Cancel from Running should fail."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Running")
        _mock_dao_retrieve(processor, stored)

        method = CancelCall(job_order_id="12345")

        result = await processor.process_cancel(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, CancelResponse)
        assert result.return_status == MethodReturnStatus.INVALID_JOB_ORDER_STATUS


class TestProcessClear:
    @pytest.mark.asyncio
    async def test_clear_from_aborted(self, processor):
        """Aborted -> EndState (Clear)."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Aborted")
        _mock_dao_retrieve(processor, stored)

        method = ClearCall(job_order_id="12345")

        result = await processor.process_clear(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, ClearResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_clear_from_ended(self, processor):
        """Ended -> EndState (Clear)."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Ended_Completed")
        _mock_dao_retrieve(processor, stored)

        method = ClearCall(job_order_id="12345")

        result = await processor.process_clear(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, ClearResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_clear_from_running_fails(self, processor):
        """Clear from Running should fail."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Running")
        _mock_dao_retrieve(processor, stored)

        method = ClearCall(job_order_id="12345")

        result = await processor.process_clear(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, ClearResponse)
        assert result.return_status == MethodReturnStatus.INVALID_JOB_ORDER_STATUS


class TestProcessRevokeStart:
    @pytest.mark.asyncio
    async def test_revoke_start_from_allowed_to_start(self, processor):
        """AllowedToStart -> NotAllowedToStart (RevokeStart)."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("AllowedToStart_Ready")
        _mock_dao_retrieve(processor, stored)

        method = RevokeStartCall(job_order_id="12345")

        result = await processor.process_revoke_start(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, RevokeStartResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR


# ===================================================================
# Processor — Update transition
# ===================================================================


class TestProcessUpdate:
    @pytest.mark.asyncio
    async def test_update_in_not_allowed_to_start(self, processor):
        """Update a job order that is in NotAllowedToStart state."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("NotAllowedToStart_Ready")
        _mock_dao_retrieve(processor, stored)

        updated_job = _make_woodworking_job_order()
        updated_job.priority = 10
        method = UpdateCall(
            job_order=updated_job,
            comment=[LocalizedText(text="Increase priority", locale="en")],
        )

        result = await processor.process_update(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, UpdateResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_update_in_allowed_to_start(self, processor):
        """Update a job order that is in AllowedToStart state."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("AllowedToStart_Ready")
        _mock_dao_retrieve(processor, stored)

        updated_job = _make_woodworking_job_order()
        updated_job.priority = 5
        method = UpdateCall(job_order=updated_job)

        result = await processor.process_update(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, UpdateResponse)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_update_none_job_order(self, processor):
        method = UpdateCall(job_order=None)

        result = await processor.process_update(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, UpdateResponse)
        assert result.return_status == MethodReturnStatus.INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_update_unknown_job_order(self, processor):
        _mock_dao_retrieve(processor, None)

        updated_job = _make_woodworking_job_order("99999")
        method = UpdateCall(job_order=updated_job)

        result = await processor.process_update(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, UpdateResponse)
        assert result.return_status == MethodReturnStatus.UNKNOWN_JOB_ORDER_ID

    @pytest.mark.asyncio
    async def test_update_from_running_fails(self, processor):
        """Update should fail when the job is Running."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("Running")
        _mock_dao_retrieve(processor, stored)

        updated_job = _make_woodworking_job_order()
        method = UpdateCall(job_order=updated_job)

        result = await processor.process_update(method, mqtt_topic=MQTT_TOPIC)

        assert isinstance(result, UpdateResponse)
        assert result.return_status == MethodReturnStatus.INVALID_JOB_ORDER_STATUS

    @pytest.mark.asyncio
    async def test_update_preserves_state(self, processor):
        """After Update, the persisted object should retain the original state."""
        _mock_dao_save(processor)
        stored = _stored_job_order_and_state("NotAllowedToStart_Ready")
        _mock_dao_retrieve(processor, stored)

        updated_job = _make_woodworking_job_order()
        updated_job.priority = 99
        method = UpdateCall(job_order=updated_job)

        await processor.process_update(method, mqtt_topic=MQTT_TOPIC)

        saved_args = processor._joborder_and_state_dao.save.call_args
        saved_obj: ISA95JobOrderAndStateDataType = saved_args[0][0]
        assert saved_obj.job_order is not None
        assert saved_obj.job_order.priority == 99
        assert saved_obj.state is not None
        state_texts = [s.state_text.text for s in saved_obj.state if s.state_text]
        assert "NotAllowedToStart" in state_texts


# ===================================================================
# Processor — Query handlers
# ===================================================================


class TestRequestJobResponseByJobOrderID:
    @pytest.mark.asyncio
    async def test_query_none_id(self, processor):
        _mock_jobresponse_dao(processor)
        method = RequestJobResponseByJobOrderIDCall(job_order_id=None)

        result = await processor.process_request_job_response_by_id(
            method, mqtt_topic=MQTT_TOPIC
        )

        assert isinstance(result, RequestJobResponseByJobOrderIDResponse)
        assert result.return_status == MethodReturnStatus.INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_query_unknown_id(self, processor):
        _mock_jobresponse_dao(processor)
        method = RequestJobResponseByJobOrderIDCall(job_order_id="nonexistent")

        result = await processor.process_request_job_response_by_id(
            method, mqtt_topic=MQTT_TOPIC
        )

        assert isinstance(result, RequestJobResponseByJobOrderIDResponse)
        assert result.return_status == MethodReturnStatus.UNKNOWN_JOB_ORDER_ID


class TestRequestJobResponseByJobOrderState:
    @pytest.mark.asyncio
    async def test_query_none_state(self, processor):
        _mock_jobresponse_dao(processor)
        method = RequestJobResponseByJobOrderStateCall(job_order_state=None)

        result = await processor.process_request_job_response_by_state(
            method, mqtt_topic=MQTT_TOPIC
        )

        assert isinstance(result, RequestJobResponseByJobOrderStateResponse)
        assert result.return_status == MethodReturnStatus.INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_query_empty_state(self, processor):
        _mock_jobresponse_dao(processor)
        method = RequestJobResponseByJobOrderStateCall(job_order_state=[])

        result = await processor.process_request_job_response_by_state(
            method, mqtt_topic=MQTT_TOPIC
        )

        assert isinstance(result, RequestJobResponseByJobOrderStateResponse)
        assert result.return_status == MethodReturnStatus.INVALID_REQUEST


# ===================================================================
# Processor — CloudEvent lifecycle
# ===================================================================


class TestProcessEvent:
    @pytest.mark.asyncio
    async def test_process_event_unknown_type_returns_none(self, processor):
        ce = CloudEvent(
            type="com.unknown.type",
            data=b"irrelevant",
            datacontenttype="application/json; charset=utf-8",
        )

        result = await processor.process_event(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_process_event_known_type(self, processor):
        _mock_dao_save(processor)
        job_order = _make_woodworking_job_order()
        store_call = StoreCall(job_order=job_order)
        ce = CloudEvent(
            type=STORE_CE_TYPE,
            data=store_call.to_jsonb(),
            datacontenttype="application/json; charset=utf-8",
            transportmetadata={"mqtt_topic": MQTT_TOPIC},
        )

        result = await processor.process_event(ce)
        assert isinstance(result, list)
        assert len(result) >= 1

    @pytest.mark.asyncio
    async def test_process_response_event_returns_none(self, processor):
        ce = CloudEvent()
        result = await processor.process_response_event(ce)
        assert result is None

    @pytest.mark.asyncio
    async def test_handle_expiration_returns_none(self, processor):
        ce = CloudEvent()
        result = await processor.handle_expiration(ce, 10)
        assert result is None


# ===================================================================
# Processor — send_event helper
# ===================================================================


class TestSendEvent:
    def test_send_event_with_effect(self, processor):
        """send_event should publish when the transition has an effect."""
        mock_handler = MagicMock()
        processor.register_publish_handler(mock_handler)

        job_order = _make_woodworking_job_order()
        job_order_and_state = ISA95JobOrderAndStateDataType(
            job_order=job_order,
            state=[
                ISA95StateDataType(
                    state_text=LocalizedText(text="NotAllowedToStart", locale="en"),
                    state_number=1,
                ),
            ],
        )

        processor.send_event("Store", job_order_and_state, "corr-1", "cause-1")
        mock_handler.assert_called_once()

    def test_send_event_without_effect(self, processor):
        """send_event should NOT publish for a transition without an effect."""
        mock_handler = MagicMock()
        processor.register_publish_handler(mock_handler)

        job_order = _make_woodworking_job_order()
        job_order_and_state = ISA95JobOrderAndStateDataType(
            job_order=job_order,
            state=[],
        )

        processor.send_event("SomeNonExistentTransition", job_order_and_state)
        mock_handler.assert_not_called()


# ===================================================================
# Processor — build_job_state_object
# ===================================================================


class TestBuildJobStateObject:
    def test_simple_state(self, processor):
        result = processor.build_job_state_object("Running")
        assert len(result) == 1
        assert result[0].state_text.text == "Running"
        assert result[0].state_number == 3

    def test_composite_state(self, processor):
        result = processor.build_job_state_object("NotAllowedToStart_Ready")
        assert len(result) == 2
        assert result[0].state_text.text == "NotAllowedToStart"
        assert result[0].state_number == 1
        assert result[1].state_text.text == "Ready"
        assert result[1].state_number == 2

    def test_aborted_state(self, processor):
        result = processor.build_job_state_object("Aborted")
        assert len(result) == 1
        assert result[0].state_text.text == "Aborted"
        assert result[0].state_number == 6


# ===================================================================
# Full lifecycle: Store → Start → Pause → Resume → Stop → Clear
# (OPC UA B.3 example flow)
# ===================================================================


class TestFullB3Lifecycle:
    """Test the complete lifecycle of the woodworking B.3 example job order."""

    @pytest.mark.asyncio
    async def test_store_start_run_pause_resume_stop_clear(self, processor):
        """Walk through: Store → Start → Run → Pause → Resume → Stop → Clear."""
        _mock_dao_save(processor)

        # 1. Store
        job_order = _make_woodworking_job_order()
        store = StoreCall(job_order=job_order)
        result = await processor.process_store(store, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

        # Capture the saved object to feed into subsequent steps
        saved = processor._joborder_and_state_dao.save.call_args[0][0]
        _mock_dao_retrieve(processor, saved)

        # 2. Start (NotAllowedToStart → AllowedToStart)
        start = StartCall(job_order_id="12345")
        result = await processor.process_start(start, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

        saved = processor._joborder_and_state_dao.save.call_args[0][0]
        _mock_dao_retrieve(processor, saved)

        # 3. Run (AllowedToStart → Running)
        # The Run transition is triggered externally by the machine; here we simulate
        # it via the state machine directly because there is no Run RPC method.
        # Use a fresh copy (like production deserialization would), and reconstruct
        # the state from the persisted state list.
        run_copy = copy.deepcopy(saved)
        # Clean up diagram artefact left by add_model (production from_json wouldn't have it)
        if hasattr(run_copy.job_order, "get_graph"):
            delattr(run_copy.job_order, "get_graph")
        current = processor.reconstruct_state_name(run_copy.state)
        processor._state_machine.add_model(run_copy.job_order, initial=current)
        run_copy.job_order.trigger("Run")
        run_copy.state = processor.build_job_state_object(run_copy.job_order._state)
        processor._state_machine.remove_model(run_copy.job_order)
        assert run_copy.job_order._state == "Running"
        saved = run_copy
        _mock_dao_retrieve(processor, saved)

        # 4. Pause (Running → Interrupted)
        pause = PauseCall(job_order_id="12345")
        result = await processor.process_pause(pause, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

        saved = processor._joborder_and_state_dao.save.call_args[0][0]
        _mock_dao_retrieve(processor, saved)

        # 5. Resume (Interrupted → Running)
        resume = ResumeCall(job_order_id="12345")
        result = await processor.process_resume(resume, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

        saved = processor._joborder_and_state_dao.save.call_args[0][0]
        _mock_dao_retrieve(processor, saved)

        # 6. Stop (Running → Ended)
        stop = StopCall(job_order_id="12345")
        result = await processor.process_stop(stop, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

        saved = processor._joborder_and_state_dao.save.call_args[0][0]
        _mock_dao_retrieve(processor, saved)

        # 7. Clear (Ended → EndState)
        clear = ClearCall(job_order_id="12345")
        result = await processor.process_clear(clear, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_store_and_start_abort_clear(self, processor):
        """Walk through: StoreAndStart → Abort → Clear (emergency path)."""
        _mock_dao_save(processor)

        # 1. StoreAndStart
        job_order = _make_woodworking_job_order()
        store_and_start = StoreAndStartCall(job_order=job_order)
        result = await processor.process_store_and_start(
            store_and_start, mqtt_topic=MQTT_TOPIC
        )
        assert result.return_status == MethodReturnStatus.NO_ERROR

        saved = processor._joborder_and_state_dao.save.call_args[0][0]
        _mock_dao_retrieve(processor, saved)

        # 2. Abort (AllowedToStart → Aborted)
        abort = AbortCall(job_order_id="12345")
        result = await processor.process_abort(abort, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

        saved = processor._joborder_and_state_dao.save.call_args[0][0]
        _mock_dao_retrieve(processor, saved)

        # 3. Clear (Aborted → EndState)
        clear = ClearCall(job_order_id="12345")
        result = await processor.process_clear(clear, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_store_cancel(self, processor):
        """Walk through: Store → Cancel (job cancelled before running)."""
        _mock_dao_save(processor)

        # 1. Store
        job_order = _make_woodworking_job_order()
        store = StoreCall(job_order=job_order)
        result = await processor.process_store(store, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

        saved = processor._joborder_and_state_dao.save.call_args[0][0]
        _mock_dao_retrieve(processor, saved)

        # 2. Cancel (NotAllowedToStart → EndState)
        cancel = CancelCall(job_order_id="12345")
        result = await processor.process_cancel(cancel, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

    @pytest.mark.asyncio
    async def test_store_start_revoke_start_cancel(self, processor):
        """Walk through: Store → Start → RevokeStart → Cancel."""
        _mock_dao_save(processor)

        # 1. Store
        job_order = _make_woodworking_job_order()
        store = StoreCall(job_order=job_order)
        result = await processor.process_store(store, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

        saved = processor._joborder_and_state_dao.save.call_args[0][0]
        _mock_dao_retrieve(processor, saved)

        # 2. Start
        start = StartCall(job_order_id="12345")
        result = await processor.process_start(start, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

        saved = processor._joborder_and_state_dao.save.call_args[0][0]
        _mock_dao_retrieve(processor, saved)

        # 3. RevokeStart (AllowedToStart → NotAllowedToStart)
        revoke = RevokeStartCall(job_order_id="12345")
        result = await processor.process_revoke_start(revoke, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

        saved = processor._joborder_and_state_dao.save.call_args[0][0]
        _mock_dao_retrieve(processor, saved)

        # 4. Cancel (NotAllowedToStart → EndState)
        cancel = CancelCall(job_order_id="12345")
        result = await processor.process_cancel(cancel, mqtt_topic=MQTT_TOPIC)
        assert result.return_status == MethodReturnStatus.NO_ERROR

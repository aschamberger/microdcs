import logging
from dataclasses import dataclass
from typing import Any

import redis.asyncio as redis
from transitions.extensions import HierarchicalGraphMachine

from microdcs import ProcessingConfig
from microdcs.common import (
    CloudEvent,
    CloudeventAttributeTuple,
    CloudEventProcessor,
    MessageIntent,
    ProcessorBinding,
    incoming,
    processor_config,
    scope_from_subject,
)
from microdcs.models.machinery_jobs import (
    AbortCall,
    AbortResponse,
    CancelCall,
    CancelResponse,
    ClearCall,
    ClearResponse,
    ISA95JobOrderAndStateDataType,
    ISA95JobOrderDataType,
    ISA95JobOrderStatusEventType,
    ISA95StateDataType,
    LocalizedText,
    Meta,
    Opc400013MachineryJobMgmtForMqtt,
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
from microdcs.models.machinery_jobs_ext import (
    JobOrderControlExt,
    MethodReturnStatus,
)
from microdcs.redis import (
    EquipmentListDAO,
    JobOrderAndStateDAO,
    JobResponseDAO,
    MaterialClassListDAO,
    MaterialDefinitionListDAO,
    PersonnelListDAO,
    PhysicalAssetListDAO,
    RedisKeySchema,
    WorkMasterDAO,
)


@dataclass(kw_only=True)
class JobAcceptanceConfig:
    check_equipment_id: bool = True
    check_material_class_id: bool = True
    check_material_definition_id: bool = True
    check_personnel_id: bool = True
    check_physical_asset_id: bool = True
    check_work_master: bool = True
    check_max_downloadable_job_orders: bool = True
    max_downloadable_job_orders: int = 0


logger = logging.getLogger("processor.machinery_jobs")

# Type alias for any Call that carries a job_order_id field (existing-job transitions)
type JobOrderIdCall = (
    AbortCall
    | CancelCall
    | ClearCall
    | PauseCall
    | ResumeCall
    | RevokeStartCall
    | StartCall
    | StopCall
)

# Type alias for any Call that carries a job_order field (new-job / store transitions)
type JobOrderCall = StoreCall | StoreAndStartCall


@processor_config(binding=ProcessorBinding.NORTHBOUND)
class MachineryJobsCloudEventProcessor(CloudEventProcessor):
    def __init__(
        self,
        instance_id: str,
        runtime_config: ProcessingConfig,
        config_identifier: str,
        redis_connection_pool: redis.ConnectionPool,
        redis_key_schema: RedisKeySchema,
        job_acceptance_config: JobAcceptanceConfig | None = None,
    ):
        super().__init__(instance_id, runtime_config, config_identifier)
        topic_prefix = runtime_config.get_topic_prefix_for_identifier(config_identifier)
        if topic_prefix is None:
            raise ValueError(f"No topic prefix configured for '{config_identifier}'")
        self._topic_prefix = topic_prefix
        self._event_attributes.extend([
            CloudeventAttributeTuple("subject", "subject"),
            CloudeventAttributeTuple("correlationid", "correlationid"),
            CloudeventAttributeTuple("cloudevent_id", "id"),
        ])
        self._redis_client: redis.Redis = redis.Redis(
            connection_pool=redis_connection_pool
        )
        self._redis_key_schema: RedisKeySchema = redis_key_schema
        self._job_acceptance_config = job_acceptance_config or JobAcceptanceConfig()
        self._joborder_and_state_dao: JobOrderAndStateDAO = JobOrderAndStateDAO(
            self._redis_client,
            redis_key_schema,
        )
        self._jobresponse_dao: JobResponseDAO = JobResponseDAO(
            self._redis_client,
            redis_key_schema,
        )
        self._equipment_list_dao = EquipmentListDAO(
            self._redis_client, redis_key_schema
        )
        self._material_class_list_dao = MaterialClassListDAO(
            self._redis_client, redis_key_schema
        )
        self._material_definition_list_dao = MaterialDefinitionListDAO(
            self._redis_client, redis_key_schema
        )
        self._personnel_list_dao = PersonnelListDAO(
            self._redis_client, redis_key_schema
        )
        self._physical_asset_list_dao = PhysicalAssetListDAO(
            self._redis_client, redis_key_schema
        )
        self._work_master_dao = WorkMasterDAO(self._redis_client, redis_key_schema)
        self._state_machine = HierarchicalGraphMachine(
            model=None,
            states=JobOrderControlExt.Config.opcua_state_machine_states,
            transitions=JobOrderControlExt.Config.opcua_state_machine_transitions,
            initial="InitialState",
            auto_transitions=False,
            queued=True,
            model_attribute="_state",
            model_override=True,
        )

    async def initialize(self) -> None:
        await self._jobresponse_dao.initialize()

    async def post_start(self) -> None:
        self.publish_metadata_event()

    # ── helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def reconstruct_state_name(
        state: list[ISA95StateDataType] | None,
    ) -> str | None:
        """Reconstruct the compound state-machine state name from the stored state list.

        Returns a string like ``'NotAllowedToStart_Ready'`` or ``'Running'``
        that can be fed to ``add_model(initial=...)``.
        Returns *None* when the state list is empty or missing text.
        """
        if not state:
            return None
        parts = [s.state_text.text for s in state if s.state_text and s.state_text.text]
        return "_".join(parts) if parts else None

    async def is_job_acceptable(
        self, job_order: ISA95JobOrderDataType, scope: str
    ) -> bool:
        cfg = self._job_acceptance_config

        # Check max downloadable job orders
        if (
            cfg.check_max_downloadable_job_orders
            and cfg.max_downloadable_job_orders > 0
        ):
            current_jobs = await self._joborder_and_state_dao.list(scope)
            if len(current_jobs) >= cfg.max_downloadable_job_orders:
                logger.warning(
                    "Max downloadable job orders reached (%d) for scope %s",
                    cfg.max_downloadable_job_orders,
                    scope,
                )
                return False

        # Check equipment IDs
        if cfg.check_equipment_id and job_order.equipment_requirements:
            for eq in job_order.equipment_requirements:
                if eq.id and not await self._equipment_list_dao.is_member(eq.id, scope):
                    logger.warning(
                        "Equipment ID %s not allowed for scope %s", eq.id, scope
                    )
                    return False

        # Check material class IDs
        if cfg.check_material_class_id and job_order.material_requirements:
            for mat in job_order.material_requirements:
                if (
                    mat.material_class_id
                    and not await self._material_class_list_dao.is_member(
                        mat.material_class_id, scope
                    )
                ):
                    logger.warning(
                        "Material class ID %s not allowed for scope %s",
                        mat.material_class_id,
                        scope,
                    )
                    return False

        # Check material definition IDs
        if cfg.check_material_definition_id and job_order.material_requirements:
            for mat in job_order.material_requirements:
                if (
                    mat.material_definition_id
                    and not await self._material_definition_list_dao.is_member(
                        mat.material_definition_id, scope
                    )
                ):
                    logger.warning(
                        "Material definition ID %s not allowed for scope %s",
                        mat.material_definition_id,
                        scope,
                    )
                    return False

        # Check personnel IDs
        if cfg.check_personnel_id and job_order.personnel_requirements:
            for pers in job_order.personnel_requirements:
                if pers.id and not await self._personnel_list_dao.is_member(
                    pers.id, scope
                ):
                    logger.warning(
                        "Personnel ID %s not allowed for scope %s", pers.id, scope
                    )
                    return False

        # Check physical asset IDs
        if cfg.check_physical_asset_id and job_order.physical_asset_requirements:
            for pa in job_order.physical_asset_requirements:
                if pa.id and not await self._physical_asset_list_dao.is_member(
                    pa.id, scope
                ):
                    logger.warning(
                        "Physical asset ID %s not allowed for scope %s", pa.id, scope
                    )
                    return False

        # Check work master IDs
        if cfg.check_work_master and job_order.work_master_id:
            for wm in job_order.work_master_id:
                if wm.id and not await self._work_master_dao.is_member(wm.id, scope):
                    logger.warning(
                        "Work master ID %s not allowed for scope %s", wm.id, scope
                    )
                    return False

        return True

    def build_job_state_object(self, state: str) -> list[ISA95StateDataType]:
        if "_" in state:
            main_state, sub_state = state.split("_")
            main_state_number, sub_state_number = (
                JobOrderControlExt.Config.opcua_state_machine_state_ids[state].split(
                    "_"
                )
            )
            return [
                ISA95StateDataType(
                    state_text=LocalizedText(text=main_state, locale="en"),
                    state_number=int(main_state_number),
                ),
                ISA95StateDataType(
                    state_text=LocalizedText(text=sub_state, locale="en"),
                    state_number=int(sub_state_number),
                ),
            ]
        else:
            return [
                ISA95StateDataType(
                    state_text=LocalizedText(text=state, locale="en"),
                    state_number=int(
                        JobOrderControlExt.Config.opcua_state_machine_state_ids[state]
                    ),
                )
            ]

    def send_event(
        self,
        transition: str,
        job_order_and_state: ISA95JobOrderAndStateDataType,
        scope: str,
        correlationid: str | None = None,
        causationid: str | None = None,
    ) -> None:
        """Send an ISA95JobOrderStatusEventType CloudEvent if the transition has an effect."""
        if transition not in JobOrderControlExt.Config.opcua_state_machine_effects:
            return

        event = ISA95JobOrderStatusEventType(
            job_order=job_order_and_state.job_order,
            job_state=job_order_and_state.state,
        )
        cloudevent = self.create_event()
        cloudevent.subject = scope
        cloudevent.correlationid = correlationid
        cloudevent.causationid = causationid
        try:
            cloudevent.serialize_payload(event)
        except ValueError as e:
            logger.exception(
                "Error serializing payload for message type %s: %s",
                cloudevent.type,
                e,
            )
            return
        self.publish_event(cloudevent, intent=MessageIntent.EVENT)

    # ── shared transition logic ──────────────────────────────────────────

    async def _handle_store_transition(
        self,
        method: JobOrderCall,
        scope: str,
        correlationid: str | None,
        causationid: str | None,
    ) -> Any:
        """Handle Store / StoreAndStart: new job order from InitialState."""
        transition: str = type(method).__name__[:-4]  # remove "Call" suffix
        logger.info(
            "Received %s for job: %s with comment: %s",
            transition,
            method.job_order.job_order_id if method.job_order else None,
            method.comment,
        )
        logger.debug("Received %s: %s", transition, method)

        if method.job_order is None:
            return method.response(return_status=MethodReturnStatus.INVALID_REQUEST)

        if not await self.is_job_acceptable(method.job_order, scope):
            return method.response(
                return_status=MethodReturnStatus.UNABLE_TO_ACCEPT_JOB_ORDER
            )

        self._state_machine.add_model(method.job_order)
        try:
            if not method.job_order.may_trigger(transition):
                logger.warning(
                    "Job order %s is not in a state that allows to trigger %s",
                    method.job_order.job_order_id,
                    transition,
                )
                return method.response(
                    return_status=MethodReturnStatus.UNABLE_TO_ACCEPT_JOB_ORDER
                )

            method.job_order.trigger(transition)
            job_order_and_state = ISA95JobOrderAndStateDataType(
                job_order=method.job_order,
                state=self.build_job_state_object(method.job_order._state),
            )
        finally:
            self._state_machine.remove_model(method.job_order)

        try:
            await self._joborder_and_state_dao.save(
                job_order_and_state, scope, transition
            )
        except Exception:
            logger.exception(
                "Failed to persist job order and state for job order %s in scope %s",
                method.job_order.job_order_id,
                scope,
            )
            return method.response(
                return_status=MethodReturnStatus.UNABLE_TO_ACCEPT_JOB_ORDER
            )

        self.send_event(
            transition, job_order_and_state, scope, correlationid, causationid
        )
        return method.response(return_status=MethodReturnStatus.NO_ERROR)

    async def _handle_existing_job_transition(
        self,
        method: JobOrderIdCall,
        scope: str,
        correlationid: str | None,
        causationid: str | None,
    ) -> Any:
        """Handle transitions on an existing job order looked up by job_order_id."""
        transition: str = type(method).__name__[:-4]  # remove "Call" suffix
        logger.info(
            "Received %s for job: %s with comment: %s",
            transition,
            method.job_order_id,
            method.comment,
        )
        logger.debug("Received %s: %s", transition, method)

        if method.job_order_id is None:
            return method.response(return_status=MethodReturnStatus.INVALID_REQUEST)

        job_order_and_state = await self._joborder_and_state_dao.retrieve(
            method.job_order_id
        )
        if job_order_and_state is None or job_order_and_state.job_order is None:
            logger.warning(
                "Job order with id %s not found for %s request",
                method.job_order_id,
                transition,
            )
            return method.response(
                return_status=MethodReturnStatus.UNKNOWN_JOB_ORDER_ID
            )

        # Reconstruct state-machine state from persisted state list
        current_state = self.reconstruct_state_name(job_order_and_state.state)
        self._state_machine.add_model(
            job_order_and_state.job_order,
            initial=current_state or "InitialState",
        )
        try:
            if not job_order_and_state.job_order.may_trigger(transition):
                logger.warning(
                    "Job order %s is not in a state that allows to trigger %s",
                    job_order_and_state.job_order.job_order_id,
                    transition,
                )
                return method.response(
                    return_status=MethodReturnStatus.INVALID_JOB_ORDER_STATUS
                )

            job_order_and_state.job_order.trigger(transition)
            job_order_and_state.state = self.build_job_state_object(
                job_order_and_state.job_order._state
            )
        finally:
            self._state_machine.remove_model(job_order_and_state.job_order)

        try:
            await self._joborder_and_state_dao.save(
                job_order_and_state, scope, transition
            )
        except Exception:
            logger.exception(
                "Failed to persist job order and state for job order %s in scope %s",
                job_order_and_state.job_order.job_order_id,
                scope,
            )
            return method.response(
                return_status=MethodReturnStatus.INVALID_JOB_ORDER_STATUS
            )

        self.send_event(
            transition, job_order_and_state, scope, correlationid, causationid
        )
        return method.response(return_status=MethodReturnStatus.NO_ERROR)

    async def _handle_update_transition(
        self,
        method: UpdateCall,
        scope: str,
        correlationid: str | None,
        causationid: str | None,
    ) -> UpdateResponse | None:
        """Handle Update: replace job order data on an existing persisted job."""
        transition = "Update"
        logger.info(
            "Received %s for job: %s with comment: %s",
            transition,
            method.job_order.job_order_id if method.job_order else None,
            method.comment,
        )
        logger.debug("Received %s: %s", transition, method)

        if method.job_order is None or method.job_order.job_order_id is None:
            return method.response(return_status=MethodReturnStatus.INVALID_REQUEST)

        if not await self.is_job_acceptable(method.job_order, scope):
            return method.response(
                return_status=MethodReturnStatus.UNABLE_TO_ACCEPT_JOB_ORDER
            )

        job_order_and_state = await self._joborder_and_state_dao.retrieve(
            method.job_order.job_order_id
        )
        if job_order_and_state is None or job_order_and_state.job_order is None:
            logger.warning(
                "Job order with id %s not found for %s request",
                method.job_order.job_order_id,
                transition,
            )
            return method.response(
                return_status=MethodReturnStatus.UNKNOWN_JOB_ORDER_ID
            )

        # Use the existing job order for state machine validation
        # Reconstruct state-machine state from persisted state list
        persisted_state = self.reconstruct_state_name(job_order_and_state.state)
        self._state_machine.add_model(
            job_order_and_state.job_order,
            initial=persisted_state or "InitialState",
        )
        try:
            if not job_order_and_state.job_order.may_trigger(transition):
                logger.warning(
                    "Job order %s is not in a state that allows to trigger %s",
                    job_order_and_state.job_order.job_order_id,
                    transition,
                )
                return method.response(
                    return_status=MethodReturnStatus.INVALID_JOB_ORDER_STATUS
                )

            job_order_and_state.job_order.trigger(transition)
            # Carry the current state over to the new job order data
            current_state = job_order_and_state.job_order._state
        finally:
            self._state_machine.remove_model(job_order_and_state.job_order)

        # Replace with updated job order, preserving state
        method.job_order._state = current_state
        job_order_and_state.job_order = method.job_order
        job_order_and_state.state = self.build_job_state_object(current_state)

        try:
            await self._joborder_and_state_dao.save(
                job_order_and_state, scope, transition
            )
        except Exception:
            logger.exception(
                "Failed to persist job order and state for job order %s in scope %s",
                method.job_order.job_order_id,
                scope,
            )
            return method.response(
                return_status=MethodReturnStatus.INVALID_JOB_ORDER_STATUS
            )

        self.send_event(
            transition, job_order_and_state, scope, correlationid, causationid
        )
        return method.response(return_status=MethodReturnStatus.NO_ERROR)

    # ── Store / StoreAndStart handlers ───────────────────────────────────

    @scope_from_subject
    @incoming(StoreCall)
    async def process_store(
        self,
        method: StoreCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> StoreResponse | None:
        return await self._handle_store_transition(
            method, scope, correlationid, cloudevent_id
        )

    @scope_from_subject
    @incoming(StoreAndStartCall)
    async def process_store_and_start(
        self,
        method: StoreAndStartCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> StoreAndStartResponse | None:
        return await self._handle_store_transition(
            method, scope, correlationid, cloudevent_id
        )

    # ── Existing-job transition handlers ─────────────────────────────────

    @scope_from_subject
    @incoming(AbortCall)
    async def process_abort(
        self,
        method: AbortCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> AbortResponse | None:
        return await self._handle_existing_job_transition(
            method, scope, correlationid, cloudevent_id
        )

    @scope_from_subject
    @incoming(CancelCall)
    async def process_cancel(
        self,
        method: CancelCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> CancelResponse | None:
        return await self._handle_existing_job_transition(
            method, scope, correlationid, cloudevent_id
        )

    @scope_from_subject
    @incoming(ClearCall)
    async def process_clear(
        self,
        method: ClearCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> ClearResponse | None:
        return await self._handle_existing_job_transition(
            method, scope, correlationid, cloudevent_id
        )

    @scope_from_subject
    @incoming(PauseCall)
    async def process_pause(
        self,
        method: PauseCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> PauseResponse | None:
        return await self._handle_existing_job_transition(
            method, scope, correlationid, cloudevent_id
        )

    @scope_from_subject
    @incoming(ResumeCall)
    async def process_resume(
        self,
        method: ResumeCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> ResumeResponse | None:
        return await self._handle_existing_job_transition(
            method, scope, correlationid, cloudevent_id
        )

    @scope_from_subject
    @incoming(RevokeStartCall)
    async def process_revoke_start(
        self,
        method: RevokeStartCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> RevokeStartResponse | None:
        return await self._handle_existing_job_transition(
            method, scope, correlationid, cloudevent_id
        )

    @scope_from_subject
    @incoming(StartCall)
    async def process_start(
        self,
        method: StartCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> StartResponse | None:
        return await self._handle_existing_job_transition(
            method, scope, correlationid, cloudevent_id
        )

    @scope_from_subject
    @incoming(StopCall)
    async def process_stop(
        self,
        method: StopCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> StopResponse | None:
        return await self._handle_existing_job_transition(
            method, scope, correlationid, cloudevent_id
        )

    # ── Update handler ───────────────────────────────────────────────────

    @scope_from_subject
    @incoming(UpdateCall)
    async def process_update(
        self,
        method: UpdateCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> UpdateResponse | None:
        return await self._handle_update_transition(
            method, scope, correlationid, cloudevent_id
        )

    # ── Query handlers ───────────────────────────────────────────

    @scope_from_subject
    @incoming(RequestJobResponseByJobOrderIDCall)
    async def process_request_job_response_by_id(
        self,
        method: RequestJobResponseByJobOrderIDCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> RequestJobResponseByJobOrderIDResponse | None:
        logger.info(
            "Received RequestJobResponseByJobOrderID for job: %s",
            method.job_order_id,
        )
        if method.job_order_id is None:
            return method.response(return_status=MethodReturnStatus.INVALID_REQUEST)

        job_response = await self._jobresponse_dao.retrieve_by_job_order_id(
            method.job_order_id
        )
        if job_response is None:
            return method.response(
                return_status=MethodReturnStatus.UNKNOWN_JOB_ORDER_ID
            )

        return method.response(
            job_response=job_response,
            return_status=MethodReturnStatus.NO_ERROR,
        )

    @scope_from_subject
    @incoming(RequestJobResponseByJobOrderStateCall)
    async def process_request_job_response_by_state(
        self,
        method: RequestJobResponseByJobOrderStateCall,
        *,
        scope: str,
        correlationid: str | None = None,
        cloudevent_id: str | None = None,
    ) -> RequestJobResponseByJobOrderStateResponse | None:
        logger.info(
            "Received RequestJobResponseByJobOrderState for state: %s",
            method.job_order_state,
        )
        if not method.job_order_state:
            return method.response(return_status=MethodReturnStatus.INVALID_REQUEST)

        job_responses = await self._jobresponse_dao.retrieve_by_state(
            scope, method.job_order_state
        )

        return method.response(
            job_responses=job_responses if job_responses else None,
            return_status=MethodReturnStatus.NO_ERROR,
        )

    # ── Publish metadata ───────────────────────────────────

    def publish_metadata_event(self) -> None:
        """Publish a metadata event describing the processor's capabilities."""

        additional_attributes = [
            (
                "supported_commands",
                [
                    "Store",
                    "StoreAndStart",
                    "Abort",
                    "Cancel",
                    "Clear",
                    "Pause",
                    "Resume",
                    "RevokeStart",
                    "Start",
                    "Stop",
                    "Update",
                    "RequestJobResponseByJobOrderID",
                    "RequestJobResponseByJobOrderState",
                ],
            ),
            (
                "event_types",
                [
                    "ISA95JobOrderStatusEventType",
                ],
            ),
        ]
        metadata = Meta(additional_attributes=additional_attributes)
        cloudevent = self.create_event()
        cloudevent.transportmetadata = {
            "mqtt_retain": True
        }  # MQTT retain flag to keep the metadata event available for late subscribers
        try:
            cloudevent.serialize_payload(metadata)
        except ValueError as e:
            logger.exception(
                "Error serializing payload for message type %s: %s",
                cloudevent.type,
                e,
            )
            return
        self.publish_event(cloudevent, intent=MessageIntent.META)

    # ── CloudEvent lifecycle overrides ───────────────────────────────────

    async def process_response_cloudevent(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        logger.debug("Response message: %s", cloudevent)

        # For error messages, we do not send any response here
        # however we could retry in some cases or log to an external system
        return None

    async def handle_cloudevent_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        logger.info("Message expired: %s", cloudevent.id)
        return None

    async def trigger_outgoing_event(
        self, *, event_type: type[Opc400013MachineryJobMgmtForMqtt], **kwargs
    ) -> None:
        logger.info("Triggering outgoing event: %s", event_type)
        logger.debug("Triggering with kwargs: %s", kwargs)
        logger.warning(
            "Triggering outgoing events is currently not implemented in this processor!"
        )
        return None

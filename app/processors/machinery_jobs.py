import logging
from typing import Any

import redis.asyncio as redis
from transitions.extensions import HierarchicalGraphMachine

from app import ProcessingConfig
from app.common import (
    CloudEvent,
    CloudeventAttributeTuple,
    CloudEventProcessor,
    incoming,
)
from app.models.machinery_jobs import (
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
from app.models.machinery_jobs_ext import (
    JobOrderControlExt,
    MethodReturnStatus,
)
from app.redis import (
    JobOrderAndStateDAO,
    JobResponseDAO,
    RedisKeySchema,
    sanitize_scope,
)

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


class MachineryJobsCloudEventProcessor(CloudEventProcessor):
    def __init__(
        self,
        instance_id: str,
        runtime_config: ProcessingConfig,
        redis_connection_pool: redis.ConnectionPool,
        redis_key_schema: RedisKeySchema,
    ):
        super().__init__(instance_id, runtime_config)
        self._event_attributes.extend(
            [
                CloudeventAttributeTuple("mqtt_topic", "transportmetadata.mqtt_topic"),
                CloudeventAttributeTuple("correlationid", "correlationid"),
                CloudeventAttributeTuple("request_id", "id"),
            ]
        )
        self._redis_client: redis.Redis = redis.Redis(
            connection_pool=redis_connection_pool
        )
        self._redis_key_schema: RedisKeySchema = redis_key_schema
        self._joborder_and_state_dao: JobOrderAndStateDAO = JobOrderAndStateDAO(
            self._redis_client,
            redis_key_schema,
        )
        self._jobresponse_dao: JobResponseDAO = JobResponseDAO(
            self._redis_client,
            redis_key_schema,
        )
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

    # ── helpers ──────────────────────────────────────────────────────────

    async def is_job_acceptable(
        self, job_order: ISA95JobOrderDataType, scope: str
    ) -> bool:
        # TODO implement logic to check if job is acceptable (e.g. check if work master is available, ...)
        return True

    def build_job_state_object(self, state: str) -> list[ISA95StateDataType]:
        if "_" in state:
            main_state, sub_state = state.split("_")
            main_state_number, sub_state_number = (
                JobOrderControlExt.Config.opcua_state_machine_state_ids[
                    main_state
                ].split("_")
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
        self.publish_event(cloudevent)

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
            await self._joborder_and_state_dao.save(job_order_and_state, scope)
        except Exception:
            logger.exception(
                "Failed to persist job order and state for job order %s in scope %s",
                method.job_order.job_order_id,
                scope,
            )
            return method.response(
                return_status=MethodReturnStatus.UNABLE_TO_ACCEPT_JOB_ORDER
            )

        self.send_event(transition, job_order_and_state, correlationid, causationid)
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

        self._state_machine.add_model(job_order_and_state.job_order)
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
            await self._joborder_and_state_dao.save(job_order_and_state, scope)
        except Exception:
            logger.exception(
                "Failed to persist job order and state for job order %s in scope %s",
                job_order_and_state.job_order.job_order_id,
                scope,
            )
            return method.response(
                return_status=MethodReturnStatus.INVALID_JOB_ORDER_STATUS
            )

        self.send_event(transition, job_order_and_state, correlationid, causationid)
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
        self._state_machine.add_model(job_order_and_state.job_order)
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
            await self._joborder_and_state_dao.save(job_order_and_state, scope)
        except Exception:
            logger.exception(
                "Failed to persist job order and state for job order %s in scope %s",
                method.job_order.job_order_id,
                scope,
            )
            return method.response(
                return_status=MethodReturnStatus.INVALID_JOB_ORDER_STATUS
            )

        self.send_event(transition, job_order_and_state, correlationid, causationid)
        return method.response(return_status=MethodReturnStatus.NO_ERROR)

    # ── Store / StoreAndStart handlers ───────────────────────────────────

    @incoming(StoreCall)
    async def process_store(
        self,
        method: StoreCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> StoreResponse | None:
        scope = sanitize_scope(mqtt_topic)
        return await self._handle_store_transition(
            method, scope, correlationid, request_id
        )

    @incoming(StoreAndStartCall)
    async def process_store_and_start(
        self,
        method: StoreAndStartCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> StoreAndStartResponse | None:
        scope = sanitize_scope(mqtt_topic)
        return await self._handle_store_transition(
            method, scope, correlationid, request_id
        )

    # ── Existing-job transition handlers ─────────────────────────────────

    @incoming(AbortCall)
    async def process_abort(
        self,
        method: AbortCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> AbortResponse | None:
        scope = sanitize_scope(mqtt_topic)
        return await self._handle_existing_job_transition(
            method, scope, correlationid, request_id
        )

    @incoming(CancelCall)
    async def process_cancel(
        self,
        method: CancelCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> CancelResponse | None:
        scope = sanitize_scope(mqtt_topic)
        return await self._handle_existing_job_transition(
            method, scope, correlationid, request_id
        )

    @incoming(ClearCall)
    async def process_clear(
        self,
        method: ClearCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> ClearResponse | None:
        scope = sanitize_scope(mqtt_topic)
        return await self._handle_existing_job_transition(
            method, scope, correlationid, request_id
        )

    @incoming(PauseCall)
    async def process_pause(
        self,
        method: PauseCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> PauseResponse | None:
        scope = sanitize_scope(mqtt_topic)
        return await self._handle_existing_job_transition(
            method, scope, correlationid, request_id
        )

    @incoming(ResumeCall)
    async def process_resume(
        self,
        method: ResumeCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> ResumeResponse | None:
        scope = sanitize_scope(mqtt_topic)
        return await self._handle_existing_job_transition(
            method, scope, correlationid, request_id
        )

    @incoming(RevokeStartCall)
    async def process_revoke_start(
        self,
        method: RevokeStartCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> RevokeStartResponse | None:
        scope = sanitize_scope(mqtt_topic)
        return await self._handle_existing_job_transition(
            method, scope, correlationid, request_id
        )

    @incoming(StartCall)
    async def process_start(
        self,
        method: StartCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> StartResponse | None:
        scope = sanitize_scope(mqtt_topic)
        return await self._handle_existing_job_transition(
            method, scope, correlationid, request_id
        )

    @incoming(StopCall)
    async def process_stop(
        self,
        method: StopCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> StopResponse | None:
        scope = sanitize_scope(mqtt_topic)
        return await self._handle_existing_job_transition(
            method, scope, correlationid, request_id
        )

    # ── Update handler ───────────────────────────────────────────────────

    @incoming(UpdateCall)
    async def process_update(
        self,
        method: UpdateCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> UpdateResponse | None:
        scope = sanitize_scope(mqtt_topic)
        return await self._handle_update_transition(
            method, scope, correlationid, request_id
        )

    # ── Query handlers (stubs) ───────────────────────────────────────────

    @incoming(RequestJobResponseByJobOrderIDCall)
    async def process_request_job_response_by_id(
        self,
        method: RequestJobResponseByJobOrderIDCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
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

    @incoming(RequestJobResponseByJobOrderStateCall)
    async def process_request_job_response_by_state(
        self,
        method: RequestJobResponseByJobOrderStateCall,
        *,
        mqtt_topic: str,
        correlationid: str | None = None,
        request_id: str | None = None,
    ) -> RequestJobResponseByJobOrderStateResponse | None:
        logger.info(
            "Received RequestJobResponseByJobOrderState for state: %s",
            method.job_order_state,
        )
        if not method.job_order_state:
            return method.response(return_status=MethodReturnStatus.INVALID_REQUEST)

        scope = sanitize_scope(mqtt_topic)
        job_responses = await self._jobresponse_dao.retrieve_by_state(
            scope, method.job_order_state
        )

        return method.response(
            job_responses=job_responses if job_responses else None,
            return_status=MethodReturnStatus.NO_ERROR,
        )

    # ── CloudEvent lifecycle overrides ───────────────────────────────────

    async def process_event(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        if not self.event_has_callback(cloudevent):
            logger.info("No handler registered for message type: %s", cloudevent.type)
            return None

        result = await self.callback_incoming(cloudevent)

        return result

    async def process_response_event(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        logger.debug("Response message: %s", cloudevent)

        # For error messages, we do not send any response here
        # however we could retry in some cases or log to an external system
        return None

    async def handle_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        logger.info("Message expired: %s", cloudevent.id)
        return None

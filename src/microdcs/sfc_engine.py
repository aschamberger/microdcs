import asyncio
import logging
from collections.abc import Mapping
from typing import Any

import redis.asyncio as redis

from microdcs.common import (
    AdditionalTask,
    CloudEventProcessor,
    MessageIntent,
)
from microdcs.models.machinery_jobs import (
    ISA95JobOrderAndStateDataType,
    ISA95JobResponseDataType,
    ISA95StateDataType,
    LocalizedText,
)
from microdcs.models.machinery_jobs_ext import JobOrderControlExt
from microdcs.models.sfc_recipe import (
    SfcActionAssociation,
    SfcBranch,
    SfcBranchType,
    SfcInteraction,
    SfcRecipe,
    SfcTransition,
)
from microdcs.models.sfc_recipe_ext import (
    SFC_CONSUMER_GROUP,
    SFC_RECIPE_DATASCHEMA,
    SfcActionExecution,
    SfcActionState,
    SfcExecutionState,
    SfcWorkAction,
)
from microdcs.redis import (
    JobOrderAndStateDAO,
    JobResponseDAO,
    RedisKeySchema,
    SfcExecutionDAO,
    WorkMasterDAO,
)

logger = logging.getLogger("sfc_engine")


class SfcEngine(AdditionalTask):
    """SFC recipe execution engine.

    Runs as an ``AdditionalTask`` on every MicroDCS instance.  Coordinates
    with other instances via a Redis Stream consumer group and atomic
    compare-and-swap (CAS) Lua scripts.
    """

    def __init__(
        self,
        redis_connection_pool: redis.ConnectionPool,
        redis_key_schema: RedisKeySchema,
        nb_processor: CloudEventProcessor,
        sb_processors: Mapping[str, CloudEventProcessor],
        *,
        consumer_name: str,
        autoclaim_min_idle_ms: int = 30_000,
        stream_block_ms: int = 2_000,
        stream_read_count: int = 10,
        recovery_idle_threshold_s: int = 60,
    ):
        super().__init__()
        self._redis_client = redis.Redis(connection_pool=redis_connection_pool)
        self._key_schema = redis_key_schema
        self._nb_processor = nb_processor
        self._sb_processors = sb_processors
        self._consumer_name = consumer_name
        self._autoclaim_min_idle_ms = autoclaim_min_idle_ms
        self._stream_block_ms = stream_block_ms
        self._stream_read_count = stream_read_count
        self._recovery_idle_threshold_s = recovery_idle_threshold_s

        self._execution_dao = SfcExecutionDAO(self._redis_client, redis_key_schema)
        self._joborder_dao = JobOrderAndStateDAO(self._redis_client, redis_key_schema)
        self._jobresponse_dao = JobResponseDAO(self._redis_client, redis_key_schema)
        self._workmaster_dao = WorkMasterDAO(self._redis_client, redis_key_schema)

        # State machine for OPC UA job transitions
        from transitions.extensions import HierarchicalGraphMachine

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

        self._known_scopes: set[str] = set()

    # ── AdditionalTask entry point ──────────────────────────────────────

    async def task(self) -> None:
        logger.info("SFC engine starting (consumer=%s)", self._consumer_name)
        try:
            await self._recovery_scan()
            await self._run_loop()
        except asyncio.CancelledError:
            logger.info("SFC engine task cancelled")
        except Exception:
            logger.exception("SFC engine task failed")
            raise

    # ── Main loop ───────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        while not self._shutdown_event.is_set():
            # Ensure consumer groups exist for all known scopes
            for scope in list(self._known_scopes):
                await self._execution_dao.ensure_consumer_group(
                    scope, SFC_CONSUMER_GROUP
                )

            # Autoclaim orphaned entries from dead consumers
            await self._autoclaim_orphaned()

            # Read new work items from all known scope streams
            if self._known_scopes:
                await self._read_work_items()

            # Wait before next iteration (or until shutdown)
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._stream_block_ms / 1000.0,
                )
                return  # shutdown signalled
            except asyncio.TimeoutError:
                pass

    async def _read_work_items(self) -> None:
        streams: dict[str, str] = {}
        for scope in self._known_scopes:
            stream_key = self._key_schema.sfc_work_stream(scope)
            streams[stream_key] = ">"  # read only new entries for this consumer

        if not streams:
            return

        try:
            results = await self._redis_client.xreadgroup(
                groupname=SFC_CONSUMER_GROUP,
                consumername=self._consumer_name,
                streams=streams,  # type: ignore[arg-type]
                block=self._stream_block_ms,
                count=self._stream_read_count,
            )
        except redis.ResponseError as e:
            if "NOGROUP" in str(e):
                # Consumer group doesn't exist yet for some scope — will be
                # created on next loop iteration
                return
            raise

        if not results:
            return

        for stream_key_raw, entries in results:
            stream_key = (
                stream_key_raw.decode()
                if isinstance(stream_key_raw, bytes)
                else stream_key_raw
            )
            for entry_id_raw, fields_raw in entries:
                entry_id = (
                    entry_id_raw.decode()
                    if isinstance(entry_id_raw, bytes)
                    else entry_id_raw
                )
                fields = {
                    (k.decode() if isinstance(k, bytes) else k): (
                        v.decode() if isinstance(v, bytes) else v
                    )
                    for k, v in fields_raw.items()
                }
                await self._process_work_item(stream_key, entry_id, fields)

    async def _autoclaim_orphaned(self) -> None:
        for scope in list(self._known_scopes):
            stream_key = self._key_schema.sfc_work_stream(scope)
            try:
                # XAUTOCLAIM returns (next_start_id, claimed_entries, deleted_ids)
                _, entries, _ = await self._redis_client.xautoclaim(
                    stream_key,
                    SFC_CONSUMER_GROUP,
                    self._consumer_name,
                    min_idle_time=self._autoclaim_min_idle_ms,
                    count=self._stream_read_count,
                )
            except redis.ResponseError:
                # Group may not exist yet
                continue

            for entry_id_raw, fields_raw in entries:
                entry_id = (
                    entry_id_raw.decode()
                    if isinstance(entry_id_raw, bytes)
                    else entry_id_raw
                )
                fields = {
                    (k.decode() if isinstance(k, bytes) else k): (
                        v.decode() if isinstance(v, bytes) else v
                    )
                    for k, v in fields_raw.items()
                }
                logger.info("Autoclaimed orphaned work item %s: %s", entry_id, fields)
                await self._process_work_item(stream_key, entry_id, fields)

    # ── Work item dispatch ──────────────────────────────────────────────

    async def _process_work_item(
        self, stream_key: str, entry_id: str, fields: dict[str, str]
    ) -> None:
        job_id = fields.get("job_id", "")
        action = fields.get("action", "")
        scope = fields.get("scope", "")

        if not job_id or not action:
            logger.warning("Invalid work item %s: %s", entry_id, fields)
            await self._ack(stream_key, entry_id)
            return

        try:
            if action == SfcWorkAction.START_RECIPE.value:
                await self._handle_start_recipe(job_id, scope)
            elif action.startswith("dispatch_action:"):
                action_name = action[len("dispatch_action:") :]
                await self._handle_dispatch_action(job_id, action_name)
            elif action == SfcWorkAction.RESUME.value:
                await self._handle_resume(job_id)
            else:
                logger.warning("Unknown work action '%s' for job %s", action, job_id)
        except Exception:
            logger.exception("Error processing work item %s for job %s", action, job_id)

        await self._ack(stream_key, entry_id)

    async def _ack(self, stream_key: str, entry_id: str) -> None:
        try:
            await self._redis_client.xack(stream_key, SFC_CONSUMER_GROUP, entry_id)
        except redis.ResponseError:
            logger.debug("Failed to ACK entry %s on %s", entry_id, stream_key)

    # ── Recipe start ────────────────────────────────────────────────────

    async def _handle_start_recipe(self, job_id: str, scope: str) -> None:
        # Check if already started (idempotent)
        existing = await self._execution_dao.retrieve(job_id)
        if existing is not None:
            logger.info("Job %s already has SFC state, skipping start", job_id)
            return

        # Load job order
        job_order_and_state = await self._joborder_dao.retrieve(job_id)
        if job_order_and_state is None or job_order_and_state.job_order is None:
            logger.warning("Job order %s not found, cannot start recipe", job_id)
            return

        job_order = job_order_and_state.job_order

        # Fall back to deriving scope from the job if not provided
        if not scope:
            scope = self._scope_from_job(job_order_and_state) or ""
        if not scope:
            logger.warning("Cannot determine scope for job %s", job_id)
            return

        # Resolve Work Master
        work_master_id = self._get_work_master_id(job_order)
        if work_master_id is None:
            logger.warning("No work master ID on job %s", job_id)
            return

        work_master = await self._workmaster_dao.retrieve(work_master_id)
        if work_master is None:
            logger.warning(
                "Work master %s not found for job %s", work_master_id, job_id
            )
            return

        # Check dataschema
        if work_master.dataschema != SFC_RECIPE_DATASCHEMA:
            logger.warning(
                "Work master %s has dataschema %s, expected %s",
                work_master_id,
                work_master.dataschema,
                SFC_RECIPE_DATASCHEMA,
            )
            return

        if work_master.data is None:
            logger.warning("Work master %s has no recipe data", work_master_id)
            return

        # Deserialize recipe
        assert isinstance(work_master.data, dict)
        recipe = SfcRecipe.from_dict(work_master.data)
        initial_step = self._find_initial_step(recipe)
        if initial_step is None:
            logger.error("No initial step in recipe for job %s", job_id)
            return

        # Trigger Run on the OPC UA state machine
        run_ok = await self._trigger_job_transition(
            job_id, scope, "Run", job_order_and_state
        )
        if not run_ok:
            logger.warning("Failed to trigger Run for job %s", job_id)
            return

        # Create execution state
        actions: dict[str, SfcActionExecution] = {}
        for assoc in recipe.actions:
            actions[assoc.name] = SfcActionExecution(name=assoc.name)

        exec_state = SfcExecutionState(
            job_id=job_id,
            scope=scope,
            work_master_id=work_master_id,
            current_step=initial_step,
            active_steps=[initial_step],
            actions=actions,
        )
        await self._execution_dao.save(exec_state)
        self._known_scopes.add(scope)

        logger.info("Started SFC recipe for job %s at step %s", job_id, initial_step)

        # Dispatch actions for the initial step
        await self._dispatch_step_actions(job_id, scope, initial_step, recipe)

    # ── Action dispatch ─────────────────────────────────────────────────

    async def _handle_dispatch_action(self, job_id: str, action_name: str) -> None:
        exec_state = await self._execution_dao.retrieve(job_id)
        if exec_state is None:
            logger.warning(
                "No SFC state for job %s, cannot dispatch %s", job_id, action_name
            )
            return

        if exec_state.completed or exec_state.failed:
            logger.debug("Job %s already finished, ignoring dispatch", job_id)
            return

        action_exec = exec_state.actions.get(action_name)
        if action_exec is None:
            logger.warning("Action %s not found in job %s", action_name, job_id)
            return

        # Load recipe for action details
        work_master = await self._workmaster_dao.retrieve(exec_state.work_master_id)
        if work_master is None or work_master.data is None:
            logger.warning("Work master lost for job %s", job_id)
            return

        assert isinstance(work_master.data, dict)
        recipe = SfcRecipe.from_dict(work_master.data)
        assoc = self._find_action(recipe, action_name)
        if assoc is None:
            logger.warning("Action %s not in recipe for job %s", action_name, job_id)
            return

        if assoc.interaction == SfcInteraction.PUSH_COMMAND:
            await self._dispatch_push_command(
                job_id, exec_state.scope, assoc, action_exec
            )
        elif assoc.interaction == SfcInteraction.PULL_EVENT:
            await self._dispatch_pull_event(
                job_id, exec_state.scope, assoc, action_exec
            )

    async def _dispatch_step_actions(
        self,
        job_id: str,
        scope: str,
        step_name: str,
        recipe: SfcRecipe,
    ) -> None:
        """Dispatch all actions associated with the given step."""
        step_actions = [a for a in recipe.actions if a.step == step_name]
        for assoc in step_actions:
            if assoc.interaction == SfcInteraction.PUSH_COMMAND:
                exec_state = await self._execution_dao.retrieve(job_id)
                if exec_state is None:
                    return
                action_exec = exec_state.actions.get(assoc.name)
                if action_exec is None:
                    return
                await self._dispatch_push_command(job_id, scope, assoc, action_exec)
            elif assoc.interaction == SfcInteraction.PULL_EVENT:
                exec_state = await self._execution_dao.retrieve(job_id)
                if exec_state is None:
                    return
                action_exec = exec_state.actions.get(assoc.name)
                if action_exec is None:
                    return
                await self._dispatch_pull_event(job_id, scope, assoc, action_exec)

    async def _dispatch_push_command(
        self,
        job_id: str,
        scope: str,
        assoc: SfcActionAssociation,
        action_exec: SfcActionExecution,
    ) -> None:
        """Send a push_command via the SB processor."""
        new_attempt = action_exec.attempt + 1
        correlation_id = f"{job_id}:{assoc.name}:{new_attempt}"

        # CAS: pending → dispatched (or re-dispatch if already dispatched via autoclaim)
        expected = action_exec.state
        if expected not in (SfcActionState.PENDING, SfcActionState.DISPATCHED):
            logger.debug(
                "Action %s on job %s in state %s, skipping push",
                assoc.name,
                job_id,
                expected,
            )
            return

        result = await self._execution_dao.cas_action_state(
            job_id=job_id,
            scope=scope,
            action_name=assoc.name,
            expected_state=expected,
            new_state=SfcActionState.DISPATCHED,
            correlation_id=correlation_id,
            attempt=new_attempt,
        )
        if result != "OK":
            logger.debug(
                "CAS for push %s on job %s returned %s", assoc.name, job_id, result
            )
            return

        # Find the SB processor that handles this CloudEvent type
        sb_processor = self._find_sb_processor(assoc.cloudevent_type)
        if sb_processor is None:
            logger.error(
                "No SB processor for CloudEvent type %s (action %s, job %s)",
                assoc.cloudevent_type,
                assoc.name,
                job_id,
            )
            await self._fail_job(job_id, f"No SB processor for {assoc.cloudevent_type}")
            return

        # Resolve the payload type class from the processor's type registry
        payload_type = sb_processor._type_classes.get(assoc.cloudevent_type)
        if payload_type is None:
            logger.error(
                "No payload type registered for %s on SB processor",
                assoc.cloudevent_type,
            )
            await self._fail_job(job_id, f"No payload type for {assoc.cloudevent_type}")
            return

        # Build kwargs for the outgoing callback
        kwargs: dict[str, Any] = {
            "job_id": job_id,
            "scope": scope,
        }
        if assoc.parameters:
            kwargs["parameters"] = assoc.parameters

        logger.info(
            "Dispatching push_command %s for job %s (correlation_id=%s)",
            assoc.name,
            job_id,
            correlation_id,
        )

        try:
            await sb_processor.callback_outgoing(
                payload_type=payload_type,
                intent=MessageIntent.COMMAND,
                subject=scope,
                correlation_id=correlation_id,
                **kwargs,
            )
        except Exception:
            logger.exception(
                "Failed to dispatch push_command %s for job %s", assoc.name, job_id
            )

    async def _dispatch_pull_event(
        self,
        job_id: str,
        scope: str,
        assoc: SfcActionAssociation,
        action_exec: SfcActionExecution,
    ) -> None:
        """Mark a pull_event action as waiting."""
        if action_exec.state not in (SfcActionState.PENDING,):
            return

        result = await self._execution_dao.cas_action_state(
            job_id=job_id,
            scope=scope,
            action_name=assoc.name,
            expected_state=SfcActionState.PENDING,
            new_state=SfcActionState.WAITING,
        )
        if result == "OK":
            logger.info(
                "Action %s on job %s now waiting for pull_event (%s)",
                assoc.name,
                job_id,
                assoc.cloudevent_type,
            )

    # ── Event completion (called by SB processors) ──────────────────────

    async def complete_action(
        self,
        job_id: str,
        action_name: str,
        result_data: dict[str, Any] | None = None,
    ) -> None:
        """Called when an action completes (push response or pull event received).

        Advances the SFC state: marks the action completed, checks if the
        step is done, evaluates transitions, and dispatches the next step's
        actions (or finishes the job).
        """
        exec_state = await self._execution_dao.retrieve(job_id)
        if exec_state is None:
            logger.warning("No SFC state for job %s on action completion", job_id)
            return

        if exec_state.completed or exec_state.failed:
            return

        action_exec = exec_state.actions.get(action_name)
        if action_exec is None:
            logger.warning("Action %s not in job %s state", action_name, job_id)
            return

        # CAS the action to completed
        expected = action_exec.state
        if expected not in (SfcActionState.DISPATCHED, SfcActionState.WAITING):
            logger.debug(
                "Action %s on job %s in state %s, not completable",
                action_name,
                job_id,
                expected,
            )
            return

        cas_result = await self._execution_dao.cas_action_state(
            job_id=job_id,
            scope=exec_state.scope,
            action_name=action_name,
            expected_state=expected,
            new_state=SfcActionState.COMPLETED,
        )
        if cas_result != "OK":
            logger.debug(
                "CAS complete for %s on job %s returned %s",
                action_name,
                job_id,
                cas_result,
            )
            return

        logger.info("Action %s completed on job %s", action_name, job_id)

        # Check if all actions for the current step are done
        await self._check_step_completion(job_id, exec_state.scope)

    async def fail_action(
        self,
        job_id: str,
        action_name: str,
        error: str = "",
    ) -> None:
        """Called when an action fails (timeout or equipment error)."""
        exec_state = await self._execution_dao.retrieve(job_id)
        if exec_state is None:
            return

        action_exec = exec_state.actions.get(action_name)
        if action_exec is None:
            return

        expected = action_exec.state
        if expected in (SfcActionState.COMPLETED, SfcActionState.FAILED):
            return

        await self._execution_dao.cas_action_state(
            job_id=job_id,
            scope=exec_state.scope,
            action_name=action_name,
            expected_state=expected,
            new_state=SfcActionState.FAILED,
        )
        await self._fail_job(job_id, error or f"Action {action_name} failed")

    # ── Step completion and transition evaluation ───────────────────────

    async def _check_step_completion(self, job_id: str, scope: str) -> None:
        """Check if any active step has all actions done and advance."""
        exec_state = await self._execution_dao.retrieve(job_id)
        if exec_state is None or exec_state.completed or exec_state.failed:
            return

        work_master = await self._workmaster_dao.retrieve(exec_state.work_master_id)
        if work_master is None or work_master.data is None:
            return

        assert isinstance(work_master.data, dict)
        recipe = SfcRecipe.from_dict(work_master.data)

        for step_name in list(exec_state.active_steps):
            step_actions = [a for a in recipe.actions if a.step == step_name]
            all_done = all(
                exec_state.actions.get(a.name) is not None
                and exec_state.actions[a.name].state == SfcActionState.COMPLETED
                for a in step_actions
            )
            if not all_done:
                continue

            # This step is done — determine how to advance.
            branch = self._find_branch(recipe, exec_state.current_step)
            if branch is not None:
                # Inside a branch — advance within or converge.
                await self._advance_branch_step(
                    job_id, scope, step_name, branch, recipe
                )
            else:
                # Linear execution — use transitions.
                await self._advance_linear_step(job_id, scope, step_name, recipe)

            # Re-read state — it may have changed.
            exec_state = await self._execution_dao.retrieve(job_id)
            if exec_state is None or exec_state.completed or exec_state.failed:
                return

    async def _advance_linear_step(
        self,
        job_id: str,
        scope: str,
        completed_step: str,
        recipe: SfcRecipe,
    ) -> None:
        """Advance from a completed step in linear (non-branch) context."""
        target = self._evaluate_transitions(recipe, completed_step)
        if target is None:
            await self._complete_job(job_id, scope)
            return

        branch = self._find_branch(recipe, target)
        if branch is not None:
            # Entering a branch construct.
            await self._enter_branch(job_id, scope, completed_step, branch, recipe)
        else:
            # Normal step-to-step transition.
            await self._advance_to_step(job_id, scope, completed_step, target, recipe)

    async def _advance_to_step(
        self,
        job_id: str,
        scope: str,
        from_step: str,
        to_step: str,
        recipe: SfcRecipe,
    ) -> None:
        """CAS advance ``current_step`` from *from_step* to *to_step*."""
        next_step_actions = [a for a in recipe.actions if a.step == to_step]
        push_actions = [
            a for a in next_step_actions if a.interaction == SfcInteraction.PUSH_COMMAND
        ]

        follow_up: list[str] | None = None
        if push_actions:
            follow_up = [
                "job_id",
                job_id,
                "action",
                f"dispatch_action:{push_actions[0].name}",
            ]

        cas_result = await self._execution_dao.cas_advance_step(
            job_id=job_id,
            scope=scope,
            expected_step=from_step,
            new_step=to_step,
            new_active_steps=[to_step],
            follow_up_stream_fields=follow_up,
        )
        if cas_result != "OK":
            logger.debug("CAS advance step for job %s returned %s", job_id, cas_result)
            return

        logger.info("Job %s advanced from %s to %s", job_id, from_step, to_step)
        await self._dispatch_non_streamed_actions(
            job_id, scope, to_step, recipe, push_actions
        )

    async def _dispatch_non_streamed_actions(
        self,
        job_id: str,
        scope: str,
        step_name: str,
        recipe: SfcRecipe,
        push_actions: list[SfcActionAssociation],
    ) -> None:
        """Dispatch pull_event actions and extra push_command work items."""
        step_actions = [a for a in recipe.actions if a.step == step_name]
        pull_actions = [
            a for a in step_actions if a.interaction == SfcInteraction.PULL_EVENT
        ]
        for assoc in pull_actions:
            exec_state_fresh = await self._execution_dao.retrieve(job_id)
            if exec_state_fresh is None:
                return
            action_exec = exec_state_fresh.actions.get(assoc.name)
            if action_exec is not None:
                await self._dispatch_pull_event(job_id, scope, assoc, action_exec)

        for assoc in push_actions[1:]:
            await self._execution_dao.enqueue_work(
                scope,
                job_id,
                f"dispatch_action:{assoc.name}",
            )

    # ── Branch entry / advancement / convergence ────────────────────────

    async def _enter_branch(
        self,
        job_id: str,
        scope: str,
        from_step: str,
        branch: SfcBranch,
        recipe: SfcRecipe,
    ) -> None:
        """Enter a branch construct (simultaneous or selection)."""
        if branch.type == SfcBranchType.SIMULTANEOUS:
            first_steps = [path[0] for path in branch.branches if path]
        else:
            # Selection: pick one path using transition priority.
            selected_path = self._select_branch_path(recipe, branch)
            first_steps = [selected_path[0]] if selected_path else []

        if not first_steps:
            logger.warning(
                "Branch %s has no valid paths for job %s", branch.name, job_id
            )
            return

        # Collect follow-up for the first push_command across all activated steps.
        follow_up: list[str] | None = None
        first_push: SfcActionAssociation | None = None
        for step in first_steps:
            step_push = [
                a
                for a in recipe.actions
                if a.step == step and a.interaction == SfcInteraction.PUSH_COMMAND
            ]
            if step_push and first_push is None:
                first_push = step_push[0]
                follow_up = [
                    "job_id",
                    job_id,
                    "action",
                    f"dispatch_action:{first_push.name}",
                ]

        cas_result = await self._execution_dao.cas_advance_step(
            job_id=job_id,
            scope=scope,
            expected_step=from_step,
            new_step=branch.name,
            new_active_steps=first_steps,
            follow_up_stream_fields=follow_up,
        )
        if cas_result != "OK":
            logger.debug(
                "CAS enter branch %s for job %s returned %s",
                branch.name,
                job_id,
                cas_result,
            )
            return

        logger.info(
            "Job %s entered branch %s (%s), active steps: %s",
            job_id,
            branch.name,
            branch.type.value,
            first_steps,
        )

        # Dispatch remaining push actions and all pull actions for activated steps.
        first_push_skipped = False
        for step in first_steps:
            step_push = [
                a
                for a in recipe.actions
                if a.step == step and a.interaction == SfcInteraction.PUSH_COMMAND
            ]
            for assoc in step_push:
                if not first_push_skipped and assoc is first_push:
                    first_push_skipped = True
                    continue
                await self._execution_dao.enqueue_work(
                    scope, job_id, f"dispatch_action:{assoc.name}"
                )

            step_pull = [
                a
                for a in recipe.actions
                if a.step == step and a.interaction == SfcInteraction.PULL_EVENT
            ]
            for assoc in step_pull:
                exec_state = await self._execution_dao.retrieve(job_id)
                if exec_state is None:
                    return
                action_exec = exec_state.actions.get(assoc.name)
                if action_exec is not None:
                    await self._dispatch_pull_event(job_id, scope, assoc, action_exec)

    async def _advance_branch_step(
        self,
        job_id: str,
        scope: str,
        completed_step: str,
        branch: SfcBranch,
        recipe: SfcRecipe,
    ) -> None:
        """Advance within a branch after *completed_step* finishes."""
        next_step = self._next_step_in_branch_path(branch, completed_step)

        if next_step is not None:
            # Intra-branch advancement: replace completed_step with next_step.
            step_push = [
                a
                for a in recipe.actions
                if a.step == next_step and a.interaction == SfcInteraction.PUSH_COMMAND
            ]
            follow_up: list[str] | None = None
            if step_push:
                follow_up = [
                    "job_id",
                    job_id,
                    "action",
                    f"dispatch_action:{step_push[0].name}",
                ]

            result = await self._execution_dao.cas_branch_advance(
                job_id=job_id,
                scope=scope,
                completed_step=completed_step,
                next_step=next_step,
                follow_up_stream_fields=follow_up,
            )
            if isinstance(result, str):
                logger.debug(
                    "CAS branch advance for job %s returned %s", job_id, result
                )
                return

            logger.info(
                "Job %s branch step %s → %s (active: %s)",
                job_id,
                completed_step,
                next_step,
                result,
            )
            await self._dispatch_non_streamed_actions(
                job_id, scope, next_step, recipe, step_push
            )
        else:
            # Last step in this branch path — remove from active_steps.
            result = await self._execution_dao.cas_branch_advance(
                job_id=job_id,
                scope=scope,
                completed_step=completed_step,
            )
            if isinstance(result, str):
                logger.debug("CAS branch remove for job %s returned %s", job_id, result)
                return

            new_active: list[str] = result
            if not new_active:
                # All branch paths done — converge.
                await self._converge_branch(job_id, scope, branch, recipe)
            else:
                logger.debug(
                    "Job %s branch path finished step %s, waiting for: %s",
                    job_id,
                    completed_step,
                    new_active,
                )

    async def _converge_branch(
        self,
        job_id: str,
        scope: str,
        branch: SfcBranch,
        recipe: SfcRecipe,
    ) -> None:
        """All branch paths completed — find exit transition and advance."""
        exit_target = self._find_branch_exit(recipe, branch)
        if exit_target is None:
            await self._complete_job(job_id, scope)
            return

        logger.info(
            "Job %s converging branch %s → %s", job_id, branch.name, exit_target
        )

        # Check if the exit target is itself another branch.
        next_branch = self._find_branch(recipe, exit_target)
        if next_branch is not None:
            await self._enter_branch(job_id, scope, branch.name, next_branch, recipe)
        else:
            await self._advance_to_step(job_id, scope, branch.name, exit_target, recipe)

    def _evaluate_transitions(self, recipe: SfcRecipe, source: str) -> str | None:
        """Find the target of the highest-priority outgoing transition."""
        outgoing: list[SfcTransition] = [
            t for t in recipe.transitions if t.source == source
        ]
        if not outgoing:
            return None

        outgoing.sort(key=lambda t: t.priority or 0)
        return outgoing[0].target

    @staticmethod
    def _select_branch_path(recipe: SfcRecipe, branch: SfcBranch) -> list[str]:
        """For a selection branch, pick the path whose entry transition has the
        lowest (= highest) priority.  Falls back to the first path."""
        best_path: list[str] = branch.branches[0] if branch.branches else []

        first_steps = {path[0] for path in branch.branches if path}
        candidates = [
            t
            for t in recipe.transitions
            if t.source == branch.name and t.target in first_steps
        ]
        if candidates:
            candidates.sort(key=lambda t: t.priority or 0)
            selected_first = candidates[0].target
            for path in branch.branches:
                if path and path[0] == selected_first:
                    return path

        # No explicit entry transitions — fall back to first path.
        return best_path

    # ── Job completion / failure ────────────────────────────────────────

    async def _complete_job(self, job_id: str, scope: str) -> None:
        """Mark job as completed in SFC state and OPC UA state machine."""
        cas_result = await self._execution_dao.cas_finish(job_id, "completed")
        if cas_result != "OK":
            return

        logger.info("Job %s recipe completed", job_id)

        # Trigger Ended state on OPC UA state machine
        job_order_and_state = await self._joborder_dao.retrieve(job_id)
        if (
            job_order_and_state is not None
            and job_order_and_state.job_order is not None
        ):
            await self._trigger_job_transition(
                job_id, scope, "End", job_order_and_state
            )

            # Write job response
            response = ISA95JobResponseDataType(
                job_response_id=f"{job_id}-response",
                job_order_id=job_id,
                job_state=self._build_job_state_object("Ended_Completed"),
            )
            await self._jobresponse_dao.save(response, scope)

    async def _fail_job(self, job_id: str, error: str = "") -> None:
        """Mark job as failed in SFC state and trigger Abort on OPC UA state machine."""
        exec_state = await self._execution_dao.retrieve(job_id)
        if exec_state is None:
            return

        cas_result = await self._execution_dao.cas_finish(job_id, "failed", error)
        if cas_result != "OK":
            return

        logger.warning("Job %s failed: %s", job_id, error)

        scope = exec_state.scope
        job_order_and_state = await self._joborder_dao.retrieve(job_id)
        if (
            job_order_and_state is not None
            and job_order_and_state.job_order is not None
        ):
            await self._trigger_job_transition(
                job_id, scope, "Abort", job_order_and_state
            )

    # ── Resume (recovery) ───────────────────────────────────────────────

    async def _handle_resume(self, job_id: str) -> None:
        exec_state = await self._execution_dao.retrieve(job_id)
        if exec_state is None:
            logger.warning("No SFC state for job %s on resume", job_id)
            return

        if exec_state.completed or exec_state.failed:
            return

        logger.info("Resuming job %s at step %s", job_id, exec_state.current_step)

        work_master = await self._workmaster_dao.retrieve(exec_state.work_master_id)
        if work_master is None or work_master.data is None:
            logger.warning("Work master lost for job %s on resume", job_id)
            return

        assert isinstance(work_master.data, dict)
        recipe = SfcRecipe.from_dict(work_master.data)

        # Re-dispatch actions for all active steps (handles branches).
        for step_name in list(exec_state.active_steps):
            step_actions = [a for a in recipe.actions if a.step == step_name]
            for assoc in step_actions:
                action_exec = exec_state.actions.get(assoc.name)
                if action_exec is None:
                    continue
                if action_exec.state in (
                    SfcActionState.PENDING,
                    SfcActionState.DISPATCHED,
                ):
                    if assoc.interaction == SfcInteraction.PUSH_COMMAND:
                        await self._dispatch_push_command(
                            job_id, exec_state.scope, assoc, action_exec
                        )
                    elif assoc.interaction == SfcInteraction.PULL_EVENT:
                        await self._dispatch_pull_event(
                            job_id, exec_state.scope, assoc, action_exec
                        )
                elif action_exec.state == SfcActionState.WAITING:
                    pass

        await self._check_step_completion(job_id, exec_state.scope)

    async def _recovery_scan(self) -> None:
        """Scan for active jobs and enqueue resume work items."""
        active_jobs = await self._execution_dao.list_active_jobs()
        if not active_jobs:
            logger.info("Recovery scan: no active SFC jobs found")
            return

        logger.info("Recovery scan: found %d active SFC jobs", len(active_jobs))
        for job_id in active_jobs:
            exec_state = await self._execution_dao.retrieve(job_id)
            if exec_state is None:
                continue
            if exec_state.completed or exec_state.failed:
                continue

            scope = exec_state.scope
            self._known_scopes.add(scope)
            await self._execution_dao.ensure_consumer_group(scope, SFC_CONSUMER_GROUP)
            await self._execution_dao.enqueue_work(scope, job_id, SfcWorkAction.RESUME)
            logger.info("Enqueued resume for job %s in scope %s", job_id, scope)

    # ── OPC UA state machine helpers ────────────────────────────────────

    async def _trigger_job_transition(
        self,
        job_id: str,
        scope: str,
        transition: str,
        job_order_and_state: ISA95JobOrderAndStateDataType,
    ) -> bool:
        """Trigger a transition on the OPC UA job state machine and persist."""
        if job_order_and_state.job_order is None:
            return False

        current_state = self._reconstruct_state_name(job_order_and_state.state)
        self._state_machine.add_model(
            job_order_and_state.job_order,
            initial=current_state or "InitialState",
        )
        try:
            if not job_order_and_state.job_order.may_trigger(transition):
                logger.warning(
                    "Job %s cannot trigger %s from state %s",
                    job_id,
                    transition,
                    current_state,
                )
                return False

            job_order_and_state.job_order.trigger(transition)
            job_order_and_state.state = self._build_job_state_object(
                job_order_and_state.job_order._state
            )
        finally:
            self._state_machine.remove_model(job_order_and_state.job_order)

        await self._joborder_dao.save(job_order_and_state, scope, transition)
        return True

    @staticmethod
    def _reconstruct_state_name(
        state: list[ISA95StateDataType] | None,
    ) -> str | None:
        if not state:
            return None
        parts = [s.state_text.text for s in state if s.state_text and s.state_text.text]
        return "_".join(parts) if parts else None

    @staticmethod
    def _build_job_state_object(state: str) -> list[ISA95StateDataType]:
        if "_" in state:
            main_state, sub_state = state.split("_")
            main_num, sub_num = JobOrderControlExt.Config.opcua_state_machine_state_ids[
                state
            ].split("_")
            return [
                ISA95StateDataType(
                    state_text=LocalizedText(text=main_state, locale="en"),
                    state_number=int(main_num),
                ),
                ISA95StateDataType(
                    state_text=LocalizedText(text=sub_state, locale="en"),
                    state_number=int(sub_num),
                ),
            ]
        return [
            ISA95StateDataType(
                state_text=LocalizedText(text=state, locale="en"),
                state_number=int(
                    JobOrderControlExt.Config.opcua_state_machine_state_ids[state]
                ),
            )
        ]

    # ── Recipe helpers ──────────────────────────────────────────────────

    @staticmethod
    def _find_initial_step(recipe: SfcRecipe) -> str | None:
        for step in recipe.steps:
            if step.initial:
                return step.name
        return None

    @staticmethod
    def _find_action(
        recipe: SfcRecipe, action_name: str
    ) -> SfcActionAssociation | None:
        for assoc in recipe.actions:
            if assoc.name == action_name:
                return assoc
        return None

    @staticmethod
    def _find_branch(recipe: SfcRecipe, name: str) -> SfcBranch | None:
        """Find a branch construct by name, or ``None`` if *name* is a step."""
        if recipe.branches:
            for branch in recipe.branches:
                if branch.name == name:
                    return branch
        return None

    @staticmethod
    def _next_step_in_branch_path(branch: SfcBranch, step_name: str) -> str | None:
        """Return the step after *step_name* in its branch path, or ``None``."""
        for path in branch.branches:
            for i, name in enumerate(path):
                if name == step_name and i + 1 < len(path):
                    return path[i + 1]
        return None

    @staticmethod
    def _find_branch_containing_step(
        recipe: SfcRecipe, step_name: str
    ) -> SfcBranch | None:
        """Return the branch that contains *step_name*, or ``None``."""
        if recipe.branches:
            for branch in recipe.branches:
                for path in branch.branches:
                    if step_name in path:
                        return branch
        return None

    @staticmethod
    def _find_branch_exit(recipe: SfcRecipe, branch: SfcBranch) -> str | None:
        """Find the exit (convergence) transition target for a branch.

        Exit transitions go from the branch name to a target that is **not**
        one of the branch's path first steps.
        """
        first_steps = {path[0] for path in branch.branches if path}
        for t in recipe.transitions:
            if t.source == branch.name and t.target not in first_steps:
                return t.target
        return None

    @staticmethod
    def _get_work_master_id(job_order) -> str | None:
        if job_order.work_master_id:
            for wm in job_order.work_master_id:
                if wm.id:
                    return wm.id
        return None

    @staticmethod
    def _scope_from_job(
        job_order_and_state: ISA95JobOrderAndStateDataType,
    ) -> str | None:
        """Extract scope from a job order's equipment requirements or fall back."""
        if (
            job_order_and_state.job_order
            and job_order_and_state.job_order.equipment_requirements
        ):
            for eq in job_order_and_state.job_order.equipment_requirements:
                if eq.id:
                    return eq.id
        return None

    def _find_sb_processor(self, cloudevent_type: str) -> CloudEventProcessor | None:
        """Find the SB processor that handles the given CloudEvent type."""
        for processor in self._sb_processors.values():
            if cloudevent_type in processor._type_callbacks_out:
                return processor
        return None

    def register_scope(self, scope: str) -> None:
        """Register a scope so the engine monitors its work stream."""
        self._known_scopes.add(scope)

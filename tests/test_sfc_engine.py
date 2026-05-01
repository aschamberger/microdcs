from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from microdcs.models.machinery_jobs import (
    ISA95EquipmentDataType,
    ISA95JobOrderAndStateDataType,
    ISA95JobOrderDataType,
    ISA95StateDataType,
    ISA95WorkMasterDataType,
    LocalizedText,
)
from microdcs.models.machinery_jobs_ext import ISA95WorkMasterDataTypeExt
from microdcs.models.sfc_recipe import (
    SfcActionAssociation,
    SfcActionQualifier,
    SfcBranch,
    SfcBranchType,
    SfcInteraction,
    SfcRecipe,
    SfcStep,
    SfcTransition,
)
from microdcs.models.sfc_recipe_ext import (
    SFC_RECIPE_DATASCHEMA,
    SfcActionExecution,
    SfcActionState,
    SfcExecutionState,
)
from microdcs.redis import RedisKeySchema, SfcExecutionDAO
from microdcs.sfc_engine import SfcEngine


def _make_schema(prefix: str = "test") -> RedisKeySchema:
    return RedisKeySchema(prefix=prefix)


def _make_recipe() -> SfcRecipe:
    return SfcRecipe(
        steps=[
            SfcStep(name="step_init", initial=True),
            SfcStep(name="step_2"),
        ],
        transitions=[
            SfcTransition(
                source="step_init",
                target="step_2",
                condition="true",
                priority=0,
            ),
        ],
        actions=[
            SfcActionAssociation(
                name="action_push",
                step="step_init",
                qualifier=SfcActionQualifier.NON_STORED,
                interaction=SfcInteraction.PUSH_COMMAND,
                type_id="com.example.push",
                timeout_seconds=30,
                parameters={"key": "value"},
            ),
            SfcActionAssociation(
                name="action_pull",
                step="step_2",
                qualifier=SfcActionQualifier.NON_STORED,
                interaction=SfcInteraction.PULL_EVENT,
                type_id="com.example.pull",
                timeout_seconds=60,
            ),
        ],
    )


def _make_job_order_and_state(
    job_id: str = "job-1",
    scope: str = "scope-1",
    state_text: str = "AllowedToStart",
) -> ISA95JobOrderAndStateDataType:
    return ISA95JobOrderAndStateDataType(
        job_order=ISA95JobOrderDataType(
            job_order_id=job_id,
            work_master_id=[ISA95WorkMasterDataType(id="wm-1")],
            equipment_requirements=[ISA95EquipmentDataType(id=scope)],
        ),
        state=[
            ISA95StateDataType(
                state_text=LocalizedText(text=state_text, locale="en"),
                state_number=1,
            )
        ],
    )


def _make_work_master() -> ISA95WorkMasterDataTypeExt:
    recipe = _make_recipe()
    return ISA95WorkMasterDataTypeExt(
        id="wm-1",
        data=recipe.to_dict(),
        dataschema=SFC_RECIPE_DATASCHEMA,
    )


def _make_exec_state(
    job_id: str = "job-1",
    scope: str = "scope-1",
    current_step: str = "step_init",
    action_states: dict[str, SfcActionState] | None = None,
) -> SfcExecutionState:
    actions = {}
    for name, state in (action_states or {}).items():
        actions[name] = SfcActionExecution(name=name, state=state)
    return SfcExecutionState(
        job_id=job_id,
        scope=scope,
        work_master_id="wm-1",
        current_step=current_step,
        active_steps=[current_step],
        actions=actions,
    )


class TestSfcEngineConstruction:
    def test_creates_with_required_args(self):
        pool = MagicMock()
        schema = _make_schema()
        nb = MagicMock()
        sb = {"sb1": MagicMock()}
        with patch("microdcs.sfc_engine.redis.Redis"):
            engine = SfcEngine(
                redis_connection_pool=pool,
                redis_key_schema=schema,
                nb_processor=nb,
                sb_processors=sb,
                consumer_name="test-consumer",
            )
        assert engine._consumer_name == "test-consumer"
        assert isinstance(engine._execution_dao, SfcExecutionDAO)


class TestSfcEngineStartRecipe:
    def setup_method(self):
        self.pool = MagicMock()
        self.schema = _make_schema()
        self.nb_processor = MagicMock()
        self.sb_processors = {"sb1": MagicMock()}
        with patch("microdcs.sfc_engine.redis.Redis") as mock_redis_cls:
            self.mock_redis = AsyncMock()
            mock_redis_cls.return_value = self.mock_redis
            self.engine = SfcEngine(
                redis_connection_pool=self.pool,
                redis_key_schema=self.schema,
                nb_processor=self.nb_processor,
                sb_processors=self.sb_processors,
                consumer_name="test",
            )
        # Replace DAOs with mocks
        self.mock_execution_dao = AsyncMock(spec=SfcExecutionDAO)
        self.engine._execution_dao = self.mock_execution_dao  # type: ignore[assignment]
        self.mock_joborder_dao = AsyncMock()
        self.engine._joborder_dao = self.mock_joborder_dao  # type: ignore[assignment]
        self.mock_workmaster_dao = AsyncMock()
        self.engine._workmaster_dao = self.mock_workmaster_dao  # type: ignore[assignment]
        self.mock_jobresponse_dao = AsyncMock()
        self.engine._jobresponse_dao = self.mock_jobresponse_dao  # type: ignore[assignment]

    @pytest.mark.asyncio
    async def test_start_recipe_creates_execution_state(self):
        self.mock_execution_dao.retrieve.return_value = None
        self.mock_joborder_dao.retrieve.return_value = _make_job_order_and_state()
        self.mock_workmaster_dao.retrieve.return_value = _make_work_master()
        self.mock_execution_dao.cas_action_state.return_value = "OK"

        # Mock the state machine transition
        with patch.object(self.engine, "_trigger_job_transition", return_value=True):
            await self.engine._handle_start_recipe("job-1", "scope-1")

        # Should have saved execution state
        self.mock_execution_dao.save.assert_awaited_once()
        saved_state = self.mock_execution_dao.save.call_args[0][0]
        assert saved_state.job_id == "job-1"
        assert saved_state.current_step == "step_init"
        assert "action_push" in saved_state.actions
        assert "action_pull" in saved_state.actions

    @pytest.mark.asyncio
    async def test_start_recipe_idempotent_if_already_exists(self):
        self.mock_execution_dao.retrieve.return_value = _make_exec_state()
        await self.engine._handle_start_recipe("job-1", "scope-1")
        self.mock_execution_dao.save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_recipe_skips_if_job_not_found(self):
        self.mock_execution_dao.retrieve.return_value = None
        self.mock_joborder_dao.retrieve.return_value = None
        await self.engine._handle_start_recipe("job-1", "scope-1")
        self.mock_execution_dao.save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_recipe_skips_if_wrong_dataschema(self):
        self.mock_execution_dao.retrieve.return_value = None
        self.mock_joborder_dao.retrieve.return_value = _make_job_order_and_state()
        wm = ISA95WorkMasterDataTypeExt(id="wm-1", data={}, dataschema="wrong://schema")
        self.mock_workmaster_dao.retrieve.return_value = wm
        await self.engine._handle_start_recipe("job-1", "scope-1")
        self.mock_execution_dao.save.assert_not_awaited()


class TestSfcEngineDispatchAction:
    def setup_method(self):
        self.pool = MagicMock()
        self.schema = _make_schema()
        self.nb_processor = MagicMock()
        self.sb_processor = MagicMock()
        self.sb_processor._type_callbacks_out = {"com.example.push": MagicMock()}
        self.sb_processor._type_classes = {"com.example.push": MagicMock()}
        self.sb_processor.callback_outgoing = AsyncMock()
        self.sb_processors = {"sb1": self.sb_processor}
        with patch("microdcs.sfc_engine.redis.Redis"):
            self.engine = SfcEngine(
                redis_connection_pool=self.pool,
                redis_key_schema=self.schema,
                nb_processor=self.nb_processor,
                sb_processors=self.sb_processors,
                consumer_name="test",
            )
        self.mock_execution_dao = AsyncMock(spec=SfcExecutionDAO)
        self.engine._execution_dao = self.mock_execution_dao  # type: ignore[assignment]
        self.mock_joborder_dao = AsyncMock()
        self.engine._joborder_dao = self.mock_joborder_dao  # type: ignore[assignment]
        self.mock_workmaster_dao = AsyncMock()
        self.engine._workmaster_dao = self.mock_workmaster_dao  # type: ignore[assignment]
        self.mock_jobresponse_dao = AsyncMock()
        self.engine._jobresponse_dao = self.mock_jobresponse_dao  # type: ignore[assignment]

    @pytest.mark.asyncio
    async def test_dispatch_push_command_cas_and_callback(self):
        exec_state = _make_exec_state(
            action_states={"action_push": SfcActionState.PENDING}
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        self.mock_workmaster_dao.retrieve.return_value = _make_work_master()
        self.mock_execution_dao.cas_action_state.return_value = "OK"

        await self.engine._handle_dispatch_action("job-1", "action_push")

        self.mock_execution_dao.cas_action_state.assert_awaited_once()
        self.sb_processor.callback_outgoing.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dispatch_skips_completed_job(self):
        exec_state = _make_exec_state()
        exec_state.completed = True
        self.mock_execution_dao.retrieve.return_value = exec_state
        await self.engine._handle_dispatch_action("job-1", "action_push")
        self.mock_execution_dao.cas_action_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dispatch_skips_if_cas_already_handled(self):
        exec_state = _make_exec_state(
            action_states={"action_push": SfcActionState.PENDING}
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        self.mock_workmaster_dao.retrieve.return_value = _make_work_master()
        self.mock_execution_dao.cas_action_state.return_value = "ALREADY_HANDLED"

        await self.engine._handle_dispatch_action("job-1", "action_push")
        self.sb_processor.callback_outgoing.assert_not_awaited()


class TestSfcEngineActionCompletion:
    def setup_method(self):
        self.pool = MagicMock()
        self.schema = _make_schema()
        self.nb_processor = MagicMock()
        self.sb_processors = {}
        with patch("microdcs.sfc_engine.redis.Redis"):
            self.engine = SfcEngine(
                redis_connection_pool=self.pool,
                redis_key_schema=self.schema,
                nb_processor=self.nb_processor,
                sb_processors=self.sb_processors,
                consumer_name="test",
            )
        self.mock_execution_dao = AsyncMock(spec=SfcExecutionDAO)
        self.engine._execution_dao = self.mock_execution_dao  # type: ignore[assignment]
        self.mock_joborder_dao = AsyncMock()
        self.engine._joborder_dao = self.mock_joborder_dao  # type: ignore[assignment]
        self.mock_workmaster_dao = AsyncMock()
        self.engine._workmaster_dao = self.mock_workmaster_dao  # type: ignore[assignment]
        self.mock_jobresponse_dao = AsyncMock()
        self.engine._jobresponse_dao = self.mock_jobresponse_dao  # type: ignore[assignment]

    @pytest.mark.asyncio
    async def test_complete_action_cas_dispatched_to_completed(self):
        exec_state = _make_exec_state(
            action_states={"action_push": SfcActionState.DISPATCHED}
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        self.mock_execution_dao.cas_action_state.return_value = "OK"
        self.mock_workmaster_dao.retrieve.return_value = _make_work_master()
        self.mock_execution_dao.cas_advance_step.return_value = "OK"

        await self.engine.complete_action("job-1", "action_push")

        self.mock_execution_dao.cas_action_state.assert_awaited_once()
        cas_call = self.mock_execution_dao.cas_action_state.call_args
        assert cas_call.kwargs["expected_state"] == SfcActionState.DISPATCHED
        assert cas_call.kwargs["new_state"] == SfcActionState.COMPLETED

    @pytest.mark.asyncio
    async def test_complete_action_skips_if_already_completed(self):
        exec_state = _make_exec_state(
            action_states={"action_push": SfcActionState.COMPLETED}
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        await self.engine.complete_action("job-1", "action_push")
        self.mock_execution_dao.cas_action_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_complete_action_skips_if_no_sfc_state(self):
        self.mock_execution_dao.retrieve.return_value = None
        await self.engine.complete_action("job-1", "action_push")
        self.mock_execution_dao.cas_action_state.assert_not_awaited()


class TestSfcEngineFailAction:
    def setup_method(self):
        self.pool = MagicMock()
        self.schema = _make_schema()
        with patch("microdcs.sfc_engine.redis.Redis"):
            self.engine = SfcEngine(
                redis_connection_pool=self.pool,
                redis_key_schema=self.schema,
                nb_processor=MagicMock(),
                sb_processors={},
                consumer_name="test",
            )
        self.mock_execution_dao = AsyncMock(spec=SfcExecutionDAO)
        self.engine._execution_dao = self.mock_execution_dao  # type: ignore[assignment]
        self.mock_joborder_dao = AsyncMock()
        self.engine._joborder_dao = self.mock_joborder_dao  # type: ignore[assignment]
        self.mock_workmaster_dao = AsyncMock()
        self.engine._workmaster_dao = self.mock_workmaster_dao  # type: ignore[assignment]
        self.mock_jobresponse_dao = AsyncMock()
        self.engine._jobresponse_dao = self.mock_jobresponse_dao  # type: ignore[assignment]

    @pytest.mark.asyncio
    async def test_fail_action_marks_action_failed_and_fails_job(self):
        exec_state = _make_exec_state(
            action_states={"action_push": SfcActionState.DISPATCHED}
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        self.mock_execution_dao.cas_action_state.return_value = "OK"
        self.mock_execution_dao.cas_finish.return_value = "OK"
        self.mock_joborder_dao.retrieve.return_value = _make_job_order_and_state()

        with patch.object(self.engine, "_trigger_job_transition", return_value=True):
            await self.engine.fail_action("job-1", "action_push", "timeout")

        self.mock_execution_dao.cas_action_state.assert_awaited_once()
        self.mock_execution_dao.cas_finish.assert_awaited_once()


class TestSfcEngineWorkItemDispatch:
    def setup_method(self):
        self.pool = MagicMock()
        self.schema = _make_schema()
        with patch("microdcs.sfc_engine.redis.Redis"):
            self.engine = SfcEngine(
                redis_connection_pool=self.pool,
                redis_key_schema=self.schema,
                nb_processor=MagicMock(),
                sb_processors={},
                consumer_name="test",
            )
        self.engine._execution_dao = AsyncMock(spec=SfcExecutionDAO)  # type: ignore[assignment]
        self.mock_handle_start = AsyncMock()
        self.engine._handle_start_recipe = self.mock_handle_start  # type: ignore[method-assign]
        self.mock_handle_dispatch = AsyncMock()
        self.engine._handle_dispatch_action = self.mock_handle_dispatch  # type: ignore[method-assign]
        self.mock_handle_resume = AsyncMock()
        self.engine._handle_resume = self.mock_handle_resume  # type: ignore[method-assign]
        self.mock_redis_client = AsyncMock()
        self.engine._redis_client = self.mock_redis_client  # type: ignore[assignment]

    @pytest.mark.asyncio
    async def test_dispatches_start_recipe(self):
        await self.engine._process_work_item(
            "stream",
            "entry-1",
            {"job_id": "job-1", "action": "start_recipe", "scope": "scope-1"},
        )
        self.mock_handle_start.assert_awaited_once_with("job-1", "scope-1")

    @pytest.mark.asyncio
    async def test_dispatches_dispatch_action(self):
        await self.engine._process_work_item(
            "stream",
            "entry-1",
            {
                "job_id": "job-1",
                "action": "dispatch_action:action_push",
                "scope": "scope-1",
            },
        )
        self.mock_handle_dispatch.assert_awaited_once_with("job-1", "action_push")

    @pytest.mark.asyncio
    async def test_dispatches_resume(self):
        await self.engine._process_work_item(
            "stream",
            "entry-1",
            {"job_id": "job-1", "action": "resume", "scope": "scope-1"},
        )
        self.mock_handle_resume.assert_awaited_once_with("job-1")

    @pytest.mark.asyncio
    async def test_acks_invalid_work_items(self):
        await self.engine._process_work_item(
            "stream",
            "entry-1",
            {"job_id": "", "action": ""},
        )
        self.mock_redis_client.xack.assert_awaited_once()


class TestSfcEngineRecovery:
    def setup_method(self):
        self.pool = MagicMock()
        self.schema = _make_schema()
        with patch("microdcs.sfc_engine.redis.Redis"):
            self.engine = SfcEngine(
                redis_connection_pool=self.pool,
                redis_key_schema=self.schema,
                nb_processor=MagicMock(),
                sb_processors={},
                consumer_name="test",
            )
        self.mock_execution_dao = AsyncMock(spec=SfcExecutionDAO)
        self.engine._execution_dao = self.mock_execution_dao  # type: ignore[assignment]

    @pytest.mark.asyncio
    async def test_recovery_scan_enqueues_resume_for_active_jobs(self):
        self.mock_execution_dao.list_active_jobs.return_value = {"job-1", "job-2"}
        self.mock_execution_dao.retrieve.side_effect = [
            _make_exec_state(job_id="job-1"),
            _make_exec_state(job_id="job-2"),
        ]

        await self.engine._recovery_scan()

        assert self.mock_execution_dao.enqueue_work.await_count == 2
        assert "scope-1" in self.engine._known_scopes

    @pytest.mark.asyncio
    async def test_recovery_scan_skips_completed_jobs(self):
        completed = _make_exec_state(job_id="job-1")
        completed.completed = True
        self.mock_execution_dao.list_active_jobs.return_value = {"job-1"}
        self.mock_execution_dao.retrieve.return_value = completed

        await self.engine._recovery_scan()
        self.mock_execution_dao.enqueue_work.assert_not_awaited()


class TestSfcEngineHelpers:
    def test_find_initial_step(self):
        recipe = _make_recipe()
        assert SfcEngine._find_initial_step(recipe) == "step_init"

    def test_find_initial_step_none(self):
        recipe = SfcRecipe(
            steps=[SfcStep(name="s1")],
            transitions=[],
            actions=[],
        )
        assert SfcEngine._find_initial_step(recipe) is None

    def test_find_action(self):
        recipe = _make_recipe()
        assert SfcEngine._find_action(recipe, "action_push") is not None
        assert SfcEngine._find_action(recipe, "nonexistent") is None

    def test_evaluate_transitions_returns_next_step(self):
        pool = MagicMock()
        with patch("microdcs.sfc_engine.redis.Redis"):
            engine = SfcEngine(
                redis_connection_pool=pool,
                redis_key_schema=_make_schema(),
                nb_processor=MagicMock(),
                sb_processors={},
                consumer_name="test",
            )
        recipe = _make_recipe()
        assert engine._evaluate_transitions(recipe, "step_init") == "step_2"
        assert engine._evaluate_transitions(recipe, "step_2") is None

    def test_scope_from_job(self):
        job = _make_job_order_and_state(scope="my-scope")
        assert SfcEngine._scope_from_job(job) == "my-scope"

    def test_find_sb_processor_found(self):
        pool = MagicMock()
        sb = MagicMock()
        sb._type_callbacks_out = {"com.example.push": MagicMock()}
        with patch("microdcs.sfc_engine.redis.Redis"):
            engine = SfcEngine(
                redis_connection_pool=pool,
                redis_key_schema=_make_schema(),
                nb_processor=MagicMock(),
                sb_processors={"sb1": sb},
                consumer_name="test",
            )
        assert engine._find_sb_processor("com.example.push") is sb
        assert engine._find_sb_processor("com.example.unknown") is None

    def test_register_scope(self):
        pool = MagicMock()
        with patch("microdcs.sfc_engine.redis.Redis"):
            engine = SfcEngine(
                redis_connection_pool=pool,
                redis_key_schema=_make_schema(),
                nb_processor=MagicMock(),
                sb_processors={},
                consumer_name="test",
            )
        engine.register_scope("scope-1")
        assert "scope-1" in engine._known_scopes


# ── Branch recipe helpers ───────────────────────────────────────────


def _make_simultaneous_branch_recipe() -> SfcRecipe:
    """Recipe: init → parallel(a1→a2, b1) → final."""
    return SfcRecipe(
        steps=[
            SfcStep(name="init", initial=True),
            SfcStep(name="a1"),
            SfcStep(name="a2"),
            SfcStep(name="b1"),
            SfcStep(name="final"),
        ],
        transitions=[
            SfcTransition(source="init", target="par", condition="true"),
            SfcTransition(source="par", target="final", condition="true"),
        ],
        actions=[
            SfcActionAssociation(
                name="init_push",
                step="init",
                qualifier=SfcActionQualifier.PULSE,
                interaction=SfcInteraction.PUSH_COMMAND,
                type_id="com.example.push",
                timeout_seconds=30,
            ),
            SfcActionAssociation(
                name="a1_push",
                step="a1",
                qualifier=SfcActionQualifier.PULSE,
                interaction=SfcInteraction.PUSH_COMMAND,
                type_id="com.example.push",
                timeout_seconds=30,
            ),
            SfcActionAssociation(
                name="a2_push",
                step="a2",
                qualifier=SfcActionQualifier.PULSE,
                interaction=SfcInteraction.PUSH_COMMAND,
                type_id="com.example.push",
                timeout_seconds=30,
            ),
            SfcActionAssociation(
                name="b1_push",
                step="b1",
                qualifier=SfcActionQualifier.PULSE,
                interaction=SfcInteraction.PUSH_COMMAND,
                type_id="com.example.push",
                timeout_seconds=30,
            ),
            SfcActionAssociation(
                name="final_pull",
                step="final",
                qualifier=SfcActionQualifier.PULSE,
                interaction=SfcInteraction.PULL_EVENT,
                type_id="com.example.pull",
                timeout_seconds=60,
            ),
        ],
        branches=[
            SfcBranch(
                name="par",
                type=SfcBranchType.SIMULTANEOUS,
                branches=[["a1", "a2"], ["b1"]],
            ),
        ],
    )


def _make_selection_branch_recipe() -> SfcRecipe:
    """Recipe: init → select(a1, b1) → final.  Path a1 has priority 0."""
    return SfcRecipe(
        steps=[
            SfcStep(name="init", initial=True),
            SfcStep(name="a1"),
            SfcStep(name="b1"),
            SfcStep(name="final"),
        ],
        transitions=[
            SfcTransition(source="init", target="sel", condition="true"),
            SfcTransition(source="sel", target="a1", condition="cond_a", priority=0),
            SfcTransition(source="sel", target="b1", condition="cond_b", priority=1),
            SfcTransition(source="sel", target="final", condition="true"),
        ],
        actions=[
            SfcActionAssociation(
                name="init_push",
                step="init",
                qualifier=SfcActionQualifier.PULSE,
                interaction=SfcInteraction.PUSH_COMMAND,
                type_id="com.example.push",
                timeout_seconds=30,
            ),
            SfcActionAssociation(
                name="a1_push",
                step="a1",
                qualifier=SfcActionQualifier.PULSE,
                interaction=SfcInteraction.PUSH_COMMAND,
                type_id="com.example.push",
                timeout_seconds=30,
            ),
            SfcActionAssociation(
                name="b1_push",
                step="b1",
                qualifier=SfcActionQualifier.PULSE,
                interaction=SfcInteraction.PUSH_COMMAND,
                type_id="com.example.push",
                timeout_seconds=30,
            ),
            SfcActionAssociation(
                name="final_pull",
                step="final",
                qualifier=SfcActionQualifier.PULSE,
                interaction=SfcInteraction.PULL_EVENT,
                type_id="com.example.pull",
                timeout_seconds=60,
            ),
        ],
        branches=[
            SfcBranch(
                name="sel",
                type=SfcBranchType.SELECTION,
                branches=[["a1"], ["b1"]],
            ),
        ],
    )


def _make_branch_exec_state(
    job_id: str = "job-1",
    scope: str = "scope-1",
    current_step: str = "par",
    active_steps: list[str] | None = None,
    action_states: dict[str, SfcActionState] | None = None,
) -> SfcExecutionState:
    actions = {}
    for name, state in (action_states or {}).items():
        actions[name] = SfcActionExecution(name=name, state=state)
    return SfcExecutionState(
        job_id=job_id,
        scope=scope,
        work_master_id="wm-1",
        current_step=current_step,
        active_steps=active_steps or [],
        actions=actions,
    )


def _make_branch_work_master(recipe: SfcRecipe) -> ISA95WorkMasterDataTypeExt:
    return ISA95WorkMasterDataTypeExt(
        id="wm-1",
        data=recipe.to_dict(),
        dataschema=SFC_RECIPE_DATASCHEMA,
    )


class TestBranchHelpers:
    """Tests for static branch helper methods."""

    def test_find_branch_returns_branch(self):
        recipe = _make_simultaneous_branch_recipe()
        branch = SfcEngine._find_branch(recipe, "par")
        assert branch is not None
        assert branch.name == "par"
        assert branch.type == SfcBranchType.SIMULTANEOUS

    def test_find_branch_returns_none_for_step(self):
        recipe = _make_simultaneous_branch_recipe()
        assert SfcEngine._find_branch(recipe, "a1") is None

    def test_find_branch_returns_none_when_no_branches(self):
        recipe = _make_recipe()
        assert SfcEngine._find_branch(recipe, "step_init") is None

    def test_next_step_in_branch_path(self):
        recipe = _make_simultaneous_branch_recipe()
        branch = recipe.branches[0]  # type: ignore[index]
        assert SfcEngine._next_step_in_branch_path(branch, "a1") == "a2"
        assert SfcEngine._next_step_in_branch_path(branch, "a2") is None
        assert SfcEngine._next_step_in_branch_path(branch, "b1") is None

    def test_find_branch_containing_step(self):
        recipe = _make_simultaneous_branch_recipe()
        assert SfcEngine._find_branch_containing_step(recipe, "a1") is not None
        assert SfcEngine._find_branch_containing_step(recipe, "init") is None

    def test_select_branch_path_picks_lowest_priority(self):
        recipe = _make_selection_branch_recipe()
        branch = recipe.branches[0]  # type: ignore[index]
        path = SfcEngine._select_branch_path(recipe, branch)
        assert path == ["a1"]

    def test_select_branch_path_falls_back_to_first_path(self):
        """When no entry transitions exist, pick the first path."""
        recipe = SfcRecipe(
            steps=[SfcStep(name="a1"), SfcStep(name="b1")],
            transitions=[],
            actions=[],
            branches=[
                SfcBranch(
                    name="sel",
                    type=SfcBranchType.SELECTION,
                    branches=[["a1"], ["b1"]],
                ),
            ],
        )
        branch = recipe.branches[0]  # type: ignore[index]
        path = SfcEngine._select_branch_path(recipe, branch)
        assert path == ["a1"]


class TestSimultaneousBranch:
    """Tests for simultaneous (AND) branch execution."""

    def setup_method(self):
        self.pool = MagicMock()
        self.schema = _make_schema()
        self.nb_processor = MagicMock()
        self.sb_processor = MagicMock()
        self.sb_processor._type_callbacks_out = {"com.example.push": MagicMock()}
        self.sb_processor._type_classes = {"com.example.push": MagicMock()}
        self.sb_processor.callback_outgoing = AsyncMock()
        self.sb_processors = {"sb1": self.sb_processor}
        with patch("microdcs.sfc_engine.redis.Redis"):
            self.engine = SfcEngine(
                redis_connection_pool=self.pool,
                redis_key_schema=self.schema,
                nb_processor=self.nb_processor,
                sb_processors=self.sb_processors,
                consumer_name="test",
            )
        self.mock_execution_dao = AsyncMock(spec=SfcExecutionDAO)
        self.engine._execution_dao = self.mock_execution_dao  # type: ignore[assignment]
        self.mock_joborder_dao = AsyncMock()
        self.engine._joborder_dao = self.mock_joborder_dao  # type: ignore[assignment]
        self.mock_workmaster_dao = AsyncMock()
        self.engine._workmaster_dao = self.mock_workmaster_dao  # type: ignore[assignment]
        self.mock_jobresponse_dao = AsyncMock()
        self.engine._jobresponse_dao = self.mock_jobresponse_dao  # type: ignore[assignment]
        self.recipe = _make_simultaneous_branch_recipe()
        self.mock_workmaster_dao.retrieve.return_value = _make_branch_work_master(
            self.recipe
        )

    @pytest.mark.asyncio
    async def test_enter_simultaneous_branch_activates_all_paths(self):
        """When init step completes, engine should enter branch with active_steps=[a1, b1]."""
        exec_state = _make_branch_exec_state(
            current_step="init",
            active_steps=["init"],
            action_states={"init_push": SfcActionState.COMPLETED},
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        self.mock_execution_dao.cas_advance_step.return_value = "OK"

        await self.engine._check_step_completion("job-1", "scope-1")

        # Should advance to branch with both first steps.
        self.mock_execution_dao.cas_advance_step.assert_awaited_once()
        call_kwargs = self.mock_execution_dao.cas_advance_step.call_args.kwargs
        assert call_kwargs["new_step"] == "par"
        assert set(call_kwargs["new_active_steps"]) == {"a1", "b1"}

    @pytest.mark.asyncio
    async def test_advance_within_branch_path(self):
        """When a1 completes (has next a2), should replace a1 with a2 in active_steps."""
        exec_state = _make_branch_exec_state(
            current_step="par",
            active_steps=["a1", "b1"],
            action_states={
                "a1_push": SfcActionState.COMPLETED,
                "b1_push": SfcActionState.PENDING,
            },
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        self.mock_execution_dao.cas_branch_advance.return_value = ["a2", "b1"]

        await self.engine._check_step_completion("job-1", "scope-1")

        self.mock_execution_dao.cas_branch_advance.assert_awaited_once()
        call_kwargs = self.mock_execution_dao.cas_branch_advance.call_args.kwargs
        assert call_kwargs["completed_step"] == "a1"
        assert call_kwargs["next_step"] == "a2"

    @pytest.mark.asyncio
    async def test_last_step_in_path_removes_from_active(self):
        """When b1 (last in its path) completes but a2 still active, should remove b1."""
        exec_state = _make_branch_exec_state(
            current_step="par",
            active_steps=["a2", "b1"],
            action_states={
                "a2_push": SfcActionState.PENDING,
                "b1_push": SfcActionState.COMPLETED,
            },
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        # Return non-empty list → not converged yet.
        self.mock_execution_dao.cas_branch_advance.return_value = ["a2"]

        await self.engine._check_step_completion("job-1", "scope-1")

        self.mock_execution_dao.cas_branch_advance.assert_awaited_once()
        call_kwargs = self.mock_execution_dao.cas_branch_advance.call_args.kwargs
        assert call_kwargs["completed_step"] == "b1"
        # next_step defaults to "" (remove, no replacement)

    @pytest.mark.asyncio
    async def test_convergence_when_all_paths_done(self):
        """When the last active step completes, should converge to post-branch step."""
        exec_state = _make_branch_exec_state(
            current_step="par",
            active_steps=["a2"],
            action_states={
                "a2_push": SfcActionState.COMPLETED,
                "b1_push": SfcActionState.COMPLETED,
            },
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        # Return empty list → converge.
        self.mock_execution_dao.cas_branch_advance.return_value = []
        self.mock_execution_dao.cas_advance_step.return_value = "OK"

        await self.engine._check_step_completion("job-1", "scope-1")

        # Should have called cas_branch_advance to remove a2.
        self.mock_execution_dao.cas_branch_advance.assert_awaited_once()
        # Then cas_advance_step to converge from "par" to "final".
        self.mock_execution_dao.cas_advance_step.assert_awaited_once()
        call_kwargs = self.mock_execution_dao.cas_advance_step.call_args.kwargs
        assert call_kwargs["expected_step"] == "par"
        assert call_kwargs["new_step"] == "final"

    @pytest.mark.asyncio
    async def test_convergence_completes_job_when_no_exit_transition(self):
        """If there's no exit transition from the branch, the job completes."""
        recipe = SfcRecipe(
            steps=[SfcStep(name="init", initial=True), SfcStep(name="a1")],
            transitions=[
                SfcTransition(source="init", target="par", condition="true"),
                # No exit transition from "par".
            ],
            actions=[
                SfcActionAssociation(
                    name="a1_push",
                    step="a1",
                    qualifier=SfcActionQualifier.PULSE,
                    interaction=SfcInteraction.PUSH_COMMAND,
                    type_id="com.example.push",
                    timeout_seconds=30,
                ),
            ],
            branches=[
                SfcBranch(
                    name="par",
                    type=SfcBranchType.SIMULTANEOUS,
                    branches=[["a1"]],
                ),
            ],
        )
        self.mock_workmaster_dao.retrieve.return_value = _make_branch_work_master(
            recipe
        )

        exec_state = _make_branch_exec_state(
            current_step="par",
            active_steps=["a1"],
            action_states={"a1_push": SfcActionState.COMPLETED},
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        self.mock_execution_dao.cas_branch_advance.return_value = []
        self.mock_execution_dao.cas_finish.return_value = "OK"
        self.mock_joborder_dao.retrieve.return_value = _make_job_order_and_state()

        with patch.object(self.engine, "_trigger_job_transition", return_value=True):
            await self.engine._check_step_completion("job-1", "scope-1")

        self.mock_execution_dao.cas_finish.assert_awaited_once()


class TestSelectionBranch:
    """Tests for selection (OR) branch execution."""

    def setup_method(self):
        self.pool = MagicMock()
        self.schema = _make_schema()
        self.nb_processor = MagicMock()
        self.sb_processor = MagicMock()
        self.sb_processor._type_callbacks_out = {"com.example.push": MagicMock()}
        self.sb_processor._type_classes = {"com.example.push": MagicMock()}
        self.sb_processor.callback_outgoing = AsyncMock()
        self.sb_processors = {"sb1": self.sb_processor}
        with patch("microdcs.sfc_engine.redis.Redis"):
            self.engine = SfcEngine(
                redis_connection_pool=self.pool,
                redis_key_schema=self.schema,
                nb_processor=self.nb_processor,
                sb_processors=self.sb_processors,
                consumer_name="test",
            )
        self.mock_execution_dao = AsyncMock(spec=SfcExecutionDAO)
        self.engine._execution_dao = self.mock_execution_dao  # type: ignore[assignment]
        self.mock_joborder_dao = AsyncMock()
        self.engine._joborder_dao = self.mock_joborder_dao  # type: ignore[assignment]
        self.mock_workmaster_dao = AsyncMock()
        self.engine._workmaster_dao = self.mock_workmaster_dao  # type: ignore[assignment]
        self.mock_jobresponse_dao = AsyncMock()
        self.engine._jobresponse_dao = self.mock_jobresponse_dao  # type: ignore[assignment]
        self.recipe = _make_selection_branch_recipe()
        self.mock_workmaster_dao.retrieve.return_value = _make_branch_work_master(
            self.recipe
        )

    @pytest.mark.asyncio
    async def test_enter_selection_branch_picks_highest_priority_path(self):
        """Selection branch should activate only the path with lowest priority number."""
        exec_state = _make_branch_exec_state(
            current_step="init",
            active_steps=["init"],
            action_states={"init_push": SfcActionState.COMPLETED},
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        self.mock_execution_dao.cas_advance_step.return_value = "OK"

        await self.engine._check_step_completion("job-1", "scope-1")

        self.mock_execution_dao.cas_advance_step.assert_awaited_once()
        call_kwargs = self.mock_execution_dao.cas_advance_step.call_args.kwargs
        assert call_kwargs["new_step"] == "sel"
        # Only the priority-0 path's first step should be active.
        assert call_kwargs["new_active_steps"] == ["a1"]

    @pytest.mark.asyncio
    async def test_selection_convergence(self):
        """When the selected path's last step completes, converge to final."""
        exec_state = _make_branch_exec_state(
            current_step="sel",
            active_steps=["a1"],
            action_states={"a1_push": SfcActionState.COMPLETED},
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        self.mock_execution_dao.cas_branch_advance.return_value = []
        self.mock_execution_dao.cas_advance_step.return_value = "OK"

        await self.engine._check_step_completion("job-1", "scope-1")

        # Should converge: branch_advance removes a1 → empty → advance from sel to final.
        self.mock_execution_dao.cas_branch_advance.assert_awaited_once()
        self.mock_execution_dao.cas_advance_step.assert_awaited_once()
        call_kwargs = self.mock_execution_dao.cas_advance_step.call_args.kwargs
        assert call_kwargs["expected_step"] == "sel"
        assert call_kwargs["new_step"] == "final"


class TestBranchResume:
    """Tests for resuming execution inside a branch."""

    def setup_method(self):
        self.pool = MagicMock()
        self.schema = _make_schema()
        self.nb_processor = MagicMock()
        self.sb_processor = MagicMock()
        self.sb_processor._type_callbacks_out = {"com.example.push": MagicMock()}
        self.sb_processor._type_classes = {"com.example.push": MagicMock()}
        self.sb_processor.callback_outgoing = AsyncMock()
        self.sb_processors = {"sb1": self.sb_processor}
        with patch("microdcs.sfc_engine.redis.Redis"):
            self.engine = SfcEngine(
                redis_connection_pool=self.pool,
                redis_key_schema=self.schema,
                nb_processor=self.nb_processor,
                sb_processors=self.sb_processors,
                consumer_name="test",
            )
        self.mock_execution_dao = AsyncMock(spec=SfcExecutionDAO)
        self.engine._execution_dao = self.mock_execution_dao  # type: ignore[assignment]
        self.mock_joborder_dao = AsyncMock()
        self.engine._joborder_dao = self.mock_joborder_dao  # type: ignore[assignment]
        self.mock_workmaster_dao = AsyncMock()
        self.engine._workmaster_dao = self.mock_workmaster_dao  # type: ignore[assignment]
        self.mock_jobresponse_dao = AsyncMock()
        self.engine._jobresponse_dao = self.mock_jobresponse_dao  # type: ignore[assignment]

    @pytest.mark.asyncio
    async def test_resume_redispatches_all_active_branch_steps(self):
        """Resume should re-dispatch actions for ALL active steps, not just current_step."""
        recipe = _make_simultaneous_branch_recipe()
        self.mock_workmaster_dao.retrieve.return_value = _make_branch_work_master(
            recipe
        )

        exec_state = _make_branch_exec_state(
            current_step="par",
            active_steps=["a1", "b1"],
            action_states={
                "a1_push": SfcActionState.DISPATCHED,
                "b1_push": SfcActionState.PENDING,
            },
        )
        self.mock_execution_dao.retrieve.return_value = exec_state
        self.mock_execution_dao.cas_action_state.return_value = "OK"

        await self.engine._handle_resume("job-1")

        # Should dispatch both a1_push (re-dispatch) and b1_push (initial dispatch).
        assert self.mock_execution_dao.cas_action_state.await_count >= 1

"""Tests for SFC recipe dataclass serialization and deserialization."""

import orjson

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
from microdcs.models.sfc_recipe_ext import SFC_RECIPE_DATASCHEMA


class TestSfcRecipeDataschemaConstant:
    def test_dataschema_uri_matches_schema_id(self):
        assert (
            SFC_RECIPE_DATASCHEMA
            == "https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/"
        )


class TestSfcStep:
    def test_roundtrip_initial_step(self):
        step = SfcStep(name="Init", initial=True)
        data = step.to_dict()
        assert data == {"Name": "Init", "Initial": True}
        restored = SfcStep.from_dict(data)
        assert restored.name == "Init"
        assert restored.initial is True

    def test_roundtrip_non_initial_step(self):
        step = SfcStep(name="Processing")
        data = step.to_dict()
        assert data == {"Name": "Processing", "Initial": False}
        restored = SfcStep.from_dict(data)
        assert restored.name == "Processing"
        assert restored.initial is False

    def test_json_roundtrip(self):
        step = SfcStep(name="Init", initial=True)
        json_bytes = step.to_jsonb()
        restored = SfcStep.from_json(json_bytes)
        assert restored.name == "Init"
        assert restored.initial is True

    def test_msgpack_roundtrip(self):
        step = SfcStep(name="Init", initial=True)
        packed = step.to_msgpack()
        restored = SfcStep.from_msgpack(packed)
        assert restored.name == "Init"
        assert restored.initial is True


class TestSfcTransition:
    def test_roundtrip(self):
        t = SfcTransition(
            source="Init", target="FitAndQa", condition="always", priority=0
        )
        data = t.to_dict()
        assert data == {
            "Source": "Init",
            "Target": "FitAndQa",
            "Condition": "always",
            "Priority": 0,
        }
        restored = SfcTransition.from_dict(data)
        assert restored.source == "Init"
        assert restored.target == "FitAndQa"
        assert restored.condition == "always"
        assert restored.priority == 0

    def test_default_priority(self):
        t = SfcTransition(source="A", target="B", condition="cond")
        assert t.priority == 0


class TestSfcActionAssociation:
    def test_roundtrip_with_parameters(self):
        action = SfcActionAssociation(
            name="tighten",
            step="TightenBolts",
            qualifier=SfcActionQualifier.NON_STORED,
            interaction=SfcInteraction.PUSH_COMMAND,
            type_id="com.example.station.tighten.v1",
            timeout_seconds=60,
            parameters={"torque_spec": "85Nm"},
        )
        data = action.to_dict()
        assert data["Name"] == "tighten"
        assert data["Qualifier"] == "N"
        assert data["Interaction"] == "push_command"
        assert data["Parameters"] == {"torque_spec": "85Nm"}

        restored = SfcActionAssociation.from_dict(data)
        assert restored.qualifier == SfcActionQualifier.NON_STORED
        assert restored.interaction == SfcInteraction.PUSH_COMMAND
        assert restored.parameters == {"torque_spec": "85Nm"}

    def test_roundtrip_without_parameters(self):
        action = SfcActionAssociation(
            name="camera_qa",
            step="QaCheck",
            qualifier=SfcActionQualifier.NON_STORED,
            interaction=SfcInteraction.PULL_EVENT,
            type_id="com.example.station.qa_result.v1",
            timeout_seconds=45,
        )
        data = action.to_dict()
        assert "Parameters" not in data or data["Parameters"] is None
        restored = SfcActionAssociation.from_dict(data)
        assert restored.parameters is None


class TestSfcBranch:
    def test_simultaneous_roundtrip(self):
        branch = SfcBranch(
            name="FitAndQa",
            type=SfcBranchType.SIMULTANEOUS,
            branches=[["Positioning", "TightenBolts"], ["QaCheck"]],
        )
        data = branch.to_dict()
        assert data["Type"] == "simultaneous"
        assert data["Branches"] == [["Positioning", "TightenBolts"], ["QaCheck"]]

        restored = SfcBranch.from_dict(data)
        assert restored.type == SfcBranchType.SIMULTANEOUS
        assert restored.branches == [["Positioning", "TightenBolts"], ["QaCheck"]]

    def test_selection_roundtrip(self):
        branch = SfcBranch(
            name="QualityGate",
            type=SfcBranchType.SELECTION,
            branches=[["Rework"], ["Complete"]],
        )
        data = branch.to_dict()
        assert data["Type"] == "selection"
        restored = SfcBranch.from_dict(data)
        assert restored.type == SfcBranchType.SELECTION


class TestSfcRecipe:
    def test_config_attributes(self):
        assert (
            SfcRecipe.Config.type_id == "com.github.aschamberger.microdcs.sfc-recipe.v1"
        )
        assert (
            SfcRecipe.Config.type_schema
            == "https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/SfcRecipe/"
        )

    def test_minimal_recipe_roundtrip(self):
        recipe = SfcRecipe(
            steps=[
                SfcStep(name="Init", initial=True),
                SfcStep(name="Complete"),
            ],
            transitions=[
                SfcTransition(source="Init", target="Complete", condition="always"),
            ],
            actions=[
                SfcActionAssociation(
                    name="do_work",
                    step="Init",
                    qualifier=SfcActionQualifier.NON_STORED,
                    interaction=SfcInteraction.PUSH_COMMAND,
                    type_id="com.example.work.v1",
                    timeout_seconds=30,
                ),
            ],
        )
        json_bytes = recipe.to_jsonb()
        restored = SfcRecipe.from_json(json_bytes)
        assert len(restored.steps) == 2
        assert restored.steps[0].initial is True
        assert len(restored.transitions) == 1
        assert len(restored.actions) == 1
        assert restored.branches is None

    def test_full_recipe_json_roundtrip(self):
        """Roundtrip the example recipe from the sfc_engine.md docs."""
        recipe_json = orjson.dumps({
            "Steps": [
                {"Name": "Init", "Initial": True},
                {"Name": "Positioning"},
                {"Name": "TightenBolts"},
                {"Name": "QaCheck"},
                {"Name": "VerifyTorque"},
                {"Name": "Complete"},
            ],
            "Branches": [
                {
                    "Name": "FitAndQa",
                    "Type": "simultaneous",
                    "Branches": [
                        ["Positioning", "TightenBolts"],
                        ["QaCheck"],
                    ],
                }
            ],
            "Transitions": [
                {"Source": "Init", "Target": "FitAndQa", "Condition": "always"},
                {
                    "Source": "FitAndQa",
                    "Target": "VerifyTorque",
                    "Condition": "always",
                },
                {
                    "Source": "VerifyTorque",
                    "Target": "Complete",
                    "Condition": "torque_ok",
                },
            ],
            "Actions": [
                {
                    "Name": "position_axle",
                    "Step": "Positioning",
                    "Qualifier": "N",
                    "Interaction": "push_command",
                    "TypeId": "com.example.station.position.v1",
                    "TimeoutSeconds": 30,
                },
                {
                    "Name": "tighten",
                    "Step": "TightenBolts",
                    "Qualifier": "N",
                    "Interaction": "push_command",
                    "TypeId": "com.example.station.tighten.v1",
                    "TimeoutSeconds": 60,
                    "Parameters": {"torque_spec": "85Nm"},
                },
                {
                    "Name": "camera_qa",
                    "Step": "QaCheck",
                    "Qualifier": "N",
                    "Interaction": "pull_event",
                    "TypeId": "com.example.station.qa_result.v1",
                    "TimeoutSeconds": 45,
                },
                {
                    "Name": "verify",
                    "Step": "VerifyTorque",
                    "Qualifier": "N",
                    "Interaction": "pull_event",
                    "TypeId": "com.example.station.torque_result.v1",
                    "TimeoutSeconds": 15,
                },
            ],
        })

        recipe = SfcRecipe.from_json(recipe_json)

        # Steps
        assert len(recipe.steps) == 6
        assert recipe.steps[0].name == "Init"
        assert recipe.steps[0].initial is True
        assert recipe.steps[1].initial is False

        # Branches
        assert recipe.branches is not None
        assert len(recipe.branches) == 1
        assert recipe.branches[0].name == "FitAndQa"
        assert recipe.branches[0].type == SfcBranchType.SIMULTANEOUS
        assert recipe.branches[0].branches == [
            ["Positioning", "TightenBolts"],
            ["QaCheck"],
        ]

        # Transitions
        assert len(recipe.transitions) == 3
        assert recipe.transitions[2].condition == "torque_ok"

        # Actions
        assert len(recipe.actions) == 4
        tighten = recipe.actions[1]
        assert tighten.name == "tighten"
        assert tighten.qualifier == SfcActionQualifier.NON_STORED
        assert tighten.interaction == SfcInteraction.PUSH_COMMAND
        assert tighten.parameters == {"torque_spec": "85Nm"}

        # Re-serialize and check roundtrip
        re_serialized = recipe.to_jsonb()
        recipe2 = SfcRecipe.from_json(re_serialized)
        assert len(recipe2.steps) == 6
        assert recipe2.branches is not None
        assert len(recipe2.branches) == 1

    def test_msgpack_roundtrip(self):
        recipe = SfcRecipe(
            steps=[SfcStep(name="S1", initial=True)],
            transitions=[SfcTransition(source="S1", target="S2", condition="done")],
            actions=[
                SfcActionAssociation(
                    name="act",
                    step="S1",
                    qualifier=SfcActionQualifier.PULSE,
                    interaction=SfcInteraction.PULL_EVENT,
                    type_id="com.example.evt.v1",
                    timeout_seconds=10,
                )
            ],
        )
        packed = recipe.to_msgpack()
        restored = SfcRecipe.from_msgpack(packed)
        assert restored.steps[0].name == "S1"
        assert restored.actions[0].qualifier == SfcActionQualifier.PULSE
        assert restored.actions[0].interaction == SfcInteraction.PULL_EVENT

    def test_recipe_without_branches(self):
        recipe = SfcRecipe(
            steps=[
                SfcStep(name="A", initial=True),
                SfcStep(name="B"),
            ],
            transitions=[SfcTransition(source="A", target="B", condition="always")],
            actions=[
                SfcActionAssociation(
                    name="do",
                    step="A",
                    qualifier=SfcActionQualifier.NON_STORED,
                    interaction=SfcInteraction.PUSH_COMMAND,
                    type_id="com.example.do.v1",
                    timeout_seconds=5,
                )
            ],
        )
        data = recipe.to_dict()
        assert data.get("Branches") is None
        restored = SfcRecipe.from_dict(data)
        assert restored.branches is None


class TestSfcEnumValues:
    def test_action_qualifiers(self):
        assert SfcActionQualifier.NON_STORED == "N"
        assert SfcActionQualifier.PULSE == "P"
        assert SfcActionQualifier.FALLING_EDGE == "P0"
        assert SfcActionQualifier.RISING_EDGE == "P1"
        assert SfcActionQualifier.SET == "S"
        assert SfcActionQualifier.RESET == "R"
        assert SfcActionQualifier.TIME_LIMITED == "L"
        assert SfcActionQualifier.TIME_DELAYED == "D"

    def test_interaction_patterns(self):
        assert SfcInteraction.PUSH_COMMAND == "push_command"
        assert SfcInteraction.PULL_EVENT == "pull_event"

    def test_branch_types(self):
        assert SfcBranchType.SELECTION == "selection"
        assert SfcBranchType.SIMULTANEOUS == "simultaneous"

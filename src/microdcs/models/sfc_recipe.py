# Auto-generated from "sfc_recipe.schema.json". Do not modify!
from microdcs.dataclass import DataClassConfig, DataClassMixin
from enum import StrEnum
from dataclasses import dataclass
from typing import Any


class SfcActionQualifier(StrEnum):
    """
    IEC 61131-3 action qualifier controlling when and how an action executes relative to its step.
    """

    NON_STORED = "N"
    PULSE = "P"
    FALLING_EDGE = "P0"
    RISING_EDGE = "P1"
    SET = "S"
    RESET = "R"
    TIME_LIMITED = "L"
    TIME_DELAYED = "D"


class SfcInteraction(StrEnum):
    """
    Equipment interaction pattern: push_command sends an outgoing command, pull_event waits for an incoming event.
    """

    PUSH_COMMAND = "push_command"
    PULL_EVENT = "pull_event"


class SfcBranchType(StrEnum):
    """
    Branch type: selection (OR — one path taken) or simultaneous (AND — all paths execute in parallel).
    """

    SELECTION = "selection"
    SIMULTANEOUS = "simultaneous"


@dataclass(kw_only=True)
class SfcStep(DataClassMixin):
    """
    A named step in the SFC sequence. Steps are the states of the SFC state machine.
    """

    name: str
    """
    Unique step identifier.
    """
    initial: bool | None = False
    """
    Whether this is the initial step (exactly one per recipe).
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "name": "Name",
            "initial": "Initial",
        }


@dataclass(kw_only=True)
class SfcTransition(DataClassMixin):
    """
    A directed edge between steps (or branch constructs). Transitions carry conditions that determine when execution advances.
    """

    source: str
    """
    Source step or branch name.
    """
    target: str
    """
    Target step or branch name.
    """
    condition: str
    """
    Condition identifier — resolved at runtime.
    """
    priority: int | None = 0
    """
    Priority for selection divergence (lower = higher priority).
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "source": "Source",
            "target": "Target",
            "condition": "Condition",
            "priority": "Priority",
        }


@dataclass(kw_only=True)
class SfcActionAssociation(DataClassMixin):
    """
    Associates an action with a step, including the interaction pattern and IEC 61131-3 action qualifier.
    """

    name: str
    """
    Action identifier.
    """
    step: str
    """
    Step name this action is associated with.
    """
    qualifier: SfcActionQualifier
    interaction: SfcInteraction
    type_id: str
    """
    Type identifier for the outgoing command or expected incoming event.
    """
    timeout_seconds: int
    """
    Maximum wait time in seconds before expiration handling.
    """
    parameters: dict[str, Any] | None = None
    """
    Step-specific parameters passed to the action.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "name": "Name",
            "step": "Step",
            "qualifier": "Qualifier",
            "interaction": "Interaction",
            "type_id": "TypeId",
            "timeout_seconds": "TimeoutSeconds",
            "parameters": "Parameters",
        }


@dataclass(kw_only=True)
class SfcBranch(DataClassMixin):
    """
    Branching construct: selection (OR) or simultaneous (AND) divergence.
    """

    name: str
    """
    Branch identifier (used as source/target in transitions).
    """
    type: SfcBranchType
    branches: list[list[str]]
    """
    Each inner list is a sequence of step names forming one branch path.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "name": "Name",
            "type": "Type",
            "branches": "Branches",
        }


@dataclass(kw_only=True)
class SfcRecipe(DataClassMixin):
    """
    An IEC 61131-3 SFC recipe definition. Contains steps, transitions, action associations, and optional branching constructs.
    """

    steps: list[SfcStep]
    """
    All steps in the recipe.
    """
    transitions: list[SfcTransition]
    """
    All transitions between steps and/or branch constructs.
    """
    actions: list[SfcActionAssociation]
    """
    All action associations (step-to-action bindings).
    """
    branches: list[SfcBranch] | None = None
    """
    All branching constructs (selection or simultaneous).
    """

    # mashumaro config class
    class Config(DataClassConfig):
        type_id: str = "com.github.aschamberger.microdcs.sfc-recipe.v1"
        type_schema: str = "https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/SfcRecipe/"
        aliases: dict[str, str] = {
            "steps": "Steps",
            "transitions": "Transitions",
            "actions": "Actions",
            "branches": "Branches",
        }

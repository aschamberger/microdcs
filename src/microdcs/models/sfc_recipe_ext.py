from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

SFC_RECIPE_DATASCHEMA = (
    "https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/"
)
"""URI identifying the SFC recipe schema. Used as the ``dataschema`` value on
``ISA95WorkMasterDataTypeExt`` to indicate that the Work Master's ``data``
payload is an SFC recipe. The SFC engine dispatches on this URI to select
the recipe interpreter."""

SFC_CONSUMER_GROUP = "sfc-engine"
"""Redis consumer group name for the SFC work stream."""


class SfcActionState(StrEnum):
    """State of an individual SFC action within a running recipe."""

    PENDING = "pending"
    DISPATCHED = "dispatched"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"


class SfcWorkAction(StrEnum):
    """Work item action types for the SFC work stream."""

    START_RECIPE = "start_recipe"
    DISPATCH_ACTION = "dispatch_action"
    RESUME = "resume"


@dataclass(kw_only=True)
class SfcActionExecution:
    """Execution state of a single action within a running recipe."""

    name: str
    state: SfcActionState = SfcActionState.PENDING
    correlation_id: str | None = None
    attempt: int = 0
    result: dict[str, Any] | None = None
    error: str | None = None


@dataclass(kw_only=True)
class SfcExecutionState:
    """Persisted execution state of an SFC recipe for a single job.

    Stored in Redis as a JSON document at ``sfc:execution:{job_id}``.
    """

    job_id: str
    scope: str
    work_master_id: str
    current_step: str
    active_steps: list[str] = field(default_factory=list)
    actions: dict[str, SfcActionExecution] = field(default_factory=dict)
    completed: bool = False
    failed: bool = False
    error: str | None = None

import typing

import pytest
import transitions
from transitions.extensions import HierarchicalGraphMachine

from microdcs.models.machinery_jobs_ext import JobOrderControlExt


class Job:
    _state: str = "InitialState"

    def trigger(self, trigger_name: str) -> bool:
        raise RuntimeError("Should be overridden!")

    def may_trigger(self, trigger_name: str) -> bool:
        raise RuntimeError("Should be overridden!")


@pytest.fixture
def machine():
    """Provides a fresh state machine for each test."""

    machine = HierarchicalGraphMachine(
        model=None,
        states=JobOrderControlExt.Config.opcua_state_machine_states,
        transitions=JobOrderControlExt.Config.opcua_state_machine_transitions,
        initial="InitialState",
        auto_transitions=False,
        queued=True,
        model_attribute="_state",
        model_override=True,
    )

    job = Job()
    machine.add_model(job)

    return machine


def test_graph(machine: HierarchicalGraphMachine):
    graph: str = str(machine.models[0].get_graph().draw(None))
    md = "# JobOrderControl State Machine\n\n"
    md += "```mermaid\n"
    md += graph.replace(
        "classDef s_default fill:white,color:black",
        "classDef s_default fill:black,color:white",
    )
    md += "\n```\n"
    # with open("tests/state_machine.md", "w") as f:
    #     f.write(md)
    assert graph is not None
    assert md == open("tests/state_machine.md").read()


def test_state_machine_states(machine: HierarchicalGraphMachine):
    states = machine.get_nested_state_names()
    assert "InitialState" in states
    assert "EndState" in states
    assert "Running" in states
    assert "Interrupted" in states
    assert "Aborted" in states
    assert "Ended" in states
    assert "AllowedToStart" in states
    assert "NotAllowedToStart" in states
    assert "Interrupted_Held" in states
    assert "Interrupted_Suspended" in states
    assert "Ended_Completed" in states
    assert "Ended_Closed" in states
    assert "AllowedToStart_Waiting" in states
    assert "AllowedToStart_Ready" in states
    assert "AllowedToStart_Loaded" in states
    assert "NotAllowedToStart_Waiting" in states
    assert "NotAllowedToStart_Ready" in states
    assert "NotAllowedToStart_Loaded" in states


def test_state_machine_triggers(machine: HierarchicalGraphMachine):
    triggers = machine.get_nested_triggers()
    print(triggers)
    assert "Store" in triggers
    assert "StoreAndStart" in triggers
    assert "Cancel" in triggers
    assert "Clear" in triggers
    assert "Run" in triggers
    assert "Abort" in triggers
    assert "Stop" in triggers
    assert "Resume" in triggers
    assert "Pause" in triggers
    assert "RevokeStart" in triggers
    assert "Update" in triggers
    assert "AllowedToStartFromLoadedToReady" in triggers
    assert "AllowedToStartFromLoadedToWaiting" in triggers
    assert "AllowedToStartFromReadyToLoaded" in triggers
    assert "AllowedToStartFromReadyToWaiting" in triggers
    assert "AllowedToStartFromWaitingToReady" in triggers
    assert "EndedFromCompletedToClosed" in triggers
    assert "InterruptedFromHeldToSuspended" in triggers
    assert "InterruptedFromSuspendedToHeld" in triggers
    assert "NotAllowedToStartFromLoadedToReady" in triggers
    assert "NotAllowedToStartFromLoadedToWaiting" in triggers
    assert "NotAllowedToStartFromReadyToLoaded" in triggers
    assert "NotAllowedToStartFromReadyToWaiting" in triggers
    assert "NotAllowedToStartFromWaitingToReady" in triggers


def test_state_machine_trigger_store(machine: HierarchicalGraphMachine):
    model: Job = typing.cast(Job, machine.models[0])
    assert model._state == "InitialState"
    model.trigger("Store")
    assert model._state == "NotAllowedToStart_Ready"
    assert model.may_trigger("Store") is False
    with pytest.raises(transitions.core.MachineError):
        model.trigger("Store")
    assert model._state == "NotAllowedToStart_Ready"


def test_state_name_to_tuple(machine: HierarchicalGraphMachine):
    state_tuples = JobOrderControlExt.Config.get_tuples_from_state_name(
        "NotAllowedToStart_Ready"
    )
    assert len(state_tuples) == 2
    assert state_tuples == [("NotAllowedToStart", "1"), ("Ready", "2")]


def test_tuple_to_state_name(machine: HierarchicalGraphMachine):
    state_name = JobOrderControlExt.Config.get_state_name_from_tuples(
        [("NotAllowedToStart", "1"), ("Ready", "2")]
    )
    assert state_name == "NotAllowedToStart_Ready"

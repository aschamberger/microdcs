from enum import IntEnum

from machinery_jobs import JobOrderControl


class MethodReturnStatus(IntEnum):
    """The job order methods’ return status codes are defined as a bit map in a
    UInt64 variable, allowing for multiple codes to be defined at the same time.
    The codes and bitmap positions are defined in Table 95. In the OPC format,
    the bit positions start at bit 0 for the least significant digit.

    Bit Position | Description | Notes
    0 | No Error | If bit 0 set (UInt64 value = 1) then there are no errors. This is returned on all successful method calls.
    1 | Unknown Job Order ID | The Job Order ID is unknown by the Information Receiver/Provider
    3 | Invalid Job Order Status | The Job Order status is unknown.
    4 | Unable to accept Job Order | The Job Order cannot be accepted
    5-31 | Reserved | Reserved for future use or specific implementations.
    32 | Invalid request | The request is invalid due to an unspecified reason.
    33-63 | Implementation-specific | These values are reserved for use in specific implementations and should be defined in the implementation specification.

    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/B.2
    """

    NO_ERROR = 1 << 0
    UNKNOWN_JOB_ORDER_ID = 1 << 1
    INVALID_JOB_ORDER_STATUS = 1 << 3
    UNABLE_TO_ACCEPT_JOB_ORDER = 1 << 4
    INVALID_REQUEST = 1 << 32


def static_init_config(cls):
    cls.init()
    return cls


class JobOrderControlExt(JobOrderControl):
    pass

    @static_init_config
    class Config(JobOrderControl.Config):
        opcua_state_machine_state_ids_reverse: dict[str, str] = {}

        @classmethod
        def init(cls):
            # add the not represented states
            cls.opcua_state_machine_states.extend(
                [
                    {"name": "InitialState"},
                    {"name": "EndState", "final": True},
                ]
            )
            # add state ids for new states
            cls.opcua_state_machine_state_ids["InitialState"] = "0"
            cls.opcua_state_machine_state_ids["EndState"] = "7"
            # add reversed state ids
            cls.opcua_state_machine_state_ids_reverse = {
                v: k for k, v in cls.opcua_state_machine_state_ids.items()
            }
            # add initial states for substates
            for state in cls.opcua_state_machine_states:
                if "children" in state:
                    if state["name"] in ["AllowedToStart", "NotAllowedToStart"]:
                        state["initial"] = "Ready"
                    elif state["name"] == "Interrupted":
                        state["initial"] = "Suspended"
                    elif state["name"] == "Ended":
                        state["initial"] = "Completed"
            # fix proper cause for FromAllowedToStartToRunning transition = Run
            for t in cls.opcua_state_machine_transitions:
                if t["trigger"] == "FromAllowedToStartToRunning":
                    t["trigger"] = "Run"
                    break
            # add the not represented transitions
            cls.opcua_state_machine_transitions.extend(
                [
                    {
                        "trigger": "Store",
                        "source": "InitialState",
                        "dest": "NotAllowedToStart",
                    },
                    {
                        "trigger": "StoreAndStart",
                        "source": "InitialState",
                        "dest": "AllowedToStart",
                    },
                    {
                        "trigger": "Cancel",
                        "source": "NotAllowedToStart",
                        "dest": "EndState",
                    },
                    {
                        "trigger": "Cancel",
                        "source": "AllowedToStart",
                        "dest": "EndState",
                    },
                    {"trigger": "Clear", "source": "Aborted", "dest": "EndState"},
                    {"trigger": "Clear", "source": "Ended", "dest": "EndState"},
                ]
            )
            # add effects for additional transitions
            cls.opcua_state_machine_effects["Store"] = "ISA95JobOrderStatusEventType"
            cls.opcua_state_machine_effects["StoreAndStart"] = (
                "ISA95JobOrderStatusEventType"
            )
            cls.opcua_state_machine_effects["Cancel"] = "ISA95JobOrderStatusEventType"
            cls.opcua_state_machine_effects["Clear"] = "ISA95JobOrderStatusEventType"

        @classmethod
        def get_tuples_from_state_name(cls, state_name: str) -> list[tuple[str, str]]:
            """Get the state tuple(s) for a given state name. This is needed to map the state name to the state id(s) for the OPC UA format."""
            state_tuples = []
            if state_name in cls.opcua_state_machine_state_ids:
                state_names = state_name.split("_")
                state_ids = cls.opcua_state_machine_state_ids[state_name].split("_")
                for name, id in zip(state_names, state_ids):
                    state_tuples.append((name, id))
            return state_tuples

        @classmethod
        def get_state_name_from_tuples(cls, state_tuples: list[tuple[str, str]]) -> str:
            """Get the state name for a given state tuple. This is needed to map the state id(s) to the state name for the OPC UA format."""
            state_name = "_".join([t[0] for t in state_tuples])
            return state_name

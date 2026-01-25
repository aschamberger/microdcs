from enum import IntEnum


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

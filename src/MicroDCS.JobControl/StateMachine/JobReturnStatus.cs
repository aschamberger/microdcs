namespace MicroDCS.JobControl.StateMachine;

/// <summary>The <c>ReturnStatus</c> bitmap of the Job Control methods (OPC 10031-4, Annex B.2). Several bits can be set.</summary>
[Flags]
#pragma warning disable S4070 // Annex B.2 assigns these values; 0 is not a defined status but "no bits yet".
public enum JobReturnStatus : ulong
#pragma warning restore S4070
{
    /// <summary>No bits set. Never returned by itself.</summary>
    None = 0,

    /// <summary>Bit 0: no error.</summary>
    Success = 1UL << 0,

    /// <summary>Bit 1: the job order id is unknown.</summary>
    UnknownJobOrderId = 1UL << 1,

    /// <summary>Bit 2: the command is not valid (V1).</summary>
    InvalidCommand = 1UL << 2,

    /// <summary>Bit 3: the state of the order does not permit the operation.</summary>
    InvalidStatus = 1UL << 3,

    /// <summary>Bit 4: the order cannot be accepted.</summary>
    UnableToAccept = 1UL << 4,

    /// <summary>Bit 32: the request is invalid.</summary>
    InvalidRequest = 1UL << 32,
}

namespace MicroDCS.JobControl.StateMachine;

/// <summary>
/// The version-neutral state of a job order: the Job Control V2 receiver states with their sub-states, the two response states
/// only V1 has, and the two pseudo states before and after the life of an order (D-34).
/// </summary>
public enum JobOrderState
{
    /// <summary>No such order yet. Only Store and StoreAndStart apply.</summary>
    Initial = 0,

    /// <summary>Stored, not allowed to start; pre-conditions not met.</summary>
    NotAllowedToStartWaiting = 1,

    /// <summary>Stored, not allowed to start; ready to run.</summary>
    NotAllowedToStartReady = 2,

    /// <summary>Stored, not allowed to start; loaded in active memory.</summary>
    NotAllowedToStartLoaded = 3,

    /// <summary>Allowed to start; pre-conditions not met.</summary>
    AllowedToStartWaiting = 4,

    /// <summary>Allowed to start; ready to run.</summary>
    AllowedToStartReady = 5,

    /// <summary>Allowed to start; loaded in active memory.</summary>
    AllowedToStartLoaded = 6,

    /// <summary>Executing.</summary>
    Running = 7,

    /// <summary>Interrupted by a constraint.</summary>
    Held = 8,

    /// <summary>Interrupted by a deliberate decision (Pause).</summary>
    Suspended = 9,

    /// <summary>Ended, no longer in execution.</summary>
    Completed = 10,

    /// <summary>Ended, post-processing finished.</summary>
    Closed = 11,

    /// <summary>Aborted.</summary>
    Aborted = 12,

    /// <summary>The V1 response state <c>Loaded</c>, which has no V2 equivalent; a V2 view shows Running.</summary>
    V1Loaded = 13,

    /// <summary>The V1 response state <c>Error</c>, which has no V2 equivalent; a V2 view shows Aborted.</summary>
    V1Error = 14,

    /// <summary>The order was removed (Cancel, Clear, V1 Stop). Nothing applies any more.</summary>
    Removed = 15,
}

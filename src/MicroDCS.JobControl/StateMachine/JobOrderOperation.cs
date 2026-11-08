namespace MicroDCS.JobControl.StateMachine;

/// <summary>Everything that can change the state of a job order (D-34).</summary>
public enum JobOrderOperation
{
    /// <summary>Stores a new order.</summary>
    Store,

    /// <summary>Stores a new order and allows it to start.</summary>
    StoreAndStart,

    /// <summary>Allows a stored order to start.</summary>
    Start,

    /// <summary>Replaces the content of a stored order. Keeps the state.</summary>
    Update,

    /// <summary>Ends a started order. In V1 this removes the order.</summary>
    Stop,

    /// <summary>Interrupts a running order; it lands in Suspended.</summary>
    Pause,

    /// <summary>Continues an interrupted order.</summary>
    Resume,

    /// <summary>Aborts an order that has not ended.</summary>
    Abort,

    /// <summary>Takes the permission to start back.</summary>
    RevokeStart,

    /// <summary>Removes an order that has not started.</summary>
    Cancel,

    /// <summary>Removes an order that has ended or was aborted.</summary>
    Clear,

    /// <summary>Execution system: the allowed order begins to run.</summary>
    BeginExecution,

    /// <summary>Execution system: a running order is held by a constraint.</summary>
    Hold,

    /// <summary>Execution system: execution has finished.</summary>
    Complete,

    /// <summary>Execution system: post-processing of a completed order has finished.</summary>
    Close,

    /// <summary>Prepare sub-state machine: to Waiting.</summary>
    PrepareToWaiting,

    /// <summary>Prepare sub-state machine: to Ready.</summary>
    PrepareToReady,

    /// <summary>Prepare sub-state machine: to Loaded.</summary>
    PrepareToLoaded,

    /// <summary>Interrupted sub-state machine: to Held.</summary>
    InterruptToHeld,

    /// <summary>Interrupted sub-state machine: to Suspended.</summary>
    InterruptToSuspended,
}

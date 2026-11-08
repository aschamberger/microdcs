namespace MicroDCS.JobControl.StateMachine;

/// <summary>What an accepted operation emits (D-34).</summary>
public enum JobOrderEffect
{
    /// <summary>Nothing: the operation was rejected or the transition has no effect in the NodeSet.</summary>
    None,

    /// <summary>An <c>ISA95JobOrderStatusEventType</c> event.</summary>
    StatusEvent,

    /// <summary>A catalog change: the order is gone. No status event.</summary>
    CatalogRemoved,
}

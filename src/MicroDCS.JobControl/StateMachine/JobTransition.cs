namespace MicroDCS.JobControl.StateMachine;

/// <summary>The result of applying an operation to a job order. A rejection is a result, not an exception (D-34).</summary>
/// <param name="Status">The return status bitmap.</param>
/// <param name="State">The state after the operation; unchanged when rejected.</param>
/// <param name="Effect">What the operation emits.</param>
public readonly record struct JobTransition(JobReturnStatus Status, JobOrderState State, JobOrderEffect Effect)
{
    /// <summary>Gets a value indicating whether the operation was accepted.</summary>
    public bool Accepted => Status == JobReturnStatus.Success;
}

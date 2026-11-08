namespace MicroDCS.JobControl.StateMachine;

/// <summary>One transition of the Job Control V2 NodeSet2, a row of the generated table (D-34).</summary>
/// <param name="Machine">The browse name of the state machine type.</param>
/// <param name="Name">The browse name of the transition.</param>
/// <param name="Number">The transition number.</param>
/// <param name="From">The source state.</param>
/// <param name="To">The target state.</param>
/// <param name="Cause">The method that causes the transition, if any.</param>
/// <param name="Effect">The event type the transition raises, if any.</param>
internal sealed record NodeSetTransition(string Machine, string Name, uint Number, string From, string To, string? Cause, string? Effect);

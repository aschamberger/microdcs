using MicroDCS.JobControl.StateMachine;
using static MicroDCS.JobControl.StateMachine.JobOrderOperation;
using static MicroDCS.JobControl.StateMachine.JobOrderState;

namespace MicroDCS.JobControl.Tests;

/// <summary>V2 rules. The scenarios are the Python <c>statemachine_test.py</c> and the processor tests on <c>python-archive</c>, re-specified for the pure function.</summary>
public sealed class JobStateMachineV2Tests
{
    private static JobTransition Apply(JobOrderState state, JobOrderOperation operation) => JobStateMachine.Apply(JobControlVersion.V2, state, operation);

    private static JobOrderState Walk(JobOrderState state, params JobOrderOperation[] operations)
    {
        foreach (var operation in operations)
        {
            var result = Apply(state, operation);
            result.Accepted.ShouldBeTrue($"{operation} from {state}");
            state = result.State;
        }

        return state;
    }

    [Fact]
    public void Store_lands_in_not_allowed_to_start_ready_and_cannot_repeat()
    {
        var stored = Apply(Initial, Store);

        stored.State.ShouldBe(NotAllowedToStartReady);
        stored.Effect.ShouldBe(JobOrderEffect.StatusEvent);

        var again = Apply(stored.State, Store);
        again.Status.ShouldBe(JobReturnStatus.UnableToAccept);
        again.State.ShouldBe(NotAllowedToStartReady);
    }

    [Fact]
    public void Store_and_start_lands_in_allowed_to_start_ready() => Apply(Initial, StoreAndStart).State.ShouldBe(AllowedToStartReady);

    [Theory]
    [InlineData(NotAllowedToStartReady, Start, AllowedToStartReady)]
    [InlineData(NotAllowedToStartLoaded, Start, AllowedToStartReady)]
    [InlineData(AllowedToStartLoaded, RevokeStart, NotAllowedToStartReady)]
    [InlineData(AllowedToStartWaiting, BeginExecution, Running)]
    [InlineData(Running, Pause, Suspended)]
    [InlineData(Running, Hold, Held)]
    [InlineData(Suspended, Resume, Running)]
    [InlineData(Held, Resume, Running)]
    [InlineData(Running, Stop, Completed)]
    [InlineData(Suspended, Stop, Completed)]
    [InlineData(Running, Complete, Completed)]
    [InlineData(Held, Complete, Completed)]
    [InlineData(NotAllowedToStartWaiting, Abort, Aborted)]
    [InlineData(AllowedToStartReady, Abort, Aborted)]
    [InlineData(Running, Abort, Aborted)]
    [InlineData(Held, Abort, Aborted)]
    public void Receiver_transitions(JobOrderState from, JobOrderOperation operation, JobOrderState to)
    {
        var result = Apply(from, operation);

        result.Status.ShouldBe(JobReturnStatus.Success);
        result.State.ShouldBe(to);
        result.Effect.ShouldBe(JobOrderEffect.StatusEvent);
    }

    [Theory]
    [InlineData(NotAllowedToStartLoaded)]
    [InlineData(AllowedToStartWaiting)]
    public void Update_keeps_the_sub_state(JobOrderState state)
    {
        var result = Apply(state, Update);

        result.Accepted.ShouldBeTrue();
        result.State.ShouldBe(state);
        result.Effect.ShouldBe(JobOrderEffect.StatusEvent);
    }

    [Theory]
    [InlineData(NotAllowedToStartReady, Cancel)]
    [InlineData(AllowedToStartLoaded, Cancel)]
    [InlineData(Aborted, Clear)]
    [InlineData(Completed, Clear)]
    [InlineData(Closed, Clear)]
    public void Removals_emit_a_catalog_change_not_a_status_event(JobOrderState from, JobOrderOperation operation)
    {
        var result = Apply(from, operation);

        result.Accepted.ShouldBeTrue();
        result.State.ShouldBe(Removed);
        result.Effect.ShouldBe(JobOrderEffect.CatalogRemoved);
    }

    [Theory]
    [InlineData(NotAllowedToStartWaiting, PrepareToReady, NotAllowedToStartReady)]
    [InlineData(NotAllowedToStartReady, PrepareToLoaded, NotAllowedToStartLoaded)]
    [InlineData(NotAllowedToStartReady, PrepareToWaiting, NotAllowedToStartWaiting)]
    [InlineData(NotAllowedToStartLoaded, PrepareToReady, NotAllowedToStartReady)]
    [InlineData(NotAllowedToStartLoaded, PrepareToWaiting, NotAllowedToStartWaiting)]
    [InlineData(AllowedToStartWaiting, PrepareToReady, AllowedToStartReady)]
    [InlineData(AllowedToStartReady, PrepareToLoaded, AllowedToStartLoaded)]
    [InlineData(AllowedToStartReady, PrepareToWaiting, AllowedToStartWaiting)]
    [InlineData(AllowedToStartLoaded, PrepareToReady, AllowedToStartReady)]
    [InlineData(AllowedToStartLoaded, PrepareToWaiting, AllowedToStartWaiting)]
    [InlineData(Held, InterruptToSuspended, Suspended)]
    [InlineData(Suspended, InterruptToHeld, Held)]
    [InlineData(Completed, Close, Closed)]
    public void Sub_state_transitions_have_no_effect(JobOrderState from, JobOrderOperation operation, JobOrderState to)
    {
        var result = Apply(from, operation);

        result.Accepted.ShouldBeTrue();
        result.State.ShouldBe(to);
        result.Effect.ShouldBe(JobOrderEffect.None);
    }

    [Theory]
    [InlineData(NotAllowedToStartWaiting, PrepareToLoaded)]
    [InlineData(AllowedToStartWaiting, PrepareToLoaded)]
    [InlineData(Running, PrepareToReady)]
    [InlineData(Running, InterruptToHeld)]
    [InlineData(Closed, Close)]
    public void Sub_state_transitions_outside_the_nodeset_are_rejected(JobOrderState from, JobOrderOperation operation)
    {
        var result = Apply(from, operation);

        result.Status.ShouldBe(JobReturnStatus.InvalidStatus);
        result.State.ShouldBe(from);
    }

    [Theory]
    [InlineData(NotAllowedToStartReady, Pause)]
    [InlineData(NotAllowedToStartReady, BeginExecution)]
    [InlineData(AllowedToStartReady, Start)]
    [InlineData(Running, Start)]
    [InlineData(Running, Update)]
    [InlineData(Running, Cancel)]
    [InlineData(Running, Clear)]
    [InlineData(Running, Resume)]
    [InlineData(Completed, Abort)]
    [InlineData(Aborted, Abort)]
    [InlineData(Closed, Stop)]
    [InlineData(Removed, Start)]
    [InlineData(V1Loaded, Start)]
    [InlineData(V1Error, Clear)]
    public void Operations_outside_the_table_report_invalid_status(JobOrderState from, JobOrderOperation operation)
    {
        var result = Apply(from, operation);

        result.Status.ShouldBe(JobReturnStatus.InvalidStatus);
        result.State.ShouldBe(from);
        result.Effect.ShouldBe(JobOrderEffect.None);
    }

    [Theory]
    [InlineData(Start)]
    [InlineData(Update)]
    [InlineData(Abort)]
    [InlineData(Cancel)]
    [InlineData(Clear)]
    [InlineData(BeginExecution)]
    public void Operations_on_an_unknown_order_report_the_unknown_id(JobOrderOperation operation) =>
        Apply(Initial, operation).Status.ShouldBe(JobReturnStatus.UnknownJobOrderId);

    [Theory]
    [InlineData(Held)]
    [InlineData(Suspended)]
    [InlineData(Running)]
    public void Begin_execution_only_starts_an_allowed_order(JobOrderState state)
    {
        var result = Apply(state, BeginExecution);

        result.Status.ShouldBe(JobReturnStatus.InvalidStatus);
        result.State.ShouldBe(state);
    }

    [Fact]
    public void Store_on_a_removed_order_is_refused() => Apply(Removed, Store).Status.ShouldBe(JobReturnStatus.UnableToAccept);

    [Fact]
    public void Lifecycle_store_start_run_pause_resume_stop_clear() =>
        Walk(Initial, Store, Start, BeginExecution, Pause, Resume, Stop, Clear).ShouldBe(Removed);

    [Fact]
    public void Lifecycle_store_and_start_abort_clear() => Walk(Initial, StoreAndStart, Abort, Clear).ShouldBe(Removed);

    [Fact]
    public void Lifecycle_store_cancel() => Walk(Initial, Store, Cancel).ShouldBe(Removed);

    [Fact]
    public void Lifecycle_store_start_revoke_cancel() => Walk(Initial, Store, Start, RevokeStart, Cancel).ShouldBe(Removed);

    [Fact]
    public void Lifecycle_execution_system() => Walk(Initial, StoreAndStart, BeginExecution, Hold, Resume, Complete, Close, Clear).ShouldBe(Removed);

    [Fact]
    public void Apply_is_defined_for_every_state_and_operation()
    {
        foreach (var version in Enum.GetValues<JobControlVersion>())
        {
            foreach (var state in Enum.GetValues<JobOrderState>())
            {
                foreach (var operation in Enum.GetValues<JobOrderOperation>())
                {
                    var result = JobStateMachine.Apply(version, state, operation);

                    if (!result.Accepted)
                    {
                        result.State.ShouldBe(state);
                        result.Effect.ShouldBe(JobOrderEffect.None);
                        result.Status.ShouldNotBe(JobReturnStatus.None);
                    }
                }
            }
        }
    }

    [Fact]
    public void Return_status_uses_the_annex_b_bits()
    {
        ((ulong)JobReturnStatus.Success).ShouldBe(1UL);
        ((ulong)JobReturnStatus.UnknownJobOrderId).ShouldBe(2UL);
        ((ulong)JobReturnStatus.InvalidCommand).ShouldBe(4UL);
        ((ulong)JobReturnStatus.InvalidStatus).ShouldBe(8UL);
        ((ulong)JobReturnStatus.UnableToAccept).ShouldBe(16UL);
        ((ulong)JobReturnStatus.InvalidRequest).ShouldBe(1UL << 32);
    }
}

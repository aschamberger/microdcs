using MicroDCS.JobControl.StateMachine;
using static MicroDCS.JobControl.StateMachine.JobOrderOperation;
using static MicroDCS.JobControl.StateMachine.JobOrderState;

namespace MicroDCS.JobControl.Tests;

/// <summary>V1 rules (D-34). The spec has no transition table, so the allowed states are the upstream provider rules and its tests are the vectors.</summary>
public sealed class JobStateMachineV1Tests
{
    private static JobTransition Apply(JobOrderState state, JobOrderOperation operation) => JobStateMachine.Apply(JobControlVersion.V1, state, operation);

    [Theory]
    [InlineData(Running)]
    [InlineData(Held)]
    [InlineData(Suspended)]
    public void Stop_removes_a_started_order(JobOrderState state)
    {
        var result = Apply(state, Stop);

        result.Accepted.ShouldBeTrue();
        result.State.ShouldBe(Removed);
        result.Effect.ShouldBe(JobOrderEffect.CatalogRemoved);
    }

    [Fact]
    public void Stop_on_a_not_started_order_is_an_invalid_command()
    {
        var result = Apply(NotAllowedToStartReady, Stop);

        result.Status.ShouldBe(JobReturnStatus.InvalidCommand);
        result.State.ShouldBe(NotAllowedToStartReady);
    }

    [Fact]
    public void Stop_on_an_unknown_order_reports_the_unknown_id() => Apply(Initial, Stop).Status.ShouldBe(JobReturnStatus.UnknownJobOrderId);

    [Fact]
    public void Starting_an_already_started_order_is_an_invalid_command() =>
        Apply(AllowedToStartReady, Start).Status.ShouldBe(JobReturnStatus.InvalidCommand);

    [Theory]
    [InlineData(Pause)]
    [InlineData(Resume)]
    [InlineData(Abort)]
    [InlineData(RevokeStart)]
    [InlineData(PrepareToLoaded)]
    [InlineData(InterruptToHeld)]
    public void Operations_v1_does_not_have_are_invalid_commands(JobOrderOperation operation) =>
        Apply(Running, operation).Status.ShouldBe(JobReturnStatus.InvalidCommand);

    [Fact]
    public void Update_is_accepted_before_execution_and_refused_after()
    {
        Apply(NotAllowedToStartReady, Update).Accepted.ShouldBeTrue();
        Apply(AllowedToStartReady, Update).Accepted.ShouldBeTrue();
        Apply(Running, Update).Status.ShouldBe(JobReturnStatus.InvalidCommand);
    }

    [Theory]
    [InlineData(NotAllowedToStartReady, Cancel)]
    [InlineData(Completed, Clear)]
    [InlineData(Closed, Clear)]
    [InlineData(Aborted, Clear)]
    public void Cancel_and_clear_remove_the_order(JobOrderState state, JobOrderOperation operation)
    {
        var result = Apply(state, operation);

        result.State.ShouldBe(Removed);
        result.Effect.ShouldBe(JobOrderEffect.CatalogRemoved);
    }

    [Theory]
    [InlineData(Running, Cancel)]
    [InlineData(Running, Clear)]
    [InlineData(V1Loaded, Start)]
    [InlineData(V1Error, Clear)]
    public void Disallowed_commands_report_invalid_command(JobOrderState state, JobOrderOperation operation) =>
        Apply(state, operation).Status.ShouldBe(JobReturnStatus.InvalidCommand);

    [Fact]
    public void Store_twice_is_unable_to_accept() => Apply(NotAllowedToStartReady, Store).Status.ShouldBe(JobReturnStatus.UnableToAccept);
}

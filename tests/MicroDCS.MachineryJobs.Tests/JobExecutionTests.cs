using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace MicroDCS.MachineryJobs.Tests;

public sealed class JobExecutionTests
{
    private const string Scope = "site.area";

    private readonly InMemoryJobStore _store = new();
    private readonly IEventPublisher _publisher = Substitute.For<IEventPublisher>();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private JobExecution Execution() => new(_store, _publisher, TimeProvider.System, NullLogger<JobExecution>.Instance);

    private async Task SeedAsync(JobOrderState state, string scope = Scope) =>
        await _store.SaveOrderAsync(new JobOrderWrite(new JobOrder { Id = "job" }, state, scope, "Store", 0, "seed", JobRecipeWork.None), Ct);

    [Fact]
    public async Task BeginExecutionMovesAnAllowedOrderToRunning()
    {
        await SeedAsync(JobOrderState.AllowedToStartReady);

        var result = await Execution().ApplyAsync("job", Scope, JobOrderOperation.BeginExecution, "op-1", Ct);

        result.ShouldBe(new JobExecutionResult(JobExecutionStatus.Applied, JobOrderState.Running));
        _store.Order("job")!.State.ShouldBe(JobOrderState.Running);
        await _publisher.Received(1).PublishAsync(Arg.Any<JobOrderStatusEvent>(), Intent.Event, Arg.Is<PublishOptions>(o => o.BindingId == MachineryJobsProcessor.BindingId), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AnInternalHoldAlsoHoldsTheRecipe()
    {
        await SeedAsync(JobOrderState.Running);

        var result = await Execution().ApplyAsync("job", Scope, JobOrderOperation.Hold, "op-1", Ct);

        result.ShouldBe(new JobExecutionResult(JobExecutionStatus.Applied, JobOrderState.Held));
        _store.RecipeWork.ShouldBe(["Hold:job"]);
    }

    [Fact]
    public async Task CompleteWritesTheJobResponse()
    {
        await SeedAsync(JobOrderState.Running);

        var result = await Execution().ApplyAsync("job", Scope, JobOrderOperation.Complete, "op-1", Ct);

        result.State.ShouldBe(JobOrderState.Completed);
        var response = await _store.GetResponseByOrderAsync("job", Ct);
        response.ShouldNotBeNull().Response.Id.ShouldBe("job-response");
        response.Response.State.ShouldBe(JobOrderState.Completed);
    }

    [Fact]
    public async Task AbortWritesNoJobResponse()
    {
        await SeedAsync(JobOrderState.Running);

        await Execution().ApplyAsync("job", Scope, JobOrderOperation.Abort, "op-1", Ct);

        _store.Order("job")!.State.ShouldBe(JobOrderState.Aborted);
        (await _store.HasResponseAsync("job", Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task ARefusedTransitionChangesNothing()
    {
        await SeedAsync(JobOrderState.Completed);

        var result = await Execution().ApplyAsync("job", Scope, JobOrderOperation.Abort, "op-1", Ct);

        result.ShouldBe(new JobExecutionResult(JobExecutionStatus.Refused, JobOrderState.Completed));
        _store.Order("job")!.Version.ShouldBe(1);
    }

    [Fact]
    public async Task AnOrderOfAnotherScopeIsMissing()
    {
        await SeedAsync(JobOrderState.Running, "other.scope");

        var result = await Execution().ApplyAsync("job", Scope, JobOrderOperation.Abort, "op-1", Ct);

        result.Status.ShouldBe(JobExecutionStatus.Missing);
        _store.Order("job")!.State.ShouldBe(JobOrderState.Running);
    }

    [Fact]
    public async Task ARepeatedOperationIsNotAppliedTwice()
    {
        await SeedAsync(JobOrderState.Running);
        var execution = Execution();

        await execution.ApplyAsync("job", Scope, JobOrderOperation.Complete, "op-1", Ct);
        var again = await execution.ApplyAsync("job", Scope, JobOrderOperation.Complete, "op-1", Ct);

        again.ShouldBe(new JobExecutionResult(JobExecutionStatus.Applied, JobOrderState.Completed));
        _store.Order("job")!.Version.ShouldBe(2);
    }

    [Fact]
    public async Task AFullQueueDoesNotFailTheStoredTransition()
    {
        await SeedAsync(JobOrderState.Running);
        _publisher.PublishAsync(Arg.Any<JobOrderStatusEvent>(), Arg.Any<Intent>(), Arg.Any<PublishOptions?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OutboundQueueFullException("machinery_jobs"));

        var result = await Execution().ApplyAsync("job", Scope, JobOrderOperation.Abort, "op-1", Ct);

        result.Status.ShouldBe(JobExecutionStatus.Applied);
    }
}

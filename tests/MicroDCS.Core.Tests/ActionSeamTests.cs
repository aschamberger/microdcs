using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using MicroDCS.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace MicroDCS.Core.Tests;

[Processor(ProcessorBinding.Southbound, Id = "station")]
public sealed partial class StationProcessor
{
    [Outgoing(typeof(Bye), Outcomes = ["good", "bad"])]
    public static Bye CreateBye(CommandRequest request) => new(request.ActionKey);

    [OnResponse]
    public static ActionOutcome OnHello(Hello hello) => hello.Name == "fail" ? ActionOutcome.Failed("broken") : new ActionOutcome(hello.Name);

    [OnResponse]
    public void OnAck(ByeAck ack) => Acknowledged = ack.Name; // Reports no outcome: the pipeline uses the default.

    public string? Acknowledged { get; private set; }

    [Incoming]
    public static Replies OnBye(Bye bye) => new Replies().With(new ByeAck(bye.Name)).With(new ActionOutcome("ignored"));
}

internal sealed class RecordingSink : IActionSink
{
    public List<ActionResponse> Responses { get; } = [];

    public int Events { get; private set; }

    public Exception? Failure { get; set; }

    public ValueTask CompleteAsync(ActionResponse response, CancellationToken cancellationToken)
    {
        if (Failure is { } failure)
        {
            throw failure;
        }

        Responses.Add(response);
        return ValueTask.CompletedTask;
    }

    public ValueTask EventReceivedAsync(EventContext context, ActionOutcome outcome, CancellationToken cancellationToken)
    {
        Events++;
        return ValueTask.CompletedTask;
    }
}

public sealed class ActionSeamTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<(ProcessorHarness Harness, RecordingSink Sink)> StartAsync()
    {
        var sink = new RecordingSink();
        var harness = await ProcessorHarness.StartAsync<StationProcessor>(s => s.AddSingleton<IActionSink>(sink), Ct);
        return (harness, sink);
    }

    [Fact]
    public async Task AResponseReportsTheOutcomeOfItsHandler()
    {
        var (harness, sink) = await StartAsync();
        await using (harness)
        {
            await harness.SendResponseAsync(new Hello("good"), "cmd-1", "site.area", Ct);

            var response = sink.Responses.ShouldHaveSingleItem();
            response.CommandId.ShouldBe("cmd-1");
            response.Outcome.Name.ShouldBe("good");
            response.Outcome.IsFailure.ShouldBeFalse();
            sink.Events.ShouldBe(0);
        }
    }

    [Fact]
    public async Task AHandlerThatReportsNoOutcomeGivesTheDefault()
    {
        var (harness, sink) = await StartAsync();
        await using (harness)
        {
            await harness.SendResponseAsync(new ByeAck("x"), "cmd-1", "site.area", Ct);

            sink.Responses.ShouldHaveSingleItem().Outcome.Name.ShouldBe(ActionOutcome.DefaultName);
        }
    }

    [Fact]
    public async Task AFailureReportsTheReservedOutcomeWithItsText()
    {
        var (harness, sink) = await StartAsync();
        await using (harness)
        {
            await harness.SendResponseAsync(new Hello("fail"), "cmd-1", "site.area", Ct);

            var outcome = sink.Responses.ShouldHaveSingleItem().Outcome;
            outcome.IsFailure.ShouldBeTrue();
            outcome.Error.ShouldBe("broken");
        }
    }

    [Fact]
    public async Task AnIncomingEventIsNotReportedAsAResponseAndItsOutcomeIsNotPublished()
    {
        var (harness, sink) = await StartAsync();
        await using (harness)
        {
            var result = await harness.SendAsync(new Bye("x"), "site.area", Ct);

            sink.Responses.ShouldBeEmpty();
            sink.Events.ShouldBe(1);
            result.Published.ShouldHaveSingleItem();
        }
    }

    [Fact]
    public async Task AnErrorResponseIsAFailureOfTheActionWithoutAHandler()
    {
        var (harness, sink) = await StartAsync();
        await using (harness)
        {
            await harness.SendResponseEnvelopeAsync(ErrorResponse(), Ct);

            var response = sink.Responses.ShouldHaveSingleItem();
            response.CommandId.ShouldBe("cmd-1");
            response.ActionKey.ShouldBe("job:act");
            response.Outcome.IsFailure.ShouldBeTrue();
            response.Outcome.Error.ShouldBe("refused");
        }
    }

    private static EventEnvelope ErrorResponse() => new()
    {
        Id = "resp-1",
        Source = "urn:peer",
        Type = MessagePipeline.ErrorEventType,
        Subject = "site.area",
        CausationId = "cmd-1",
        ActionKey = "job:act",
        Error = new EnvelopeError(ErrorKind.PermissionDenied, "refused"),
    };
}

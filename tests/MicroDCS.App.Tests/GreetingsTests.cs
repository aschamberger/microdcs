using MicroDCS.App.Greetings;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using MicroDCS.Testing;

namespace MicroDCS.App.Tests;

public sealed class GreetingsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Hello_is_answered_with_two_hellos_and_the_carried_attribute_comes_back()
    {
        await using var harness = await ProcessorHarness.StartAsync<GreetingsProcessor>(cancellationToken: Ct);

        var result = await harness.SendAsync(new Hello("Bob") { HiddenStr = "secret" }, "plant1.line2/res", Ct);

        result.Outcome.ShouldBe(DeliveryOutcome.Ack);
        result.Published.Length.ShouldBe(2);
        harness.Decode<Hello>(result.Published[0]).ShouldBe(new Hello("Bob") { HiddenStr = "secret" });
        harness.Decode<Hello>(result.Published[0]).HiddenStr.ShouldBe("secret");
        harness.Decode<Hello>(result.Published[1]).Name.ShouldBe("Alice");
        result.Published[1].Envelope.Extensions["hiddenstr"].ShouldBe("secret");
    }

    [Fact]
    public async Task Replies_follow_the_request_correlation_and_causation()
    {
        await using var harness = await ProcessorHarness.StartAsync<GreetingsProcessor>(cancellationToken: Ct);
        var request = new EventEnvelope
        {
            Id = "req-1",
            Source = "urn:client",
            Type = "com.github.aschamberger.microdcs.greetings.hello.v1",
            Subject = "plant1/res",
            CorrelationId = "corr-7",
            Data = """{"Name":"Bob"}"""u8.ToArray(),
        };

        var result = await harness.SendEnvelopeAsync(request, Ct);

        result.Published.ShouldAllBe(m => m.Envelope.CausationId == "req-1" && m.Envelope.CorrelationId == "corr-7" && m.Envelope.Subject == "plant1/res");
        result.Published.ShouldAllBe(m => m.Envelope.Source == "urn:test:processor" && !string.IsNullOrEmpty(m.Envelope.Id));
    }

    [Fact]
    public async Task Event_without_a_handler_is_echoed_as_a_raw_event()
    {
        await using var harness = await ProcessorHarness.StartAsync<GreetingsProcessor>(cancellationToken: Ct);
        var request = new EventEnvelope
        {
            Id = "raw-1",
            Source = "urn:client",
            Type = "something.else.v1",
            DataContentType = "text/plain",
            Subject = "plant1/res",
            Data = "hi"u8.ToArray(),
        };

        var result = await harness.SendEnvelopeAsync(request, Ct);

        var echo = result.Published.Single().Envelope;
        echo.Type.ShouldBe("com.github.aschamberger.microdcs.greetings.raw.v1");
        echo.DataContentType.ShouldBe("text/plain");
        echo.Data.ToArray().ShouldBe("hi"u8.ToArray());
        echo.CausationId.ShouldBe("raw-1");
    }

    [Fact]
    public async Task Subject_scope_must_match_the_address_scope()
    {
        await using var harness = await ProcessorHarness.StartAsync<GreetingsProcessor>(cancellationToken: Ct);
        var envelope = new EventEnvelope
        {
            Id = "x",
            Source = "urn:client",
            Type = "com.github.aschamberger.microdcs.greetings.hello.v1",
            Subject = "other/res",
            Data = """{"Name":"Bob"}"""u8.ToArray(),
        };
        var message = new InboundMessage
        {
            BindingId = "greetings",
            Kind = InboundKind.Request,
            Envelope = envelope,
            Address = new LogicalAddress("test", ["plant1"], null, Intent.Data),
        };

        (await harness.Transport.InjectAsync(message, Ct)).ShouldBe(DeliveryOutcome.Drop);
        harness.Transport.Published.ShouldBeEmpty();
    }

    [Fact]
    public void Bye_factory_builds_the_command_payload()
    {
        var bye = GreetingsProcessor.CreateBye(new CommandRequest { Scope = "plant1", ActionKey = "j:a", CommandId = "c" });

        bye.ShouldBe(new Bye("plant1"));
    }
}

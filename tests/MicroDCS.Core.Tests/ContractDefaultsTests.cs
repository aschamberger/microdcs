using System.Collections.Immutable;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;

namespace MicroDCS.Core.Tests;

/// <summary>Defaults and construction rules that D-35 to D-40 fix as contract: they are documented values, not accidents.</summary>
public class ContractDefaultsTests
{
    [Fact]
    public void OptionsDefaultsMatchTheDecisions()
    {
        var options = new MicroDcsOptions();

        options.Roles.ShouldBe(InstanceRoles.All);
        options.ShutdownGracePeriod.ShouldBe(TimeSpan.FromSeconds(30));
        options.OutgoingQueueMaxSize.ShouldBe(1000);
        options.MessageWorkers.ShouldBe(5);
        options.DedupeLease.ShouldBe(TimeSpan.FromSeconds(30));
        options.DedupeRetention.ShouldBe(TimeSpan.FromMinutes(10));
        options.StartLeaseTtl.ShouldBe(TimeSpan.FromSeconds(30));
        options.Bindings.ShouldBeEmpty();
        options.Management.Url.ShouldBe("http://127.0.0.1:8081");
        options.Management.McpEnabled.ShouldBeFalse();
        MicroDcsOptions.SectionName.ShouldBe("MicroDcs");
    }

    [Fact]
    public void BindingDefaultsAreTheStrictOnes()
    {
        var binding = new BindingOptions();

        binding.Transport.ShouldBe("mqtt");
        binding.ScopeLevels.ShouldBe(0);
        binding.ScopeCheck.ShouldBe(ScopeCheck.Enforce);
        binding.Level0Subjects.ShouldBe(Level0Subjects.Reject);
        binding.OutgoingQueueSize.ShouldBe(0);
    }

    [Fact]
    public void EnvelopeAndCommandRequestStartWithEmptyCollections()
    {
        var envelope = new EventEnvelope { Id = "1", Source = "urn:test", Type = "t" };
        var request = new CommandRequest { Scope = "plant1", ActionKey = "j:a", CommandId = "c1" };

        envelope.Extensions.ShouldBeEmpty();
        envelope.Data.IsEmpty.ShouldBeTrue();
        request.Parameters.ShouldBeEmpty();
    }

    [Fact]
    public void QueueFullExceptionNamesTheBinding() =>
        new OutboundQueueFullException("machinery-jobs").Message.ShouldContain("machinery-jobs");

    [Fact]
    public void PayloadExceptionKeepsMessageAndCause()
    {
        var cause = new InvalidOperationException("inner");

        var exception = new PayloadException("Field 'Name' is invalid.", cause);

        exception.Message.ShouldBe("Field 'Name' is invalid.");
        exception.InnerException.ShouldBe(cause);
    }

    [Fact]
    public void AttributesKeepTheirArguments()
    {
        var type = new CloudEventTypeAttribute("com.example.hello.v1") { Schema = "https://example/hello" };
        var carried = new CarriedAsAttribute("x-hidden-str");
        var processor = new ProcessorAttribute(ProcessorBinding.Southbound) { Id = "greetings" };
        var outgoing = new OutgoingAttribute(typeof(string));
        var started = new OnStartedAttribute { Singleton = true };
        var part = new SubjectPartAttribute(1);

        type.Id.ShouldBe("com.example.hello.v1");
        type.Codec.ShouldBe(PayloadCodec.Json);
        type.Schema.ShouldBe("https://example/hello");
        carried.Name.ShouldBe("x-hidden-str");
        processor.Binding.ShouldBe(ProcessorBinding.Southbound);
        processor.Id.ShouldBe("greetings");
        processor.SubscribeIntents.ShouldBeEmpty();
        processor.PublishIntents.ShouldBeEmpty();
        outgoing.PayloadType.ShouldBe(typeof(string));
        started.Singleton.ShouldBeTrue();
        part.Index.ShouldBe(1);
    }

    [Fact]
    public void RepliesCollectPayloadsOfDifferentTypes()
    {
        var replies = new Replies().With("a").With(42);

        replies.Select(static r => r.Payload).ShouldBe(["a", (object)42]);
        replies.ShouldAllBe(static r => r.Options == null);
    }

    [Fact]
    public async Task ADescriptorCanBeBuiltByHandWithoutTheGenerator()
    {
        HandlerInvoker invoke = (_, _, input, _) =>
            ValueTask.FromResult<IReadOnlyList<Reply>>([new Reply(input ?? "none")]);
        var handler = new HandlerDescriptor(HandlerKind.Incoming, "com.example.hello.v1", null, invoke);
        var descriptor = new ProcessorDescriptor(
            typeof(ContractDefaultsTests),
            "greetings",
            ProcessorBinding.Southbound,
            [Intent.Data, Intent.Event, Intent.Meta],
            [Intent.Command],
            [handler],
            new LifecycleDescriptor(null, null, false, null));
        var context = new EventContext
        {
            EventId = "1",
            Source = "urn:test",
            Type = "com.example.hello.v1",
            BindingId = "greetings",
            InstanceId = "i1",
            Envelope = new EventEnvelope { Id = "1", Source = "urn:test", Type = "com.example.hello.v1" },
        };

        var replies = await descriptor.Handlers[0].Invoke(null!, context, "hello", CancellationToken.None);

        replies.Single().Payload.ShouldBe("hello");
        descriptor.Binding.ShouldBe(ProcessorBinding.Southbound);
    }

    [Fact]
    public void ScopeIsTheDottedPath()
    {
        var address = new LogicalAddress("dcs/jobs", ["plant1", "line2"], null, Intent.Event);

        address.Scope.ShouldBe("plant1.line2");
        new LogicalAddress("dcs/jobs", ImmutableArray<string>.Empty, null, Intent.Event).Scope.ShouldBe(string.Empty);
    }

    [Fact]
    public void UndefinedIntentHasNoSegment() =>
        Should.Throw<ArgumentOutOfRangeException>(() => IntentNames.ToSegment((Intent)99));

    [Fact]
    public void RedactionRejectsNull() =>
        Should.Throw<ArgumentNullException>(() => Redaction.Apply(null!, includePayloads: false));
}

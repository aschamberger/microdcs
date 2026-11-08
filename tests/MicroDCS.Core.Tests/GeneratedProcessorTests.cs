using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;
using Microsoft.Extensions.DependencyInjection;

namespace MicroDCS.Core.Tests;

public sealed class GeneratedProcessorTests
{
    private sealed class ListLog : ILog
    {
        public List<string> Entries { get; } = [];

        public void Add(string entry) => Entries.Add(entry);
    }

    private static EventContext Context(string? subject = "plant1/res") => new()
    {
        EventId = "e1",
        Source = "s",
        Type = "t",
        Subject = subject,
        BindingId = "sample",
        InstanceId = "i1",
        Envelope = new EventEnvelope { Id = "e1", Source = "s", Type = "t" },
    };

    private static (ProcessorDescriptor Descriptor, IServiceProvider Services, ListLog Log) Setup()
    {
        var log = new ListLog();
        var services = new ServiceCollection().AddSingleton<ILog>(log).AddSingleton<SampleProcessor>().BuildServiceProvider();
        return (SampleProcessor.Describe(), services, log);
    }

    [Fact]
    public void Descriptor_has_binding_defaults_and_all_handlers()
    {
        var descriptor = SampleProcessor.Describe();

        descriptor.BindingId.ShouldBe("sample");
        descriptor.Binding.ShouldBe(ProcessorBinding.Southbound);
        descriptor.SubscribeIntents.ShouldBe([Intent.Data, Intent.Event, Intent.Meta]);
        descriptor.PublishIntents.ShouldBe([Intent.Command]);
        descriptor.Handlers.Select(h => (h.Kind, h.TypeId)).ShouldBe(
        [
            (HandlerKind.Incoming, "test.hello.v1"),
            (HandlerKind.Incoming, "test.bye.v1"),
            (HandlerKind.Response, "test.ack.v1"),
            (HandlerKind.Outgoing, "test.bye.v1"),
            (HandlerKind.Fallback, null),
        ]);
        descriptor.Lifecycle.StartedSingleton.ShouldBeTrue();
        descriptor.Lifecycle.Initialize.ShouldNotBeNull();
        descriptor.Lifecycle.Shutdown.ShouldNotBeNull();
    }

    [Fact]
    public async Task Incoming_handler_binds_scope_and_returns_many_replies()
    {
        var (descriptor, services, log) = Setup();
        var handler = descriptor.Handlers.First(h => h.TypeId == "test.hello.v1");

        var replies = await handler.Invoke(services, Context(), new Hello("Bob") { HiddenStr = "h" }, CancellationToken.None);

        log.Entries.ShouldBe(["hello:Bob:plant1"]);
        replies.Count.ShouldBe(2);
        ((Hello)replies[0].Payload).HiddenStr.ShouldBe("h");
        ((Hello)replies[1].Payload).Name.ShouldBe("Alice");
    }

    [Fact]
    public async Task Async_handler_returning_null_gives_no_replies()
    {
        var (descriptor, services, log) = Setup();
        var handler = descriptor.Handlers.First(h => h.TypeId == "test.bye.v1" && h.Kind == HandlerKind.Incoming);

        (await handler.Invoke(services, Context(), new Bye(""), CancellationToken.None)).ShouldBeEmpty();
        (await handler.Invoke(services, Context(), new Bye("x"), CancellationToken.None)).Single().Payload.ShouldBe(new ByeAck("x"));
        log.Entries.ShouldBe(["bye:e1", "bye:e1"]);
    }

    [Fact]
    public async Task Static_outgoing_factory_receives_the_request()
    {
        var (descriptor, services, _) = Setup();
        var handler = descriptor.Handlers.First(h => h.Kind == HandlerKind.Outgoing);
        var request = new CommandRequest { Scope = "plant1", ActionKey = "j:a", CommandId = "c1" };

        var replies = await handler.Invoke(services, Context(), request, CancellationToken.None);

        replies.Single().Payload.ShouldBe(new Bye("plant1"));
    }

    [Fact]
    public async Task Fallback_and_lifecycle_hooks_run()
    {
        var (descriptor, services, log) = Setup();
        var fallback = descriptor.Handlers.Single(h => h.Kind == HandlerKind.Fallback);

        await fallback.Invoke(services, Context(), new EventEnvelope { Id = "1", Source = "s", Type = "other" }, CancellationToken.None);
        await descriptor.Lifecycle.Initialize!(services, CancellationToken.None);
        await descriptor.Lifecycle.Started!(services, CancellationToken.None);
        await descriptor.Lifecycle.Shutdown!(services, CancellationToken.None);

        log.Entries.ShouldBe(["fallback:other", "init", "started", "shutdown"]);
    }

    [Fact]
    public void Generated_payload_descriptor_carries_the_attribute()
    {
        var descriptor = MicroDcsPayloads.MicroDCS_Core_Tests_Hello;
        var carried = new Dictionary<string, string>();

        descriptor.Encode(new Hello("Bob") { HiddenStr = "h" }, carried);

        descriptor.TypeId.ShouldBe("test.hello.v1");
        descriptor.Schema.ShouldBe("https://example.com/hello");
        carried["hiddenstr"].ShouldBe("h");
    }

    [Fact]
    public void Subject_without_scope_is_rejected()
    {
        Should.Throw<PayloadException>(() => GeneratedSupport.Scope(Context(subject: null)));
        GeneratedSupport.SubjectPart(Context("a/b"), 1).ShouldBe("b");
    }
}

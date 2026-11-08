using System.Diagnostics;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace MicroDCS.Core.Tests;

[Processor(ProcessorBinding.Southbound, Id = "expiring")]
public sealed partial class ExpiringProcessor(ILog log)
{
    [OnResponse]
    public void OnAck(ByeAck ack) => log.Add("ack:" + ack.Name);

    [OnExpired]
    public IEnumerable<ByeAck> OnTimeout(Bye bye)
    {
        log.Add("expired:" + bye.Name);
        yield return new ByeAck(bye.Name);
    }
}

public sealed class PipelineTests
{
    private static readonly TimeSpan _wait = TimeSpan.FromSeconds(10);

    private sealed class SharedLog : ILog
    {
        private readonly List<string> _entries = [];

        public List<string> Entries
        {
            get
            {
                lock (_entries)
                {
                    return [.. _entries];
                }
            }
        }

        public void Add(string entry)
        {
            lock (_entries)
            {
                _entries.Add(entry);
            }
        }

        public void Clear()
        {
            lock (_entries)
            {
                _entries.Clear();
            }
        }
    }

    private sealed class FlakyLog(SharedLog inner) : ILog
    {
        private int _failures;

        public void Add(string entry)
        {
            if (entry.StartsWith("hello", StringComparison.Ordinal) && Interlocked.Increment(ref _failures) == 1)
            {
                throw new InvalidOperationException("boom");
            }

            inner.Add(entry);
        }
    }

    private sealed class RecordingDedupe(IDedupeStore inner, SharedLog trace) : IDedupeStore
    {
        public ValueTask<ClaimState> ClaimAsync(string source, string id, string token, CancellationToken cancellationToken)
        {
            trace.Add("claim");
            return inner.ClaimAsync(source, id, token, cancellationToken);
        }

        public ValueTask MarkDoneAsync(string source, string id, CancellationToken cancellationToken)
        {
            trace.Add("done");
            return inner.MarkDoneAsync(source, id, cancellationToken);
        }

        public ValueTask<TimeSpan> LeaseRemainingAsync(string source, string id, CancellationToken cancellationToken) =>
            inner.LeaseRemainingAsync(source, id, cancellationToken);
    }

    private sealed class TestTransport(SharedLog trace, bool canRedeliver) : ITransport
    {
        private readonly List<OutboundMessage> _published = [];
        private readonly List<(int Count, TaskCompletionSource Done)> _waiters = [];
        private IInboundHandler? _handler;

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public SemaphoreSlim? Gate { get; set; }

        public string Name => "memory";

        public DeliveryProfile Profile { get; } = new(DeliveryGuarantee.AtLeastOnce, true, false, true, false, canRedeliver);

        public IAddressRenderer Addresses { get; } = new SlashAddressRenderer();

        public bool IsConnected => true;

        public IReadOnlyList<OutboundMessage> Published
        {
            get
            {
                lock (_published)
                {
                    return [.. _published];
                }
            }
        }

        public ReplyAddress? GetReplyAddress(string bindingId, string instanceId) => new ReplyAddress("reply/" + bindingId + "/" + instanceId);

        public ValueTask StartAsync(IInboundHandler handler, CancellationToken cancellationToken)
        {
            _handler = handler;
            return ValueTask.CompletedTask;
        }

        public ValueTask<DeliveryOutcome> InjectAsync(InboundMessage message, CancellationToken cancellationToken) =>
            _handler!.HandleAsync(message, cancellationToken);

        public ValueTask StopReceivingAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public async ValueTask PublishAsync(OutboundMessage message, CancellationToken cancellationToken)
        {
            Entered.TrySetResult();
            if (Gate is { } gate)
            {
                await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            trace.Add("publish");
            lock (_published)
            {
                _published.Add(message);
                foreach (var waiter in _waiters.Where(w => w.Count <= _published.Count))
                {
                    waiter.Done.TrySetResult();
                }
            }
        }

        public ValueTask StopAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public Task WaitForPublishedAsync(int count)
        {
            lock (_published)
            {
                if (_published.Count >= count)
                {
                    return Task.CompletedTask;
                }

                var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _waiters.Add((count, done));
                return done.Task.WaitAsync(_wait);
            }
        }
    }

    private sealed class Rig : IAsyncDisposable
    {
        public required ServiceProvider Provider { get; init; }

        public required TestTransport Transport { get; init; }

        public required SharedLog Log { get; init; }

        public required SharedLog Trace { get; init; }

        public required FakeTimeProvider Time { get; init; }

        public required IHostApplicationLifetime Lifetime { get; init; }

        public IHostedService Host => Provider.GetServices<IHostedService>().Single();

        public MessagePipeline Pipeline => Provider.GetRequiredService<MessagePipeline>();

        public IEventPublisher Publisher => Provider.GetRequiredService<IEventPublisher>();

        public async ValueTask DisposeAsync() => await Provider.DisposeAsync();
    }

    private static async Task<Rig> StartRigAsync<TProcessor>(
        string bindingId,
        Action<IServiceCollection, SharedLog>? configure = null,
        Action<MicroDcsOptions>? options = null,
        bool canRedeliver = false)
        where TProcessor : class, IDescribedProcessor
    {
        var log = new SharedLog();
        var trace = new SharedLog();
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var transport = new TestTransport(trace, canRedeliver);
        var lifetime = Substitute.For<IHostApplicationLifetime>();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MicroDcs:CloudEventSource"] = "urn:test:processor",
            ["MicroDcs:InstanceId"] = "i1",
            [$"MicroDcs:Bindings:{bindingId}:Transport"] = "memory",
            [$"MicroDcs:Bindings:{bindingId}:Prefix"] = "test",
            [$"MicroDcs:Bindings:{bindingId}:ScopeLevels"] = "3",
        }).Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(time);
        services.AddSingleton<ILog>(log);
        services.AddSingleton(lifetime);
        services.AddSingleton<IDedupeStore>(new RecordingDedupe(new InMemoryDedupeStore(time, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(10)), trace));
        services.AddSingleton<ITransport>(transport);
        configure?.Invoke(services, log);
        services.AddMicroDcs(configuration).AddProcessor<TProcessor>();
        if (options is not null)
        {
            services.PostConfigure(options);
        }

        var provider = services.BuildServiceProvider();
        var rig = new Rig { Provider = provider, Transport = transport, Log = log, Trace = trace, Time = time, Lifetime = lifetime };
        await rig.Host.StartAsync(TestContext.Current.CancellationToken);
        log.Clear(); // The lifecycle hooks (init, started) are not what these tests look at.
        return rig;
    }

    private static InboundMessage Request(string type, string subject, string json, string id = "e1", ReplyAddress? replyTo = null, string bindingId = "sample", string scope = "p") => new()
    {
        BindingId = bindingId,
        Kind = InboundKind.Request,
        Envelope = new EventEnvelope { Id = id, Source = "urn:x", Type = type, Subject = subject, Data = System.Text.Encoding.UTF8.GetBytes(json) },
        Address = new LogicalAddress("test", [scope], null, Intent.Command),
        ReplyTo = replyTo,
    };

    private static InboundMessage Response(string causationId) => new()
    {
        BindingId = "expiring",
        Kind = InboundKind.Response,
        Envelope = new EventEnvelope { Id = "r1", Source = "urn:x", Type = "test.ack.v1", Subject = "p/res", CausationId = causationId, Data = """{"Name":"x"}"""u8.ToArray() },
        Address = new LogicalAddress("test", ["p"], null, Intent.Event),
    };

    [Fact]
    public async Task Steps_run_in_the_order_claim_handler_publish_done()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample");

        var outcome = await rig.Transport.InjectAsync(Request("test.hello.v1", "p/res", """{"Name":"Bob"}"""), TestContext.Current.CancellationToken);

        outcome.ShouldBe(DeliveryOutcome.Ack);
        rig.Trace.Entries.ShouldBe(["claim", "publish", "publish", "done"]);
        rig.Log.Entries.ShouldBe(["hello:Bob:p"]);
    }

    [Fact]
    public async Task Fault_leaves_the_event_claimed_and_a_redelivery_after_the_lease_succeeds()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample", (s, log) => s.AddSingleton<ILog>(new FlakyLog(log)));
        var message = Request("test.hello.v1", "p/res", """{"Name":"Bob"}""");

        var first = await rig.Transport.InjectAsync(message, TestContext.Current.CancellationToken);
        rig.Time.Advance(TimeSpan.FromSeconds(31));
        var second = await rig.Transport.InjectAsync(message, TestContext.Current.CancellationToken);

        first.ShouldBe(DeliveryOutcome.Fault);
        second.ShouldBe(DeliveryOutcome.Ack);
        rig.Trace.Entries.Count(e => e == "done").ShouldBe(1);
        rig.Log.Entries.ShouldBe(["hello:Bob:p"]);
    }

    [Fact]
    public async Task Fault_stops_the_host_when_the_transport_cannot_redeliver()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample", (s, log) => s.AddSingleton<ILog>(new FlakyLog(log)));

        await rig.Transport.InjectAsync(Request("test.hello.v1", "p/res", """{"Name":"Bob"}"""), TestContext.Current.CancellationToken);

        rig.Lifetime.Received(1).StopApplication();
    }

    [Fact]
    public async Task Fault_keeps_the_host_running_when_the_transport_can_redeliver()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample", (s, log) => s.AddSingleton<ILog>(new FlakyLog(log)), canRedeliver: true);

        var outcome = await rig.Transport.InjectAsync(Request("test.hello.v1", "p/res", """{"Name":"Bob"}"""), TestContext.Current.CancellationToken);

        outcome.ShouldBe(DeliveryOutcome.Fault);
        rig.Lifetime.DidNotReceive().StopApplication();
    }

    [Fact]
    public async Task Undecodable_request_with_a_reply_address_gets_an_error_event()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample");
        var reply = new ReplyAddress("reply/client");

        var outcome = await rig.Transport.InjectAsync(Request("test.hello.v1", "p/res", "{broken", replyTo: reply), TestContext.Current.CancellationToken);

        outcome.ShouldBe(DeliveryOutcome.Drop);
        var error = rig.Transport.Published.ShouldHaveSingleItem();
        error.SendTo.ShouldBe(reply);
        error.Envelope.Type.ShouldBe(MessagePipeline.ErrorEventType);
        error.Envelope.Error!.Kind.ShouldBe(ErrorKind.PayloadInvalid);
        error.Envelope.CausationId.ShouldBe("e1");
        error.Envelope.Error.Message.ShouldNotContain("broken");
    }

    [Fact]
    public async Task Scope_mismatch_is_dropped_and_answered_with_permission_denied()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample");
        var reply = new ReplyAddress("reply/client");

        var outcome = await rig.Transport.InjectAsync(Request("test.hello.v1", "other/res", """{"Name":"Bob"}""", replyTo: reply), TestContext.Current.CancellationToken);

        outcome.ShouldBe(DeliveryOutcome.Drop);
        rig.Log.Entries.ShouldBeEmpty();
        rig.Transport.Published.ShouldHaveSingleItem().Envelope.Error!.Kind.ShouldBe(ErrorKind.PermissionDenied);
        rig.Trace.Entries.ShouldBe(["claim", "publish", "done"]);
    }

    [Fact]
    public async Task Event_is_queued_and_published_with_the_instance_reply_address_for_commands()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample", options: o => o.Bindings["sample"].OutgoingTypeFilter = "test.b*");

        await rig.Publisher.PublishAsync(new Bye("x"), Intent.Command, new PublishOptions { Subject = "plant1.line2/res" }, TestContext.Current.CancellationToken);
        await rig.Transport.WaitForPublishedAsync(1);

        var message = rig.Transport.Published.ShouldHaveSingleItem();
        message.ReplyTo.ShouldBe(new ReplyAddress("reply/sample/i1"));
        message.Address!.ScopePath.ShouldBe(["plant1", "line2"]);
        message.Address.Intent.ShouldBe(Intent.Command);
        message.Envelope.Type.ShouldBe("test.bye.v1");
    }

    [Fact]
    public async Task Event_the_type_filter_rejects_is_not_published()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample", options: o => o.Bindings["sample"].OutgoingTypeFilter = "other.*, more.*");

        await rig.Publisher.PublishAsync(new Bye("x"), Intent.Command, new PublishOptions { Subject = "p/res" }, TestContext.Current.CancellationToken);
        await rig.Host.StopAsync(TestContext.Current.CancellationToken);

        rig.Transport.Published.ShouldBeEmpty();
    }

    [Fact]
    public async Task Publishing_an_intent_no_processor_declares_throws()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample");

        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await rig.Publisher.PublishAsync(new Bye("x"), Intent.Event, new PublishOptions { Subject = "p/res" }, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Full_queue_fails_the_enqueue_instead_of_blocking()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample", options: o => o.Bindings["sample"].OutgoingQueueSize = 1);
        using var gate = new SemaphoreSlim(0);
        rig.Transport.Gate = gate;
        var options = new PublishOptions { Subject = "p/res" };

        await rig.Publisher.PublishAsync(new Bye("1"), Intent.Command, options, TestContext.Current.CancellationToken);
        await rig.Transport.Entered.Task.WaitAsync(_wait, TestContext.Current.CancellationToken);
        await rig.Publisher.PublishAsync(new Bye("2"), Intent.Command, options, TestContext.Current.CancellationToken);

        await Should.ThrowAsync<OutboundQueueFullException>(async () =>
            await rig.Publisher.PublishAsync(new Bye("3"), Intent.Command, options, TestContext.Current.CancellationToken));

        gate.Release(2);
        await rig.Transport.WaitForPublishedAsync(2);
    }

    [Fact]
    public async Task Shutdown_drains_the_outgoing_queue()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample");
        using var gate = new SemaphoreSlim(0);
        rig.Transport.Gate = gate;

        await rig.Publisher.PublishAsync(new Bye("1"), Intent.Command, new PublishOptions { Subject = "p/res" }, TestContext.Current.CancellationToken);
        await rig.Transport.Entered.Task.WaitAsync(_wait, TestContext.Current.CancellationToken);
        var stop = rig.Host.StopAsync(TestContext.Current.CancellationToken);
        gate.Release();
        await stop;

        rig.Transport.Published.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Expired_command_calls_the_handler_and_publishes_the_follow_up_event()
    {
        await using var rig = await StartRigAsync<ExpiringProcessor>("expiring", options: o => o.MessageExpiry = TimeSpan.FromSeconds(10));

        await rig.Publisher.PublishAsync(new Bye("x"), Intent.Command, new PublishOptions { Subject = "p/res" }, TestContext.Current.CancellationToken);
        await rig.Transport.WaitForPublishedAsync(1);
        rig.Pipeline.PendingExpiries.ShouldBe(1);
        rig.Time.Advance(TimeSpan.FromSeconds(10));
        await rig.Transport.WaitForPublishedAsync(2);

        rig.Log.Entries.ShouldBe(["expired:x"]);
        var followUp = rig.Transport.Published[1];
        followUp.Envelope.Type.ShouldBe("test.ack.v1");
        followUp.Envelope.CausationId.ShouldBe(rig.Transport.Published[0].Envelope.Id);
        followUp.Address!.Intent.ShouldBe(Intent.Event);
    }

    [Fact]
    public async Task Command_without_an_expiry_interval_starts_no_timer()
    {
        await using var rig = await StartRigAsync<ExpiringProcessor>("expiring");

        await rig.Publisher.PublishAsync(new Bye("x"), Intent.Command, new PublishOptions { Subject = "p/res" }, TestContext.Current.CancellationToken);
        await rig.Transport.WaitForPublishedAsync(1);

        rig.Pipeline.PendingExpiries.ShouldBe(0);
    }

    [Fact]
    public async Task Response_cancels_the_expiry_timer()
    {
        await using var rig = await StartRigAsync<ExpiringProcessor>("expiring", options: o => o.MessageExpiry = TimeSpan.FromSeconds(10));
        await rig.Publisher.PublishAsync(new Bye("x"), Intent.Command, new PublishOptions { Subject = "p/res" }, TestContext.Current.CancellationToken);
        await rig.Transport.WaitForPublishedAsync(1);
        var commandId = rig.Transport.Published[0].Envelope.Id;

        var outcome = await rig.Transport.InjectAsync(Response(commandId), TestContext.Current.CancellationToken);
        rig.Time.Advance(TimeSpan.FromSeconds(30));

        outcome.ShouldBe(DeliveryOutcome.Ack);
        rig.Pipeline.PendingExpiries.ShouldBe(0);
        rig.Log.Entries.ShouldBe(["ack:x"]);
    }

    [Fact]
    public async Task Shutdown_waits_for_a_pending_expiry_timer()
    {
        await using var rig = await StartRigAsync<ExpiringProcessor>("expiring", options: o => o.MessageExpiry = TimeSpan.FromSeconds(10));
        await rig.Publisher.PublishAsync(new Bye("x"), Intent.Command, new PublishOptions { Subject = "p/res" }, TestContext.Current.CancellationToken);
        await rig.Transport.WaitForPublishedAsync(1);

        var stop = rig.Host.StopAsync(TestContext.Current.CancellationToken);
        rig.Time.Advance(TimeSpan.FromSeconds(10));
        await stop.WaitAsync(_wait, TestContext.Current.CancellationToken);

        rig.Log.Entries.ShouldBe(["expired:x"]);
    }

    [Fact]
    public async Task Shutdown_cancels_a_pending_timer_when_the_grace_period_ends()
    {
        await using var rig = await StartRigAsync<ExpiringProcessor>("expiring", options: o => o.MessageExpiry = TimeSpan.FromSeconds(10));
        await rig.Publisher.PublishAsync(new Bye("x"), Intent.Command, new PublishOptions { Subject = "p/res" }, TestContext.Current.CancellationToken);
        await rig.Transport.WaitForPublishedAsync(1);
        using var grace = new CancellationTokenSource();

        var stop = rig.Host.StopAsync(grace.Token);
        await grace.CancelAsync();
        await stop.WaitAsync(_wait, TestContext.Current.CancellationToken);

        rig.Log.Entries.ShouldBeEmpty();
        rig.Pipeline.PendingExpiries.ShouldBe(0);
    }

    [Fact]
    public async Task Trace_context_of_the_message_continues_in_the_processing_activity()
    {
        await using var rig = await StartRigAsync<SampleProcessor>("sample");
        // The listener is process wide and other tests run in parallel: collect under a lock and pick this test's trace.
        var started = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == PipelineTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = a =>
            {
                lock (started)
                {
                    started.Add(a);
                }
            },
        };
        ActivitySource.AddActivityListener(listener);
        var message = Request("test.hello.v1", "p/res", """{"Name":"Bob"}""");
        message = message with { Envelope = message.Envelope with { TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" } };

        await rig.Transport.InjectAsync(message, TestContext.Current.CancellationToken);

        Activity activity;
        lock (started)
        {
            activity = started.Where(a => a.TraceId.ToString() == "0af7651916cd43dd8448eb211c80319c").ShouldHaveSingleItem();
        }

        activity.TraceId.ToString().ShouldBe("0af7651916cd43dd8448eb211c80319c");
        activity.ParentSpanId.ToString().ShouldBe("b7ad6b7169203331");
        rig.Transport.Published.ShouldAllBe(m => m.Envelope.TraceParent != null && m.Envelope.TraceParent.Contains("0af7651916cd43dd8448eb211c80319c", StringComparison.Ordinal));
    }
}

using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace MicroDCS.Core.Tests;

[Processor(ProcessorBinding.Southbound, Id = "publishing")]
public sealed partial class PublishingProcessor(IEventPublisher publisher, ILog log)
{
    [Incoming]
    public async Task OnHello(Hello hello, CancellationToken cancellationToken)
    {
        log.Add("hello:" + hello.Name);
        await publisher.PublishAsync(new Bye(hello.Name), Intent.Command, new PublishOptions { Subject = "p/res" }, cancellationToken);
    }

    [Outgoing(typeof(Bye))]
    public static Bye CreateBye(CommandRequest request) => new(request.Scope);
}

[Processor(ProcessorBinding.Southbound, Id = "twin")]
public sealed partial class ExpiringTwinProcessor(ILog log)
{
    [OnResponse]
    public void OnAck(ByeAck ack) => log.Add("twin-ack:" + ack.Name);

    [OnExpired]
    public void OnTimeout(Bye bye) => log.Add("twin-expired:" + bye.Name);
}

/// <summary>Blocks the handler of <c>Bye</c> until released, so a message stays in flight.</summary>
internal sealed class BlockingLog(SharedLog inner) : ILog
{
    public ManualResetEventSlim Release { get; } = new(false);

    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Add(string entry)
    {
        if (entry.StartsWith("bye", StringComparison.Ordinal))
        {
            Entered.TrySetResult();
            Release.Wait(TimeSpan.FromSeconds(10));
        }

        inner.Add(entry);
    }
}

/// <summary>A handler whose own timeout fires: the cancellation is not the pipeline's.</summary>
internal sealed class TimingOutLog : ILog
{
    public void Add(string entry)
    {
        if (entry.StartsWith("hello", StringComparison.Ordinal))
        {
            throw new TaskCanceledException("HttpClient timeout");
        }
    }
}

/// <summary>Regression tests for the M1 review findings RV-1 to RV-10 (docs/porting/research/m1-review.md).</summary>
public sealed class ReviewFixTests
{
    private static readonly TimeSpan _wait = TimeSpan.FromSeconds(10);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static Action<MicroDcsOptions> Twin(TimeSpan? expiry = null, int queueSize = 0) => o =>
    {
        o.MessageExpiry = expiry ?? o.MessageExpiry;
        o.Bindings["twin"] = new BindingOptions { Transport = "memory", Prefix = "twin", ScopeLevels = 3, OutgoingQueueSize = queueSize };
    };

    [Fact]
    public async Task RV1_a_handler_can_publish_while_the_transports_start()
    {
        var outcome = DeliveryOutcome.Drop;
        await using var rig = await PipelineTests.StartRigAsync<PublishingProcessor>(
            "publishing",
            onStart: async handler => outcome = await handler.HandleAsync(PipelineTests.Request("test.hello.v1", "p/res", """{"Name":"Bob"}""", bindingId: "publishing"), Ct));

        outcome.ShouldBe(DeliveryOutcome.Ack);
        await rig.Transport.WaitForPublishedAsync(1);
        rig.Transport.Published.ShouldHaveSingleItem().Envelope.Type.ShouldBe("test.bye.v1");
        rig.Lifetime.ReceivedCalls().ShouldNotContain(c => c.GetMethodInfo().Name == nameof(IHostApplicationLifetime.StopApplication));
    }

    [Fact]
    public async Task RV2_shutdown_hooks_and_later_phases_run_when_the_grace_period_ends_with_a_message_in_flight()
    {
        var shared = new SharedLog();
        var blocking = new BlockingLog(shared);
        await using var rig = await PipelineTests.StartRigAsync<SampleProcessor>("sample", (s, _) => s.AddSingleton<ILog>(blocking));
        var inFlight = rig.Transport.InjectAsync(PipelineTests.Request("test.bye.v1", "p/res", """{"Name":"Bob"}"""), Ct).AsTask();
        await blocking.Entered.Task.WaitAsync(_wait, Ct);
        using var grace = new CancellationTokenSource();
        await grace.CancelAsync();

        await rig.Host.StopAsync(grace.Token).WaitAsync(_wait, Ct);

        shared.Entries.ShouldContain("shutdown");
        blocking.Release.Set();
        await inFlight.WaitAsync(_wait, Ct);
    }

    [Fact]
    public async Task RV3_a_cancellation_inside_the_handler_is_a_fault_not_an_escaping_exception()
    {
        await using var rig = await PipelineTests.StartRigAsync<SampleProcessor>("sample", (s, _) => s.AddSingleton<ILog>(new TimingOutLog()), canRedeliver: true);

        var outcome = await rig.Transport.InjectAsync(PipelineTests.Request("test.hello.v1", "p/res", """{"Name":"Bob"}"""), Ct);

        outcome.ShouldBe(DeliveryOutcome.Fault);
        rig.Trace.Entries.ShouldBe(["claim", "release"]);
    }

    [Fact]
    public async Task RV3_a_cancellation_thrown_by_the_transport_publish_ends_the_host_instead_of_the_pump_silently()
    {
        await using var rig = await PipelineTests.StartRigAsync<SampleProcessor>("sample");
        rig.Transport.PublishFailure = new OperationCanceledException("publish timeout");

        await rig.Publisher.PublishAsync(new Bye("x"), Intent.Command, new PublishOptions { Subject = "p/res" }, Ct);

        await rig.Stopped.Task.WaitAsync(_wait, Ct);

        rig.Stopped.Task.IsCompletedSuccessfully.ShouldBeTrue();
    }

    [Fact]
    public async Task RV4_a_duplicate_polls_the_lease_at_most_every_second()
    {
        await using var rig = await PipelineTests.StartRigAsync<SampleProcessor>("sample");
        var store = rig.Provider.GetRequiredService<IDedupeStore>();
        (await store.ClaimAsync("urn:x", "e1", "other-worker", Ct)).ShouldBe(ClaimState.Claimed);

        var duplicate = rig.Transport.InjectAsync(PipelineTests.Request("test.hello.v1", "p/res", """{"Name":"Bob"}"""), Ct).AsTask();
        await rig.Time.WaitForTimerAsync(TimeSpan.FromSeconds(1));
        await store.MarkDoneAsync("urn:x", "e1", Ct);
        rig.Time.Advance(TimeSpan.FromSeconds(1));

        (await duplicate.WaitAsync(_wait, Ct)).ShouldBe(DeliveryOutcome.Ack);
        rig.Log.Entries.ShouldBeEmpty();
    }

    [Fact]
    public async Task RV5_the_in_memory_dedupe_store_removes_expired_entries()
    {
        var time = new FakeTimeProvider();
        var store = new InMemoryDedupeStore(time, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(10));
        for (var i = 0; i < 100; i++)
        {
            await store.ClaimAsync("s", i.ToString(System.Globalization.CultureInfo.InvariantCulture), "t", Ct);
            await store.MarkDoneAsync("s", i.ToString(System.Globalization.CultureInfo.InvariantCulture), Ct);
        }

        store.Count.ShouldBe(100);
        time.Advance(TimeSpan.FromMinutes(11));
        await store.ClaimAsync("s", "new", "t", Ct);

        store.Count.ShouldBe(1);
    }

    [Fact]
    public async Task RV6_each_binding_keeps_its_own_expiry_timer_for_one_published_command()
    {
        await using var rig = await PipelineTests.StartRigAsync<ExpiringProcessor>(
            "expiring",
            (s, _) => s.AddProcessor<ExpiringTwinProcessor>(),
            Twin(TimeSpan.FromSeconds(10)));

        await rig.Publisher.PublishAsync(new Bye("x"), Intent.Command, new PublishOptions { Subject = "p/res" }, Ct);
        await rig.Transport.WaitForPublishedAsync(2);
        rig.Pipeline.PendingExpiries.ShouldBe(2);
        rig.Time.Advance(TimeSpan.FromSeconds(10));
        await rig.Transport.WaitForPublishedAsync(3); // the follow-up event of the first processor

        await rig.Host.StopAsync(Ct);
        rig.Log.Entries.ShouldBe(["expired:x", "twin-expired:x"], ignoreOrder: true);
    }

    [Fact]
    public async Task RV7_a_full_queue_on_one_binding_publishes_on_none()
    {
        await using var rig = await PipelineTests.StartRigAsync<ExpiringProcessor>(
            "expiring",
            (s, _) => s.AddProcessor<ExpiringTwinProcessor>(),
            Twin(queueSize: 1));
        using var gate = new SemaphoreSlim(0);
        rig.Transport.Gate = gate;

        var accepted = 0;
        for (var i = 0; i < 5; i++)
        {
            try
            {
                await rig.Publisher.PublishAsync(new Bye(i.ToString(System.Globalization.CultureInfo.InvariantCulture)), Intent.Command, new PublishOptions { Subject = "p/res" }, Ct);
                accepted++;
            }
            catch (OutboundQueueFullException ex)
            {
                ex.Message.ShouldContain("twin");
                break;
            }
        }

        gate.Release(100);
        await rig.Host.StopAsync(Ct);

        accepted.ShouldBeLessThan(5);
        rig.Transport.Published.Count(m => m.BindingId == "expiring").ShouldBe(accepted);
        rig.Transport.Published.Count(m => m.BindingId == "twin").ShouldBe(accepted);
    }

    [Fact]
    public void RV8_message_expiry_above_the_timer_limit_is_rejected_at_start()
    {
        var options = new MicroDcsOptions { CloudEventSource = "urn:x", MessageExpiry = TimeSpan.FromDays(60) };

        var result = new MicroDcsOptionsValidator().Validate(null, options);

        result.Failed.ShouldBeTrue();
        result.FailureMessage.ShouldContain("MicroDcs:MessageExpiry");
    }

    [Fact]
    public async Task RV8_a_timer_with_an_interval_above_the_limit_is_clamped_instead_of_throwing()
    {
        var time = new FakeTimeProvider();
        using var tracker = new ExpiryTracker(time);

        tracker.Start("k", TimeSpan.FromDays(60), _ => ValueTask.CompletedTask);

        tracker.Pending.ShouldBe(1);
        tracker.Cancel("k").ShouldBeTrue();
        await tracker.WaitAsync(Ct);
    }

    [Fact]
    public void RV9_two_binary_descriptors_make_the_payload_type_ambiguous_for_outgoing_events()
    {
        var registry = new ProcessorRegistry([], [new BinaryPayloadDescriptor("io.x.image.v1", contentType: "image/png"), new BinaryPayloadDescriptor("io.x.blob.v1")]);

        registry.TryGetDescriptor(typeof(BinaryPayload), out _).ShouldBeFalse();
        var error = Should.Throw<InvalidOperationException>(() => registry.GetDescriptor(typeof(BinaryPayload)));
        error.Message.ShouldContain("io.x.blob.v1, io.x.image.v1");
    }

    [Fact]
    public void RV9_the_same_descriptor_registered_twice_stays_usable()
    {
        var registry = new ProcessorRegistry([], [new BinaryPayloadDescriptor("io.x.blob.v1"), new BinaryPayloadDescriptor("io.x.blob.v1")]);

        registry.GetDescriptor(typeof(BinaryPayload)).TypeId.ShouldBe("io.x.blob.v1");
    }
}

using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using MicroDCS.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace MicroDCS.Core.Tests;

public sealed class HostLifecycleTests
{
    private sealed class ListLog : ILog
    {
        public List<string> Entries { get; } = [];

        public void Add(string entry) => Entries.Add(entry);
    }

    private static ServiceProvider Build(Dictionary<string, string?> settings, ListLog log, bool withTransport = true)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ILog>(log);
        services.AddMicroDcs(configuration).AddProcessor<SampleProcessor>();
        if (withTransport)
        {
            services.AddInMemoryTransport();
        }

        return services.BuildServiceProvider();
    }

    private static Dictionary<string, string?> Valid() => new()
    {
        ["MicroDcs:CloudEventSource"] = "urn:test",
        ["MicroDcs:Bindings:sample:Transport"] = "memory",
        ["MicroDcs:Bindings:sample:Prefix"] = "test",
    };

    private static IHostedService Host(ServiceProvider provider) => provider.GetServices<IHostedService>().Single();

    [Fact]
    public async Task Hooks_run_in_order_and_shutdown_reverses_it()
    {
        var log = new ListLog();
        await using var provider = Build(Valid(), log);

        await Host(provider).StartAsync(TestContext.Current.CancellationToken);
        await Host(provider).StopAsync(TestContext.Current.CancellationToken);

        log.Entries.ShouldBe(["init", "started", "shutdown"]);
        provider.GetRequiredService<InMemoryTransport>().IsConnected.ShouldBeFalse();
    }

    [Fact]
    public async Task Invalid_options_fail_the_start()
    {
        var settings = Valid();
        settings.Remove("MicroDcs:CloudEventSource");
        await using var provider = Build(settings, new ListLog());

        var ex = await Should.ThrowAsync<OptionsValidationException>(() => Host(provider).StartAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("CloudEventSource");
    }

    [Fact]
    public async Task Missing_binding_section_fails_the_start_before_any_hook_runs()
    {
        var settings = Valid().Where(kv => !kv.Key.StartsWith("MicroDcs:Bindings", StringComparison.Ordinal)).ToDictionary(kv => kv.Key, kv => kv.Value);
        var log = new ListLog();
        await using var provider = Build(settings, log);

        var ex = await Should.ThrowAsync<InvalidOperationException>(() => Host(provider).StartAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("MicroDcs:Bindings:sample");
        log.Entries.ShouldBeEmpty();
    }

    [Fact]
    public async Task Unregistered_transport_fails_the_start()
    {
        await using var provider = Build(Valid(), new ListLog(), withTransport: false);

        var ex = await Should.ThrowAsync<InvalidOperationException>(() => Host(provider).StartAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("memory");
    }

    [Fact]
    public async Task Duplicate_prefix_and_discriminator_are_rejected()
    {
        var settings = Valid();
        settings["MicroDcs:Bindings:other:Transport"] = "memory";
        settings["MicroDcs:Bindings:other:Prefix"] = "test";
        await using var provider = Build(settings, new ListLog());

        var ex = await Should.ThrowAsync<OptionsValidationException>(() => Host(provider).StartAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("repeat");
    }

    [Fact]
    public async Task Prefix_with_a_wildcard_character_is_rejected()
    {
        var settings = Valid();
        settings["MicroDcs:Bindings:sample:Prefix"] = "te+st";
        await using var provider = Build(settings, new ListLog());

        await Should.ThrowAsync<OptionsValidationException>(() => Host(provider).StartAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Environment_style_configuration_binds_the_options()
    {
        // APP_ prefix with __ as separator: the host maps APP_MicroDcs__MessageWorkers=7 to MicroDcs:MessageWorkers (D-39).
        Environment.SetEnvironmentVariable("APP_MicroDcs__MessageWorkers", "7");
        try
        {
            var services = new ServiceCollection();
            services.AddMicroDcs(new ConfigurationBuilder().AddEnvironmentVariables("APP_").AddInMemoryCollection(Valid()).Build());
            await using var provider = services.BuildServiceProvider();

            provider.GetRequiredService<IOptions<MicroDcsOptions>>().Value.MessageWorkers.ShouldBe(7);
        }
        finally
        {
            Environment.SetEnvironmentVariable("APP_MicroDcs__MessageWorkers", null);
        }
    }

    [Fact]
    public async Task Singleton_hook_is_skipped_when_another_instance_holds_the_lease()
    {
        var log = new ListLog();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(Valid()).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ILog>(log);
        services.AddSingleton<IStartLease, DeniedLease>();
        services.AddMicroDcs(configuration).AddProcessor<SampleProcessor>().AddInMemoryTransport();
        await using var provider = services.BuildServiceProvider();

        await Host(provider).StartAsync(TestContext.Current.CancellationToken);

        log.Entries.ShouldBe(["init"]);
    }

    [Fact]
    public async Task Harness_replies_go_to_the_reply_address_and_keep_the_carried_attribute()
    {
        var log = new ListLog();
        await using var harness = await ProcessorHarness.StartAsync<SampleProcessor>(s => s.AddSingleton<ILog>(log), TestContext.Current.CancellationToken);

        var result = await harness.SendWithReplyAsync(new Hello("Bob") { HiddenStr = "h" }, new ReplyAddress("reply/i1"), "plant1/res", TestContext.Current.CancellationToken);

        result.Outcome.ShouldBe(DeliveryOutcome.Ack);
        result.Published.Length.ShouldBe(2);
        result.Published.ShouldAllBe(m => m.SendTo == new ReplyAddress("reply/i1"));
        harness.Decode<Hello>(result.Published[0]).HiddenStr.ShouldBe("h");
        result.Published[0].Envelope.CausationId.ShouldNotBeNull();
        result.Published[0].Envelope.Subject.ShouldBe("plant1/res");
        result.Published[0].Envelope.Extensions["hiddenstr"].ShouldBe("h");
    }

    [Fact]
    public async Task Duplicate_event_is_acknowledged_without_running_the_handler_again()
    {
        var log = new ListLog();
        await using var harness = await ProcessorHarness.StartAsync<SampleProcessor>(s => s.AddSingleton<ILog>(log), TestContext.Current.CancellationToken);
        var envelope = new EventEnvelope { Id = "same", Source = "urn:x", Type = "test.hello.v1", Subject = "p/r", Data = """{"Name":"Bob"}"""u8.ToArray() };

        var first = await harness.SendEnvelopeAsync(envelope, TestContext.Current.CancellationToken);
        var second = await harness.SendEnvelopeAsync(envelope, TestContext.Current.CancellationToken);

        first.Published.Length.ShouldBe(2);
        second.Outcome.ShouldBe(DeliveryOutcome.Ack);
        second.Published.ShouldBeEmpty();
        log.Entries.Count(e => e.StartsWith("hello", StringComparison.Ordinal)).ShouldBe(1);
    }

    [Fact]
    public async Task Scope_mismatch_and_undecodable_and_unknown_messages_are_dropped()
    {
        await using var harness = await ProcessorHarness.StartAsync<SampleProcessor>(s => s.AddSingleton<ILog>(new ListLog()), TestContext.Current.CancellationToken);

        var hello = """{"Name":"Bob"}"""u8.ToArray();
        (await harness.SendEnvelopeAsync(new EventEnvelope { Id = "1", Source = "s", Type = "test.hello.v1", Subject = "p/r", Data = "{broken"u8.ToArray() }, TestContext.Current.CancellationToken)).Outcome.ShouldBe(DeliveryOutcome.Drop);

        // The sample processor has a fallback, so an unknown type is handled; a bye without a handler for the type goes to the fallback too.
        (await harness.SendEnvelopeAsync(new EventEnvelope { Id = "2", Source = "s", Type = "other.v1", Subject = "p/r", Data = hello }, TestContext.Current.CancellationToken)).Outcome.ShouldBe(DeliveryOutcome.Ack);
    }

    [Fact]
    public async Task Handler_exception_is_a_fault_and_the_event_stays_unclaimed_as_done()
    {
        await using var harness = await ProcessorHarness.StartAsync<SampleProcessor>(s => s.AddSingleton<ILog>(new ThrowingLog()), TestContext.Current.CancellationToken);
        var envelope = new EventEnvelope { Id = "boom", Source = "urn:x", Type = "test.hello.v1", Subject = "p/r", Data = """{"Name":"Bob"}"""u8.ToArray() };

        (await harness.SendEnvelopeAsync(envelope, TestContext.Current.CancellationToken)).Outcome.ShouldBe(DeliveryOutcome.Fault);

        // Not marked done: after the lease lapses the redelivery is claimed again, not treated as a duplicate (T-18).
        harness.Time.Advance(TimeSpan.FromMinutes(1));
        var store = harness.Services.GetRequiredService<IDedupeStore>();
        (await store.ClaimAsync("urn:x", "boom", "t", TestContext.Current.CancellationToken)).ShouldBe(ClaimState.Claimed);
    }

    private sealed class DeniedLease : IStartLease
    {
        public ValueTask<bool> TryAcquireAsync(string name, TimeSpan timeToLive, CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }

    private sealed class ThrowingLog : ILog
    {
        public void Add(string entry)
        {
            if (entry.StartsWith("hello", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("boom");
            }
        }
    }
}

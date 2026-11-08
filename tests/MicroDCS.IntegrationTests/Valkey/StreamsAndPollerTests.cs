using MicroDCS.Valkey.Streams;
using StackExchange.Redis;

namespace MicroDCS.IntegrationTests.Valkey;

[Trait("Category", "Integration")]
public sealed class StreamsAndPollerTests
{
    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task TrimmingNeverRemovesPendingOrUndeliveredEntries()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var streams = new ValkeyStreams(env.Context);
        var stream = env.Keys.SfcWork("s");
        await streams.EnsureGroupAsync(stream, "g", ct);
        await streams.EnsureGroupAsync(stream, "g", ct);

        for (var i = 0; i < 5; i++)
        {
            await streams.AppendAsync(stream, "sfc", "s", [new("n", i.ToString(System.Globalization.CultureInfo.InvariantCulture))], ct);
        }

        var read = await streams.ReadAsync(stream, "g", "c1", 3, ct);
        read.Count.ShouldBe(3);

        // Acknowledge the first and the third; the second stays pending, so nothing at or after it may go.
        await streams.AcknowledgeAsync(stream, "g", [read[0].Id, read[2].Id], ct);
        var remaining = await env.Database.StreamRangeAsync(stream);
        remaining.Select(e => e.Id.ToString()).ShouldNotContain(read[0].Id.ToString());
        remaining.Select(e => e.Id.ToString()).ShouldContain(read[1].Id.ToString());
        remaining.Length.ShouldBe(4);

        // Acknowledging the pending one releases everything delivered; the two undelivered entries stay.
        await streams.AcknowledgeAsync(stream, "g", [read[1].Id], ct);
        var left = await env.Database.StreamRangeAsync(stream);
        left.Select(e => e.Values[0].Value.ToString()).ShouldContain("3");
        left.Select(e => e.Values[0].Value.ToString()).ShouldContain("4");
        (await streams.ReadAsync(stream, "g", "c1", 10, ct)).Count.ShouldBe(2);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task AutoclaimTakesOverIdleEntriesAndCountsDeletedOnes()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var streams = new ValkeyStreams(env.Context);
        var stream = env.Keys.SfcWork("s");
        await streams.EnsureGroupAsync(stream, "g", ct);
        await streams.AppendAsync(stream, "sfc", "s", [new("n", "1")], ct);
        await streams.AppendAsync(stream, "sfc", "s", [new("n", "2")], ct);
        var read = await streams.ReadAsync(stream, "g", "dead-consumer", 10, ct);
        read.Count.ShouldBe(2);

        // Another process trims an entry that is still pending.
        await env.Database.StreamDeleteAsync(stream, [read[0].Id]);
        await Task.Delay(50, ct);

        var claimed = await streams.AutoClaimAsync(stream, "g", "c2", TimeSpan.FromMilliseconds(10), 10, ct);
        claimed.Select(e => e.Id.ToString()).ShouldBe([read[1].Id.ToString()]);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task ANudgeWakesThePollerBeforeTheFallbackInterval()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync(o => o.PollInterval = TimeSpan.FromSeconds(30));
        await env.Database.SetAddAsync(env.Keys.Scopes(), "s");
        var consumer = new RecordingConsumer(env, "t");
        using var poller = new ValkeyPoller(env.Context, [consumer], []);
        await poller.StartAsync(ct);
        try
        {
            await consumer.Discovered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
            var streams = new ValkeyStreams(env.Context);
            await streams.AppendAsync(env.Keys.SfcWork("s"), "t", "s", [new("n", "1")], ct);

            (await consumer.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5), ct)).ShouldBe("1");
        }
        finally
        {
            await poller.StopAsync(CancellationToken.None);
        }
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task TheFallbackPollFindsEntriesWhenTheNudgeIsLost()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync(o => o.PollInterval = TimeSpan.FromMilliseconds(200));
        await env.Database.SetAddAsync(env.Keys.Scopes(), "s");
        var consumer = new RecordingConsumer(env, "t");
        using var poller = new ValkeyPoller(env.Context, [consumer], []);
        await poller.StartAsync(ct);
        try
        {
            await consumer.Discovered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);

            // No PUBLISH: only the fallback poll can find it.
            await env.Database.StreamAddAsync(env.Keys.SfcWork("s"), "n", "2");

            (await consumer.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5), ct)).ShouldBe("2");
        }
        finally
        {
            await poller.StopAsync(CancellationToken.None);
        }
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task AScopeThatAppearsLaterIsDiscovered()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync(o =>
        {
            o.PollInterval = TimeSpan.FromMilliseconds(100);
            o.ScopeRefreshInterval = TimeSpan.FromMilliseconds(100);
        });
        var consumer = new RecordingConsumer(env, "t");
        using var poller = new ValkeyPoller(env.Context, [consumer], []);
        await poller.StartAsync(ct);
        try
        {
            await env.Database.SetAddAsync(env.Keys.Scopes(), "s");
            await consumer.Discovered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
            poller.Scopes.ShouldBe(["s"]);
        }
        finally
        {
            await poller.StopAsync(CancellationToken.None);
        }
    }

    private sealed class RecordingConsumer(ValkeyTestEnvironment env, string kind) : IStreamConsumer
    {
        private readonly ValkeyStreams _streams = new(env.Context);

        public TaskCompletionSource Discovered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<string> Handled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string Kind => kind;

        public async ValueTask ScopeDiscoveredAsync(string scope, CancellationToken cancellationToken)
        {
            await _streams.EnsureGroupAsync(env.Keys.SfcWork(scope), "g", cancellationToken);
            Discovered.TrySetResult();
        }

        public async ValueTask<bool> PollAsync(string scope, CancellationToken cancellationToken)
        {
            var entries = await _streams.ReadAsync(env.Keys.SfcWork(scope), "g", "c", 10, cancellationToken);
            foreach (var entry in entries)
            {
                Handled.TrySetResult(entry.Values[0].Value.ToString());
            }

            await _streams.AcknowledgeAsync(env.Keys.SfcWork(scope), "g", [.. entries.Select(static e => e.Id)], cancellationToken);
            return entries.Count > 0;
        }
    }
}

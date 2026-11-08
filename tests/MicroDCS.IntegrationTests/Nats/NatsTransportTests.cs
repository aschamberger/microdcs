using System.Text;
using MicroDCS.App.Greetings;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using MicroDCS.Transport.Nats;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;

namespace MicroDCS.IntegrationTests.Nats;

/// <summary>End-to-end tests of the NATS transport against a JetStream server in the compose harness (D-15, S14).</summary>
[Trait("Category", "Integration")]
public sealed class NatsTransportTests
{
    private const string Skip = $"{TestEndpoints.NatsVariable} is not set";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task EventuallyAsync(Func<bool> condition, int seconds = 15)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (!condition())
        {
            (DateTime.UtcNow < deadline).ShouldBeTrue($"The condition did not hold within {seconds} seconds.");
            await Task.Delay(20, Ct);
        }
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task Hello_over_nats_is_answered_on_the_reply_subject()
    {
        await using var host = await NatsTestHost.StartAsync(ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync();
        var reply = $"{host.Prefix}-test.replies";
        await client.SubscribeAsync(reply, Ct);

        await client.PublishEventAsync($"{host.Prefix}.site.area.events", "req-1", NatsTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", reply, Ct);

        var names = new List<string>();
        for (var i = 0; i < 2; i++)
        {
            var message = await client.ReceiveAsync(Ct);
            NatsTestClient.Header(message, "type").ShouldBe(NatsTestHost.HelloType);
            NatsTestClient.Header(message, "causationid").ShouldBe("req-1");
            NatsTestClient.Header(message, "subject").ShouldBe("site.area");
            names.Add(Encoding.UTF8.GetString(message.Data!));
        }

        names.ShouldContain(n => n.Contains("Bob", StringComparison.Ordinal));
        names.ShouldContain(n => n.Contains("Alice", StringComparison.Ordinal));
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_duplicate_is_processed_once_and_every_delivery_is_acknowledged()
    {
        await using var host = await NatsTestHost.StartAsync(ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync();
        var reply = $"{host.Prefix}-test.replies";
        await client.SubscribeAsync(reply, Ct);
        var subject = $"{host.Prefix}.site.area.events";

        // Sequential on purpose: a duplicate that arrives while the first is still running waits for its lease (T-20).
        // The publisher-side message id differs per publish here because the stream would drop an identical one; the
        // pipeline's dedupe is what this test is about, so the second publish carries a new header-only difference.
        var causes = new List<string>();
        foreach (var (id, name) in new[] { ("dup-1", "Bob"), ("dup-1", "Bob"), ("next-1", "Eve") })
        {
            await client.PublishEventAsync(subject, id, NatsTestHost.HelloType, "site.area", $"{{\"Name\":\"{name}\"}}", reply, Ct);
            if (id == "dup-1" && causes.Count == 0)
            {
                causes.Add(NatsTestClient.Header(await client.ReceiveAsync(Ct), "causationid"));
                causes.Add(NatsTestClient.Header(await client.ReceiveAsync(Ct), "causationid"));
                await EventuallyAsync(() => host.Transport.Status.Acknowledged == 1);
            }
        }

        causes.Add(NatsTestClient.Header(await client.ReceiveAsync(Ct), "causationid"));
        causes.Add(NatsTestClient.Header(await client.ReceiveAsync(Ct), "causationid"));

        causes.ShouldBe(["dup-1", "dup-1", "next-1", "next-1"]);
        await EventuallyAsync(() => host.Transport.Status.Acknowledged == 3);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_subject_outside_the_scope_is_dropped_with_an_error_event()
    {
        await using var host = await NatsTestHost.StartAsync(ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync();
        var reply = $"{host.Prefix}-test.replies";
        await client.SubscribeAsync(reply, Ct);

        await client.PublishEventAsync($"{host.Prefix}.site.area.events", "evil-1", NatsTestHost.HelloType, "other.area", "{\"Name\":\"Bob\"}", reply, Ct);

        var error = await client.ReceiveAsync(Ct);
        NatsTestClient.Header(error, "type").ShouldBe("io.microdcs.error.v1");
        NatsTestClient.Header(error, "mdcserrorkind").ShouldBe("PermissionDenied", StringCompareShould.IgnoreCase);
        await EventuallyAsync(() => host.Transport.Status.Acknowledged == 1);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task An_undecodable_message_is_terminated_and_counted()
    {
        await using var host = await NatsTestHost.StartAsync(ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync();
        var reply = $"{host.Prefix}-test.replies";
        await client.SubscribeAsync(reply, Ct);
        var subject = $"{host.Prefix}.site.area.events";

        // No headers at all: not a CloudEvent.
        await client.PublishRawAsync(subject, "garbage"u8.ToArray(), Ct);
        await client.PublishEventAsync(subject, "ok-1", NatsTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", reply, Ct);

        NatsTestClient.Header(await client.ReceiveAsync(Ct), "causationid").ShouldBe("ok-1");
        await EventuallyAsync(() => host.Transport.Status.Acknowledged == 2);
        host.Transport.Status.Dropped.ShouldBe(1);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_command_carries_the_per_instance_reply_subject_and_the_time_to_live()
    {
        await using var host = await NatsTestHost.StartAsync(ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync();
        await client.SubscribeAsync($"{host.Prefix}.site.area.commands", Ct);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        await publisher.PublishAsync(new Bye("site.area"), Intent.Command, new PublishOptions { Subject = "site.area", EventId = "cmd-1" }, Ct);

        var command = await client.ReceiveAsync(Ct);
        NatsTestClient.Header(command, NatsMessageMapper.ReplyToHeader).ShouldBe(host.ReplySubject);
        NatsTestClient.Header(command, "id").ShouldBe("cmd-1");
        NatsTestClient.Header(command, NatsMessageMapper.MessageIdHeader).ShouldContain("cmd-1");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_response_on_the_instance_reply_subject_reaches_the_pipeline()
    {
        await using var host = await NatsTestHost.StartAsync(ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync();

        await client.PublishCoreEventAsync(host.ReplySubject, "resp-1", NatsTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", Ct);

        await EventuallyAsync(() => host.Transport.Status.Received == 1);
        host.Transport.Status.Dropped.ShouldBe(0);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task State_topics_are_keys_with_a_time_to_live_and_are_deleted()
    {
        await using var host = await NatsTestHost.StartAsync(ct: Ct);
        var port = host.Services.GetRequiredService<IStateTopicPort>();
        port.ShouldBeSameAs(host.Transport);
        var address = new LogicalAddress(host.Prefix, ["site", "area"], null, Intent.Data);
        await using var client = await NatsTestClient.ConnectAsync();
        var store = await new NatsKVContext(new NatsJSContext(client.Connection)).GetStoreAsync("it-state-" + host.Prefix, Ct);
        var key = $"{host.Prefix}.site.area.data.order.42";

        await port.PublishAsync(address, "order/42", "state-1"u8.ToArray(), TimeSpan.FromMinutes(5), Ct);
        var entry = await store.GetEntryAsync<byte[]>(key, serializer: NatsRawSerializer<byte[]>.Default, cancellationToken: Ct);
        Encoding.UTF8.GetString(entry.Value!).ShouldBe("state-1");

        await port.DeleteAsync(address, "order/42", Ct);
        await Should.ThrowAsync<NatsKVKeyDeletedException>(async () => await store.GetEntryAsync<byte[]>(key, serializer: NatsRawSerializer<byte[]>.Default, cancellationToken: Ct));

        // The server removes the value when the time to live ends, with nobody to delete it (the publisher is gone).
        await port.PublishAsync(address, "order/43", "short"u8.ToArray(), TimeSpan.FromSeconds(1), Ct);
        var shortKey = $"{host.Prefix}.site.area.data.order.43";
        await EventuallyAsync(() =>
        {
            try
            {
                _ = store.GetEntryAsync<byte[]>(shortKey, serializer: NatsRawSerializer<byte[]>.Default, cancellationToken: Ct).AsTask().GetAwaiter().GetResult();
                return false;
            }
            catch (NatsKVException)
            {
                return true;
            }
        });
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_shared_group_splits_the_load_between_instances()
    {
        var prefix = "it" + Guid.NewGuid().ToString("N")[..10];
        await using var one = await NatsTestHost.StartAsync(prefix, "workers", ct: Ct);
        await using var two = await NatsTestHost.StartAsync(prefix, "workers", ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync();
        var reply = $"{prefix}-test.replies";
        await client.SubscribeAsync(reply, Ct);

        const int Count = 20;
        for (var i = 0; i < Count; i++)
        {
            await client.PublishEventAsync($"{prefix}.site.area.events", "m-" + i, NatsTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", reply, Ct);
        }

        var causes = new HashSet<string>();
        for (var i = 0; i < Count * 2; i++)
        {
            causes.Add(NatsTestClient.Header(await client.ReceiveAsync(Ct), "causationid"));
        }

        causes.Count.ShouldBe(Count);
        (one.Transport.Status.Received + two.Transport.Status.Received).ShouldBe(Count);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task Instances_without_a_group_each_receive_every_message()
    {
        var prefix = "it" + Guid.NewGuid().ToString("N")[..10];
        await using var one = await NatsTestHost.StartAsync(prefix, ct: Ct);
        await using var two = await NatsTestHost.StartAsync(prefix, ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync();

        var reply = $"{prefix}-test.replies";
        await client.SubscribeAsync(reply, Ct);

        // With a reply subject: without one the replies would go back to the events subject, and both instances would answer each other.
        await client.PublishEventAsync($"{prefix}.site.area.events", "all-1", NatsTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", reply, Ct);

        for (var i = 0; i < 4; i++)
        {
            NatsTestClient.Header(await client.ReceiveAsync(Ct), "causationid").ShouldBe("all-1");
        }

        one.Transport.Status.Received.ShouldBe(1);
        two.Transport.Status.Received.ShouldBe(1);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_faulted_message_is_redelivered_and_a_poison_message_is_terminated()
    {
        var prefix = "it" + Guid.NewGuid().ToString("N")[..10];
        await using var host = await NatsTestHost.StartAsync(prefix, extra: new() { ["MicroDcs:Nats:MaxDeliver"] = "3" }, start: false, ct: Ct);
        var handler = new FaultingHandler();
        var transport = host.Transport;
        await transport.StartAsync(handler, Ct);
        try
        {
            await using var client = await NatsTestClient.ConnectAsync();
            await client.PublishEventAsync($"{prefix}.site.area.events", "poison-1", NatsTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", null, Ct);

            await EventuallyAsync(() => transport.Status.Poisoned == 1);
            handler.Deliveries.ShouldBe([false, true, true]);
            transport.Status.Nacked.ShouldBe(2);
        }
        finally
        {
            await transport.StopReceivingAsync(Ct);
            await transport.StopAsync(Ct);
        }
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task The_transport_reconnects_after_the_connection_is_cut()
    {
        await using var proxy = new NatsTestProxy();
        await using var host = await NatsTestHost.StartAsync(url: proxy.Url, ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync();
        var reply = $"{host.Prefix}-test.replies";
        await client.SubscribeAsync(reply, Ct);

        proxy.CloseConnections();
        await EventuallyAsync(() => host.Transport.Status.Reconnects >= 1 && host.Transport.IsConnected);

        await client.PublishEventAsync($"{host.Prefix}.site.area.events", "after-1", NatsTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", reply, Ct);
        NatsTestClient.Header(await client.ReceiveAsync(Ct), "causationid").ShouldBe("after-1");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task Accounts_isolate_tenants_that_use_the_same_subjects()
    {
        var prefix = "it" + Guid.NewGuid().ToString("N")[..10];
        await using var tenantA = await NatsTestHost.StartAsync(prefix, login: ("a", "a"), ct: Ct);
        await using var tenantB = await NatsTestHost.StartAsync(prefix, login: ("b", "b"), ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync("a", "a");
        var reply = $"{prefix}-test.replies";
        await client.SubscribeAsync(reply, Ct);

        await client.PublishEventAsync($"{prefix}.site.area.events", "tenant-1", NatsTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", reply, Ct);

        NatsTestClient.Header(await client.ReceiveAsync(Ct), "causationid").ShouldBe("tenant-1");
        tenantA.Transport.Status.Received.ShouldBe(1);
        tenantB.Transport.Status.Received.ShouldBe(0);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task RV14_a_consumer_deleted_on_the_server_is_created_again()
    {
        await using var host = await NatsTestHost.StartAsync(ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync();
        var reply = $"{host.Prefix}-test.replies";
        await client.SubscribeAsync(reply, Ct);
        var consumer = host.Transport.Status.Consumers.Single(c => c.Kind == InboundKind.Request);
        var js = new NatsJSContext(client.Connection);

        (await js.DeleteConsumerAsync(consumer.Stream, consumer.Consumer, Ct)).ShouldBeTrue();
        await EventuallyAsync(() =>
        {
            try
            {
                return js.GetConsumerAsync(consumer.Stream, consumer.Consumer, Ct).AsTask().GetAwaiter().GetResult() is not null;
            }
            catch (NatsJSApiException)
            {
                return false;
            }
        });

        await client.PublishEventAsync($"{host.Prefix}.site.area.events", "again-1", NatsTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", reply, Ct);
        NatsTestClient.Header(await client.ReceiveAsync(Ct), "causationid").ShouldBe("again-1");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task RV18_a_reply_address_in_the_jetstream_api_is_rejected_and_nothing_is_published_there()
    {
        await using var host = await NatsTestHost.StartAsync(ct: Ct);
        await using var client = await NatsTestClient.ConnectAsync();
        var consumer = host.Transport.Status.Consumers.Single(c => c.Kind == InboundKind.Request);
        var js = new NatsJSContext(client.Connection);

        await client.PublishEventAsync($"{host.Prefix}.site.area.events", "deputy-1", NatsTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", "$JS.API.STREAM.DELETE." + consumer.Stream, Ct);
        await EventuallyAsync(() => host.Transport.Status.Dropped == 1);

        (await js.GetStreamAsync(consumer.Stream, cancellationToken: Ct)).Info.Config.Name.ShouldBe(consumer.Stream);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task RV19_instances_of_a_shared_group_with_different_filters_get_separate_consumers()
    {
        var prefix = "it" + Guid.NewGuid().ToString("N")[..10];
        await using var one = await NatsTestHost.StartAsync(prefix, "workers", ct: Ct);
        await using var same = await NatsTestHost.StartAsync(prefix, "workers", ct: Ct);
        await using var changed = await NatsTestHost.StartAsync(prefix, "workers", extra: new() { ["MicroDcs:Bindings:greetings:ScopeLevels"] = "3" }, ct: Ct);

        static string Name(NatsTestHost host) => host.Transport.Status.Consumers.Single(c => c.Kind == InboundKind.Request).Consumer;

        Name(same).ShouldBe(Name(one));
        Name(changed).ShouldNotBe(Name(one));
    }

    [Fact(SkipUnless = nameof(TestEndpoints.NatsConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task RV20_messages_waiting_for_a_worker_are_kept_in_progress_and_not_redelivered()
    {
        var prefix = "it" + Guid.NewGuid().ToString("N")[..10];
        await using var host = await NatsTestHost.StartAsync(
            prefix,
            extra: new() { ["MicroDcs:Nats:AckWait"] = "00:00:01", ["MicroDcs:MessageWorkers"] = "1" },
            start: false,
            ct: Ct);
        var handler = new SlowHandler(TimeSpan.FromMilliseconds(1500));
        var transport = host.Transport;
        await transport.StartAsync(handler, Ct);
        try
        {
            await using var client = await NatsTestClient.ConnectAsync();
            for (var i = 0; i < 3; i++)
            {
                await client.PublishEventAsync($"{prefix}.site.area.events", "slow-" + i, NatsTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", null, Ct);
            }

            // The third message waits about 3 s for the only worker, three times the AckWait.
            await EventuallyAsync(() => transport.Status.Acknowledged == 3, seconds: 20);
            handler.Deliveries.ShouldBe([false, false, false]);
        }
        finally
        {
            await transport.StopReceivingAsync(Ct);
            await transport.StopAsync(Ct);
        }
    }

    private sealed class SlowHandler(TimeSpan delay) : IInboundHandler
    {
        private readonly List<bool> _deliveries = [];

        public IReadOnlyList<bool> Deliveries
        {
            get
            {
                lock (_deliveries)
                {
                    return [.. _deliveries];
                }
            }
        }

        public async ValueTask<DeliveryOutcome> HandleAsync(InboundMessage message, CancellationToken cancellationToken)
        {
            lock (_deliveries)
            {
                _deliveries.Add(message.Redelivered);
            }

            await Task.Delay(delay, cancellationToken);
            return DeliveryOutcome.Ack;
        }
    }

    private sealed class FaultingHandler : IInboundHandler
    {
        private readonly List<bool> _deliveries = [];

        public IReadOnlyList<bool> Deliveries
        {
            get
            {
                lock (_deliveries)
                {
                    return [.. _deliveries];
                }
            }
        }

        public ValueTask<DeliveryOutcome> HandleAsync(InboundMessage message, CancellationToken cancellationToken)
        {
            lock (_deliveries)
            {
                _deliveries.Add(message.Redelivered);
            }

            return ValueTask.FromResult(DeliveryOutcome.Fault);
        }
    }
}

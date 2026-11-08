using System.Text;
using MicroDCS.App.Greetings;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet.Protocol;

namespace MicroDCS.IntegrationTests.Mqtt;

/// <summary>End-to-end tests of the MQTT transport against Mosquitto in the compose harness (D-15, S13).</summary>
[Trait("Category", "Integration")]
public sealed class MqttTransportTests
{
    private const string Skip = $"{TestEndpoints.MqttVariable} is not set";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task EventuallyAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (!condition())
        {
            (DateTime.UtcNow < deadline).ShouldBeTrue("The condition did not hold within 15 seconds.");
            await Task.Delay(20, Ct);
        }
    }

    [Fact(SkipUnless = nameof(TestEndpoints.MqttConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task Hello_over_mqtt_is_answered_on_the_response_topic()
    {
        await using var host = await MqttTestHost.StartAsync(ct: Ct);
        await using var client = await MqttTestClient.ConnectAsync(null, Ct);
        var reply = $"{host.Prefix}-test/replies";
        await client.SubscribeAsync(reply, Ct);

        await client.PublishEventAsync($"{host.Prefix}/site/area/events", "req-1", MqttTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", reply, Ct);

        var names = new List<string>();
        for (var i = 0; i < 2; i++)
        {
            var message = await client.ReceiveAsync(Ct);
            MqttTestClient.Property(message, "type").ShouldBe(MqttTestHost.HelloType);
            MqttTestClient.Property(message, "causationid").ShouldBe("req-1");
            MqttTestClient.Property(message, "subject").ShouldBe("site.area");
            names.Add(Encoding.UTF8.GetString(message.Payload));
        }

        names.ShouldContain(n => n.Contains("Bob", StringComparison.Ordinal));
        names.ShouldContain(n => n.Contains("Alice", StringComparison.Ordinal));
    }

    [Fact(SkipUnless = nameof(TestEndpoints.MqttConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_duplicate_is_processed_once_and_every_delivery_is_acknowledged()
    {
        await using var host = await MqttTestHost.StartAsync(ct: Ct);
        await using var client = await MqttTestClient.ConnectAsync(null, Ct);
        var reply = $"{host.Prefix}-test/replies";
        await client.SubscribeAsync(reply, Ct);
        var topic = $"{host.Prefix}/site/area/events";

        // Sequential on purpose: a duplicate that arrives while the first is still running waits for its lease (T-20).
        var causes = new List<string>();
        foreach (var (id, name) in new[] { ("dup-1", "Bob"), ("dup-1", "Bob"), ("next-1", "Eve") })
        {
            await client.PublishEventAsync(topic, id, MqttTestHost.HelloType, "site.area", $"{{\"Name\":\"{name}\"}}", reply, Ct);
            if (id == "dup-1" && causes.Count == 0)
            {
                causes.Add(MqttTestClient.Property(await client.ReceiveAsync(Ct), "causationid"));
                causes.Add(MqttTestClient.Property(await client.ReceiveAsync(Ct), "causationid"));
                await EventuallyAsync(() => host.Transport.Status.Acknowledged == 1);
            }
        }

        // The duplicate produced no replies, so the next two replies belong to the third message.
        causes.Add(MqttTestClient.Property(await client.ReceiveAsync(Ct), "causationid"));
        causes.Add(MqttTestClient.Property(await client.ReceiveAsync(Ct), "causationid"));

        causes.ShouldBe(["dup-1", "dup-1", "next-1", "next-1"]);

        // No unacknowledged message stays behind: the duplicate was acknowledged as well (D-37, unverified list of S02).
        await EventuallyAsync(() => host.Transport.Status.Acknowledged == 3);
        host.Transport.Status.Received.ShouldBe(3);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.MqttConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_subject_outside_the_topic_scope_is_dropped_with_an_error_event()
    {
        await using var host = await MqttTestHost.StartAsync(ct: Ct);
        await using var client = await MqttTestClient.ConnectAsync(null, Ct);
        var reply = $"{host.Prefix}-test/replies";
        await client.SubscribeAsync(reply, Ct);

        await client.PublishEventAsync($"{host.Prefix}/site/area/events", "evil-1", MqttTestHost.HelloType, "other.area", "{\"Name\":\"Bob\"}", reply, Ct);

        var error = await client.ReceiveAsync(Ct);
        MqttTestClient.Property(error, "type").ShouldBe("io.microdcs.error.v1");
        MqttTestClient.Property(error, "mdcserrorkind").ShouldBe("PermissionDenied", StringCompareShould.IgnoreCase);
        await EventuallyAsync(() => host.Transport.Status.Acknowledged == 1);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.MqttConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task An_undecodable_message_is_dropped_and_acknowledged()
    {
        await using var host = await MqttTestHost.StartAsync(ct: Ct);
        await using var client = await MqttTestClient.ConnectAsync(null, Ct);
        var reply = $"{host.Prefix}-test/replies";
        await client.SubscribeAsync(reply, Ct);
        var topic = $"{host.Prefix}/site/area/events";

        // No user properties at all: not a CloudEvent.
        await client.PublishRawAsync(topic, "garbage"u8.ToArray(), Ct);
        await client.PublishEventAsync(topic, "ok-1", MqttTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", reply, Ct);

        MqttTestClient.Property(await client.ReceiveAsync(Ct), "causationid").ShouldBe("ok-1");
        await EventuallyAsync(() => host.Transport.Status.Acknowledged == 2);
        host.Transport.Status.Dropped.ShouldBe(1);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.MqttConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_command_carries_the_per_instance_reply_topic_and_expiry()
    {
        await using var host = await MqttTestHost.StartAsync(ct: Ct);
        await using var client = await MqttTestClient.ConnectAsync(null, Ct);
        await client.SubscribeAsync($"{host.Prefix}/site/area/commands", Ct);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        await publisher.PublishAsync(new Bye("site.area"), Intent.Command, new PublishOptions { Subject = "site.area", EventId = "cmd-1" }, Ct);

        var command = await client.ReceiveAsync(Ct);
        command.ResponseTopic.ShouldBe(host.ReplyTopic);
        MqttTestClient.Property(command, "id").ShouldBe("cmd-1");
        Encoding.UTF8.GetString(command.CorrelationData).ShouldBe("cmd-1");
        command.QualityOfServiceLevel.ShouldBe(MqttQualityOfServiceLevel.AtLeastOnce);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.MqttConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_response_on_the_instance_reply_topic_reaches_the_pipeline()
    {
        await using var host = await MqttTestHost.StartAsync(ct: Ct);
        await using var client = await MqttTestClient.ConnectAsync(null, Ct);

        await client.PublishEventAsync(host.ReplyTopic, "resp-1", MqttTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", null, Ct);

        await EventuallyAsync(() => host.Transport.Status.Acknowledged == 1);
        host.Transport.Status.Dropped.ShouldBe(0);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.MqttConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task State_topics_are_retained_with_expiry_and_deleted_by_an_empty_message()
    {
        await using var host = await MqttTestHost.StartAsync(ct: Ct);
        var port = host.Services.GetRequiredService<IStateTopicPort>();
        port.ShouldBeSameAs(host.Transport);
        var address = new LogicalAddress(host.Prefix, ["site", "area"], null, Intent.Data);
        var topic = $"{host.Prefix}/site/area/data/order/42";

        await port.PublishAsync(address, "order/42", "state-1"u8.ToArray(), TimeSpan.FromMinutes(5), Ct);

        await using (var late = await MqttTestClient.ConnectAsync(null, Ct))
        {
            await late.SubscribeAsync(topic, Ct);
            var retained = await late.ReceiveAsync(Ct);
            retained.Retain.ShouldBeTrue();
            Encoding.UTF8.GetString(retained.Payload).ShouldBe("state-1");
            retained.MessageExpiryInterval.ShouldBeInRange(1u, 300u);
        }

        await port.DeleteAsync(address, "order/42", Ct);

        await using var after = await MqttTestClient.ConnectAsync(null, Ct);
        await after.SubscribeAsync(topic, Ct);
        await after.SubscribeAsync($"{host.Prefix}/site/area/data/marker", Ct);
        await port.PublishAsync(address, "marker", "m"u8.ToArray(), TimeSpan.FromMinutes(1), Ct);

        // The marker arrives; a retained value of the deleted topic would have arrived before it.
        Encoding.UTF8.GetString((await after.ReceiveAsync(Ct)).Payload).ShouldBe("m");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.MqttConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task Shared_subscriptions_split_the_load_between_instances()
    {
        var prefix = "it" + Guid.NewGuid().ToString("N")[..10];
        await using var one = await MqttTestHost.StartAsync(prefix, "workers", Ct);
        await using var two = await MqttTestHost.StartAsync(prefix, "workers", Ct);
        await using var client = await MqttTestClient.ConnectAsync(null, Ct);
        var reply = $"{prefix}-test/replies";
        await client.SubscribeAsync(reply, Ct);

        const int Count = 20;
        for (var i = 0; i < Count; i++)
        {
            await client.PublishEventAsync($"{prefix}/site/area/events", "m-" + i, MqttTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", reply, Ct);
        }

        var causes = new HashSet<string>();
        for (var i = 0; i < Count * 2; i++)
        {
            causes.Add(MqttTestClient.Property(await client.ReceiveAsync(Ct), "causationid"));
        }

        causes.Count.ShouldBe(Count);
        (one.Transport.Status.Received + two.Transport.Status.Received).ShouldBe(Count);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.MqttConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task The_transport_reconnects_after_the_broker_closes_the_connection()
    {
        await using var host = await MqttTestHost.StartAsync(ct: Ct);
        await using var client = await MqttTestClient.ConnectAsync(null, Ct);
        var reply = $"{host.Prefix}-test/replies";
        await client.SubscribeAsync(reply, Ct);
        var clientId = host.Transport.Status.ClientId;

        // A second connection with the same client id makes the broker close the first one (session takeover).
        var intruder = await MqttTestClient.ConnectAsync(clientId, Ct, cleanStart: false);
        await intruder.DisposeAsync();

        await EventuallyAsync(() => host.Transport.Status.Reconnects >= 1 && host.Transport.IsConnected);

        await client.PublishEventAsync($"{host.Prefix}/site/area/events", "after-1", MqttTestHost.HelloType, "site.area", "{\"Name\":\"Bob\"}", reply, Ct);
        MqttTestClient.Property(await client.ReceiveAsync(Ct), "causationid").ShouldBe("after-1");
    }
}

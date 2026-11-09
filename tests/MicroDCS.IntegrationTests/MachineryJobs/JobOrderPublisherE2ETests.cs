using System.Buffers;
using System.Globalization;
using System.Text;
using MicroDCS.Core.Hosting;
using MicroDCS.IntegrationTests.Mqtt;
using MicroDCS.IntegrationTests.Nats;
using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Conversions;
using MicroDCS.JobControl.Extension;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.MachineryJobs;
using MicroDCS.MachineryJobs.Publishing;
using MicroDCS.Transport.Mqtt;
using MicroDCS.Transport.Nats;
using MicroDCS.Valkey;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MQTTnet;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using Opc.Ua;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.IntegrationTests.MachineryJobs;

/// <summary>The job order publisher against real brokers and Valkey: split deployment roles, failover and both state-topic transports (S18).</summary>
[Trait("Category", "Integration")]
public sealed class JobOrderPublisherE2ETests
{
    private const string MqttSkip = $"{TestEndpoints.MqttVariable} or {TestEndpoints.ValkeyVariable} is not set";
    private const string NatsSkip = $"{TestEndpoints.NatsVariable} or {TestEndpoints.ValkeyVariable} is not set";
    private const string Subject = "site.area";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static bool MqttConfigured => TestEndpoints.MqttConfigured && TestEndpoints.ValkeyConfigured;

    public static bool NatsConfigured => TestEndpoints.NatsConfigured && TestEndpoints.ValkeyConfigured;

    private static string Json<T>(T value)
        where T : class, IEncodeable => Encoding.UTF8.GetString(OpcUaJsonCodec.Encode(value));

    private static IHost Build(string prefix, string instance, InstanceRoles roles, string transport)
    {
        var (valkey, valkeyPort) = TestEndpoints.Valkey!.Value;
        var builder = Host.CreateApplicationBuilder();
        var settings = new Dictionary<string, string?>
        {
            ["MicroDcs:InstanceId"] = instance,
            ["MicroDcs:Roles"] = roles.ToString(),
            ["MicroDcs:CloudEventSource"] = "urn:it:publisher",
            [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:Transport"] = transport,
            [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:Prefix"] = prefix,
            [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:ScopeLevels"] = "2",
            [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:ReplyBase"] = prefix + "-reply",
            ["MicroDcs:Valkey:ConnectionString"] = $"{valkey}:{valkeyPort}",
            ["MicroDcs:Valkey:KeyPrefix"] = prefix,
            ["MicroDcs:JobPublisher:LeaseTtl"] = "00:00:03",
        };
        if (transport == "mqtt")
        {
            var (broker, port) = TestEndpoints.Mqtt!.Value;
            settings["MicroDcs:Mqtt:Hostname"] = broker;
            settings["MicroDcs:Mqtt:Port"] = port.ToString(CultureInfo.InvariantCulture);
            settings["MicroDcs:Mqtt:TlsCaFile"] = "/nonexistent/ca.crt";
            settings["MicroDcs:Mqtt:SatTokenFile"] = "/nonexistent/sat";
        }
        else
        {
            var (server, port) = TestEndpoints.Nats!.Value;
            settings["MicroDcs:Nats:Url"] = $"nats://{server}:{port}";
            settings["MicroDcs:Nats:TlsCaFile"] = "/nonexistent/ca.crt";
            settings["MicroDcs:Nats:StateBucket"] = prefix + "-state";
            settings["MicroDcs:Nats:StreamPrefix"] = "IT_" + prefix;
        }

        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.AddMicroDcs(builder.Configuration);
        if (transport == "mqtt")
        {
            builder.Services.AddMqttTransport(builder.Configuration);
        }
        else
        {
            builder.Services.AddNatsTransport(builder.Configuration);
        }

        builder.Services.AddMicroDcsValkey(builder.Configuration);
        builder.Services.AddMachineryJobs(builder.Configuration);
        return builder.Build();
    }

    private static string NewPrefix() => "pub" + Guid.NewGuid().ToString("N")[..10];

    private static async Task WaitUntilAsync(Func<bool> condition, string because)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (!condition())
        {
            (DateTime.UtcNow < deadline).ShouldBeTrue(because);
            await Task.Delay(20, Ct);
        }
    }

    private static async Task<MqttApplicationMessage> ReceiveTopicAsync(MqttTestClient client, string topic)
    {
        while (true)
        {
            var message = await client.ReceiveAsync(Ct);
            if (message.Topic == topic)
            {
                return message;
            }
        }
    }

    private static async Task<MqttApplicationMessage> ReceiveIndexAsync(MqttTestClient client, string topic, int jobs)
    {
        while (true)
        {
            var message = await ReceiveTopicAsync(client, topic);
            if (OpcUaJsonCodec.Decode<JobOrderStateIndexDataType>(message.Payload.ToArray()).Jobs.Count == jobs)
            {
                return message;
            }
        }
    }

    private static async Task StopAsync(params IHost[] hosts)
    {
        foreach (var host in hosts)
        {
            await host.StopAsync(CancellationToken.None);
            host.Dispose();
        }
    }

    [Fact(SkipUnless = nameof(MqttConfigured), SkipType = typeof(JobOrderPublisherE2ETests), Skip = MqttSkip)]
    public async Task Separate_processor_and_publisher_instances_keep_the_retained_topics()
    {
        var prefix = NewPrefix();
        var processor = Build(prefix, "i-proc" + prefix, InstanceRoles.Processor, "mqtt");
        var publisher = Build(prefix, "i-pub" + prefix, InstanceRoles.Publisher, "mqtt");
        await using var client = await MqttTestClient.ConnectAsync(null, Ct);
        var replies = prefix + "-test/replies";
        var commands = $"{prefix}/site/area/commands";
        var data = $"{prefix}/site/area/data";
        await client.SubscribeAsync($"{data}/#", Ct);
        await client.SubscribeAsync(replies, Ct);
        await processor.StartAsync(Ct);
        await publisher.StartAsync(Ct);
        try
        {
            await WaitUntilAsync(() => publisher.Services.GetRequiredService<JobOrderPublisher>().IsActive, "The publisher instance took no lease.");
            processor.Services.GetRequiredService<JobOrderPublisher>().IsActive.ShouldBeFalse();

            var call = new StoreCall { JobOrder = JobControlConversions.ToV2Order(new JobOrder { Id = "job-1" }) };
            await client.PublishEventAsync(commands, "cmd-1", JobControlTypes.Store, Subject, Json(call), replies, Ct);

            var order = await ReceiveTopicAsync(client, $"{data}/order/job-1");
            OpcUaJsonCodec.Decode<V2.ISA95JobOrderDataType>(order.Payload.ToArray()).JobOrderID.ShouldBe("job-1");
            await ReceiveIndexAsync(client, $"{data}/state-index", 1);

            // A subscriber that arrives later gets the last values, as an MES resync does.
            await using (var late = await MqttTestClient.ConnectAsync(null, Ct))
            {
                await late.SubscribeAsync($"{data}/order/job-1", Ct);
                (await late.ReceiveAsync(Ct)).Retain.ShouldBeTrue();
            }

            await client.PublishEventAsync(commands, "cmd-2", JobControlTypes.Cancel, Subject, Json(new CancelCall { JobOrderID = "job-1" }), replies, Ct);
            (await ReceiveTopicAsync(client, $"{data}/order/job-1")).Payload.Length.ShouldBe(0);
            await ReceiveIndexAsync(client, $"{data}/state-index", 0);
        }
        finally
        {
            await StopAsync(publisher, processor);
        }
    }

    [Fact(SkipUnless = nameof(MqttConfigured), SkipType = typeof(JobOrderPublisherE2ETests), Skip = MqttSkip)]
    public async Task Only_one_publisher_is_active_and_the_other_takes_over()
    {
        var prefix = NewPrefix();
        var first = Build(prefix, "i-a" + prefix, InstanceRoles.Publisher, "mqtt");
        var second = Build(prefix, "i-b" + prefix, InstanceRoles.Publisher, "mqtt");
        var firstPublisher = first.Services.GetRequiredService<JobOrderPublisher>();
        var secondPublisher = second.Services.GetRequiredService<JobOrderPublisher>();
        await first.StartAsync(Ct);
        await second.StartAsync(Ct);
        var stopped = false;
        try
        {
            await WaitUntilAsync(() => firstPublisher.IsActive || secondPublisher.IsActive, "No publisher took the lease.");
            await Task.Delay(TimeSpan.FromSeconds(2), Ct);
            (firstPublisher.IsActive ^ secondPublisher.IsActive).ShouldBeTrue();

            var (active, standby) = firstPublisher.IsActive ? (first, secondPublisher) : (second, firstPublisher);
            await active.StopAsync(Ct);
            stopped = true;

            await WaitUntilAsync(() => standby.IsActive, "The standby did not take over.");
        }
        finally
        {
            if (stopped)
            {
                await StopAsync(firstPublisher.IsActive ? first : second);
                (firstPublisher.IsActive ? second : first).Dispose();
            }
            else
            {
                await StopAsync(first, second);
            }
        }
    }

    [Fact(SkipUnless = nameof(NatsConfigured), SkipType = typeof(JobOrderPublisherE2ETests), Skip = NatsSkip)]
    public async Task A_publisher_only_instance_keeps_the_kv_state_topics_on_nats()
    {
        var prefix = NewPrefix();
        var publisher = Build(prefix, "i-pub" + prefix, InstanceRoles.Publisher, "nats");
        await publisher.StartAsync(Ct);
        await using var client = await NatsTestClient.ConnectAsync();
        var js = new NatsJSContext(client.Connection);
        try
        {
            await foreach (var stream in js.ListStreamNamesAsync(cancellationToken: Ct))
            {
                stream.ShouldNotStartWith("IT_" + prefix, Case.Sensitive, "A publisher-only instance must not create the command streams.");
            }

            var store = publisher.Services.GetRequiredService<IJobStore>();
            await store.SaveOrderAsync(new JobOrderWrite(new JobOrder { Id = "job-1" }, JobOrderState.Running, Subject, "Store", 0, "op-1", JobRecipeWork.None), Ct);
            var kv = await new NatsKVContext(js).GetStoreAsync(prefix + "-state", Ct);
            var orderKey = $"{prefix}.site.area.data.order.job-1";
            var indexKey = $"{prefix}.site.area.data.state-index";

            var order = await WaitForKeyAsync(kv, orderKey);
            OpcUaJsonCodec.Decode<V2.ISA95JobOrderDataType>(order).JobOrderID.ShouldBe("job-1");
            OpcUaJsonCodec.Decode<JobOrderStateIndexDataType>(await WaitForKeyAsync(kv, indexKey)).Jobs.Count.ShouldBe(1);

            await store.RemoveOrderAsync("job-1", Subject, "Clear", 1, "op-2", Ct);

            var deadline = DateTime.UtcNow.AddSeconds(20);
            while (await TryGetAsync(kv, orderKey) is not null)
            {
                (DateTime.UtcNow < deadline).ShouldBeTrue("The state topic was not removed.");
                await Task.Delay(50, Ct);
            }
        }
        finally
        {
            await StopAsync(publisher);
        }
    }

    private static async Task<byte[]> WaitForKeyAsync(INatsKVStore kv, string key)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (true)
        {
            if (await TryGetAsync(kv, key) is { } value)
            {
                return value;
            }

            (DateTime.UtcNow < deadline).ShouldBeTrue($"The key {key} did not appear.");
            await Task.Delay(50, Ct);
        }
    }

    private static async Task<byte[]?> TryGetAsync(INatsKVStore kv, string key)
    {
        var result = await kv.TryGetEntryAsync(key, serializer: NatsRawSerializer<byte[]>.Default, cancellationToken: Ct);
        return result.Success ? result.Value.Value : null;
    }
}

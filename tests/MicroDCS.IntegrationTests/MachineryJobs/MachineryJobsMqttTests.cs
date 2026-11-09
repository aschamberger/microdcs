using System.Buffers;
using System.Text;
using MicroDCS.Core.Hosting;
using MicroDCS.IntegrationTests.Mqtt;
using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Conversions;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.MachineryJobs;
using MicroDCS.Transport.Mqtt;
using MicroDCS.Valkey;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MQTTnet;
using Opc.Ua;
using CancelResponse = MicroDCS.MachineryJobs.CancelResponse;

namespace MicroDCS.IntegrationTests.MachineryJobs;

/// <summary>The Machinery Jobs processor end to end: MQTT broker, Valkey and the real processor (S17).</summary>
[Trait("Category", "Integration")]
public sealed class MachineryJobsMqttTests
{
    private const string Skip = $"{TestEndpoints.MqttVariable} or {TestEndpoints.ValkeyVariable} is not set";
    private const string Subject = "site.area";
    private const string Equipment = "eq-1";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static bool Configured => TestEndpoints.MqttConfigured && TestEndpoints.ValkeyConfigured;

    private static string Json<T>(T value)
        where T : class, IEncodeable => Encoding.UTF8.GetString(OpcUaJsonCodec.Encode(value));

    private static StoreCall Store(string id, string? equipment = null) => new()
    {
        JobOrder = JobControlConversions.ToV2Order(new JobOrder
        {
            Id = id,
            EquipmentRequirements = equipment is null ? [] : [new JobResource { Id = equipment }],
        }),
    };

    private sealed class Rig : IAsyncDisposable
    {
        private readonly IHost _host;
        private readonly List<MqttApplicationMessage> _held = [];

        private Rig(IHost host, MqttTestClient client, string prefix)
        {
            _host = host;
            Client = client;
            Prefix = prefix;
        }

        public MqttTestClient Client { get; }

        public string Prefix { get; }

        public string Commands => $"{Prefix}/site/area/commands";

        public string Replies => $"{Prefix}-test/replies";

        public IJobStore JobStore => _host.Services.GetRequiredService<IJobStore>();

        public static async Task<Rig> StartAsync()
        {
            var (broker, port) = TestEndpoints.Mqtt!.Value;
            var (valkey, valkeyPort) = TestEndpoints.Valkey!.Value;
            var prefix = "mj" + Guid.NewGuid().ToString("N")[..10];
            var builder = Host.CreateApplicationBuilder();
            builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["MicroDcs:InstanceId"] = "i" + prefix,
                ["MicroDcs:CloudEventSource"] = "urn:it:jobs",
                [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:Transport"] = "mqtt",
                [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:Prefix"] = prefix,
                [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:ScopeLevels"] = "2",
                [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:ReplyBase"] = prefix + "-reply",
                ["MicroDcs:Mqtt:Hostname"] = broker,
                ["MicroDcs:Mqtt:Port"] = port.ToString(System.Globalization.CultureInfo.InvariantCulture),
                ["MicroDcs:Mqtt:TlsCaFile"] = "/nonexistent/ca.crt",
                ["MicroDcs:Mqtt:SatTokenFile"] = "/nonexistent/sat",
                ["MicroDcs:Valkey:ConnectionString"] = $"{valkey}:{valkeyPort}",
                ["MicroDcs:Valkey:KeyPrefix"] = prefix,
            });
            builder.Services.AddMicroDcs(builder.Configuration).AddMqttTransport(builder.Configuration);
            builder.Services.AddMicroDcsValkey(builder.Configuration);
            builder.Services.AddMachineryJobs(builder.Configuration);
            var host = builder.Build();

            var client = await MqttTestClient.ConnectAsync(null, Ct);
            var rig = new Rig(host, client, prefix);
            await client.SubscribeAsync(rig.Replies, Ct);
            await client.SubscribeAsync($"{prefix}/site/area/events", Ct);
            await host.StartAsync(Ct);
            return rig;
        }

        // A response and a status event travel on different topics, so either can arrive first; the other waits here.
        public async Task<MqttApplicationMessage> ReceiveAsync(string type)
        {
            var held = _held.Find(m => MqttTestClient.Property(m, "type") == type);
            if (held is not null)
            {
                _held.Remove(held);
                return held;
            }

            while (true)
            {
                var message = await Client.ReceiveAsync(Ct);
                if (MqttTestClient.Property(message, "type") == type)
                {
                    return message;
                }

                _held.Add(message);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Client.DisposeAsync();
            await _host.StopAsync();
            _host.Dispose();
        }
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(MachineryJobsMqttTests), Skip = Skip)]
    public async Task A_job_goes_through_store_start_and_cancel_over_mqtt()
    {
        await using var rig = await Rig.StartAsync();

        await rig.Client.PublishEventAsync(rig.Commands, "cmd-1", JobControlTypes.Store, Subject, Json(Store("job-1")), rig.Replies, Ct);
        var stored = await rig.ReceiveAsync(JobControlTypes.StoreResponse);
        OpcUaJsonCodec.Decode<StoreResponse>(stored.Payload.ToArray()).ReturnStatus.ShouldBe((ulong)JobReturnStatus.Success);
        MqttTestClient.Property(stored, "causationid").ShouldBe("cmd-1");

        var status = await rig.ReceiveAsync(JobControlTypes.JobOrderStatusEvent);
        JobStateArray.FromV2(OpcUaJsonCodec.Decode<JobOrderStatusEvent>(status.Payload.ToArray()).JobState).ShouldBe(JobOrderState.NotAllowedToStartReady);

        await rig.Client.PublishEventAsync(rig.Commands, "cmd-2", JobControlTypes.Start, Subject, Json(new StartCall { JobOrderID = "job-1" }), rig.Replies, Ct);
        OpcUaJsonCodec.Decode<StartResponse>((await rig.ReceiveAsync(JobControlTypes.StartResponse)).Payload.ToArray()).ReturnStatus.ShouldBe((ulong)JobReturnStatus.Success);
        (await rig.JobStore.GetOrderAsync("job-1", Ct))!.State.ShouldBe(JobOrderState.AllowedToStartReady);

        await rig.Client.PublishEventAsync(rig.Commands, "cmd-3", JobControlTypes.Cancel, Subject, Json(new CancelCall { JobOrderID = "job-1" }), rig.Replies, Ct);
        OpcUaJsonCodec.Decode<CancelResponse>((await rig.ReceiveAsync(JobControlTypes.CancelResponse)).Payload.ToArray()).ReturnStatus.ShouldBe((ulong)JobReturnStatus.Success);
        (await rig.JobStore.GetOrderAsync("job-1", Ct)).ShouldBeNull();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(MachineryJobsMqttTests), Skip = Skip)]
    public async Task Station_configuration_over_mqtt_decides_what_is_accepted()
    {
        await using var rig = await Rig.StartAsync();

        await rig.Client.PublishEventAsync(rig.Commands, "cmd-1", JobControlTypes.Store, Subject, Json(Store("job-1", Equipment)), rig.Replies, Ct);
        OpcUaJsonCodec.Decode<StoreResponse>((await rig.ReceiveAsync(JobControlTypes.StoreResponse)).Payload.ToArray()).ReturnStatus.ShouldBe((ulong)JobReturnStatus.UnableToAccept);

        await rig.Client.PublishEventAsync(rig.Commands, "cfg-1", JobControlTypes.ConfigEquipment, Subject, Json(new ConfigEquipment { ID = Equipment }), null, Ct);
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (!await rig.JobStore.IsReferenceAsync(JobReferenceKind.Equipment, Equipment, Subject, Ct))
        {
            (DateTime.UtcNow < deadline).ShouldBeTrue("The configuration was not stored.");
            await Task.Delay(20, Ct);
        }

        await rig.Client.PublishEventAsync(rig.Commands, "cmd-2", JobControlTypes.Store, Subject, Json(Store("job-1", Equipment)), rig.Replies, Ct);
        OpcUaJsonCodec.Decode<StoreResponse>((await rig.ReceiveAsync(JobControlTypes.StoreResponse)).Payload.ToArray()).ReturnStatus.ShouldBe((ulong)JobReturnStatus.Success);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(MachineryJobsMqttTests), Skip = Skip)]
    public async Task The_metadata_event_is_retained_for_late_subscribers()
    {
        await using var rig = await Rig.StartAsync();

        // The first client proves the event reached the broker; MQTT sets the retain flag only on delivery to a later subscription.
        await using var first = await MqttTestClient.ConnectAsync(null, Ct);
        await first.SubscribeAsync($"{rig.Prefix}/#", Ct);
        MqttTestClient.Property(await first.ReceiveAsync(Ct), "type").ShouldBe(JobControlTypes.Metadata);

        await using var late = await MqttTestClient.ConnectAsync(null, Ct);
        await late.SubscribeAsync($"{rig.Prefix}/#", Ct);

        var message = await late.ReceiveAsync(Ct);
        MqttTestClient.Property(message, "type").ShouldBe(JobControlTypes.Metadata);
        message.Retain.ShouldBeTrue();
    }
}

using System.Text;
using System.Threading.Channels;
using MicroDCS.App.Greetings;
using MicroDCS.Core.Hosting;
using MicroDCS.Transport.Mqtt;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MQTTnet;
using MQTTnet.Protocol;

namespace MicroDCS.IntegrationTests.Mqtt;

/// <summary>A plain MQTT client for tests: subscribes and collects what arrives; publishes raw CloudEvents in binary mode.</summary>
internal sealed class MqttTestClient : IAsyncDisposable
{
    private static readonly TimeSpan _wait = TimeSpan.FromSeconds(15);

    private readonly IMqttClient _client = new MqttClientFactory().CreateMqttClient();
    private readonly Channel<MqttApplicationMessage> _received = Channel.CreateUnbounded<MqttApplicationMessage>();

    public static async Task<MqttTestClient> ConnectAsync(string? clientId, CancellationToken ct, bool cleanStart = true)
    {
        var (host, port) = TestEndpoints.Mqtt!.Value;
        var test = new MqttTestClient();
        test._client.ApplicationMessageReceivedAsync += args =>
        {
            test._received.Writer.TryWrite(args.ApplicationMessage);
            return Task.CompletedTask;
        };
        var options = new MqttClientOptionsBuilder().WithTcpServer(host, port).WithCleanStart(cleanStart);
        if (clientId is not null)
        {
            options.WithClientId(clientId);
        }

        await test._client.ConnectAsync(options.Build(), ct);
        return test;
    }

    public async Task SubscribeAsync(string filter, CancellationToken ct) =>
        await _client.SubscribeAsync(new MqttClientSubscribeOptionsBuilder().WithTopicFilter(filter, MqttQualityOfServiceLevel.AtLeastOnce).Build(), ct);

    public async Task<MqttApplicationMessage> ReceiveAsync(CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(_wait);
        return await _received.Reader.ReadAsync(timeout.Token);
    }

    public async Task PublishEventAsync(string topic, string id, string type, string subject, string json, string? responseTopic, CancellationToken ct, string? causationId = null, string? correlationId = null)
    {
        var builder = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(Encoding.UTF8.GetBytes(json))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .WithContentType("application/json");
        foreach (var (name, value) in new Dictionary<string, string>
        {
            ["specversion"] = "1.0",
            ["id"] = id,
            ["source"] = "urn:it:test",
            ["type"] = type,
            ["subject"] = subject,
        })
        {
            builder.WithUserProperty(name, Encoding.UTF8.GetBytes(value).AsMemory());
        }

        if (causationId is not null)
        {
            builder.WithUserProperty("causationid", Encoding.UTF8.GetBytes(causationId).AsMemory());
        }

        if (correlationId is not null)
        {
            builder.WithUserProperty("correlationid", Encoding.UTF8.GetBytes(correlationId).AsMemory());
        }

        if (responseTopic is not null)
        {
            builder.WithResponseTopic(responseTopic);
        }

        await _client.PublishAsync(builder.Build(), ct);
    }

    public async Task PublishRawAsync(string topic, byte[] payload, CancellationToken ct) =>
        await _client.PublishAsync(new MqttApplicationMessageBuilder().WithTopic(topic).WithPayload(payload).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce).Build(), ct);

    public static string Property(MqttApplicationMessage message, string name) =>
        message.UserProperties?.FirstOrDefault(p => p.Name == name) is { } p ? Encoding.UTF8.GetString(p.ValueBuffer.Span) : string.Empty;

    public async ValueTask DisposeAsync()
    {
        if (_client.IsConnected)
        {
            await _client.DisconnectAsync();
        }

        _client.Dispose();
    }
}

/// <summary>The greetings application on MQTT, hosted in the test process, with its own prefix so tests do not see each other.</summary>
internal sealed class MqttTestHost : IAsyncDisposable
{
    public const string HelloType = "com.github.aschamberger.microdcs.greetings.hello.v1";

    private readonly IHost _host;

    private MqttTestHost(IHost host, string prefix, string instanceId)
    {
        _host = host;
        Prefix = prefix;
        InstanceId = instanceId;
    }

    public string Prefix { get; }

    public string InstanceId { get; }

    public string ReplyTopic => $"{Prefix}-reply/{InstanceId}";

    public MqttTransport Transport => _host.Services.GetRequiredService<MqttTransport>();

    public IServiceProvider Services => _host.Services;

    public static async Task<MqttTestHost> StartAsync(string? prefix = null, string? sharedGroup = null, Dictionary<string, string?>? extra = null, CancellationToken ct = default)
    {
        var (broker, port) = TestEndpoints.Mqtt!.Value;
        prefix ??= "it" + Guid.NewGuid().ToString("N")[..10];
        var instance = "i" + Guid.NewGuid().ToString("N")[..8];
        var settings = new Dictionary<string, string?>
        {
            ["MicroDcs:InstanceId"] = instance,
            ["MicroDcs:CloudEventSource"] = "urn:it:app",
            ["MicroDcs:Bindings:greetings:Transport"] = "mqtt",
            ["MicroDcs:Bindings:greetings:Prefix"] = prefix,
            ["MicroDcs:Bindings:greetings:ScopeLevels"] = "2",
            ["MicroDcs:Bindings:greetings:ReplyBase"] = prefix + "-reply",
            ["MicroDcs:Bindings:greetings:SharedGroup"] = sharedGroup,
            ["MicroDcs:Mqtt:Hostname"] = broker,
            ["MicroDcs:Mqtt:Port"] = port.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["MicroDcs:Mqtt:TlsCaFile"] = "/nonexistent/ca.crt",
            ["MicroDcs:Mqtt:SatTokenFile"] = "/nonexistent/sat",
            ["MicroDcs:Mqtt:ReconnectMinDelay"] = "00:00:00.200",
        };
        foreach (var (key, value) in extra ?? [])
        {
            settings[key] = value;
        }

        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.AddMicroDcs(builder.Configuration).AddMqttTransport(builder.Configuration).AddProcessor<GreetingsProcessor>();
        var host = builder.Build();
        await host.StartAsync(ct);
        return new MqttTestHost(host, prefix, instance);
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }
}

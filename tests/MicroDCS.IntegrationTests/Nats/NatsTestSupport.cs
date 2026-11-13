using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using MicroDCS.App.Greetings;
using MicroDCS.Core.Hosting;
using MicroDCS.Transport.Nats;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace MicroDCS.IntegrationTests.Nats;

/// <summary>A plain NATS client for tests: subscribes (core) and collects what arrives; publishes raw CloudEvents in binary mode through JetStream.</summary>
internal sealed class NatsTestClient : IAsyncDisposable
{
    private static readonly TimeSpan _wait = TimeSpan.FromSeconds(15);

    private readonly NatsConnection _connection;
    private readonly NatsJSContext _js;
    private readonly Channel<NatsMsg<byte[]>> _received = Channel.CreateUnbounded<NatsMsg<byte[]>>();
    private readonly List<INatsSub<byte[]>> _subscriptions = [];

    private NatsTestClient(NatsConnection connection)
    {
        _connection = connection;
        _js = new NatsJSContext(connection);
    }

    public NatsConnection Connection => _connection;

    public static async Task<NatsTestClient> ConnectAsync(string? username = null, string? password = null)
    {
        var (host, port) = TestEndpoints.Nats!.Value;
        var opts = NatsOpts.Default with
        {
            Url = $"nats://{host}:{port}",
            AuthOpts = username is null ? NatsAuthOpts.Default : new NatsAuthOpts { Username = username, Password = password },
        };
        var connection = new NatsConnection(opts);
        await connection.ConnectAsync();
        return new NatsTestClient(connection);
    }

    public async Task SubscribeAsync(string subject, CancellationToken ct)
    {
        var subscription = await _connection.SubscribeCoreAsync(subject, serializer: NatsRawSerializer<byte[]>.Default, cancellationToken: ct);
        _subscriptions.Add(subscription);

        // The subscribe command is only queued when SubscribeCoreAsync returns; a round trip makes sure the server knows it before anything is published.
        await _connection.PingAsync(ct);
        _ = Task.Run(async () =>
        {
            await foreach (var message in subscription.Msgs.ReadAllAsync(CancellationToken.None))
            {
                _received.Writer.TryWrite(message);
            }
        }, CancellationToken.None);
    }

    public async Task<NatsMsg<byte[]>> ReceiveAsync(CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(_wait);
        return await _received.Reader.ReadAsync(timeout.Token);
    }

    public async Task PublishEventAsync(string subject, string id, string type, string eventSubject, string json, string? replyTo, CancellationToken ct)
    {
        var headers = new NatsHeaders
        {
            ["specversion"] = "1.0",
            ["id"] = id,
            ["source"] = "urn:it:test",
            ["type"] = type,
            ["subject"] = eventSubject,
            ["Content-Type"] = "application/json",
        };
        if (replyTo is not null)
        {
            headers[NatsMessageMapper.ReplyToHeader] = replyTo;
        }

        var ack = await _js.PublishAsync(subject, Encoding.UTF8.GetBytes(json), NatsRawSerializer<byte[]>.Default, headers: headers, cancellationToken: ct);
        ack.EnsureSuccess();
    }

    public async Task PublishRawAsync(string subject, byte[] payload, CancellationToken ct)
    {
        var ack = await _js.PublishAsync(subject, payload, NatsRawSerializer<byte[]>.Default, cancellationToken: ct);
        ack.EnsureSuccess();
    }

    /// <summary>A plain (non-JetStream) message, the way a response is sent.</summary>
    public async Task PublishCoreEventAsync(string subject, string id, string type, string eventSubject, string json, CancellationToken ct)
    {
        var headers = new NatsHeaders
        {
            ["specversion"] = "1.0",
            ["id"] = id,
            ["source"] = "urn:it:test",
            ["type"] = type,
            ["subject"] = eventSubject,
            ["Content-Type"] = "application/json",
        };
        await _connection.PublishAsync(subject, Encoding.UTF8.GetBytes(json), headers, serializer: NatsRawSerializer<byte[]>.Default, cancellationToken: ct);
    }

    public static string Header(NatsMsg<byte[]> message, string name) =>
        message.Headers is { } headers && headers.TryGetValue(name, out var values) ? values.ToString() : string.Empty;

    public async ValueTask DisposeAsync()
    {
        foreach (var subscription in _subscriptions)
        {
            await subscription.DisposeAsync();
        }

        await _connection.DisposeAsync();
    }
}

/// <summary>A TCP forwarder in front of the NATS server. Closing its connections makes the client reconnect, as a server restart would.</summary>
internal sealed class NatsTestProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly List<TcpClient> _open = [];
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _accept;
    private readonly string _host;
    private readonly int _port;

    public NatsTestProxy()
    {
        (_host, _port) = TestEndpoints.Nats!.Value;
        _listener.Start();
        _accept = Task.Run(AcceptAsync, _stop.Token);
    }

    public string Url => $"nats://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";

    /// <summary>While set, new connections are closed at once, so the client stays disconnected until it is cleared.</summary>
    public bool Refusing { get; set; }

    public void CloseConnections()
    {
        lock (_open)
        {
            foreach (var client in _open)
            {
                client.Dispose();
            }

            _open.Clear();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        CloseConnections();
        try
        {
            await _accept;
        }
        catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException or SocketException)
        {
            // Stopped.
        }

        _stop.Dispose();
    }

    private async Task AcceptAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            var client = await _listener.AcceptTcpClientAsync(_stop.Token);
            if (Refusing)
            {
                client.Dispose();
                continue;
            }

            var upstream = new TcpClient();
            await upstream.ConnectAsync(_host, _port, _stop.Token);
            lock (_open)
            {
                _open.Add(client);
                _open.Add(upstream);
            }

            _ = Pump(client, upstream);
            _ = Pump(upstream, client);
        }
    }

    private static async Task Pump(TcpClient from, TcpClient to)
    {
        try
        {
            await from.GetStream().CopyToAsync(to.GetStream());
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
        {
            // One side closed.
        }
        finally
        {
            from.Dispose();
            to.Dispose();
        }
    }
}

/// <summary>The greetings application on NATS, hosted in the test process, with its own prefix so tests do not see each other.</summary>
internal sealed class NatsTestHost : IAsyncDisposable
{
    public const string HelloType = "com.github.aschamberger.microdcs.greetings.hello.v1";

    private readonly IHost _host;
    private readonly bool _started;

    private NatsTestHost(IHost host, string prefix, string instanceId, bool started)
    {
        _host = host;
        _started = started;
        Prefix = prefix;
        InstanceId = instanceId;
    }

    public string Prefix { get; }

    public string InstanceId { get; }

    public string ReplySubject => $"{Prefix}-reply.{InstanceId}";

    public NatsTransport Transport => _host.Services.GetRequiredService<NatsTransport>();

    public IServiceProvider Services => _host.Services;

    public static async Task<NatsTestHost> StartAsync(
        string? prefix = null,
        string? sharedGroup = null,
        string? url = null,
        (string User, string Password)? login = null,
        Dictionary<string, string?>? extra = null,
        bool start = true,
        CancellationToken ct = default)
    {
        var (server, port) = TestEndpoints.Nats!.Value;
        prefix ??= "it" + Guid.NewGuid().ToString("N")[..10];
        var instance = "i" + Guid.NewGuid().ToString("N")[..8];
        var settings = new Dictionary<string, string?>
        {
            ["MicroDcs:InstanceId"] = instance,
            ["MicroDcs:CloudEventSource"] = "urn:it:app",
            ["MicroDcs:Bindings:greetings:Transport"] = "nats",
            ["MicroDcs:Bindings:greetings:Prefix"] = prefix,
            ["MicroDcs:Bindings:greetings:ScopeLevels"] = "2",
            ["MicroDcs:Bindings:greetings:ReplyBase"] = prefix + "-reply",
            ["MicroDcs:Bindings:greetings:SharedGroup"] = sharedGroup,
            ["MicroDcs:Nats:Url"] = url ?? $"nats://{server}:{port.ToString(CultureInfo.InvariantCulture)}",
            ["MicroDcs:Nats:TlsCaFile"] = "/nonexistent/ca.crt",
            ["MicroDcs:Nats:ReconnectMinDelay"] = "00:00:00.200",
            ["MicroDcs:Nats:NakDelay"] = "00:00:00.200",

            // A message that the server hands to the pull request of a lost connection is redelivered after AckWait; keep it short so a reconnect test recovers quickly.
            ["MicroDcs:Nats:AckWait"] = "00:00:02",
            ["MicroDcs:Nats:StateBucket"] = "it-state-" + prefix,
        };
        if (login is { } l)
        {
            settings["MicroDcs:Nats:Username"] = l.User;
            settings["MicroDcs:Nats:Password"] = l.Password;
        }

        foreach (var (key, value) in extra ?? [])
        {
            settings[key] = value;
        }

        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.AddMicroDcs(builder.Configuration).AddNatsTransport(builder.Configuration).AddProcessor<GreetingsProcessor>();
        var host = builder.Build();
        if (start)
        {
            await host.StartAsync(ct);
        }

        return new NatsTestHost(host, prefix, instance, start);
    }

    public async ValueTask DisposeAsync()
    {
        if (_started)
        {
            await _host.StopAsync();
        }

        _host.Dispose();
    }
}

using MicroDCS.Valkey;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MicroDCS.IntegrationTests.Valkey;

/// <summary>A connection to the harness Valkey with a random key prefix, so tests do not see each other's keys.</summary>
internal sealed class ValkeyTestEnvironment : IDisposable
{
    public const string SkipReason = $"{TestEndpoints.ValkeyVariable} is not set";

    private ValkeyTestEnvironment(ConnectionMultiplexer multiplexer, ValkeyContext context)
    {
        Multiplexer = multiplexer;
        Context = context;
    }

    public ConnectionMultiplexer Multiplexer { get; }

    public ValkeyContext Context { get; }

    public IDatabase Database => Multiplexer.GetDatabase();

    public KeySchema Keys => Context.Keys;

    public static async Task<ValkeyTestEnvironment> CreateAsync(Action<ValkeyOptions>? configure = null)
    {
        var (host, port) = TestEndpoints.Valkey!.Value;
        var prefix = "t" + Guid.NewGuid().ToString("N")[..12];
        var multiplexer = await ConnectionMultiplexer.ConnectAsync($"{host}:{port},allowAdmin=true,abortConnect=true,name={prefix}");
        var options = new ValkeyOptions { KeyPrefix = prefix, WriteRetryDelay = TimeSpan.FromMilliseconds(20) };
        configure?.Invoke(options);
        return new ValkeyTestEnvironment(multiplexer, new ValkeyContext(multiplexer, Options.Create(options), TimeProvider.System, NullLogger<ValkeyContext>.Instance));
    }

    /// <summary>Closes this environment's own connections from the server side; other tests on the same server are not touched.</summary>
    public async Task KillOwnConnectionsAsync()
    {
        var server = Multiplexer.GetServers()[0];
        var list = (string?)await server.ExecuteAsync("CLIENT", "LIST") ?? string.Empty;
        var ids = list.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(static line => line.Split(' ').ToDictionary(static f => f.Split('=')[0], static f => f.Contains('=') ? f[(f.IndexOf('=') + 1)..] : string.Empty))
            .Where(f => f.GetValueOrDefault("name") == Context.Options.KeyPrefix)
            .Select(static f => f["id"]);
        foreach (var id in ids)
        {
            await server.ExecuteAsync("CLIENT", "KILL", "ID", id);
        }
    }

    public void Dispose() => Multiplexer.Dispose();
}

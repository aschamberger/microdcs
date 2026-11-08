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
        var multiplexer = await ConnectionMultiplexer.ConnectAsync($"{host}:{port},allowAdmin=true,abortConnect=true");
        var options = new ValkeyOptions { KeyPrefix = "t" + Guid.NewGuid().ToString("N")[..12], WriteRetryDelay = TimeSpan.FromMilliseconds(20) };
        configure?.Invoke(options);
        return new ValkeyTestEnvironment(multiplexer, new ValkeyContext(multiplexer, Options.Create(options), TimeProvider.System, NullLogger<ValkeyContext>.Instance));
    }

    public void Dispose() => Multiplexer.Dispose();
}

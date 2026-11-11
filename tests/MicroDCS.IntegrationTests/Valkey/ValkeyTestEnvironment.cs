using MicroDCS.Valkey;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MicroDCS.IntegrationTests.Valkey;

/// <summary>
/// A connection to the harness Valkey with a random key prefix, so tests do not see each other's keys. With the <c>valkey-acl</c> service
/// configured (<see cref="TestEndpoints.ValkeyAcl"/>), <see cref="Context"/> connects as an ACL user limited by the documented island
/// rule (<see cref="TestEndpoints.ValkeyIslandAclRule"/>) through <see cref="ValkeyConnection"/>, as in production; <see cref="Multiplexer"/>
/// and <see cref="Database"/> stay an admin connection for setup and assertions.
/// </summary>
internal sealed class ValkeyTestEnvironment : IDisposable
{
    public const string SkipReason = $"{TestEndpoints.ValkeyVariable} is not set";

    private readonly ConnectionMultiplexer _contextMultiplexer;
    private readonly string? _passwordFile;

    private ValkeyTestEnvironment(ConnectionMultiplexer multiplexer, ConnectionMultiplexer contextMultiplexer, ValkeyContext context, string? passwordFile)
    {
        Multiplexer = multiplexer;
        _contextMultiplexer = contextMultiplexer;
        Context = context;
        _passwordFile = passwordFile;
    }

    /// <summary>Admin connection (setup, assertions, killing the context's connections).</summary>
    public ConnectionMultiplexer Multiplexer { get; }

    /// <summary>The Valkey layer under test, connected as the island user when <c>valkey-acl</c> is configured.</summary>
    public ValkeyContext Context { get; }

    public IDatabase Database => Multiplexer.GetDatabase();

    public KeySchema Keys => Context.Keys;

    public static async Task<ValkeyTestEnvironment> CreateAsync(Action<ValkeyOptions>? configure = null)
    {
        var prefix = "t" + Guid.NewGuid().ToString("N")[..12];
        var options = new ValkeyOptions { KeyPrefix = prefix, WriteRetryDelay = TimeSpan.FromMilliseconds(20) };
        configure?.Invoke(options);

        if (TestEndpoints.ValkeyAcl is not { } acl)
        {
            var (host, port) = TestEndpoints.Valkey!.Value;
            var multiplexer = await ConnectionMultiplexer.ConnectAsync($"{host}:{port},allowAdmin=true,abortConnect=true,name={prefix}");
            return new ValkeyTestEnvironment(multiplexer, multiplexer, CreateContext(multiplexer, options), null);
        }

        // The admin connection has another name, so KillOwnConnectionsAsync only cuts the island user's connections.
        var admin = await ConnectionMultiplexer.ConnectAsync(
            $"{acl.Host}:{acl.Port},password={TestEndpoints.ValkeyAclAdminPassword},allowAdmin=true,abortConnect=true,name={prefix}-admin");
        var password = Guid.NewGuid().ToString("N");
        var passwordFile = Path.GetTempFileName();
        try
        {
            object[] setUser = ["SETUSER", prefix, .. TestEndpoints.ValkeyIslandAclRule(prefix), ">" + password];
            await admin.GetServers()[0].ExecuteAsync("ACL", (ICollection<object>)setUser);
            await File.WriteAllTextAsync(passwordFile, password);
            options.ConnectionString = $"{acl.Host}:{acl.Port},name={prefix}";
            options.User = prefix;
            options.PasswordFile = passwordFile;
            var island = await ConnectionMultiplexer.ConnectAsync(ValkeyConnection.CreateConfiguration(options));
            return new ValkeyTestEnvironment(admin, island, CreateContext(island, options), passwordFile);
        }
        catch
        {
            // Leave no ACL user, password file or admin connection behind on a failed setup.
            await admin.GetServers()[0].ExecuteAsync("ACL", "DELUSER", prefix);
            File.Delete(passwordFile);
            await admin.DisposeAsync();
            throw;
        }
    }

    private static ValkeyContext CreateContext(ConnectionMultiplexer multiplexer, ValkeyOptions options) =>
        new(multiplexer, Options.Create(options), TimeProvider.System, NullLogger<ValkeyContext>.Instance);

    /// <summary>Closes this environment's own connections from the server side; other tests on the same server are not touched.</summary>
    public async Task KillOwnConnectionsAsync()
    {
        var server = Multiplexer.GetServers()[0];
        var list = (string?)await server.ExecuteAsync("CLIENT", "LIST") ?? string.Empty;
        // One line per client, fields separated by spaces; a line without an id (partial output) is skipped, not an error.
        var ids = list.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(static line => line.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .GroupBy(static f => f.Split('=')[0])
                .ToDictionary(static g => g.Key, static g => g.First().Contains('=') ? g.First()[(g.First().IndexOf('=') + 1)..] : string.Empty))
            .Where(f => f.GetValueOrDefault("name") == Context.Options.KeyPrefix && f.ContainsKey("id"))
            .Select(static f => f["id"]);
        foreach (var id in ids)
        {
            await server.ExecuteAsync("CLIENT", "KILL", "ID", id);
        }
    }

    public void Dispose()
    {
        try
        {
            if (_passwordFile is not null)
            {
                _contextMultiplexer.Dispose();
                File.Delete(_passwordFile);
                Multiplexer.GetServers()[0].Execute("ACL", "DELUSER", Context.Options.KeyPrefix);
            }
        }
        finally
        {
            Multiplexer.Dispose();
        }
    }
}

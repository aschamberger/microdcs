using MicroDCS.Valkey;
using StackExchange.Redis;

namespace MicroDCS.IntegrationTests.Valkey;

/// <summary>
/// ACL user and password file of the Valkey connection (S22, D-49), against the harness Valkey that requires authentication: a rejected
/// AUTH leaves the connection unusable there, as in production. On a server whose default user needs no password the connection would
/// silently fall back to that user.
/// </summary>
[Trait("Category", "Integration")]
public sealed class CredentialTests
{
    private const string Skip = $"{TestEndpoints.ValkeyAclVariable} is not set";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task EventuallyAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (!await condition())
        {
            (DateTime.UtcNow < deadline).ShouldBeTrue("The condition did not hold within 30 seconds.");
            await Task.Delay(100, Ct);
        }
    }

    private static async Task<bool> AuthenticatedAsAsync(ConnectionMultiplexer multiplexer, string user)
    {
        try
        {
            return (string?)await multiplexer.GetDatabase().ExecuteAsync("ACL", "WHOAMI") == user;
        }
        catch (RedisException)
        {
            return false;
        }
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyAclConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task RV17_a_rotated_password_file_is_read_on_every_reconnect_attempt()
    {
        var (host, port) = TestEndpoints.ValkeyAcl!.Value;
        using var admin = await ConnectionMultiplexer.ConnectAsync($"{host}:{port},password={TestEndpoints.ValkeyAclAdminPassword},allowAdmin=true");
        var server = admin.GetServers()[0];
        var user = "rotate-" + Guid.NewGuid().ToString("N")[..8];
        await server.ExecuteAsync("ACL", "SETUSER", user, "on", ">first-secret", "~*", "&*", "+@all");
        var file = Path.GetTempFileName();
        await File.WriteAllTextAsync(file, "first-secret\n", Ct);
        try
        {
            var configuration = ValkeyConnection.CreateConfiguration(new ValkeyOptions
            {
                ConnectionString = $"{host}:{port},connectTimeout=2000",
                User = user,
                PasswordFile = file,
            });
            using var multiplexer = await ConnectionMultiplexer.ConnectAsync(configuration);
            (await AuthenticatedAsAsync(multiplexer, user)).ShouldBeTrue();

            // The server changes the password and drops the connections; the file still holds the old one, so reconnects must fail.
            await server.ExecuteAsync("ACL", "SETUSER", user, "resetpass", ">second-secret");
            ((long)await server.ExecuteAsync("CLIENT", "KILL", "USER", user)).ShouldBeGreaterThan(0);
            await Task.Delay(TimeSpan.FromSeconds(3), Ct);
            (await AuthenticatedAsAsync(multiplexer, user)).ShouldBeFalse("the password in the file is wrong now");

            // The rotated file reaches the next connect attempt; a password read once at start would never get through.
            await File.WriteAllTextAsync(file, "second-secret\n", Ct);
            await EventuallyAsync(() => AuthenticatedAsAsync(multiplexer, user));
        }
        finally
        {
            File.Delete(file);
            await server.ExecuteAsync("ACL", "DELUSER", user);
        }
    }
}

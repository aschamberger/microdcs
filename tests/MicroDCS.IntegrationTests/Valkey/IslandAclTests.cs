using MicroDCS.Valkey;
using StackExchange.Redis;

namespace MicroDCS.IntegrationTests.Valkey;

/// <summary>
/// The island ACL rule (<c>deploy/valkey-island-acl.conf</c>, docs/deployment.md#tenancy) keeps one island from harming or watching another
/// on a shared Valkey (S22): the user reaches its own keys and channels, and Valkey refuses it everything server-wide.
/// </summary>
[Trait("Category", "Integration")]
public sealed class IslandAclTests
{
    private const string Skip = $"{TestEndpoints.ValkeyAclVariable} is not set";

    public static TheoryData<string> ServerWideCommands =>
    [
        "FLUSHALL",
        "FLUSHDB",
        "SWAPDB 0 1",
        "KEYS *",
        "RANDOMKEY",
        "DBSIZE",
        "CLIENT LIST",
        "CLIENT KILL USER default",
        "CLIENT PAUSE 1",
        "SCRIPT FLUSH",
        "SCRIPT KILL",
        "FUNCTION FLUSH",
        "PUBSUB CHANNELS",
        "SORT {prefix}:list",
        "MOVE {prefix}:key 1",
        "COPY {prefix}:key {prefix}:copy",
        "GET other-island:key",
        "PUBLISH other-island:nudge:changes x",
    ];

    [Theory(SkipUnless = nameof(TestEndpoints.ValkeyAclConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    [MemberData(nameof(ServerWideCommands))]
    public async Task The_island_user_is_refused_what_reaches_beyond_its_island(string command)
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var prefix = env.Context.Options.KeyPrefix;
        var parts = command.Replace("{prefix}", prefix, StringComparison.Ordinal).Split(' ');
        object[] arguments = [.. parts.Skip(1)];

        // Same user, with the client's own admin guard off, so that Valkey itself has to refuse the command.
        var configuration = ValkeyConnection.CreateConfiguration(env.Context.Options);
        configuration.AllowAdmin = true;
        using var island = await ConnectionMultiplexer.ConnectAsync(configuration);

        var error = await Should.ThrowAsync<RedisServerException>(() => island.GetDatabase().ExecuteAsync(parts[0], arguments));
        error.Message.ShouldStartWith("NOPERM");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyAclConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task The_island_user_reaches_its_own_keys_and_channels()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var db = env.Context.Multiplexer.GetDatabase();
        var key = env.Keys.JobOrder("acl-check");

        (await db.StringSetAsync(key, "x")).ShouldBeTrue();
        ((string?)await db.StringGetAsync(key)).ShouldBe("x");
        await db.PublishAsync(RedisChannel.Literal(env.Keys.Nudge("changes")), "x");
        (await db.ExecuteAsync("TIME")).Length.ShouldBe(2);
    }
}

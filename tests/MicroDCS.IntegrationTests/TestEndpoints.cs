namespace MicroDCS.IntegrationTests;

/// <summary>
/// Service endpoints of the compose harness, read from the environment (D-15). The same variables
/// work in host mode (`docker compose -f compose.test.yaml up -d valkey mosquitto` plus published
/// ports). Unset variables skip the tests; the compose run uses `--fail-skips on`.
/// </summary>
internal static class TestEndpoints
{
    public const string ValkeyVariable = "MICRODCS_TEST_VALKEY";
    public const string MqttVariable = "MICRODCS_TEST_MQTT";
    public const string NatsVariable = "MICRODCS_TEST_NATS";
    public const string ValkeyAclVariable = "MICRODCS_TEST_VALKEY_ACL";
    public const string AppVariable = "MICRODCS_TEST_APP";

    /// <summary>Password of the default user of the <c>valkey-acl</c> harness service (a test fixture).</summary>
    public const string ValkeyAclAdminPassword = "harness-admin";

    public static bool ValkeyConfigured => Valkey is not null;

    public static bool MqttConfigured => Mqtt is not null;

    public static bool NatsConfigured => Nats is not null;

    public static bool ValkeyAclConfigured => ValkeyAcl is not null;

    /// <summary>The application image runs as the <c>app</c> service of the <c>image</c> profile and the broker is reachable (S23).</summary>
    public static bool AppConfigured => App is not null && MqttConfigured;

    /// <summary>Topic prefix of the greetings binding of the <c>app</c> service; the Machinery Jobs binding uses it with <c>-mes</c>.</summary>
    public static string? App => Environment.GetEnvironmentVariable(AppVariable) is { Length: > 0 } prefix ? prefix : null;

    /// <summary>
    /// The ACL rule of an island's user from <c>deploy/valkey-island-acl.conf</c> (the single source that docs/deployment.md includes), for
    /// <c>ACL SETUSER</c>: the rules after the user name, without the password, with <c>site1</c> replaced by <paramref name="prefix"/>.
    /// With the <c>valkey-acl</c> service configured, the Valkey tests run the production code as such a user.
    /// </summary>
    /// <param name="prefix">The island's key prefix.</param>
    /// <returns>The rule arguments.</returns>
    public static string[] ValkeyIslandAclRule(string prefix)
    {
        var line = File.ReadLines(Path.Combine(AppContext.BaseDirectory, "valkey-island-acl.conf"))
            .Single(static l => l.StartsWith("user microdcs ", StringComparison.Ordinal));
        return [.. line.Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Skip(2)
            .Where(static token => token != ">PASSWORD")
            .Select(token => token is "~site1:*" or "&site1:*" ? token.Replace("site1", prefix, StringComparison.Ordinal) : token)];
    }

    /// <summary>Valkey endpoint as <c>host:port</c>.</summary>
    public static (string Host, int Port)? Valkey => Parse(ValkeyVariable);

    /// <summary>MQTT broker endpoint as <c>host:port</c>.</summary>
    public static (string Host, int Port)? Mqtt => Parse(MqttVariable);

    /// <summary>NATS server endpoint as <c>host:port</c>.</summary>
    public static (string Host, int Port)? Nats => Parse(NatsVariable);

    /// <summary>Endpoint of the Valkey that requires authentication, as <c>host:port</c>.</summary>
    public static (string Host, int Port)? ValkeyAcl => Parse(ValkeyAclVariable);

    private static (string Host, int Port)? Parse(string variable)
    {
        var value = Environment.GetEnvironmentVariable(variable);
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var separator = value.LastIndexOf(':');
        return separator > 0 && int.TryParse(value.AsSpan(separator + 1), out var port)
            ? (value[..separator], port)
            : throw new InvalidOperationException($"{variable} must be host:port, got '{value}'");
    }
}

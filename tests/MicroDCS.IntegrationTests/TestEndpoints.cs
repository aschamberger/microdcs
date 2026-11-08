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

    public static bool ValkeyConfigured => Valkey is not null;

    public static bool MqttConfigured => Mqtt is not null;

    public static bool NatsConfigured => Nats is not null;

    /// <summary>Valkey endpoint as <c>host:port</c>.</summary>
    public static (string Host, int Port)? Valkey => Parse(ValkeyVariable);

    /// <summary>MQTT broker endpoint as <c>host:port</c>.</summary>
    public static (string Host, int Port)? Mqtt => Parse(MqttVariable);

    /// <summary>NATS server endpoint as <c>host:port</c>.</summary>
    public static (string Host, int Port)? Nats => Parse(NatsVariable);

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

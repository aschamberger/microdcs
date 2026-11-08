namespace MicroDCS.Transport.Mqtt;

/// <summary>
/// Options of the MQTT transport, bound from <c>MicroDcs:Mqtt</c>. Environment variables use <c>APP_MicroDcs__Mqtt__...</c> (D-39).
/// Python parity: the names follow <c>MQTTConfig</c> of the archived implementation, in .NET casing.
/// </summary>
public sealed class MqttOptions
{
    /// <summary>The configuration section name.</summary>
    public const string SectionName = "MicroDcs:Mqtt";

    /// <summary>The transport name used by <c>MicroDcs:Bindings:{id}:Transport</c>.</summary>
    public const string TransportName = "mqtt";

    /// <summary>Broker host name.</summary>
    public string Hostname { get; set; } = "localhost";

    /// <summary>Broker port.</summary>
    public int Port { get; set; } = 1883;

    /// <summary>MQTT client id. Empty means <c>microdcs-{instance id}</c>. Instances must not share a client id: the broker disconnects the older one.</summary>
    public string? ClientId { get; set; }

    /// <summary>Username, or <see langword="null"/> for none.</summary>
    public string? Username { get; set; }

    /// <summary>Password. Prefer <see cref="PasswordFile"/>; setting both is an error.</summary>
    public string? Password { get; set; }

    /// <summary>File holding the password (a mounted secret). One trailing line break is removed.</summary>
    public string? PasswordFile { get; set; }

    /// <summary>File with a Kubernetes service account token. When it exists it is sent as enhanced authentication (method <c>K8S-SAT</c>).</summary>
    public string SatTokenFile { get; set; } = "/var/run/secrets/tokens/broker-sat";

    /// <summary>PEM file with the CA that signed the broker certificate. When it exists the connection uses TLS and trusts only this CA.</summary>
    public string TlsCaFile { get; set; } = "/var/run/certs/ca.crt";

    /// <summary>Fail at start instead of connecting without TLS when <see cref="TlsCaFile"/> does not exist.</summary>
    public bool RequireTls { get; set; }

    /// <summary>PEM client certificate for mutual TLS; needs <see cref="TlsClientKeyFile"/>.</summary>
    public string? TlsClientCertFile { get; set; }

    /// <summary>PEM private key of the client certificate.</summary>
    public string? TlsClientKeyFile { get; set; }

    /// <summary>How long the broker keeps the session (subscriptions, queued QoS 1 messages) after a disconnect.</summary>
    public TimeSpan SessionExpiry { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Keep-alive period.</summary>
    public TimeSpan KeepAlive { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Timeout of connect, subscribe and publish round trips.</summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>First wait before a reconnect attempt; doubles up to <see cref="ReconnectMaxDelay"/>.</summary>
    public TimeSpan ReconnectMinDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Longest wait between reconnect attempts.</summary>
    public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Quality of service of published messages: 0 or 1. QoS 2 is not used (the dedupe claim covers duplicates).</summary>
    public int PublishQos { get; set; } = 1;
}

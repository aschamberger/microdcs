namespace MicroDCS.Transport.Nats;

/// <summary>
/// Options of the NATS transport, bound from <c>MicroDcs:Nats</c>. Environment variables use <c>APP_MicroDcs__Nats__...</c> (D-39).
/// Tenancy is a server matter: one NATS account per tenant, selected by the credentials of the connection (R-20, D-47).
/// </summary>
public sealed class NatsOptions
{
    /// <summary>The configuration section name.</summary>
    public const string SectionName = "MicroDcs:Nats";

    /// <summary>The transport name used by <c>MicroDcs:Bindings:{id}:Transport</c>.</summary>
    public const string TransportName = "nats";

    /// <summary>Server URL. A cluster lists several URLs separated by commas (D-30).</summary>
    public string Url { get; set; } = "nats://localhost:4222";

    /// <summary>Connection name shown by the server. Empty means <c>microdcs-{instance id}</c>.</summary>
    public string? ClientName { get; set; }

    /// <summary>Username, or <see langword="null"/> for none.</summary>
    public string? Username { get; set; }

    /// <summary>Password. Prefer <see cref="PasswordFile"/>; setting both is an error.</summary>
    public string? Password { get; set; }

    /// <summary>File holding the password (a mounted secret). One trailing line break is removed.</summary>
    public string? PasswordFile { get; set; }

    /// <summary>File holding an authentication token. Excludes username and password.</summary>
    public string? TokenFile { get; set; }

    /// <summary>A <c>.creds</c> file (JWT and NKey seed). It decides the account of the connection.</summary>
    public string? CredsFile { get; set; }

    /// <summary>File with an NKey seed, for NKey authentication without a JWT.</summary>
    public string? NKeyFile { get; set; }

    /// <summary>PEM file with the CA that signed the server certificate. When it exists the connection uses TLS and trusts only this CA.</summary>
    public string TlsCaFile { get; set; } = "/var/run/certs/ca.crt";

    /// <summary>Fail at start instead of connecting without TLS when <see cref="TlsCaFile"/> does not exist.</summary>
    public bool RequireTls { get; set; }

    /// <summary>PEM client certificate for mutual TLS; needs <see cref="TlsClientKeyFile"/>.</summary>
    public string? TlsClientCertFile { get; set; }

    /// <summary>PEM private key of the client certificate.</summary>
    public string? TlsClientKeyFile { get; set; }

    /// <summary>JetStream domain (leaf nodes), or <see langword="null"/> for the default.</summary>
    public string? JetStreamDomain { get; set; }

    /// <summary>Prefix of the stream names. A binding prefix <c>a.b</c> is stored in the stream <c>{StreamPrefix}_a_b</c>.</summary>
    public string StreamPrefix { get; set; } = "MDCS";

    /// <summary>How long a stream keeps a message nobody acknowledged. Zero keeps them until the stream limits apply.</summary>
    public TimeSpan StreamMaxAge { get; set; } = TimeSpan.FromHours(24);

    /// <summary>Number of replicas of streams and the state bucket.</summary>
    public int Replicas { get; set; } = 1;

    /// <summary>How long the server keeps a consumer that no client uses. A restarted instance with a new id leaves its consumer behind; this removes it.</summary>
    public TimeSpan ConsumerInactiveThreshold { get; set; } = TimeSpan.FromHours(1);

    /// <summary>How long the server waits for an acknowledgement before it redelivers.</summary>
    public TimeSpan AckWait { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Delivery attempts before a message is dropped as a poison message (counted and logged).</summary>
    public int MaxDeliver { get; set; } = 5;

    /// <summary>Delay before the server redelivers a message whose processing failed.</summary>
    public TimeSpan NakDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Name of the key-value bucket that holds the state topics (<see cref="MicroDCS.Core.Transport.IStateTopicPort"/>).</summary>
    public string StateBucket { get; set; } = "microdcs-state";

    /// <summary>Timeout of connect and request round trips (stream and consumer set-up, publish acknowledgement).</summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>First wait before a reconnect attempt; the client library doubles it up to <see cref="ReconnectMaxDelay"/>.</summary>
    public TimeSpan ReconnectMinDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Longest wait between reconnect attempts.</summary>
    public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(30);
}

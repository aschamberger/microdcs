using MicroDCS.Core.Transport;

namespace MicroDCS.Core.Hosting;

/// <summary>Which parts of the application this instance runs (T-15, Q-ARCH-9 decided in S18). Both by default (single container).</summary>
[Flags]
public enum InstanceRoles
{
    /// <summary>Nothing.</summary>
    None = 0,

    /// <summary>Transports and processors.</summary>
    Processor = 1,

    /// <summary>State publishers.</summary>
    Publisher = 2,

    /// <summary>Both.</summary>
    All = Processor | Publisher,
}

/// <summary>
/// Core options. Bound from the <c>MicroDcs</c> section; environment variables use the prefix <c>APP_</c> and <c>__</c> as
/// section separator, for example <c>APP_MicroDcs__Processing__ShutdownGracePeriod</c>. Lists are comma separated strings (D-30).
/// </summary>
public sealed class MicroDcsOptions
{
    /// <summary>The configuration section name.</summary>
    public const string SectionName = "MicroDcs";

    /// <summary>Instance id; <c>POD_ID</c> or a generated value when empty.</summary>
    public string? InstanceId { get; set; }

    /// <summary>The roles of this instance.</summary>
    public InstanceRoles Roles { get; set; } = InstanceRoles.All;

    /// <summary>The CloudEvent <c>source</c> of events this application creates.</summary>
    public string? CloudEventSource { get; set; }

    /// <summary>Default expiry interval for created events; zero or absent means none.</summary>
    public TimeSpan? MessageExpiry { get; set; }

    /// <summary>Time to finish in-flight work on shutdown; maps to the host shutdown timeout.</summary>
    public TimeSpan ShutdownGracePeriod { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Upper bound for any binding's outgoing queue (D-28).</summary>
    public int OutgoingQueueMaxSize { get; set; } = 1000;

    /// <summary>Parallel workers per transport.</summary>
    public int MessageWorkers { get; set; } = 5;

    /// <summary>Lease time of the dedupe claim; must exceed the slowest handler.</summary>
    public TimeSpan DedupeLease { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long a processed event stays a duplicate.</summary>
    public TimeSpan DedupeRetention { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Lifetime of the singleton start lease.</summary>
    public TimeSpan StartLeaseTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>The bindings by identifier, <c>MicroDcs:Bindings:{id}</c>.</summary>
    public Dictionary<string, BindingOptions> Bindings { get; } = [];

    /// <summary>The management listener (health, metrics, REST, MCP), Q-OPS-1.</summary>
    public ManagementOptions Management { get; } = new();
}

/// <summary>Configuration of one binding: where a processor sits in the address space.</summary>
public sealed class BindingOptions
{
    /// <summary>The transport name, for example <c>mqtt</c>.</summary>
    public string Transport { get; set; } = "mqtt";

    /// <summary>The address prefix. Required.</summary>
    public string? Prefix { get; set; }

    /// <summary>Scope levels below the prefix; zero is a level-0 address (R-6).</summary>
    public int ScopeLevels { get; set; }

    /// <summary>Separates processors that share a prefix. The pair prefix + discriminator is unique, checked at startup.</summary>
    public string? Discriminator { get; set; }

    /// <summary>Base of the per-instance reply address.</summary>
    public string? ReplyBase { get; set; }

    /// <summary>Competing-consumer group; absent means every instance receives every message.</summary>
    public string? SharedGroup { get; set; }

    /// <summary>Outgoing queue size; zero uses the maximum.</summary>
    public int OutgoingQueueSize { get; set; }

    /// <summary>Comma separated CloudEvent type ids or <c>*</c> patterns that may be published; empty allows all (T-5).</summary>
    public string? OutgoingTypeFilter { get; set; }

    /// <summary>Scope check (A-7).</summary>
    public ScopeCheck ScopeCheck { get; set; } = ScopeCheck.Enforce;

    /// <summary>Handling of subjects on level-0 addresses (R-6).</summary>
    public Level0Subjects Level0Subjects { get; set; } = Level0Subjects.Reject;
}

/// <summary>The management listener. Loopback by default; anything else needs authentication (D-19, R-8).</summary>
public sealed class ManagementOptions
{
    /// <summary>The listen URL.</summary>
    public string Url { get; set; } = "http://127.0.0.1:8081";

    /// <summary>Serve the read-only inspection tools over MCP (optional package, dev and test only).</summary>
    public bool McpEnabled { get; set; }
}

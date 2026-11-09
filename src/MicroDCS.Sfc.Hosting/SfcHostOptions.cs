namespace MicroDCS.Sfc.Hosting;

/// <summary>Options of the SFC host, section <c>MicroDcs:Sfc</c> (D-44).</summary>
public sealed class SfcHostOptions
{
    /// <summary>The configuration section.</summary>
    public const string SectionName = "MicroDcs:Sfc";

    /// <summary>Whether this instance runs recipes. It also needs the <c>Processor</c> role (D-51).</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>The time between due-index sweeps.</summary>
    public TimeSpan SweepInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>The heartbeat gap that counts as a platform outage: open deadlines move by the gap instead of expiring (D-18).</summary>
    public TimeSpan OutageThreshold { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>The dispatch attempts of a push action whose recipe sets none.</summary>
    public int MaxAttempts { get; set; } = 3;

    /// <summary>How long before the deadline the wire expiry of a command ends.</summary>
    public TimeSpan ExpiryGrace { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>How long a work entry stays with a consumer before another one takes it over.</summary>
    public TimeSpan AutoclaimIdle { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>The deliveries of a work entry before it goes to the dead stream.</summary>
    public int MaxDeliveries { get; set; } = 5;

    /// <summary>The most work entries read at once.</summary>
    public int ReadCount { get; set; } = 10;

    /// <summary>The most jobs one ordinary sweep of a scope takes.</summary>
    public int SweepLimit { get; set; } = 100;

    internal bool IsValid() =>
        SweepInterval > TimeSpan.Zero && OutageThreshold > SweepInterval && MaxAttempts > 0 && ExpiryGrace >= TimeSpan.Zero
        && AutoclaimIdle > TimeSpan.Zero && MaxDeliveries > 0 && ReadCount > 0 && SweepLimit > 0;
}

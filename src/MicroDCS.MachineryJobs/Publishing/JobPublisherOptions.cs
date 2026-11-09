namespace MicroDCS.MachineryJobs.Publishing;

/// <summary>Options of the job order publisher, section <c>MicroDcs:JobPublisher</c> (D-51).</summary>
public sealed class JobPublisherOptions
{
    /// <summary>The configuration section name.</summary>
    public const string SectionName = "MicroDcs:JobPublisher";

    /// <summary>How long a state topic stays when nothing refreshes it. The publisher refreshes every topic at half of this.</summary>
    public TimeSpan RetainedTtl { get; set; } = TimeSpan.FromHours(48);

    /// <summary>The most change entries handled per scope in one round.</summary>
    public int ReadCount { get; set; } = 50;

    /// <summary>Lifetime of the publisher lease; it is renewed at a third of this.</summary>
    public TimeSpan LeaseTtl { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Checks the values.</summary>
    /// <returns><see langword="true"/> when they are usable.</returns>
    internal bool IsValid() => RetainedTtl >= TimeSpan.FromSeconds(2) && ReadCount > 0 && LeaseTtl >= TimeSpan.FromSeconds(3);
}

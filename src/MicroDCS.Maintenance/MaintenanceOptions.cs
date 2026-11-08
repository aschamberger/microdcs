using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace MicroDCS.Maintenance;

/// <summary>
/// Options of the maintenance operations, bound from <c>MicroDcs:Maintenance</c>. Environment names use the prefix <c>APP_</c>
/// and <c>__</c>, for example <c>APP_MicroDcs__Maintenance__Enabled</c>. The group is off by default and only starts in the
/// environments <c>Development</c> and <c>Test</c> (D-45).
/// </summary>
public sealed class MaintenanceOptions
{
    /// <summary>The configuration section name.</summary>
    public const string SectionName = "MicroDcs:Maintenance";

    /// <summary>Turns the maintenance operations on. Any environment other than Development and Test fails the start.</summary>
    public bool Enabled { get; set; }

    /// <summary>Directory of the snapshot files. Relative paths resolve against the working directory. Dev only, git-ignored.</summary>
    public string SnapshotDirectory { get; set; } = ".microdcs/snapshots";

    /// <summary>Largest number of targets a plan may touch without <c>allow_large</c>.</summary>
    public int ItemCap { get; set; } = 100;

    /// <summary>Number of snapshots kept; older ones are deleted after a new one is written.</summary>
    public int SnapshotRetention { get; set; } = 20;

    /// <summary>Largest snapshot file in bytes; a larger apply is refused.</summary>
    public long SnapshotMaxBytes { get; set; } = 5 * 1024 * 1024;

    /// <summary>How long a plan token stays valid.</summary>
    public TimeSpan PlanLifetime { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Second guard: the key <c>{prefix}:env</c> must hold the environment name, otherwise every operation is refused. Protects
    /// against a connection string that points at real data.
    /// </summary>
    public bool RequireEnvironmentMarker { get; set; }
}

/// <summary>Validates <see cref="MaintenanceOptions"/> before the host starts, including the environment guard (D-27).</summary>
/// <param name="environment">The host environment.</param>
public sealed class MaintenanceOptionsValidator(IHostEnvironment environment) : IValidateOptions<MaintenanceOptions>
{
    /// <summary>Whether maintenance may run in an environment.</summary>
    /// <param name="environmentName">The environment name.</param>
    /// <returns><see langword="true"/> for Development and Test.</returns>
    public static bool IsAllowedEnvironment(string environmentName) =>
        string.Equals(environmentName, Environments.Development, StringComparison.OrdinalIgnoreCase)
        || string.Equals(environmentName, "Test", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, MaintenanceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var errors = new List<string>();
        if (options.Enabled && !IsAllowedEnvironment(environment.EnvironmentName))
        {
            errors.Add($"MicroDcs:Maintenance:Enabled is only allowed in the environments Development and Test, not '{environment.EnvironmentName}'.");
        }

        Require(errors, !string.IsNullOrWhiteSpace(options.SnapshotDirectory), "MicroDcs:Maintenance:SnapshotDirectory is required.");
        Require(errors, options.ItemCap >= 1, "MicroDcs:Maintenance:ItemCap must be at least 1.");
        Require(errors, options.SnapshotRetention >= 1, "MicroDcs:Maintenance:SnapshotRetention must be at least 1.");
        Require(errors, options.SnapshotMaxBytes >= 1024, "MicroDcs:Maintenance:SnapshotMaxBytes must be at least 1024.");
        Require(errors, options.PlanLifetime > TimeSpan.Zero, "MicroDcs:Maintenance:PlanLifetime must be positive.");
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }

    private static void Require(List<string> errors, bool condition, string message)
    {
        if (!condition)
        {
            errors.Add(message);
        }
    }
}

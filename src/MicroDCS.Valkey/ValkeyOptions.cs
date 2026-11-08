using Microsoft.Extensions.Options;

namespace MicroDCS.Valkey;

/// <summary>
/// Options of the Valkey layer, bound from <c>MicroDcs:Valkey</c>. Environment names use the prefix <c>APP_</c> and
/// <c>__</c>, for example <c>APP_MicroDcs__Valkey__ConnectionString</c>.
/// </summary>
public sealed class ValkeyOptions
{
    /// <summary>The configuration section name.</summary>
    public const string SectionName = "MicroDcs:Valkey";

    /// <summary>StackExchange.Redis connection string; may carry the password, so it is never logged.</summary>
    public string ConnectionString { get; set; } = "localhost:6379";

    /// <summary>Prefix of every key, one per island (D-42).</summary>
    public string KeyPrefix { get; set; } = "microdcs";

    /// <summary>Fallback read interval of the poller when no nudge arrives (D-43).</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>How often the poller reads the scope registry.</summary>
    public TimeSpan ScopeRefreshInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Attempts of a write when the outcome is unknown after a connection error (R-4).</summary>
    public int WriteAttempts { get; set; } = 4;

    /// <summary>Pause before a write is checked and repeated after a connection error.</summary>
    public TimeSpan WriteRetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Approximate length limit of the change streams read by cursor (not of work streams, D-42).</summary>
    public int ChangeStreamMaxLength { get; set; } = 5000;
}

/// <summary>Validates <see cref="ValkeyOptions"/> before the host starts (D-27).</summary>
public sealed class ValkeyOptionsValidator : IValidateOptions<ValkeyOptions>
{
    private const string GlobCharacters = "*?[]\\";

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, ValkeyOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var errors = new List<string>();
        Require(errors, !string.IsNullOrWhiteSpace(options.ConnectionString), "MicroDcs:Valkey:ConnectionString is required.");
        Require(errors, !string.IsNullOrWhiteSpace(options.KeyPrefix) && options.KeyPrefix.AsSpan().IndexOfAny(GlobCharacters) < 0 && !options.KeyPrefix.EndsWith(':'),
            "MicroDcs:Valkey:KeyPrefix is required, must not end with ':' and must not contain glob characters.");
        Require(errors, options.PollInterval > TimeSpan.Zero, "MicroDcs:Valkey:PollInterval must be positive.");
        Require(errors, options.ScopeRefreshInterval > TimeSpan.Zero, "MicroDcs:Valkey:ScopeRefreshInterval must be positive.");
        Require(errors, options.WriteAttempts >= 1, "MicroDcs:Valkey:WriteAttempts must be at least 1.");
        Require(errors, options.WriteRetryDelay >= TimeSpan.Zero, "MicroDcs:Valkey:WriteRetryDelay must not be negative.");
        Require(errors, options.ChangeStreamMaxLength > 0, "MicroDcs:Valkey:ChangeStreamMaxLength must be positive.");
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

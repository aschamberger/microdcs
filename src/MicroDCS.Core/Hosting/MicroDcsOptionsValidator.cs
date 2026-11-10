using MicroDCS.Core.Transport;
using Microsoft.Extensions.Options;

namespace MicroDCS.Core.Hosting;

/// <summary>Validates <see cref="MicroDcsOptions"/> before the host starts; every error ends the process (D-27, C-2).</summary>
public sealed class MicroDcsOptionsValidator : IValidateOptions<MicroDcsOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, MicroDcsOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var errors = new List<string>();

        Require(errors, !string.IsNullOrWhiteSpace(options.CloudEventSource), "MicroDcs:CloudEventSource is required.");
        Require(errors, options.ShutdownGracePeriod > TimeSpan.Zero, "MicroDcs:ShutdownGracePeriod must be positive.");
        Require(errors, options.OutgoingQueueMaxSize > 0, "MicroDcs:OutgoingQueueMaxSize must be positive.");
        Require(errors, options.MessageWorkers > 0, "MicroDcs:MessageWorkers must be positive.");
        Require(errors, options.DedupeLease > TimeSpan.Zero, "MicroDcs:DedupeLease must be positive.");
        Require(errors, options.DedupeRetention >= options.DedupeLease, "MicroDcs:DedupeRetention must not be shorter than DedupeLease.");
        Require(errors, options.MessageExpiry is null || options.MessageExpiry >= TimeSpan.Zero, "MicroDcs:MessageExpiry must not be negative.");
        Require(errors, options.MessageExpiry is null || options.MessageExpiry <= ExpiryTracker.MaxInterval, $"MicroDcs:MessageExpiry must not be longer than {ExpiryTracker.MaxInterval} (the longest timer).");

        ValidateManagement(errors, options.Management);

        var seen = new HashSet<(string, string?)>();
        foreach (var (id, binding) in options.Bindings)
        {
            var path = $"MicroDcs:Bindings:{id}";
            ValidateBinding(errors, path, binding, options);
            Require(errors, seen.Add((binding.Prefix ?? string.Empty, binding.Discriminator)), $"{path}: prefix and discriminator repeat in another binding.");
        }

        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }

    private static void ValidateManagement(List<string> errors, ManagementOptions management)
    {
        var path = "MicroDcs:Management";
        Require(errors, management.MaxResults > 0, $"{path}:MaxResults must be positive.");
        Require(errors, management.RequestsPerMinute > 0, $"{path}:RequestsPerMinute must be positive.");
        Require(errors, management.DecisionBufferSize >= 0, $"{path}:DecisionBufferSize must not be negative.");
        if (!management.Enabled)
        {
            Require(errors, !management.ApiEnabled && !management.McpEnabled && !management.WriteEnabled && !management.IncludePayloads, $"{path}: ApiEnabled, McpEnabled, WriteEnabled and IncludePayloads need Enabled.");
            return;
        }

        Require(errors, !management.WriteEnabled || management.McpEnabled, $"{path}:WriteEnabled needs McpEnabled.");
        if (!Uri.TryCreate(management.Url, UriKind.Absolute, out var url) || url.Scheme is not ("http" or "https"))
        {
            errors.Add($"{path}:Url must be an absolute http or https URL.");
            return;
        }

        Require(errors, url.IsLoopback || !string.IsNullOrWhiteSpace(management.BearerTokenFile), $"{path}:BearerTokenFile is required when Url is not a loopback address (R-8).");
    }

    private static void ValidateBinding(List<string> errors, string path, BindingOptions binding, MicroDcsOptions options)
    {
        Require(errors, !string.IsNullOrWhiteSpace(binding.Prefix), $"{path}:Prefix is required.");
        if (!string.IsNullOrWhiteSpace(binding.Prefix))
        {
            Require(errors, binding.Prefix.Split('/').All(LogicalAddress.IsValidSegment), $"{path}:Prefix segments may only use letters, digits, '_' and '-'.");
        }

        Require(errors, binding.Discriminator is null || LogicalAddress.IsValidSegment(binding.Discriminator), $"{path}:Discriminator may only use letters, digits, '_' and '-'.");
        Require(errors, binding.ScopeLevels >= 0, $"{path}:ScopeLevels must not be negative.");
        Require(errors, binding.OutgoingQueueSize >= 0 && binding.OutgoingQueueSize <= options.OutgoingQueueMaxSize, $"{path}:OutgoingQueueSize must be between 0 and OutgoingQueueMaxSize.");
        Require(errors, !string.IsNullOrWhiteSpace(binding.Transport), $"{path}:Transport is required.");
    }

    private static void Require(List<string> errors, bool condition, string message)
    {
        if (!condition)
        {
            errors.Add(message);
        }
    }
}

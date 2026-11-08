using MicroDCS.Core.Hosting;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Options;

namespace MicroDCS.Transport.Nats;

/// <summary>Validates <see cref="NatsOptions"/> and the NATS bindings at start; every error ends the process (D-27).</summary>
public sealed class NatsOptionsValidator : IValidateOptions<NatsOptions>
{
    private readonly MicroDcsOptions _core;

    /// <summary>Initializes a new instance of the <see cref="NatsOptionsValidator"/> class.</summary>
    /// <param name="core">The core options, for the bindings that use NATS.</param>
    public NatsOptionsValidator(IOptions<MicroDcsOptions> core)
    {
        ArgumentNullException.ThrowIfNull(core);
        _core = core.Value;
    }

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, NatsOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var errors = new List<string>();
        var path = NatsOptions.SectionName;

        Require(errors, SplitUrls(options.Url).Length > 0 && SplitUrls(options.Url).All(static u => Uri.TryCreate(u, UriKind.Absolute, out var uri) && uri.Scheme is "nats" or "tls" or "ws" or "wss"), $"{path}:Url must list nats://, tls://, ws:// or wss:// URLs.");
        Require(errors, options.Password is null || options.PasswordFile is null, $"{path}: set Password or PasswordFile, not both.");
        Require(errors, options.PasswordFile is null || File.Exists(options.PasswordFile), $"{path}:PasswordFile does not exist.");
        Require(errors, (options.Password is null && options.PasswordFile is null) || options.Username is not null, $"{path}: a password needs Username.");
        Require(errors, options.TokenFile is null || File.Exists(options.TokenFile), $"{path}:TokenFile does not exist.");
        Require(errors, options.CredsFile is null || File.Exists(options.CredsFile), $"{path}:CredsFile does not exist.");
        Require(errors, options.NKeyFile is null || File.Exists(options.NKeyFile), $"{path}:NKeyFile does not exist.");
        var methods = new[] { options.Username is not null, options.TokenFile is not null, options.CredsFile is not null, options.NKeyFile is not null }.Count(static m => m);
        Require(errors, methods <= 1, $"{path}: use one authentication method (Username with Password, TokenFile, CredsFile or NKeyFile).");
        Require(errors, (options.TlsClientCertFile is null) == (options.TlsClientKeyFile is null), $"{path}: TlsClientCertFile and TlsClientKeyFile are set together.");

        var tls = File.Exists(options.TlsCaFile);
        Require(errors, tls || !options.RequireTls, $"{path}:RequireTls is set but TlsCaFile '{options.TlsCaFile}' does not exist.");
        Require(errors, tls || options.TlsClientCertFile is null, $"{path}:TlsClientCertFile is set but TLS is off: TlsCaFile '{options.TlsCaFile}' does not exist.");
        Require(errors, options.TlsClientCertFile is null || File.Exists(options.TlsClientCertFile), $"{path}:TlsClientCertFile does not exist.");
        Require(errors, options.TlsClientKeyFile is null || File.Exists(options.TlsClientKeyFile), $"{path}:TlsClientKeyFile does not exist.");

        Require(errors, options.StreamPrefix.Length > 0 && options.StreamPrefix.All(static c => char.IsAsciiLetterOrDigit(c) || c is '_' or '-'), $"{path}:StreamPrefix may only use letters, digits, '_' and '-'.");
        Require(errors, options.StateBucket.Length > 0 && options.StateBucket.All(static c => char.IsAsciiLetterOrDigit(c) || c is '_' or '-'), $"{path}:StateBucket may only use letters, digits, '_' and '-'.");
        Require(errors, options.StreamMaxAge >= TimeSpan.Zero, $"{path}:StreamMaxAge must not be negative.");
        Require(errors, options.Replicas is >= 1 and <= 5, $"{path}:Replicas must be between 1 and 5.");
        Require(errors, options.ConsumerInactiveThreshold > TimeSpan.Zero, $"{path}:ConsumerInactiveThreshold must be positive.");
        Require(errors, options.AckWait > TimeSpan.Zero, $"{path}:AckWait must be positive.");
        Require(errors, options.MaxDeliver >= 1, $"{path}:MaxDeliver must be at least 1.");
        Require(errors, options.NakDelay >= TimeSpan.Zero, $"{path}:NakDelay must not be negative.");
        Require(errors, options.OperationTimeout > TimeSpan.Zero, $"{path}:OperationTimeout must be positive.");
        Require(errors, options.ReconnectMinDelay > TimeSpan.Zero, $"{path}:ReconnectMinDelay must be positive.");
        Require(errors, options.ReconnectMaxDelay >= options.ReconnectMinDelay, $"{path}:ReconnectMaxDelay must not be shorter than ReconnectMinDelay.");

        ValidateBindings(errors);
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }

    internal static string[] SplitUrls(string? value) =>
        (value ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    private void ValidateBindings(List<string> errors)
    {
        foreach (var (id, binding) in _core.Bindings)
        {
            if (!string.Equals(binding.Transport, NatsOptions.TransportName, StringComparison.Ordinal))
            {
                continue;
            }

            var where = $"{MicroDcsOptions.SectionName}:Bindings:{id}";
            Require(errors, !string.IsNullOrEmpty(binding.Prefix) && NatsAddressRenderer.IsValidPrefix(binding.Prefix), $"{where}:Prefix needs segments of letters, digits, '_' and '-' separated by '.' or '/' for NATS.");
            Require(errors, string.IsNullOrEmpty(binding.ReplyBase) || NatsAddressRenderer.IsValidPrefix(binding.ReplyBase), $"{where}:ReplyBase needs segments of letters, digits, '_' and '-' separated by '.' or '/' for NATS.");
            Require(errors, binding.Discriminator is null || LogicalAddress.IsValidSegment(binding.Discriminator), $"{where}:Discriminator may only use letters, digits, '_' and '-'.");
            Require(errors, binding.SharedGroup is null || LogicalAddress.IsValidSegment(binding.SharedGroup), $"{where}:SharedGroup may only use letters, digits, '_' and '-'.");
        }
    }

    private static void Require(List<string> errors, bool condition, string message)
    {
        if (!condition)
        {
            errors.Add(message);
        }
    }
}

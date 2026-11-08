using Microsoft.Extensions.Options;

namespace MicroDCS.Transport.Mqtt;

/// <summary>Validates <see cref="MqttOptions"/> at start; every error ends the process (D-27).</summary>
public sealed class MqttOptionsValidator : IValidateOptions<MqttOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, MqttOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var errors = new List<string>();
        var path = MqttOptions.SectionName;

        Require(errors, !string.IsNullOrWhiteSpace(options.Hostname), $"{path}:Hostname is required.");
        Require(errors, options.Port is > 0 and <= 65535, $"{path}:Port must be between 1 and 65535.");
        Require(errors, options.PublishQos is 0 or 1, $"{path}:PublishQos must be 0 or 1.");
        Require(errors, options.SessionExpiry >= TimeSpan.Zero && options.SessionExpiry <= TimeSpan.FromSeconds(uint.MaxValue), $"{path}:SessionExpiry must be between zero and 4294967295 seconds.");
        Require(errors, options.KeepAlive >= TimeSpan.Zero && options.KeepAlive.TotalSeconds < 65536, $"{path}:KeepAlive must be between zero and 65535 seconds.");
        Require(errors, options.OperationTimeout > TimeSpan.Zero, $"{path}:OperationTimeout must be positive.");
        Require(errors, options.ReconnectMinDelay > TimeSpan.Zero, $"{path}:ReconnectMinDelay must be positive.");
        Require(errors, options.ReconnectMaxDelay >= options.ReconnectMinDelay, $"{path}:ReconnectMaxDelay must not be shorter than ReconnectMinDelay.");
        Require(errors, options.ClientId is null || options.ClientId.Length <= 65535, $"{path}:ClientId is too long.");
        Require(errors, options.Password is null || options.PasswordFile is null, $"{path}: set Password or PasswordFile, not both.");
        Require(errors, options.PasswordFile is null || File.Exists(options.PasswordFile), $"{path}:PasswordFile does not exist.");
        Require(errors, (options.Password is null && options.PasswordFile is null) || options.Username is not null, $"{path}: a password needs Username.");
        Require(errors, (options.TlsClientCertFile is null) == (options.TlsClientKeyFile is null), $"{path}: TlsClientCertFile and TlsClientKeyFile are set together.");

        var tls = File.Exists(options.TlsCaFile);
        Require(errors, tls || !options.RequireTls, $"{path}:RequireTls is set but TlsCaFile '{options.TlsCaFile}' does not exist.");
        Require(errors, tls || options.TlsClientCertFile is null, $"{path}:TlsClientCertFile is set but TLS is off: TlsCaFile '{options.TlsCaFile}' does not exist.");
        Require(errors, options.TlsClientCertFile is null || File.Exists(options.TlsClientCertFile), $"{path}:TlsClientCertFile does not exist.");
        Require(errors, options.TlsClientKeyFile is null || File.Exists(options.TlsClientKeyFile), $"{path}:TlsClientKeyFile does not exist.");

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

using System.Security.Cryptography.X509Certificates;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Formatter;

namespace MicroDCS.Transport.Mqtt;

/// <summary>
/// Builds the MQTT client options from <see cref="MqttOptions"/>: credentials, enhanced authentication, TLS and client certificate.
/// The transport builds them for every connect attempt, so rotated files are used (D-49).
/// </summary>
internal static class MqttClientOptionsFactory
{
    private const string SatMethod = "K8S-SAT";

    /// <param name="options">The options.</param>
    /// <param name="clientId">The client id.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="resumeSession">Keep a persistent session. An instance that does not subscribe must not resume the subscriptions of an earlier run.</param>
    public static MqttClientOptions Build(MqttOptions options, string clientId, ILogger logger, bool resumeSession = true)
    {
        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(options.Hostname, options.Port)
            .WithClientId(clientId)
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithCleanStart(!resumeSession)
            .WithSessionExpiryInterval(resumeSession ? (uint)Math.Min(uint.MaxValue, options.SessionExpiry.TotalSeconds) : 0)
            .WithKeepAlivePeriod(options.KeepAlive)
            .WithTimeout(options.OperationTimeout);

        var password = ResolvePassword(options);
        var hasToken = File.Exists(options.SatTokenFile);
        if (options.Username is not null)
        {
            builder.WithCredentials(options.Username, password);
        }

        var tls = File.Exists(options.TlsCaFile);
        if (tls)
        {
            var trust = new X509Certificate2Collection();
            trust.ImportFromPemFile(options.TlsCaFile);
            var clientCertificates = new List<X509Certificate2>();
            if (options.TlsClientCertFile is not null && options.TlsClientKeyFile is not null)
            {
                clientCertificates.Add(X509Certificate2.CreateFromPemFile(options.TlsClientCertFile, options.TlsClientKeyFile));
            }

            builder.WithTlsOptions(o =>
            {
                o.UseTls();
                o.WithTrustChain(trust);
                if (clientCertificates.Count > 0)
                {
                    o.WithClientCertificates(clientCertificates);
                }
            });
        }
        else if (hasToken || password is not null)
        {
            Log.CredentialsWithoutTls(logger);
        }

        var built = builder.Build();
        if (hasToken)
        {
            // Enhanced authentication with a Kubernetes service account token (Python parity: K8S-SAT).
            built.AuthenticationMethod = SatMethod;
            built.AuthenticationData = File.ReadAllBytes(options.SatTokenFile);
        }

        return built;
    }

    /// <summary>The password from <see cref="MqttOptions.PasswordFile"/> (one trailing line break removed) or <see cref="MqttOptions.Password"/>.</summary>
    public static string? ResolvePassword(MqttOptions options)
    {
        return options.PasswordFile is { } file ? SecretFile.Read(file) : options.Password;
    }
}

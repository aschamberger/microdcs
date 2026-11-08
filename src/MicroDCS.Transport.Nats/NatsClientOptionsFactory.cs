using Microsoft.Extensions.Logging;
using NATS.Client.Core;

namespace MicroDCS.Transport.Nats;

/// <summary>Builds the <see cref="NatsOpts"/> of the client from <see cref="NatsOptions"/>: authentication, TLS, reconnect. Credentials are read from files so they stay out of the environment.</summary>
internal static class NatsClientOptionsFactory
{
    /// <summary>Builds the client options.</summary>
    /// <param name="options">The transport options.</param>
    /// <param name="clientName">The connection name.</param>
    /// <param name="loggerFactory">The logger factory for the client library.</param>
    /// <param name="logger">Logger for warnings of this factory.</param>
    /// <returns>The client options.</returns>
    public static NatsOpts Build(NatsOptions options, string clientName, ILoggerFactory loggerFactory, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        ArgumentNullException.ThrowIfNull(logger);
        var urls = NatsOptionsValidator.SplitUrls(options.Url);
        var tls = File.Exists(options.TlsCaFile);
        var auth = new NatsAuthOpts();
        var hasCredentials = false;

        if (options.Username is not null)
        {
            auth = auth with { Username = options.Username, Password = options.PasswordFile is { } file ? ReadSecret(file) : options.Password };
            hasCredentials = true;
        }
        else if (options.TokenFile is { } tokenFile)
        {
            auth = auth with { Token = ReadSecret(tokenFile) };
            hasCredentials = true;
        }
        else if (options.CredsFile is { } creds)
        {
            auth = auth with { CredsFile = creds };
            hasCredentials = true;
        }
        else if (options.NKeyFile is { } nkey)
        {
            auth = auth with { NKeyFile = nkey };
            hasCredentials = true;
        }

        if (hasCredentials && !tls)
        {
            Log.CredentialsWithoutTls(logger);
        }

        var tlsOptions = tls
            ? new NatsTlsOpts
            {
                Mode = TlsMode.Require,
                CaFile = options.TlsCaFile,
                CertFile = options.TlsClientCertFile,
                KeyFile = options.TlsClientKeyFile,
            }
            : new NatsTlsOpts { Mode = TlsMode.Disable };

        return new NatsOpts
        {
            Url = string.Join(',', urls),
            Name = clientName,
            AuthOpts = auth,
            TlsOpts = tlsOptions,
            ConnectTimeout = options.OperationTimeout,
            RequestTimeout = options.OperationTimeout,
            CommandTimeout = options.OperationTimeout,
            ReconnectWaitMin = options.ReconnectMinDelay,
            ReconnectWaitMax = options.ReconnectMaxDelay,
            MaxReconnectRetry = -1,
            RetryOnInitialConnect = false,
            LoggerFactory = loggerFactory,
        };
    }

    private static string ReadSecret(string path)
    {
        var text = File.ReadAllText(path);
        if (text.EndsWith("\r\n", StringComparison.Ordinal))
        {
            return text[..^2];
        }

        return text.EndsWith('\n') ? text[..^1] : text;
    }
}

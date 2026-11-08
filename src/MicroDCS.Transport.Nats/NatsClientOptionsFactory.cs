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
        var hasCredentials = options.Username is not null || options.TokenFile is not null || options.CredsFile is not null || options.NKeyFile is not null;
        if (hasCredentials)
        {
            // Read on every connect, not once at start: a rotated secret file is picked up by the next reconnect (RV-17).
            auth = auth with { AuthCredCallback = (_, _) => ValueTask.FromResult(ReadCredentials(options)) };
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

    /// <summary>The credentials as the files hold them now, in the order of precedence of the options.</summary>
    /// <param name="options">The transport options.</param>
    /// <returns>The credentials for one connect.</returns>
    internal static NatsAuthCred ReadCredentials(NatsOptions options)
    {
        if (options.Username is not null)
        {
            return NatsAuthCred.FromUserInfo(options.Username, options.PasswordFile is { } file ? ReadSecret(file) : options.Password ?? string.Empty);
        }

        if (options.TokenFile is { } tokenFile)
        {
            return NatsAuthCred.FromToken(ReadSecret(tokenFile));
        }

        return options.CredsFile is { } creds
            ? NatsAuthCred.FromCredsFile(creds)
            : NatsAuthCred.FromNkeyFile(options.NKeyFile!);
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

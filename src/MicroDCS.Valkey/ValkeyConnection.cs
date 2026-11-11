using MicroDCS.Core.Transport;
using StackExchange.Redis;

namespace MicroDCS.Valkey;

/// <summary>Builds the StackExchange.Redis connection settings from <see cref="ValkeyOptions"/>.</summary>
public static class ValkeyConnection
{
    private static readonly HashSet<string> _disabledCommands = ["INFO", "CONFIG", "CLUSTER"];

    /// <summary>
    /// Parses <see cref="ValkeyOptions.ConnectionString"/> and applies the user, the password file and the CA file. With a password file
    /// the password is read before every socket connect, so the multiplexer's reconnects use the file as it is then (D-49); the
    /// returned instance must be the one passed to <see cref="ConnectionMultiplexer.Connect(ConfigurationOptions, TextWriter?)"/>.
    /// </summary>
    /// <param name="options">The validated options.</param>
    /// <returns>The connection settings; <see cref="ConfigurationOptions.AbortOnConnectFail"/> is on (D-27).</returns>
    public static ConfigurationOptions CreateConfiguration(ValkeyOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var configuration = ConfigurationOptions.Parse(options.ConnectionString);
        configuration.AbortOnConnectFail = true;

        // One endpoint, no cluster, no replica election (D-12): the client's housekeeping is not needed. Without it an ACL user limited
        // to its key prefix and channels (docs/deployment.md, R-20) sees no denials: the tiebreaker key and the configuration channel lie
        // outside the prefix, and INFO, CONFIG and CLUSTER would show server-wide data to one island's user.
        configuration.TieBreaker = string.Empty;
        configuration.ConfigurationChannel = string.Empty;
        configuration.CommandMap = CommandMap.Create(_disabledCommands, available: false);

        // Without INFO the client cannot read the server version and would assume 6.0. Valkey forked from Redis 7.2 (D-26), so features
        // such as XAUTOCLAIM must not be judged against 6.0; a `version=` in the connection string still wins.
        if (!options.ConnectionString.Contains("version=", StringComparison.OrdinalIgnoreCase))
        {
            configuration.DefaultVersion = new Version(7, 2);
        }
        if (options.User is { } user)
        {
            configuration.User = user;
        }

        if (options.TlsCaFile is { } caFile)
        {
            configuration.Ssl = true;
            configuration.TrustIssuer(caFile);
        }

        if (options.PasswordFile is { } passwordFile)
        {
            var password = SecretFile.Read(passwordFile);
            if (string.IsNullOrWhiteSpace(password))
            {
                // An empty password makes the client skip AUTH, and the connection would run as the default user (D-27: fail).
                throw new InvalidOperationException("MicroDcs:Valkey:PasswordFile is empty.");
            }

            configuration.Password = password;
            var previous = configuration.BeforeSocketConnect;
            configuration.BeforeSocketConnect = (endpoint, type, socket) =>
            {
                // A file that cannot be read keeps the last password; the connect then fails or succeeds on its own, and the next
                // attempt reads again. Rotation writes the file in place, so a short read failure is expected (D-49).
                try
                {
                    // An empty read (a file being rewritten) would skip AUTH; keep the last password instead.
                    var current = SecretFile.Read(passwordFile);
                    if (!string.IsNullOrWhiteSpace(current))
                    {
                        configuration.Password = current;
                    }
                }
                catch (Exception e) when (e is IOException or UnauthorizedAccessException)
                {
                    // Keep the last password (see above).
                }

                previous?.Invoke(endpoint, type, socket);
            };
        }

        return configuration;
    }
}

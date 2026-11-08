using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;

namespace MicroDCS.Transport.Nats.Tests;

public sealed class NatsClientOptionsFactoryTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("nats-client").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static NatsOptions Defaults() => new() { TlsCaFile = "/nonexistent/ca.crt" };

    private static NatsOpts Build(NatsOptions options) =>
        NatsClientOptionsFactory.Build(options, "client-1", NullLoggerFactory.Instance, NullLogger.Instance);

    private string File(string name, string content)
    {
        var path = Path.Combine(_dir, name);
        System.IO.File.WriteAllText(path, content);
        return path;
    }

    [Fact]
    public void Defaults_connect_without_tls_or_credentials()
    {
        var opts = Build(Defaults());

        opts.Url.ShouldBe("nats://localhost:4222");
        opts.Name.ShouldBe("client-1");
        opts.TlsOpts.Mode.ShouldBe(TlsMode.Disable);
        opts.MaxReconnectRetry.ShouldBe(-1);
        opts.RetryOnInitialConnect.ShouldBeFalse();
    }

    [Fact]
    public void A_password_file_loses_one_trailing_line_break()
    {
        var options = Defaults();
        options.Username = "u";
        options.PasswordFile = File("pw", "secret\n");

        Build(options).AuthOpts.AuthCredCallback.ShouldNotBeNull();
        var credentials = NatsClientOptionsFactory.ReadCredentials(options);

        credentials.ShouldBe(NatsAuthCred.FromUserInfo("u", "secret"));
    }

    [Fact]
    public void A_token_file_and_a_creds_file_select_their_methods()
    {
        var token = Defaults();
        token.TokenFile = File("token", "tok\r\n");
        NatsClientOptionsFactory.ReadCredentials(token).ShouldBe(NatsAuthCred.FromToken("tok"));

        var creds = Defaults();
        creds.CredsFile = File("a.creds", "x");
        NatsClientOptionsFactory.ReadCredentials(creds).ShouldBe(NatsAuthCred.FromCredsFile(creds.CredsFile));
    }

    [Fact]
    public async Task RV17_a_rotated_token_file_is_read_on_the_next_connect()
    {
        var options = Defaults();
        options.TokenFile = File("token", "first\n");
        var callback = Build(options).AuthOpts.AuthCredCallback.ShouldNotBeNull();

        (await callback(new Uri("nats://localhost:4222"), TestContext.Current.CancellationToken)).ShouldBe(NatsAuthCred.FromToken("first"));
        await System.IO.File.WriteAllTextAsync(options.TokenFile, "rotated\n", TestContext.Current.CancellationToken);
        (await callback(new Uri("nats://localhost:4222"), TestContext.Current.CancellationToken)).ShouldBe(NatsAuthCred.FromToken("rotated"));
    }

    [Fact]
    public void A_ca_file_turns_tls_on_and_pins_the_ca()
    {
        var options = Defaults();
        options.TlsCaFile = File("ca.crt", "x");
        options.TlsClientCertFile = File("c.pem", "x");
        options.TlsClientKeyFile = File("k.pem", "x");

        var tls = Build(options).TlsOpts;

        tls.Mode.ShouldBe(TlsMode.Require);
        tls.CaFile.ShouldBe(options.TlsCaFile);
        tls.CertFile.ShouldBe(options.TlsClientCertFile);
        tls.KeyFile.ShouldBe(options.TlsClientKeyFile);
    }

    [Fact]
    public void Credentials_without_tls_log_a_warning()
    {
        var logger = new CapturingLogger();
        var options = Defaults();
        options.Username = "u";
        options.Password = "p";

        NatsClientOptionsFactory.Build(options, "c", NullLoggerFactory.Instance, logger);

        logger.Warnings.ShouldBe(1);
    }

    private sealed class CapturingLogger : ILogger
    {
        public int Warnings { get; private set; }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings++;
            }
        }
    }
}

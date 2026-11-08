using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using MicroDCS.Transport.Mqtt;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace MicroDCS.Transport.Mqtt.Tests;

public sealed class MqttClientOptionsFactoryTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("mqtt-factory").FullName;
    private readonly ILogger _logger = Substitute.For<ILogger>();

    public MqttClientOptionsFactoryTests() => _logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private MqttOptions Defaults() => new()
    {
        Hostname = "broker.example",
        Port = 8883,
        TlsCaFile = Path.Combine(_dir, "none-ca.crt"),
        SatTokenFile = Path.Combine(_dir, "none-sat"),
    };

    private string Write(string name, string content)
    {
        var path = Path.Combine(_dir, name);
        File.WriteAllText(path, content);
        return path;
    }

    private (string Cert, string Key) WriteCertificate(string name)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=" + name, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        return (Write(name + ".crt", certificate.ExportCertificatePem()), Write(name + ".key", key.ExportPkcs8PrivateKeyPem()));
    }

    private void CredentialWarningLogged(int times) =>
        _logger.Received(times).Log(LogLevel.Warning, Arg.Any<EventId>(), Arg.Any<object>(), null, Arg.Any<Func<object, Exception?, string>>());

    [Fact]
    public void Config_values_reach_the_client_options()
    {
        var options = Defaults();
        options.SessionExpiry = TimeSpan.FromMinutes(2);
        options.KeepAlive = TimeSpan.FromSeconds(10);

        var built = MqttClientOptionsFactory.Build(options, "client-1", _logger);

        built.ClientId.ShouldBe("client-1");
        built.SessionExpiryInterval.ShouldBe(120u);
        built.KeepAlivePeriod.ShouldBe(TimeSpan.FromSeconds(10));
        built.ProtocolVersion.ShouldBe(MQTTnet.Formatter.MqttProtocolVersion.V500);
        built.CleanSession.ShouldBeFalse();
        built.ChannelOptions.ShouldBeOfType<MQTTnet.MqttClientTcpOptions>().TlsOptions.UseTls.ShouldBeFalse();
    }

    [Fact]
    public void Without_credentials_nothing_is_sent_and_nothing_is_warned()
    {
        var built = MqttClientOptionsFactory.Build(Defaults(), "c", _logger);

        built.Credentials.ShouldBeNull();
        built.AuthenticationMethod.ShouldBeNull();
        CredentialWarningLogged(0);
    }

    [Fact]
    public void Username_and_password_are_sent_as_bytes_and_warn_without_tls()
    {
        var options = Defaults();
        options.Username = "u";
        options.Password = "secret";

        var built = MqttClientOptionsFactory.Build(options, "c", _logger);

        built.Credentials.ShouldNotBeNull().GetUserName(built).ShouldBe("u");
        Encoding.UTF8.GetString(built.Credentials.GetPassword(built)).ShouldBe("secret");
        CredentialWarningLogged(1);
    }

    [Theory]
    [InlineData("secret\n", "secret")]
    [InlineData("secret\r\n", "secret")]
    [InlineData("secret", "secret")]
    [InlineData("secret\n\n", "secret\n")]
    public void The_password_file_loses_one_trailing_line_break(string content, string expected)
    {
        var options = Defaults();
        options.Username = "u";
        options.PasswordFile = Write("pw", content);

        MqttClientOptionsFactory.ResolvePassword(options).ShouldBe(expected);
    }

    [Fact]
    public void A_sat_token_becomes_enhanced_authentication()
    {
        var options = Defaults();
        options.SatTokenFile = Write("sat", "token-bytes");

        var built = MqttClientOptionsFactory.Build(options, "c", _logger);

        built.AuthenticationMethod.ShouldBe("K8S-SAT");
        Encoding.UTF8.GetString(built.AuthenticationData).ShouldBe("token-bytes");
        CredentialWarningLogged(1);
    }

    [Fact]
    public void A_ca_file_turns_tls_on_and_silences_the_warning()
    {
        var (ca, _) = WriteCertificate("ca");
        var options = Defaults();
        options.TlsCaFile = ca;
        options.Username = "u";
        options.Password = "p";

        var built = MqttClientOptionsFactory.Build(options, "c", _logger);

        var tls = built.ChannelOptions.ShouldBeOfType<MQTTnet.MqttClientTcpOptions>().TlsOptions;
        tls.UseTls.ShouldBeTrue();
        tls.TrustChain.ShouldNotBeNull().Count.ShouldBe(1);
        tls.ClientCertificatesProvider.ShouldBeNull();
        CredentialWarningLogged(0);
    }

    [Fact]
    public void A_client_certificate_is_loaded_for_mutual_tls()
    {
        var (ca, _) = WriteCertificate("ca");
        var (cert, key) = WriteCertificate("client");
        var options = Defaults();
        options.TlsCaFile = ca;
        options.TlsClientCertFile = cert;
        options.TlsClientKeyFile = key;

        var built = MqttClientOptionsFactory.Build(options, "c", _logger);

        var provider = built.ChannelOptions.ShouldBeOfType<MQTTnet.MqttClientTcpOptions>().TlsOptions.ClientCertificatesProvider.ShouldNotBeNull();
        var certificates = provider.GetCertificates();
        certificates.ShouldNotBeNull().Count.ShouldBe(1);
        certificates[0].ShouldBeOfType<X509Certificate2>().HasPrivateKey.ShouldBeTrue();
    }
}

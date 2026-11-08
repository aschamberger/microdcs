using MicroDCS.Transport.Mqtt;

namespace MicroDCS.Transport.Mqtt.Tests;

public sealed class MqttOptionsValidatorTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("mqtt-opts").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static MqttOptions Defaults() => new() { TlsCaFile = "/nonexistent/ca.crt", SatTokenFile = "/nonexistent/sat" };

    private static string[] Failures(MqttOptions options) =>
        new MqttOptionsValidator().Validate(null, options).Failures?.ToArray() ?? [];

    private string Touch(string name)
    {
        var path = Path.Combine(_dir, name);
        File.WriteAllText(path, "x");
        return path;
    }

    [Fact]
    public void Defaults_are_valid_without_tls()
    {
        Failures(Defaults()).ShouldBeEmpty();
    }

    [Fact]
    public void Require_tls_without_a_ca_file_fails()
    {
        var options = Defaults();
        options.RequireTls = true;

        Failures(options).ShouldContain(f => f.Contains("RequireTls", StringComparison.Ordinal));
    }

    [Fact]
    public void Require_tls_with_a_ca_file_is_valid()
    {
        var options = Defaults();
        options.RequireTls = true;
        options.TlsCaFile = Touch("ca.crt");

        Failures(options).ShouldBeEmpty();
    }

    [Fact]
    public void Client_certificate_needs_key_and_tls()
    {
        var options = Defaults();
        options.TlsClientCertFile = Touch("c.crt");

        var failures = Failures(options);

        failures.ShouldContain(f => f.Contains("set together", StringComparison.Ordinal));
        failures.ShouldContain(f => f.Contains("TLS is off", StringComparison.Ordinal));
    }

    [Fact]
    public void Password_and_password_file_are_exclusive_and_need_a_user()
    {
        var options = Defaults();
        options.Password = "p";
        options.PasswordFile = Touch("pw");

        Failures(options).ShouldContain(f => f.Contains("not both", StringComparison.Ordinal));

        options.PasswordFile = null;
        Failures(options).ShouldContain(f => f.Contains("needs Username", StringComparison.Ordinal));
    }

    [Fact]
    public void A_missing_password_file_fails()
    {
        var options = Defaults();
        options.Username = "u";
        options.PasswordFile = Path.Combine(_dir, "missing");

        Failures(options).ShouldContain(f => f.Contains("PasswordFile does not exist", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(70000)]
    public void Port_is_checked(int port)
    {
        var options = Defaults();
        options.Port = port;

        Failures(options).ShouldContain(f => f.Contains("Port", StringComparison.Ordinal));
    }

    [Fact]
    public void Qos_two_is_refused()
    {
        var options = Defaults();
        options.PublishQos = 2;

        Failures(options).ShouldContain(f => f.Contains("PublishQos", StringComparison.Ordinal));
    }

    [Fact]
    public void Reconnect_delays_are_ordered()
    {
        var options = Defaults();
        options.ReconnectMaxDelay = TimeSpan.FromMilliseconds(1);

        Failures(options).ShouldContain(f => f.Contains("ReconnectMaxDelay", StringComparison.Ordinal));
    }
}

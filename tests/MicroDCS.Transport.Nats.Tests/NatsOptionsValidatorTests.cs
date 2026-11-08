using MicroDCS.Core.Hosting;
using Microsoft.Extensions.Options;

namespace MicroDCS.Transport.Nats.Tests;

public sealed class NatsOptionsValidatorTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("nats-opts").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static NatsOptions Defaults() => new() { TlsCaFile = "/nonexistent/ca.crt" };

    private static string[] Failures(NatsOptions options, Action<MicroDcsOptions>? core = null)
    {
        var coreOptions = new MicroDcsOptions();
        core?.Invoke(coreOptions);
        return new NatsOptionsValidator(Options.Create(coreOptions)).Validate(null, options).Failures?.ToArray() ?? [];
    }

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
    public void Several_urls_are_valid_and_a_wrong_scheme_is_not()
    {
        var options = Defaults();
        options.Url = "nats://a:4222, tls://b:4222";
        Failures(options).ShouldBeEmpty();

        options.Url = "http://a:4222";
        Failures(options).ShouldContain(f => f.Contains("Url", StringComparison.Ordinal));
    }

    [Fact]
    public void Require_tls_without_a_ca_file_fails()
    {
        var options = Defaults();
        options.RequireTls = true;

        Failures(options).ShouldContain(f => f.Contains("RequireTls", StringComparison.Ordinal));
    }

    [Fact]
    public void Two_authentication_methods_fail()
    {
        var options = Defaults();
        options.Username = "u";
        options.CredsFile = Touch("a.creds");

        Failures(options).ShouldContain(f => f.Contains("one authentication method", StringComparison.Ordinal));
    }

    [Fact]
    public void A_password_needs_a_username_and_both_password_forms_exclude_each_other()
    {
        var options = Defaults();
        options.Password = "p";
        Failures(options).ShouldContain(f => f.Contains("needs Username", StringComparison.Ordinal));

        options.Username = "u";
        options.PasswordFile = Touch("pw");
        Failures(options).ShouldContain(f => f.Contains("not both", StringComparison.Ordinal));
    }

    [Fact]
    public void Missing_credential_files_fail()
    {
        var options = Defaults();
        options.CredsFile = Path.Combine(_dir, "none.creds");

        Failures(options).ShouldContain(f => f.Contains("CredsFile does not exist", StringComparison.Ordinal));
    }

    [Fact]
    public void Client_certificate_and_key_are_set_together()
    {
        var options = Defaults();
        options.TlsClientCertFile = Touch("c.pem");

        Failures(options).ShouldContain(f => f.Contains("set together", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("StreamPrefix")]
    [InlineData("StateBucket")]
    public void Names_follow_the_segment_rule(string property)
    {
        var options = Defaults();
        if (property == "StreamPrefix")
        {
            options.StreamPrefix = "a.b";
        }
        else
        {
            options.StateBucket = "a b";
        }

        Failures(options).ShouldContain(f => f.Contains(property, StringComparison.Ordinal));
    }

    [Fact]
    public void Limits_are_checked()
    {
        var options = Defaults();
        options.MaxDeliver = 0;
        options.AckWait = TimeSpan.Zero;
        options.Replicas = 9;
        options.ReconnectMaxDelay = TimeSpan.FromMilliseconds(1);

        var failures = Failures(options);
        failures.ShouldContain(f => f.Contains("MaxDeliver", StringComparison.Ordinal));
        failures.ShouldContain(f => f.Contains("AckWait", StringComparison.Ordinal));
        failures.ShouldContain(f => f.Contains("Replicas", StringComparison.Ordinal));
        failures.ShouldContain(f => f.Contains("ReconnectMaxDelay", StringComparison.Ordinal));
    }

    [Fact]
    public void A_nats_binding_with_an_invalid_prefix_or_group_fails_and_other_transports_are_ignored()
    {
        var failures = Failures(Defaults(), core =>
        {
            core.Bindings["a"] = new BindingOptions { Transport = "nats", Prefix = "dcs/jobs", ReplyBase = "dcs-reply", SharedGroup = "g1" };
            core.Bindings["b"] = new BindingOptions { Transport = "nats", Prefix = "dcs.*", SharedGroup = "g.1" };
            core.Bindings["c"] = new BindingOptions { Transport = "mqtt", Prefix = "dcs.*" };
        });

        failures.ShouldContain(f => f.Contains("Bindings:b:Prefix", StringComparison.Ordinal));
        failures.ShouldContain(f => f.Contains("Bindings:b:SharedGroup", StringComparison.Ordinal));
        failures.ShouldNotContain(f => f.Contains("Bindings:a", StringComparison.Ordinal));
        failures.ShouldNotContain(f => f.Contains("Bindings:c", StringComparison.Ordinal));
    }
}

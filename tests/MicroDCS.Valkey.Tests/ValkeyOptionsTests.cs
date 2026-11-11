using MicroDCS.Core.Hosting;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MicroDCS.Valkey.Tests;

/// <summary>Options validation and registration of the Valkey layer.</summary>
public class ValkeyOptionsTests
{
    private readonly ValkeyOptionsValidator _validator = new();

    [Fact]
    public void DefaultsAreValid() => _validator.Validate(null, new ValkeyOptions()).Succeeded.ShouldBeTrue();

    [Theory]
    [InlineData("")]
    [InlineData("prefix:")]
    [InlineData("pre*fix")]
    [InlineData("pre[fix]")]
    public void BadPrefixesAreRejected(string prefix)
    {
        var result = _validator.Validate(null, new ValkeyOptions { KeyPrefix = prefix });
        result.Failed.ShouldBeTrue();
        (result.Failures ?? []).ShouldContain(f => f.Contains("KeyPrefix", StringComparison.Ordinal));
    }

    [Fact]
    public void NonPositiveIntervalsAndCountsAreRejected()
    {
        var result = _validator.Validate(null, new ValkeyOptions
        {
            ConnectionString = " ",
            PollInterval = TimeSpan.Zero,
            ScopeRefreshInterval = TimeSpan.Zero,
            WriteAttempts = 0,
            WriteRetryDelay = TimeSpan.FromSeconds(-1),
            ChangeStreamMaxLength = 0,
        });
        (result.Failures ?? []).Count().ShouldBe(6);
    }

    [Fact]
    public void CredentialFilesMustExistAndThePasswordComesFromOnePlace()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, "secret\n");
            var result = _validator.Validate(null, new ValkeyOptions
            {
                ConnectionString = "valkey:6379,password=inline",
                PasswordFile = file,
                TlsCaFile = "/nonexistent/ca.crt",
            });
            var failures = result.Failures?.ToList() ?? [];
            failures.Count.ShouldBe(2);
            failures.ShouldContain(f => f.Contains("TlsCaFile does not exist", StringComparison.Ordinal));
            failures.ShouldContain(f => f.Contains("not both", StringComparison.Ordinal));

            _validator.Validate(null, new ValkeyOptions { PasswordFile = "/nonexistent/password" }).Failures!.ShouldHaveSingleItem().ShouldContain("PasswordFile");
            _validator.Validate(null, new ValkeyOptions { PasswordFile = file, User = "app" }).Succeeded.ShouldBeTrue();
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("\n")]
    [InlineData("  \r\n")]
    public void AnEmptyPasswordFileIsRejectedBecauseItWouldSkipAuth(string content)
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, content);
            _validator.Validate(null, new ValkeyOptions { PasswordFile = file }).Failures!.ShouldHaveSingleItem().ShouldContain("PasswordFile is empty");
            Should.Throw<InvalidOperationException>(() => ValkeyConnection.CreateConfiguration(new ValkeyOptions { PasswordFile = file }));
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public void AReconnectKeepsTheLastPasswordWhenTheFileIsEmptyForAMoment()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, "first\n");
            var configuration = ValkeyConnection.CreateConfiguration(new ValkeyOptions { PasswordFile = file });
            using var socket = new System.Net.Sockets.Socket(System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            var beforeConnect = configuration.BeforeSocketConnect.ShouldNotBeNull();

            File.WriteAllText(file, string.Empty);
            beforeConnect(new System.Net.DnsEndPoint("valkey", 6379), ConnectionType.Interactive, socket);
            configuration.Password.ShouldBe("first");

            File.WriteAllText(file, "second\n");
            beforeConnect(new System.Net.DnsEndPoint("valkey", 6379), ConnectionType.Interactive, socket);
            configuration.Password.ShouldBe("second");
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Theory]
    [InlineData("valkey:6379", null, false)]
    [InlineData("valkey:6379,ssl=true", null, true)]
    [InlineData("valkey:6379", "ca", true)]
    public void RequireTlsNeedsTlsFromTheCaFileOrTheConnectionString(string connectionString, string? caFile, bool valid)
    {
        var file = caFile is null ? null : Path.GetTempFileName();
        try
        {
            _validator.Validate(null, new ValkeyOptions { ConnectionString = connectionString, TlsCaFile = file, RequireTls = true }).Succeeded.ShouldBe(valid);
        }
        finally
        {
            if (file is not null)
            {
                File.Delete(file);
            }
        }
    }

    [Fact]
    public void AnUnparsableConnectionStringIsReportedWithoutItsText()
    {
        var failure = _validator.Validate(null, new ValkeyOptions { ConnectionString = "valkey:6379,password=secret,nosuchkey=1" }).Failures!.ShouldHaveSingleItem();
        failure.ShouldContain("cannot be parsed");
        failure.ShouldNotContain("secret");
    }

    [Fact]
    public void TheConfigurationAppliesUserPasswordFileAndKeepsAbortOnConnectFail()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, "from-file\n");
            var configuration = ValkeyConnection.CreateConfiguration(new ValkeyOptions { ConnectionString = "valkey:6379,user=old", User = "app", PasswordFile = file });
            (configuration.User, configuration.Password, configuration.AbortOnConnectFail, configuration.Ssl).ShouldBe(("app", "from-file", true, false));
            configuration.BeforeSocketConnect.ShouldNotBeNull();

            // No housekeeping outside the island's prefix or server-wide (R-20): an ACL user limited to its prefix sees no denials.
            (configuration.TieBreaker, configuration.ConfigurationChannel).ShouldBe((string.Empty, string.Empty));
            configuration.CommandMap.ToString().ShouldContain("INFO=");
            configuration.DefaultVersion.ShouldBe(new Version(7, 2), "INFO is off, so the version is not detected");
            ValkeyConnection.CreateConfiguration(new ValkeyOptions { ConnectionString = "valkey:6379,version=8.1" }).DefaultVersion.ShouldBe(new Version(8, 1));
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public void ConfigurationBindsFromTheMicroDcsSection()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MicroDcs:Valkey:ConnectionString"] = "valkey:6379,password=x",
            ["MicroDcs:Valkey:KeyPrefix"] = "island1",
            ["MicroDcs:Valkey:PollInterval"] = "00:00:02",
        }).Build();
        var services = new ServiceCollection().AddMicroDcsValkey(configuration);

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<ValkeyOptions>>().Value;
        (options.ConnectionString, options.KeyPrefix, options.PollInterval).ShouldBe(("valkey:6379,password=x", "island1", TimeSpan.FromSeconds(2)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ValkeyReplacesTheInMemoryDefaultsInEitherCallOrder(bool valkeyFirst)
    {
        var configuration = new ConfigurationBuilder().Build();
        var services = new ServiceCollection();
        if (valkeyFirst)
        {
            services.AddMicroDcsValkey(configuration);
            services.AddMicroDcs(configuration);
        }
        else
        {
            services.AddMicroDcs(configuration);
            services.AddMicroDcsValkey(configuration);
        }

        var dedupe = services.Last(d => d.ServiceType == typeof(IDedupeStore));
        var lease = services.Last(d => d.ServiceType == typeof(IStartLease));
        lease.ImplementationType.ShouldBe(typeof(ValkeyStartLease));
        dedupe.ImplementationFactory.ShouldNotBeNull();
        services.Count(d => d.ServiceType == typeof(IDedupeStore)).ShouldBe(1);
        services.Count(d => d.ServiceType == typeof(IStartLease)).ShouldBe(1);
    }
}

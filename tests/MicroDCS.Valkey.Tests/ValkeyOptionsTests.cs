using MicroDCS.Core.Hosting;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

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

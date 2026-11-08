using MicroDCS.Core.Hosting;
using MicroDCS.Core.Transport;
using MicroDCS.Maintenance;
using MicroDCS.Valkey;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace MicroDCS.App;

/// <summary>
/// Native AOT smoke check of the Valkey wiring: binding and validation of the options (also the maintenance options with the environment guard), and the Valkey implementations
/// replacing the in-memory defaults. It does not connect (the smoke check runs without a Valkey server); the end-to-end
/// behaviour is covered by the compose integration tests. Compiling and publishing this puts StackExchange.Redis and the
/// Valkey layer under the AOT and trim analysis.
/// </summary>
internal static class ValkeySmoke
{
    public static Task<int> RunAsync(TextWriter output)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MicroDcs:Valkey:KeyPrefix"] = "smoke",
            ["MicroDcs:Valkey:ConnectionString"] = "valkey.invalid:6379",
            ["MicroDcs:Maintenance:Enabled"] = "true",
            ["MicroDcs:Maintenance:ItemCap"] = "7",
        }).Build();

        var services = new ServiceCollection();
        services.AddSingleton<IHostEnvironment>(new SmokeEnvironment());
        services.AddMicroDcs(configuration).AddMicroDcsValkey(configuration).AddMicroDcsMaintenance(configuration);
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<ValkeyOptions>>().Value;
        var dedupeReplaced = services.Last(d => d.ServiceType == typeof(IDedupeStore)).ImplementationFactory is not null;
        var leaseReplaced = services.Last(d => d.ServiceType == typeof(IStartLease)).ImplementationType == typeof(ValkeyStartLease);
        var maintenance = provider.GetRequiredService<IOptions<MaintenanceOptions>>().Value;
        var ok = options.KeyPrefix == "smoke" && dedupeReplaced && leaseReplaced && maintenance is { Enabled: true, ItemCap: 7 };
        return Report(output, ok);
    }

    private static async Task<int> Report(TextWriter output, bool ok)
    {
        await output.WriteLineAsync(ok ? "ok   valkey-registration" : "FAIL valkey-registration").ConfigureAwait(false);
        return ok ? 0 : 1;
    }
}

/// <summary>A development environment for the smoke check, which runs without a host.</summary>
internal sealed class SmokeEnvironment : IHostEnvironment
{
    public string EnvironmentName { get; set; } = Environments.Development;

    public string ApplicationName { get; set; } = "microdcs";

    public string ContentRootPath { get; set; } = AppContext.BaseDirectory;

    public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
}

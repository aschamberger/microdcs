using MicroDCS.Core.Hosting;
using MicroDCS.Core.Transport;
using MicroDCS.Valkey;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MicroDCS.App;

/// <summary>
/// Native AOT smoke check of the Valkey wiring: binding and validation of the options, and the Valkey implementations
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
        }).Build();

        var services = new ServiceCollection();
        services.AddMicroDcs(configuration).AddMicroDcsValkey(configuration);
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<ValkeyOptions>>().Value;
        var dedupeReplaced = services.Last(d => d.ServiceType == typeof(IDedupeStore)).ImplementationFactory is not null;
        var leaseReplaced = services.Last(d => d.ServiceType == typeof(IStartLease)).ImplementationType == typeof(ValkeyStartLease);
        var ok = options.KeyPrefix == "smoke" && dedupeReplaced && leaseReplaced;
        return Report(output, ok);
    }

    private static async Task<int> Report(TextWriter output, bool ok)
    {
        await output.WriteLineAsync(ok ? "ok   valkey-registration" : "FAIL valkey-registration").ConfigureAwait(false);
        return ok ? 0 : 1;
    }
}

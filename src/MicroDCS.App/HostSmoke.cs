using MicroDCS.App.Greetings;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MicroDCS.App;

/// <summary>
/// Native AOT smoke check of the core: options binding, the generated processor descriptor and payload descriptors, the
/// host lifecycle and the pipeline, run end to end on the in-memory transport. Run with <c>microdcs smoke</c>.
/// </summary>
internal static class HostSmoke
{
    public static async Task<int> RunAsync(TextWriter output)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MicroDcs:CloudEventSource"] = "urn:smoke",
            ["MicroDcs:Bindings:greetings:Transport"] = "memory",
            ["MicroDcs:Bindings:greetings:Prefix"] = "smoke",
            ["MicroDcs:Bindings:greetings:ScopeLevels"] = "2",
        }).Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMicroDcs(configuration).AddInMemoryTransport().AddProcessor<GreetingsProcessor>();
        using var provider = services.BuildServiceProvider();
        var host = provider.GetServices<IHostedService>().OfType<MicroDcsHostedService>().Single();
        var transport = provider.GetRequiredService<InMemoryTransport>();

        await host.StartAsync(CancellationToken.None).ConfigureAwait(false);
        var carried = new Dictionary<string, string>();
        var data = MicroDcsPayloads.MicroDCS_App_Greetings_Hello.Encode(new Hello("Bob") { HiddenStr = "h" }, carried);
        var request = new EventEnvelope
        {
            Id = "smoke-1",
            Source = "urn:smoke:client",
            Type = MicroDcsPayloads.MicroDCS_App_Greetings_Hello.TypeId,
            Subject = "plant1.line2/res",
            Extensions = carried,
            Data = data,
        };
        var outcome = await transport.InjectAsync(
            new InboundMessage
            {
                BindingId = "greetings",
                Kind = InboundKind.Request,
                Envelope = request,
                Address = new LogicalAddress("smoke", ["plant1", "line2"], null, Intent.Data),
            },
            CancellationToken.None).ConfigureAwait(false);
        await host.StopAsync(CancellationToken.None).ConfigureAwait(false);

        var ok = outcome == DeliveryOutcome.Ack
            && transport.Published.Length == 2
            && transport.Published[0].Envelope.Extensions.GetValueOrDefault("hiddenstr") == "h"
            && transport.Published[0].Envelope.CausationId == "smoke-1";
        await output.WriteLineAsync(ok ? "ok   host-greetings-end-to-end" : "FAIL host-greetings-end-to-end").ConfigureAwait(false);
        return ok ? 0 : 1;
    }
}

using MicroDCS.App.Greetings;
using MicroDCS.Core.Hosting;
using MicroDCS.Inspection;
using MicroDCS.Mcp;
using MicroDCS.Valkey;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace MicroDCS.App;

/// <summary>
/// <c>microdcs serve</c>: the greetings demo processor on the in-memory transport, with Valkey when
/// <c>MicroDcs:Valkey:ConnectionString</c> is set and the management listener when <c>MicroDcs:Management:Enabled</c> is true.
/// It exists so the inspection tools have a running application until the real transports arrive (S13). Configuration comes from
/// environment variables with the prefix <c>APP_</c> and <c>__</c> as separator (D-39).
/// </summary>
internal static class ServeCommand
{
    public static async Task RunAsync(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Configuration.AddEnvironmentVariables("APP_");

        builder.Services.AddMicroDcs(builder.Configuration)
            .AddInMemoryTransport()
            .AddProcessor<GreetingsProcessor>();
        if (!string.IsNullOrEmpty(builder.Configuration["MicroDcs:Valkey:ConnectionString"]))
        {
            builder.Services.AddMicroDcsValkey(builder.Configuration);
        }

        builder.Services.AddMicroDcsManagement().AddMicroDcsMcp();
        using var host = builder.Build();
        await host.RunAsync().ConfigureAwait(false);
    }
}

using System.Net;
using System.Net.Http.Headers;
using MicroDCS.Core.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Inspection.Tests;

public sealed class ManagementServerTests
{
    private static ManagementServer Create(Action<ManagementOptions> configure)
    {
        var options = new MicroDcsOptions();
        configure(options.Management);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Options.Create(options));
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<IEnumerable<MicroDCS.Core.Inspection.IInspector>>([]);
        services.AddSingleton<InspectionService>();
        services.AddSingleton<IManagementModule, PingModule>();
        var provider = services.BuildServiceProvider();
        return new ManagementServer(provider, provider.GetRequiredService<IOptions<MicroDcsOptions>>(), provider.GetRequiredService<ILoggerFactory>());
    }

    [Fact]
    public async Task DisabledMeansNoListener()
    {
        var server = Create(m => m.Enabled = false);

        await server.StartAsync(TestContext.Current.CancellationToken);

        server.GetAddresses().ShouldBeEmpty();
    }

    [Fact]
    public async Task LoopbackServesHealthAndRejectsForeignHostNames()
    {
        var ct = TestContext.Current.CancellationToken;
        var server = Create(m =>
        {
            m.Enabled = true;
            m.Url = "http://127.0.0.1:0";
        });
        await server.StartAsync(ct);
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(server.GetAddresses()[0]) };

            (await client.GetAsync("/health/live", ct)).StatusCode.ShouldBe(HttpStatusCode.OK);
            (await client.GetAsync("/health/ready", ct)).StatusCode.ShouldBe(HttpStatusCode.OK);
            (await client.GetStringAsync("/ping", ct)).ShouldBe("pong");

            using var rebinding = new HttpRequestMessage(HttpMethod.Get, "/ping");
            rebinding.Headers.Host = "evil.example";
            (await client.SendAsync(rebinding, ct)).StatusCode.ShouldBe(HttpStatusCode.MisdirectedRequest);
        }
        finally
        {
            await server.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task RV24LoopbackRejectsBrowserRequestsFromForeignOrigins()
    {
        var ct = TestContext.Current.CancellationToken;
        var server = Create(m =>
        {
            m.Enabled = true;
            m.Url = "http://127.0.0.1:0";
        });
        await server.StartAsync(ct);
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(server.GetAddresses()[0]) };

            using var foreign = new HttpRequestMessage(HttpMethod.Get, "/ping");
            foreign.Headers.Add("Origin", "https://evil.example");
            (await client.SendAsync(foreign, ct)).StatusCode.ShouldBe(HttpStatusCode.Forbidden);

            using var opaque = new HttpRequestMessage(HttpMethod.Get, "/ping");
            opaque.Headers.Add("Origin", "null");
            (await client.SendAsync(opaque, ct)).StatusCode.ShouldBe(HttpStatusCode.Forbidden);

            using var local = new HttpRequestMessage(HttpMethod.Get, "/ping");
            local.Headers.Add("Origin", "http://localhost:6274");
            (await client.SendAsync(local, ct)).StatusCode.ShouldBe(HttpStatusCode.OK);
        }
        finally
        {
            await server.StopAsync(CancellationToken.None);
        }
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData("http://127.0.0.1:3000", true)]
    [InlineData("http://[::1]:3000", true)]
    [InlineData("http://localhost", true)]
    [InlineData("https://localhost.evil.example", false)]
    [InlineData("file://", false)]
    [InlineData("null", false)]
    public void RV24OriginRule(string? origin, bool allowed) => ManagementServer.IsAllowedOrigin(origin).ShouldBe(allowed);

    [Fact]
    public async Task NonLoopbackNeedsTheBearerTokenExceptForHealth()
    {
        var ct = TestContext.Current.CancellationToken;
        var file = Path.GetTempFileName();
        await File.WriteAllTextAsync(file, "0123456789abcdef-token\n", ct);
        var server = Create(m =>
        {
            m.Enabled = true;
            m.Url = "http://0.0.0.0:0";
            m.BearerTokenFile = file;
        });
        await server.StartAsync(ct);
        try
        {
            var port = new Uri(server.GetAddresses()[0]).Port;
            using var client = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };

            (await client.GetAsync("/health/live", ct)).StatusCode.ShouldBe(HttpStatusCode.OK);
            (await client.GetAsync("/ping", ct)).StatusCode.ShouldBe(HttpStatusCode.Unauthorized);

            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", "wrong-token-value-1234");
            (await client.GetAsync("/ping", ct)).StatusCode.ShouldBe(HttpStatusCode.Unauthorized);

            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", "0123456789abcdef-token");
            (await client.GetStringAsync("/ping", ct)).ShouldBe("pong");
        }
        finally
        {
            await server.StopAsync(CancellationToken.None);
            File.Delete(file);
        }
    }

    [Fact]
    public async Task ShortTokenFailsStartup()
    {
        var ct = TestContext.Current.CancellationToken;
        var file = Path.GetTempFileName();
        await File.WriteAllTextAsync(file, "short", ct);
        var server = Create(m =>
        {
            m.Enabled = true;
            m.Url = "http://0.0.0.0:0";
            m.BearerTokenFile = file;
        });

        try
        {
            await Should.ThrowAsync<InvalidOperationException>(async () => await server.StartAsync(ct));
        }
        finally
        {
            File.Delete(file);
        }
    }
}

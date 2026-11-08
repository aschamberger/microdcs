using System.Net;
using System.Security.Cryptography;
using System.Text;
using MicroDCS.Core.Hosting;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Inspection;

/// <summary>
/// The management listener (D-40): one Kestrel host with <c>/health/live</c>, <c>/health/ready</c> and the endpoints of the
/// registered modules. Off by default: when <c>MicroDcs:Management:Enabled</c> is false no port is opened. On loopback it checks
/// the <c>Host</c> header (DNS rebinding) and the <c>Origin</c> header of browser requests; on any other address every request
/// except health needs the bearer token (R-8).
/// A listener that cannot start ends the process (D-27).
/// </summary>
public sealed class ManagementServer : IHostedService, IAsyncDisposable
{
    private const string BearerPrefix = "Bearer ";

    private readonly IServiceProvider _services;
    private readonly ManagementOptions _options;
    private readonly ILogger _logger;
    private WebApplication? _app;

    /// <summary>Initializes a new instance of the <see cref="ManagementServer"/> class.</summary>
    /// <param name="services">The application services.</param>
    /// <param name="options">The core options.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public ManagementServer(IServiceProvider services, IOptions<MicroDcsOptions> options, ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _services = services;
        _options = options.Value.Management;
        _logger = loggerFactory.CreateLogger<ManagementServer>();
    }

    /// <summary>Gets the addresses the listener is bound to.</summary>
    /// <returns>The addresses, or an empty list when it is not running.</returns>
    public IReadOnlyList<string> GetAddresses() => _app is null ? [] : [.. _app.Urls];

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            Log.ListenerDisabled(_logger);
            return;
        }

        var url = new Uri(_options.Url);
        var token = url.IsLoopback ? null : await ReadTokenAsync(cancellationToken).ConfigureAwait(false);
        var loggerFactory = _services.GetRequiredService<ILoggerFactory>();

        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls(_options.Url);
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(loggerFactory);
        builder.Services.AddSingleton(_services.GetRequiredService<InspectionService>());
        builder.Services.AddSingleton(_services.GetRequiredService<IOptions<MicroDcsOptions>>());
        var modules = _services.GetServices<IManagementModule>().Where(m => m.IsEnabled(_services)).ToList();
        foreach (var module in modules)
        {
            module.ConfigureServices(builder.Services, _services);
        }

        var app = builder.Build();
        app.Use((context, next) => Guard(context, next, url.IsLoopback, token is null ? null : SHA256.HashData(Encoding.UTF8.GetBytes(token))));
        app.MapGet("/health/live", static context => WriteTextAsync(context, StatusCodes.Status200OK, "live"));
        app.MapGet("/health/ready", ReadyAsync);
        foreach (var module in modules)
        {
            module.Map(app);
        }

        await app.StartAsync(cancellationToken).ConfigureAwait(false);
        _app = app;
        var urls = string.Join(", ", app.Urls);
        var authentication = token is null ? "none (loopback)" : "bearer";
        Log.ListenerStarted(_logger, urls, authentication);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_app is { } app)
        {
            _app = null;
            await app.StopAsync(cancellationToken).ConfigureAwait(false);
            await app.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_app is { } app)
        {
            _app = null;
            await app.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static bool IsLoopbackHost(HostString host) =>
        host.Host is "localhost" or "[::1]" or "::1" || (IPAddress.TryParse(host.Host, out var address) && IPAddress.IsLoopback(address));

    /// <summary>
    /// A browser request carries <c>Origin</c>; on loopback only a loopback origin is accepted (MCP Streamable HTTP: servers must
    /// validate <c>Origin</c>). Clients that are not browsers send none and pass.
    /// </summary>
    internal static bool IsAllowedOrigin(string? origin)
    {
        if (string.IsNullOrEmpty(origin))
        {
            return true;
        }

        return Uri.TryCreate(origin, UriKind.Absolute, out var uri)
            && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps)
            && (uri.IsLoopback || uri.Host == "localhost");
    }

    private static bool BearerMatches(string? header, byte[] expected)
    {
        if (header is null || !header.StartsWith(BearerPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var given = SHA256.HashData(Encoding.UTF8.GetBytes(header[BearerPrefix.Length..].Trim()));
        return CryptographicOperations.FixedTimeEquals(given, expected);
    }

    private static Task WriteTextAsync(HttpContext context, int status, string text)
    {
        context.Response.StatusCode = status;
        context.Response.ContentType = "text/plain";
        return context.Response.WriteAsync(text, context.RequestAborted);
    }

    private static async Task ReadyAsync(HttpContext context)
    {
        // Health checks are optional: an application that registered some gets them here.
        var checks = context.RequestServices.GetService<HealthCheckService>();
        var healthy = checks is null || (await checks.CheckHealthAsync(context.RequestAborted).ConfigureAwait(false)).Status == HealthStatus.Healthy;
        await WriteTextAsync(context, healthy ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable, healthy ? "ready" : "not ready").ConfigureAwait(false);
    }

    // expected is the SHA-256 of the token and is set exactly when the listener is not on loopback.
    private async Task Guard(HttpContext context, RequestDelegate next, bool loopback, byte[]? expected)
    {
        var health = context.Request.Path.StartsWithSegments("/health", StringComparison.Ordinal);
        if (loopback && !IsLoopbackHost(context.Request.Host))
        {
            Log.RequestRejected(_logger, "host header is not a loopback name");
            context.Response.StatusCode = StatusCodes.Status421MisdirectedRequest;
            return;
        }

        if (loopback && !IsAllowedOrigin(context.Request.Headers.Origin.ToString()))
        {
            Log.RequestRejected(_logger, "origin is not a loopback origin");
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return;
        }

        if (!loopback && !health && !BearerMatches(context.Request.Headers.Authorization.ToString(), expected!))
        {
            Log.RequestRejected(_logger, "missing or wrong bearer token");
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            context.Response.Headers.WWWAuthenticate = "Bearer";
            return;
        }

        await next(context).ConfigureAwait(false);
    }

    private async Task<string> ReadTokenAsync(CancellationToken cancellationToken)
    {
        var path = _options.BearerTokenFile ?? throw new InvalidOperationException("MicroDcs:Management:BearerTokenFile is required for a non-loopback listener.");
        var token = (await File.ReadAllTextAsync(path, cancellationToken).ConfigureAwait(false)).Trim();
        return token.Length >= 16
            ? token
            : throw new InvalidOperationException("The management bearer token must have at least 16 characters.");
    }
}

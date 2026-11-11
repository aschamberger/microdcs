using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MicroDCS.Inspection;

/// <summary>
/// The logger factory of the management listener's web host: the host's own factory, with the ASP.NET Core categories held at Warning
/// unless <c>Logging:LogLevel:Microsoft.AspNetCore</c> in the host configuration names another level. Without it Kestrel and the
/// hosting diagnostics log every probe request at Information, every few seconds per pod (S22).
/// </summary>
internal sealed class ManagementLoggerFactory : ILoggerFactory
{
    private const string AspNetCorePrefix = "Microsoft.AspNetCore";

    private readonly ILoggerFactory _inner;
    private readonly LogLevel _aspNetCoreMinimum;

    public ManagementLoggerFactory(ILoggerFactory inner, IConfiguration? configuration)
    {
        _inner = inner;
        _aspNetCoreMinimum = Enum.TryParse<LogLevel>(configuration?["Logging:LogLevel:Microsoft.AspNetCore"], ignoreCase: true, out var level)
            ? level
            : LogLevel.Warning;
    }

    public ILogger CreateLogger(string categoryName)
    {
        var logger = _inner.CreateLogger(categoryName);
        return categoryName.StartsWith(AspNetCorePrefix, StringComparison.Ordinal) ? new MinimumLevelLogger(logger, _aspNetCoreMinimum) : logger;
    }

    public void AddProvider(ILoggerProvider provider) => _inner.AddProvider(provider);

    /// <summary>The inner factory belongs to the host; the listener must not dispose it.</summary>
    public void Dispose()
    {
        // Nothing of its own to release.
    }

    private sealed class MinimumLevelLogger(ILogger inner, LogLevel minimum) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => logLevel >= minimum && inner.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= minimum)
            {
                inner.Log(logLevel, eventId, state, exception, formatter);
            }
        }
    }
}

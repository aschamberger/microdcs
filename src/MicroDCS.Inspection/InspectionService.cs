using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Inspection;

/// <summary>
/// The one entry point for adapters (D-19, D-40): finds the inspector, caps the result size, applies the redaction policy,
/// limits the call rate and writes the audit line (caller, tool, arguments; never results). REST and MCP only call this.
/// </summary>
public sealed class InspectionService
{
    /// <summary>Longest text a value keeps; longer values are cut so one value cannot fill a model's context.</summary>
    public const int MaxValueLength = 2000;

    private readonly Dictionary<string, IInspector> _inspectors;
    private readonly ManagementOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger _audit;
    private readonly Lock _gate = new();

    // One window per caller (rest, mcp), so an MES that polls the REST API cannot use up the budget of the MCP tools (D-54).
    private readonly Dictionary<string, (long Start, int Count)> _windows = new(StringComparer.Ordinal);

    /// <summary>Initializes a new instance of the <see cref="InspectionService"/> class.</summary>
    /// <param name="inspectors">The registered inspectors.</param>
    /// <param name="options">The core options.</param>
    /// <param name="time">The clock.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public InspectionService(IEnumerable<IInspector> inspectors, IOptions<MicroDcsOptions> options, TimeProvider time, ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(inspectors);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _inspectors = inspectors.ToDictionary(static i => i.Name, StringComparer.Ordinal);
        _options = options.Value.Management;
        _time = time;
        _audit = loggerFactory.CreateLogger(PipelineTelemetry.SecurityCategory);
    }

    /// <summary>The names of the registered inspectors.</summary>
    public IReadOnlyCollection<string> InspectorNames => _inspectors.Keys;

    /// <summary>Runs a query.</summary>
    /// <param name="caller">Who asks, for the audit line, for example <c>mcp</c>.</param>
    /// <param name="tool">The tool name, for the audit line.</param>
    /// <param name="inspector">The inspector name.</param>
    /// <param name="query">The query; the limit is capped by <c>MicroDcs:Management:MaxResults</c>.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The redacted, size limited page.</returns>
    /// <exception cref="InspectionRefusedException">The inspector is unknown or the rate limit is reached.</exception>
    public ValueTask<InspectionPage> QueryAsync(string caller, string tool, string inspector, InspectionQuery query, CancellationToken cancellationToken) =>
        QueryCoreAsync(caller, tool, inspector, query, acquire: true, truncate: true, cancellationToken);

    /// <summary>
    /// Runs a query for an adapter that maps the fields to its own typed contract (REST): with <paramref name="acquire"/> false a
    /// further query of the same request takes no second rate-limit token, and values are not cut, because a cut value would
    /// corrupt a typed field. Audit, cap and redaction apply as in <see cref="QueryAsync"/>.
    /// </summary>
    internal ValueTask<InspectionPage> QueryTypedAsync(string caller, string tool, string inspector, InspectionQuery query, bool acquire, CancellationToken cancellationToken) =>
        QueryCoreAsync(caller, tool, inspector, query, acquire, truncate: false, cancellationToken);

    private async ValueTask<InspectionPage> QueryCoreAsync(string caller, string tool, string inspector, InspectionQuery query, bool acquire, bool truncate, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(caller);
        ArgumentNullException.ThrowIfNull(tool);
        ArgumentNullException.ThrowIfNull(inspector);
        ArgumentNullException.ThrowIfNull(query);

        if (acquire && !TryAcquire(caller))
        {
            Log.CallRefused(_audit, caller, "rate limit");
            throw new InspectionRefusedException($"Rate limit of {_options.RequestsPerMinute} calls per minute reached. Try again later.", rateLimited: true);
        }

        if (!_inspectors.TryGetValue(inspector, out var found))
        {
            Log.CallRefused(_audit, caller, "unknown inspector");
            throw new InspectionRefusedException($"Unknown inspector '{inspector}'. Known: {string.Join(", ", _inspectors.Keys.Order(StringComparer.Ordinal))}.");
        }

        var capped = query with { Limit = Math.Clamp(query.Limit, 1, _options.MaxResults) };
        var arguments = FormatArguments(capped.Arguments);
        Log.ToolCalled(_audit, caller, tool, inspector, capped.Kind, capped.Key, arguments);

        var page = await found.InspectAsync(capped, cancellationToken).ConfigureAwait(false);
        return Limit(Redaction.Apply(page, _options.IncludePayloads), capped.Limit, truncate);
    }

    /// <summary>Writes the audit line of a call that does not go through <see cref="QueryAsync"/>, such as an injected event.</summary>
    /// <param name="caller">Who asks.</param>
    /// <param name="tool">The tool name.</param>
    /// <exception cref="InspectionRefusedException">The rate limit is reached.</exception>
    public void Admit(string caller, string tool)
    {
        ArgumentNullException.ThrowIfNull(caller);
        ArgumentNullException.ThrowIfNull(tool);
        if (!TryAcquire(caller))
        {
            Log.CallRefused(_audit, caller, "rate limit");
            throw new InspectionRefusedException($"Rate limit of {_options.RequestsPerMinute} calls per minute reached. Try again later.", rateLimited: true);
        }

        Log.ToolCalled(_audit, caller, tool, "-", null, null, string.Empty);
    }

    private static InspectionPage Limit(InspectionPage page, int limit, bool truncate)
    {
        if (!truncate)
        {
            return new InspectionPage([.. page.Entries.Take(limit)], page.NextCursor);
        }

        var entries = page.Entries.Take(limit).Select(static e => e with
        {
            Fields = e.Fields.ToDictionary(
                static f => f.Key,
                static f => f.Value.Text.Length > MaxValueLength
                    ? f.Value with { Text = string.Concat(f.Value.Text.AsSpan(0, MaxValueLength), "...") }
                    : f.Value),
        }).ToList();
        return new InspectionPage(entries, page.NextCursor);
    }

    private static string FormatArguments(IReadOnlyDictionary<string, string>? arguments) =>
        arguments is null ? string.Empty : string.Join(",", arguments.OrderBy(static a => a.Key, StringComparer.Ordinal).Select(static a => a.Key + "=" + a.Value));

    private bool TryAcquire(string caller)
    {
        lock (_gate)
        {
            if (!_windows.TryGetValue(caller, out var window) || _time.GetElapsedTime(window.Start) >= TimeSpan.FromMinutes(1))
            {
                window = (_time.GetTimestamp(), 0);
            }

            if (window.Count >= _options.RequestsPerMinute)
            {
                _windows[caller] = window;
                return false;
            }

            _windows[caller] = (window.Start, window.Count + 1);
            return true;
        }
    }
}

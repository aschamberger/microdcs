using System.Globalization;
using System.Reflection;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Options;

namespace MicroDCS.Core.Inspection;

/// <summary>Small helpers to build inspection entries.</summary>
public static class InspectionEntries
{
    /// <summary>Builds an entry from name, text and sensitivity triples.</summary>
    /// <param name="kind">The kind.</param>
    /// <param name="key">The key.</param>
    /// <param name="fields">The fields.</param>
    /// <returns>The entry.</returns>
    public static InspectionEntry Create(string kind, string key, params (string Name, string Text, Sensitivity Sensitivity)[] fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        return new InspectionEntry(kind, key, fields.ToDictionary(static f => f.Name, static f => new InspectionValue(f.Text, f.Sensitivity), StringComparer.Ordinal));
    }

    /// <summary>Formats a number invariantly.</summary>
    /// <param name="value">The number.</param>
    /// <returns>The text.</returns>
    public static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>Whether an entry kind passes the query's kind filter.</summary>
    /// <param name="query">The query.</param>
    /// <param name="kind">The entry kind.</param>
    /// <returns><see langword="true"/> when the kind is wanted.</returns>
    public static bool Wants(InspectionQuery query, string kind)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.Kind is null || string.Equals(query.Kind, kind, StringComparison.Ordinal);
    }
}

/// <summary>Inspector <c>pipeline</c>: counters of the pipeline and the ring buffer of its last decisions.</summary>
public sealed class PipelineInspector : IInspector
{
    private readonly MessagePipeline _pipeline;
    private readonly PipelineDecisionLog _decisions;

    /// <summary>Initializes a new instance of the <see cref="PipelineInspector"/> class.</summary>
    /// <param name="pipeline">The pipeline.</param>
    /// <param name="decisions">The decision log.</param>
    public PipelineInspector(MessagePipeline pipeline, PipelineDecisionLog decisions)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(decisions);
        _pipeline = pipeline;
        _decisions = decisions;
    }

    /// <inheritdoc />
    public string Name => "pipeline";

    /// <inheritdoc />
    public ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var entries = new List<InspectionEntry>();
        if (InspectionEntries.Wants(query, "pipeline.status"))
        {
            entries.Add(InspectionEntries.Create(
                "pipeline.status",
                "pipeline",
                ("inFlight", InspectionEntries.Number(_pipeline.InFlight), Sensitivity.Public),
                ("pendingExpiries", InspectionEntries.Number(_pipeline.PendingExpiries), Sensitivity.Public),
                ("decisionBuffer", InspectionEntries.Number(_decisions.Capacity), Sensitivity.Public)));
        }

        if (InspectionEntries.Wants(query, "pipeline.decision"))
        {
            foreach (var d in _decisions.Newest(Math.Max(query.Limit, 0)))
            {
                if (query.Key is not null && d.BindingId != query.Key)
                {
                    continue;
                }

                entries.Add(InspectionEntries.Create(
                    "pipeline.decision",
                    InspectionEntries.Number(d.Sequence),
                    ("at", d.At.ToString("O", CultureInfo.InvariantCulture), Sensitivity.Public),
                    ("binding", d.BindingId, Sensitivity.Identifier),
                    ("eventId", d.EventId, Sensitivity.Identifier),
                    ("source", d.Source, Sensitivity.Identifier),
                    ("type", d.Type, Sensitivity.Identifier),
                    ("outcome", d.Outcome, Sensitivity.Public),
                    ("durationMs", d.Duration.TotalMilliseconds.ToString("F2", CultureInfo.InvariantCulture), Sensitivity.Public)));
            }
        }

        return ValueTask.FromResult(new InspectionPage(entries, null));
    }
}

/// <summary>Inspector <c>processors</c>: processors, their bindings and handlers.</summary>
public sealed class ProcessorsInspector : IInspector
{
    private readonly ProcessorRegistry _registry;
    private readonly MicroDcsOptions _options;

    /// <summary>Initializes a new instance of the <see cref="ProcessorsInspector"/> class.</summary>
    /// <param name="registry">The processors.</param>
    /// <param name="options">The core options.</param>
    public ProcessorsInspector(ProcessorRegistry registry, IOptions<MicroDcsOptions> options)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(options);
        _registry = registry;
        _options = options.Value;
    }

    /// <inheritdoc />
    public string Name => "processors";

    /// <inheritdoc />
    public ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var entries = new List<InspectionEntry>();
        foreach (var processor in _registry.Processors.OrderBy(static p => p.BindingId, StringComparer.Ordinal))
        {
            if (query.Key is not null && processor.BindingId != query.Key)
            {
                continue;
            }

            _options.Bindings.TryGetValue(processor.BindingId, out var binding);
            var handlers = string.Join(", ", processor.Handlers.Select(static h => h.Kind + ":" + (h.TypeId ?? "*")));
            entries.Add(InspectionEntries.Create(
                "processor",
                processor.BindingId,
                ("processor", processor.ProcessorType.Name, Sensitivity.Identifier),
                ("direction", processor.Binding.ToString(), Sensitivity.Public),
                ("transport", binding?.Transport ?? "none", Sensitivity.Public),
                ("prefix", binding?.Prefix ?? string.Empty, Sensitivity.Identifier),
                ("discriminator", binding?.Discriminator ?? string.Empty, Sensitivity.Identifier),
                ("scopeLevels", InspectionEntries.Number(binding?.ScopeLevels ?? 0), Sensitivity.Public),
                ("scopeCheck", binding?.ScopeCheck.ToString() ?? "none", Sensitivity.Public),
                ("sharedGroup", binding?.SharedGroup ?? string.Empty, Sensitivity.Identifier),
                ("subscribe", string.Join(", ", processor.SubscribeIntents), Sensitivity.Public),
                ("publish", string.Join(", ", processor.PublishIntents), Sensitivity.Public),
                ("handlers", handlers, Sensitivity.Identifier)));
        }

        return ValueTask.FromResult(new InspectionPage(entries.Take(Math.Max(query.Limit, 0)).ToList(), null));
    }
}

/// <summary>Inspector <c>app</c>: status of the instance and its effective core configuration (no secrets exist in these options).</summary>
public sealed class AppInspector : IInspector
{
    private readonly MicroDcsOptions _options;
    private readonly IReadOnlyList<ITransport> _transports;
    private readonly TimeProvider _time;
    private readonly long _started;

    /// <summary>Initializes a new instance of the <see cref="AppInspector"/> class.</summary>
    /// <param name="options">The core options.</param>
    /// <param name="transports">The transports.</param>
    /// <param name="time">The clock.</param>
    public AppInspector(IOptions<MicroDcsOptions> options, IEnumerable<ITransport> transports, TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(transports);
        ArgumentNullException.ThrowIfNull(time);
        _options = options.Value;
        _transports = [.. transports];
        _time = time;
        _started = time.GetTimestamp();
    }

    /// <inheritdoc />
    public string Name => "app";

    /// <inheritdoc />
    public ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var entries = new List<InspectionEntry>();
        if (InspectionEntries.Wants(query, "app.status"))
        {
            var version = typeof(AppInspector).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "unknown";
            var transports = string.Join(", ", _transports.Select(static t => $"{t.Name} ({t.Profile.Guarantee})"));
            entries.Add(InspectionEntries.Create(
                "app.status",
                _options.InstanceId ?? string.Empty,
                ("version", version, Sensitivity.Public),
                ("roles", _options.Roles.ToString(), Sensitivity.Public),
                ("uptimeSeconds", InspectionEntries.Number((long)_time.GetElapsedTime(_started).TotalSeconds), Sensitivity.Public),
                ("transports", transports, Sensitivity.Public)));
        }

        if (InspectionEntries.Wants(query, "app.config"))
        {
            entries.Add(InspectionEntries.Create(
                "app.config",
                "MicroDcs",
                ("cloudEventSource", _options.CloudEventSource ?? string.Empty, Sensitivity.Identifier),
                ("messageExpiry", _options.MessageExpiry?.ToString() ?? "none", Sensitivity.Public),
                ("shutdownGracePeriod", _options.ShutdownGracePeriod.ToString(), Sensitivity.Public),
                ("outgoingQueueMaxSize", InspectionEntries.Number(_options.OutgoingQueueMaxSize), Sensitivity.Public),
                ("messageWorkers", InspectionEntries.Number(_options.MessageWorkers), Sensitivity.Public),
                ("dedupeLease", _options.DedupeLease.ToString(), Sensitivity.Public),
                ("dedupeRetention", _options.DedupeRetention.ToString(), Sensitivity.Public),
                ("startLeaseTtl", _options.StartLeaseTtl.ToString(), Sensitivity.Public),
                ("bindings", string.Join(", ", _options.Bindings.Keys.Order(StringComparer.Ordinal)), Sensitivity.Identifier),
                ("managementUrl", _options.Management.Url, Sensitivity.Identifier),
                ("mcpEnabled", _options.Management.McpEnabled.ToString(), Sensitivity.Public),
                ("writeEnabled", _options.Management.WriteEnabled.ToString(), Sensitivity.Public),
                ("includePayloads", _options.Management.IncludePayloads.ToString(), Sensitivity.Public)));
        }

        return ValueTask.FromResult(new InspectionPage(entries, null));
    }
}

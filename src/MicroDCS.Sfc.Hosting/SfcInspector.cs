using System.Globalization;
using MicroDCS.Core.Inspection;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Sfc;
using MicroDCS.Valkey.Streams;
using StackExchange.Redis;

namespace MicroDCS.Sfc.Hosting;

/// <summary>
/// Inspector <c>sfc</c> (D-19): one job, the due-index and the pending work of a scope. Read-only.
/// Kinds: <c>sfc.job</c> (argument <c>id</c>), <c>sfc.due</c> and <c>sfc.work</c> (argument <c>scope</c>), <c>sfc.waits</c> (arguments <c>scope</c> and <c>type</c>).
/// </summary>
internal sealed class SfcInspector : IInspector
{
    private readonly SfcStateDao _states;
    private readonly ValkeyStreams _streams;
    private readonly ValkeyContext _context;
    private readonly SfcPendingCommands _pending;

    public SfcInspector(SfcStateDao states, ValkeyStreams streams, ValkeyContext context, SfcPendingCommands pending)
    {
        _states = states;
        _streams = streams;
        _context = context;
        _pending = pending;
    }

    public string Name => "sfc";

    public async ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var entries = query.Kind switch
        {
            "sfc.job" => await JobAsync(Required(query, "id"), cancellationToken).ConfigureAwait(false),
            "sfc.due" => await DueAsync(Required(query, "scope"), Math.Max(query.Limit, 1), cancellationToken).ConfigureAwait(false),
            "sfc.work" => await WorkAsync(Required(query, "scope"), Math.Max(query.Limit, 1), cancellationToken).ConfigureAwait(false),
            "sfc.waits" => await WaitsAsync(Required(query, "scope"), Required(query, "type"), Math.Max(query.Limit, 1), cancellationToken).ConfigureAwait(false),
            _ => throw new ArgumentException("Kind must be sfc.job, sfc.due, sfc.work or sfc.waits.", nameof(query)),
        };
        return new InspectionPage(entries, null);
    }

    private static string Required(InspectionQuery query, string name) =>
        query.Argument(name) ?? throw new ArgumentException($"Argument '{name}' is required.", nameof(query));

    private static InspectionValue Public(string text) => new(text, Sensitivity.Public);

    private static InspectionValue Identifier(string text) => new(text, Sensitivity.Identifier);

    private static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);

    private static string Time(long unixMs) => DateTimeOffset.FromUnixTimeMilliseconds(unixMs).ToString("O", CultureInfo.InvariantCulture);

    private async ValueTask<IReadOnlyList<InspectionEntry>> JobAsync(string id, CancellationToken cancellationToken)
    {
        var document = await _states.GetAsync(id, cancellationToken).ConfigureAwait(false);
        if (document is null)
        {
            return [];
        }

        var state = SfcJobRunner.Parse(document);
        var actions = string.Join(
            "; ",
            state.Actions.Values.Select(static a => $"{a.Name} {a.Status} attempt={a.Attempt} round={a.Round}"
                + (a.Deadline is { } d ? $" deadline={Time(d)}" : string.Empty)
                + (a.Outcome is { } o ? $" outcome={o}" : string.Empty)
                + (a.Hold is { } h ? $" hold={h}" : string.Empty)
                + (a.HoldDeadline is { } hd ? $" holdDeadline={Time(hd)}" : string.Empty)
                + (a.Restarted ? " restarted" : string.Empty)));
        var fields = new Dictionary<string, InspectionValue>(StringComparer.Ordinal)
        {
            ["scope"] = Identifier(state.Scope),
            ["status"] = Public(state.Status.ToString()),
            ["version"] = Public(Number(document.Version)),
            ["recipeHash"] = Identifier(state.RecipeHash),
            ["current"] = Public(state.Current ?? string.Empty),
            ["activeSteps"] = Public(string.Join(",", state.ActiveSteps)),
            ["actions"] = Public(actions),
            ["sequence"] = Public(Number(state.Sequence)),
            ["nextDeadline"] = Public(state.NextDeadline is { } next ? Time(next) : string.Empty),
            ["heldSince"] = Public(state.IsHeld && state.HeldAt is { } held ? Time(held) : string.Empty),
            ["faultCode"] = Public(state.FaultCode ?? string.Empty),
            ["faultMessage"] = Identifier(state.FaultMessage ?? string.Empty),
            ["errors"] = new InspectionValue(string.Join("; ", state.Actions.Values.Where(static a => a.Error is not null).Select(static a => $"{a.Name}: {a.Error}")), Sensitivity.Payload),
        };
        return [new InspectionEntry("sfc.job", id, fields)];
    }

    private async ValueTask<IReadOnlyList<InspectionEntry>> DueAsync(string scope, int limit, CancellationToken cancellationToken)
    {
        var due = await _states.ListDueAsync(scope, limit, cancellationToken).ConfigureAwait(false);
        return [.. due.Select(d => new InspectionEntry("sfc.due", d.JobId, new Dictionary<string, InspectionValue>(StringComparer.Ordinal) { ["scope"] = Identifier(scope), ["deadline"] = Public(Time(d.DeadlineMs)) }))];
    }

    private async ValueTask<IReadOnlyList<InspectionEntry>> WaitsAsync(string scope, string typeId, int limit, CancellationToken cancellationToken)
    {
        var waiting = await _states.ListWaitingAsync(scope, typeId, limit, cancellationToken).ConfigureAwait(false);
        return [.. waiting.Select(w => new InspectionEntry("sfc.waits", w.JobId, new Dictionary<string, InspectionValue>(StringComparer.Ordinal) { ["scope"] = Identifier(scope), ["type"] = Identifier(typeId), ["since"] = Public(Time(w.Since)) }))];
    }

    private async ValueTask<IReadOnlyList<InspectionEntry>> WorkAsync(string scope, int limit, CancellationToken cancellationToken)
    {
        var stream = _context.Keys.SfcWork(scope);
        var database = _context.Multiplexer.GetDatabase();
        var length = await database.StreamLengthAsync(stream).WaitAsync(cancellationToken).ConfigureAwait(false);
        var dead = await database.StreamLengthAsync(_context.Keys.SfcDead(scope)).WaitAsync(cancellationToken).ConfigureAwait(false);
        var heartbeat = await _states.GetHeartbeatAsync(scope, cancellationToken).ConfigureAwait(false);
        var entries = new List<InspectionEntry>
        {
            new("sfc.work", scope, new Dictionary<string, InspectionValue>(StringComparer.Ordinal)
            {
                ["length"] = Public(Number(length)),
                ["dead"] = Public(Number(dead)),
                ["lastSweep"] = Public(heartbeat is { } ms ? Time(ms) : string.Empty),
                ["instancePendingCommands"] = Public(Number(_pending.Count)),
            }),
        };

        if (length == 0)
        {
            return entries;
        }

        IReadOnlyList<StreamPendingMessageInfo> pending;
        try
        {
            pending = await _streams.PendingAsync(stream, SfcWork.Group, "-", "+", limit, RedisValue.Null, cancellationToken).ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("NOGROUP", StringComparison.Ordinal))
        {
            return entries;
        }

        foreach (var info in pending)
        {
            var found = await database.StreamRangeAsync(stream, info.MessageId, info.MessageId, 1).WaitAsync(cancellationToken).ConfigureAwait(false);
            var entry = found.FirstOrDefault();
            entries.Add(new InspectionEntry("sfc.work.pending", info.MessageId.ToString(), new Dictionary<string, InspectionValue>(StringComparer.Ordinal)
            {
                ["consumer"] = Identifier(info.ConsumerName.ToString()),
                ["idleMs"] = Public(Number(info.IdleTimeInMilliseconds)),
                ["deliveries"] = Public(Number(info.DeliveryCount)),
                ["jobId"] = Identifier(entry.IsNull ? string.Empty : entry[SfcWork.JobIdField].ToString()),
                ["action"] = Identifier(entry.IsNull ? string.Empty : entry[SfcWork.ActionField].ToString()),
            }));
        }

        return entries;
    }
}

using System.Globalization;
using MicroDCS.Core.Inspection;

namespace MicroDCS.Valkey;

/// <summary>
/// Inspector <c>valkey</c> (D-19): schema-aware, read-only access to the stored state. It reads through the key schema by kind and
/// scope; there is no raw command or key scan. Document bodies are <see cref="Sensitivity.Payload"/>, so the redaction policy hides them.
/// Kinds: <c>valkey.keys</c>, <c>valkey.document</c>, <c>valkey.dedupe</c>, <c>valkey.stream</c>.
/// </summary>
public sealed class ValkeyInspector : IInspector
{
    private readonly ValkeyContext _context;

    /// <summary>Initializes a new instance of the <see cref="ValkeyInspector"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public ValkeyInspector(ValkeyContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _context = context;
    }

    /// <inheritdoc />
    public string Name => "valkey";

    /// <inheritdoc />
    public async ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var entries = query.Kind switch
        {
            "valkey.keys" => await KeysAsync(query, cancellationToken).ConfigureAwait(false),
            "valkey.document" => await DocumentAsync(query, cancellationToken).ConfigureAwait(false),
            "valkey.dedupe" => await DedupeAsync(query, cancellationToken).ConfigureAwait(false),
            "valkey.stream" => await StreamsAsync(query, cancellationToken).ConfigureAwait(false),
            _ => throw new ArgumentException("Kind must be valkey.keys, valkey.document, valkey.dedupe or valkey.stream.", nameof(query)),
        };
        return new InspectionPage(entries, null);
    }

    private async Task<IReadOnlyList<InspectionEntry>> KeysAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        var kind = Required(query, "kind");
        var scope = kind == "scopes" ? string.Empty : Required(query, "scope");
        var db = _context.Database;
        var limit = Math.Max(query.Limit, 1);
        var keys = _context.Keys;

        (string Key, bool Sorted)? target = kind switch
        {
            "scopes" => (keys.Scopes(), false),
            "joborder" => (keys.JobOrderList(scope), true),
            "jobresponse" => (keys.JobResponseList(scope), true),
            "workmaster" => (keys.WorkMasterList(scope), false),
            "sfcactive" => (keys.SfcActive(scope), false),
            "sfcdue" => (keys.SfcDue(scope), true),
            _ => null,
        };
        if (target is not { } found)
        {
            throw new ArgumentException("kind must be scopes, joborder, jobresponse, workmaster, sfcactive or sfcdue.", nameof(query));
        }

        long total;
        List<string> ids;
        if (found.Sorted)
        {
            total = await db.SortedSetLengthAsync(found.Key).WaitAsync(cancellationToken).ConfigureAwait(false);
            var values = await db.SortedSetRangeByRankAsync(found.Key, 0, limit - 1).WaitAsync(cancellationToken).ConfigureAwait(false);
            ids = [.. values.Select(static v => v.ToString())];
        }
        else
        {
            total = await db.SetLengthAsync(found.Key).WaitAsync(cancellationToken).ConfigureAwait(false);
            ids = [.. db.SetScan(found.Key, pageSize: limit).Take(limit).Select(static v => v.ToString()).Order(StringComparer.Ordinal)];
        }

        return
        [
            InspectionEntries.Create(
                "valkey.keys",
                kind,
                ("scope", scope, Sensitivity.Identifier),
                ("total", InspectionEntries.Number(total), Sensitivity.Public),
                ("shown", InspectionEntries.Number(ids.Count), Sensitivity.Public),
                ("ids", string.Join(", ", ids), Sensitivity.Identifier)),
        ];
    }

    private async Task<IReadOnlyList<InspectionEntry>> DocumentAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        var kind = Required(query, "kind");
        var id = Required(query, "id");
        var key = kind switch
        {
            "joborder" => _context.Keys.JobOrder(id),
            "jobresponse" => _context.Keys.JobResponse(id),
            "workmaster" => _context.Keys.WorkMaster(id),
            "sfcstate" => _context.Keys.SfcState(id),
            _ => throw new ArgumentException("kind must be joborder, jobresponse, workmaster or sfcstate.", nameof(query)),
        };

        var hash = await _context.Database.HashGetAllAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        if (hash.Length == 0)
        {
            return [InspectionEntries.Create("valkey.document", id, ("kind", kind, Sensitivity.Public), ("exists", "false", Sensitivity.Public))];
        }

        var fields = new Dictionary<string, InspectionValue>(StringComparer.Ordinal)
        {
            ["kind"] = new(kind, Sensitivity.Public),
            ["exists"] = new("true", Sensitivity.Public),
        };
        foreach (var entry in hash)
        {
            var name = entry.Name.ToString();
            var sensitivity = name switch
            {
                "doc" => Sensitivity.Payload,
                "v" => Sensitivity.Public,
                _ => Sensitivity.Identifier,
            };
            fields[name] = new InspectionValue((string?)entry.Value ?? string.Empty, sensitivity);
        }

        return [new InspectionEntry("valkey.document", id, fields)];
    }

    private async Task<IReadOnlyList<InspectionEntry>> DedupeAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        var source = Required(query, "source");
        var id = Required(query, "id");
        var key = _context.Keys.CloudEventDedupe(source, id);
        var db = _context.Database;
        var value = (string?)await db.StringGetAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        var ttl = await db.KeyTimeToLiveAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);

        // The lease token holds the instance id; the state is enough.
        var state = value switch
        {
            null => "none",
            "done" => "done",
            _ => "processing",
        };
        return
        [
            InspectionEntries.Create(
                "valkey.dedupe",
                id,
                ("source", source, Sensitivity.Identifier),
                ("state", state, Sensitivity.Public),
                ("remainingSeconds", ttl is { } t ? t.TotalSeconds.ToString("F0", CultureInfo.InvariantCulture) : "none", Sensitivity.Public)),
        ];
    }

    private async Task<IReadOnlyList<InspectionEntry>> StreamsAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        var scope = Required(query, "scope");
        var db = _context.Database;
        var entries = new List<InspectionEntry>();
        foreach (var (name, key) in new[] { ("sfc.work", _context.Keys.SfcWork(scope)), ("sfc.dead", _context.Keys.SfcDead(scope)), ("changes", _context.Keys.Changes(scope)) })
        {
            if (!await db.KeyExistsAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false))
            {
                entries.Add(InspectionEntries.Create("valkey.stream", name, ("scope", scope, Sensitivity.Identifier), ("exists", "false", Sensitivity.Public)));
                continue;
            }

            var length = await db.StreamLengthAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
            var groups = await db.StreamGroupInfoAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
            var text = string.Join("; ", groups.Select(static g => string.Create(
                CultureInfo.InvariantCulture,
                $"{g.Name}: pending={g.PendingMessageCount}, consumers={g.ConsumerCount}, lag={(g.Lag is { } lag ? lag.ToString(CultureInfo.InvariantCulture) : "unknown")}")));
            entries.Add(InspectionEntries.Create(
                "valkey.stream",
                name,
                ("scope", scope, Sensitivity.Identifier),
                ("exists", "true", Sensitivity.Public),
                ("length", InspectionEntries.Number(length), Sensitivity.Public),
                ("groups", text, Sensitivity.Identifier)));
        }

        return entries;
    }

    private static string Required(InspectionQuery query, string name) =>
        query.Argument(name) ?? throw new ArgumentException($"Argument '{name}' is required.", nameof(query));
}

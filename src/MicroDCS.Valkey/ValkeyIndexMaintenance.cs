using System.Globalization;
using StackExchange.Redis;

namespace MicroDCS.Valkey;

/// <summary>A difference between an index and the documents it is derived from.</summary>
/// <param name="Index">The index key.</param>
/// <param name="Kind"><c>missing</c> (the documents say it should be there) or <c>extra</c>.</param>
/// <param name="Member">The member, or <c>score</c>/<c>value</c> details.</param>
public sealed record IndexDifference(string Index, string Kind, string Member);

/// <summary>
/// Checks and rebuilds the secondary indexes from the document hashes (D-41): the scope lists of job orders, job responses and work
/// masters, the by-order and by-state indexes and the scope registry. It scans the keyspace, so it is a maintenance routine; run the
/// rebuild while no writer is active.
/// </summary>
public sealed class ValkeyIndexMaintenance
{
    private const int ScanPageSize = 500;

    private readonly ValkeyContext _context;

    /// <summary>Initializes a new instance of the <see cref="ValkeyIndexMaintenance"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public ValkeyIndexMaintenance(ValkeyContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _context = context;
    }

    /// <summary>Compares every index with a scan of the documents.</summary>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The differences; empty when every index is consistent.</returns>
    public async Task<IReadOnlyList<IndexDifference>> VerifyAsync(CancellationToken cancellationToken = default)
    {
        var expected = await ExpectedAsync(cancellationToken).ConfigureAwait(false);
        var actual = await ActualAsync(cancellationToken).ConfigureAwait(false);
        var differences = new List<IndexDifference>();
        foreach (var (key, members) in expected.Indexes)
        {
            actual.Indexes.TryGetValue(key, out var found);
            foreach (var (member, value) in members)
            {
                if (found is null || !found.TryGetValue(member, out var have) || have != value)
                {
                    differences.Add(new IndexDifference(key, "missing", member));
                }
            }
        }

        foreach (var (key, members) in actual.Indexes)
        {
            expected.Indexes.TryGetValue(key, out var wanted);
            differences.AddRange(members.Keys.Where(m => wanted is null || !wanted.ContainsKey(m)).Select(m => new IndexDifference(key, "extra", m)));
        }

        var registered = (await _context.Database.SetMembersAsync(_context.Keys.Scopes()).WaitAsync(cancellationToken).ConfigureAwait(false)).Select(static v => v.ToString()).ToHashSet(StringComparer.Ordinal);
        differences.AddRange(expected.Scopes.Where(s => !registered.Contains(s)).Select(s => new IndexDifference(_context.Keys.Scopes(), "missing", s)));
        foreach (var difference in differences)
        {
            Log.IndexDifference(_context.Logger, difference.Index, difference.Kind, difference.Member);
        }

        return differences;
    }

    /// <summary>Rewrites every index from the documents and removes index keys that no document supports.</summary>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The number of index keys written.</returns>
    public async Task<int> RebuildAsync(CancellationToken cancellationToken = default)
    {
        var expected = await ExpectedAsync(cancellationToken).ConfigureAwait(false);
        var actual = await ActualAsync(cancellationToken).ConfigureAwait(false);
        var db = _context.Database;
        foreach (var key in actual.Indexes.Keys.Where(k => !expected.Indexes.ContainsKey(k)))
        {
            await db.KeyDeleteAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        foreach (var (key, members) in expected.Indexes)
        {
            var batch = db.CreateBatch();
            var tasks = new List<Task> { batch.KeyDeleteAsync(key) };
            switch (expected.Kinds[key])
            {
                case IndexKind.SortedSet:
                    tasks.Add(batch.SortedSetAddAsync(key, [.. members.Select(m => new SortedSetEntry(m.Key, double.Parse(m.Value, CultureInfo.InvariantCulture)))]));
                    break;
                case IndexKind.Set:
                    tasks.Add(batch.SetAddAsync(key, [.. members.Keys.Select(static m => (RedisValue)m)]));
                    break;
                default:
                    tasks.Add(batch.StringSetAsync(key, members.Keys.Single()));
                    break;
            }

            batch.Execute();
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        if (expected.Scopes.Count > 0)
        {
            await db.SetAddAsync(_context.Keys.Scopes(), [.. expected.Scopes.Select(static s => (RedisValue)s)]).WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        Log.IndexesRebuilt(_context.Logger, expected.Indexes.Count, expected.Documents);
        return expected.Indexes.Count;
    }

    private enum IndexKind
    {
        SortedSet,
        Set,
        String,
    }

    private IServer Server() =>
        _context.Multiplexer.GetServers().FirstOrDefault(static s => s.IsConnected && !s.IsReplica)
        ?? throw new InvalidOperationException("No connected primary to scan.");

    private async Task<List<string>> HashKeysAsync(string pattern, CancellationToken cancellationToken)
    {
        var keys = new List<string>();
        var db = _context.Database;
        await foreach (var key in Server().KeysAsync(db.Database, pattern, ScanPageSize).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (await db.KeyTypeAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false) == RedisType.Hash)
            {
                keys.Add(key.ToString());
            }
        }

        return keys;
    }

    private async Task<Snapshot> ExpectedAsync(CancellationToken cancellationToken)
    {
        var snapshot = new Snapshot();
        var db = _context.Database;
        var keys = _context.Keys;
        foreach (var (pattern, idPrefix, type) in new[]
        {
            (keys.JobOrder("*"), keys.JobOrder(string.Empty), "order"),
            (keys.JobResponse("*"), keys.JobResponse(string.Empty), "response"),
            (keys.WorkMaster("*"), keys.WorkMaster(string.Empty), "master"),
        })
        {
            foreach (var key in await HashKeysAsync(pattern, cancellationToken).ConfigureAwait(false))
            {
                var fields = await db.HashGetAsync(key, ["scope", "score", "state", "order"]).WaitAsync(cancellationToken).ConfigureAwait(false);
                var id = key[idPrefix.Length..];
                var scope = fields[0].ToString();
                snapshot.Documents++;
                snapshot.Scopes.Add(scope);
                var score = fields[1].IsNull ? "0" : fields[1].ToString();
                switch (type)
                {
                    case "order":
                        snapshot.Add(keys.JobOrderList(scope), IndexKind.SortedSet, id, Normalize(score));
                        break;
                    case "response":
                        snapshot.Add(keys.JobResponseList(scope), IndexKind.SortedSet, id, Normalize(score));
                        if (!string.IsNullOrEmpty(fields[2]))
                        {
                            snapshot.Add(keys.JobResponseByState(scope, fields[2].ToString()), IndexKind.Set, id, string.Empty);
                        }

                        if (!string.IsNullOrEmpty(fields[3]))
                        {
                            snapshot.Add(keys.JobResponseByOrder(fields[3].ToString()), IndexKind.String, id, string.Empty);
                        }

                        break;
                    default:
                        snapshot.Add(keys.WorkMasterList(scope), IndexKind.Set, id, string.Empty);
                        break;
                }
            }
        }

        return snapshot;
    }

    private async Task<Snapshot> ActualAsync(CancellationToken cancellationToken)
    {
        var snapshot = new Snapshot();
        var db = _context.Database;
        var keys = _context.Keys;
        foreach (var pattern in new[] { keys.JobOrderList("*"), keys.JobResponseList("*"), keys.JobResponseByOrder("*"), keys.JobResponseByState("*", "*"), keys.WorkMasterList("*") })
        {
            await foreach (var redisKey in Server().KeysAsync(db.Database, pattern, ScanPageSize).WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var key = redisKey.ToString();
                switch (await db.KeyTypeAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false))
                {
                    case RedisType.SortedSet:
                        foreach (var entry in await db.SortedSetRangeByRankWithScoresAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false))
                        {
                            snapshot.Add(key, IndexKind.SortedSet, entry.Element.ToString(), Normalize(entry.Score.ToString("R", CultureInfo.InvariantCulture)));
                        }

                        break;
                    case RedisType.Set:
                        foreach (var member in await db.SetMembersAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false))
                        {
                            snapshot.Add(key, IndexKind.Set, member.ToString(), string.Empty);
                        }

                        break;
                    case RedisType.String:
                        snapshot.Add(key, IndexKind.String, (await db.StringGetAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false)).ToString(), string.Empty);
                        break;
                    default:
                        break;
                }
            }
        }

        return snapshot;
    }

    private static string Normalize(string score) =>
        double.TryParse(score, CultureInfo.InvariantCulture, out var value) ? value.ToString("R", CultureInfo.InvariantCulture) : score;

    private sealed class Snapshot
    {
        public Dictionary<string, Dictionary<string, string>> Indexes { get; } = new(StringComparer.Ordinal);

        public Dictionary<string, IndexKind> Kinds { get; } = new(StringComparer.Ordinal);

        public HashSet<string> Scopes { get; } = new(StringComparer.Ordinal);

        public int Documents { get; set; }

        public void Add(string key, IndexKind kind, string member, string value)
        {
            if (!Indexes.TryGetValue(key, out var members))
            {
                Indexes[key] = members = new Dictionary<string, string>(StringComparer.Ordinal);
                Kinds[key] = kind;
            }

            members[member] = value;
        }
    }
}

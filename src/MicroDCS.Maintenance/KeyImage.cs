using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;
using StackExchange.Redis;

namespace MicroDCS.Maintenance;

/// <summary>A name and a value, one hash field or stream field.</summary>
/// <param name="Name">The name.</param>
/// <param name="Value">The value as stored.</param>
internal sealed record Pair(string Name, string Value);

/// <summary>A sorted set member.</summary>
/// <param name="Member">The member.</param>
/// <param name="Score">The score.</param>
internal sealed record ScoredMember(string Member, double Score);

/// <summary>A stream entry.</summary>
/// <param name="Id">The entry id at capture time.</param>
/// <param name="Fields">The fields.</param>
internal sealed record StreamEntryImage(string Id, List<Pair> Fields);

/// <summary>Where a maintenance operation looks: a whole key, or one set member or hash field of a shared key.</summary>
/// <param name="Key">The full key.</param>
/// <param name="Part">The set member or hash field, or <see langword="null"/> for the whole key.</param>
/// <param name="PartKind">The kind of the part, <c>setmember</c> or <c>hashfield</c>; <see langword="null"/> for a whole key.</param>
internal sealed record Target(string Key, string? Part = null, string? PartKind = null);

/// <summary>
/// The stored value of a target exactly as it is in Valkey (strings are not re-serialised), enough to write it back.
/// Hash documents carry their version for the compare-and-delete.
/// </summary>
internal sealed class KeyImage
{
    public required string Key { get; init; }

    public string? Part { get; init; }

    /// <summary><c>string</c>, <c>hash</c>, <c>set</c>, <c>zset</c>, <c>stream</c>, <c>setmember</c> or <c>hashfield</c>.</summary>
    public required string Type { get; init; }

    public long? TtlMs { get; init; }

    public long? Version { get; init; }

    public string? Value { get; init; }

    public List<Pair>? Pairs { get; init; }

    public List<string>? Members { get; init; }

    public List<ScoredMember>? Scored { get; init; }

    public List<StreamEntryImage>? Entries { get; init; }

    [JsonIgnore]
    public string Describe => Type switch
    {
        "string" => "string",
        "hash" => $"hash, {Pairs?.Count ?? 0} fields" + (Version is { } v ? $", version {v}" : string.Empty),
        "set" => $"set, {Members?.Count ?? 0} members",
        "zset" => $"sorted set, {Scored?.Count ?? 0} members",
        "stream" => $"stream, {Entries?.Count ?? 0} entries",
        "setmember" => "set member",
        _ => "hash field",
    };

    /// <summary>Text that is equal for equal content; the time to live is left out because it moves.</summary>
    public string Canonical()
    {
        var text = new StringBuilder();
        Add(text, Type);
        Add(text, Key);
        Add(text, Part);
        Add(text, Value);
        foreach (var pair in Pairs?.OrderBy(static p => p.Name, StringComparer.Ordinal) ?? Enumerable.Empty<Pair>())
        {
            Add(text, pair.Name);
            Add(text, pair.Value);
        }

        foreach (var member in Members?.Order(StringComparer.Ordinal) ?? Enumerable.Empty<string>())
        {
            Add(text, member);
        }

        foreach (var scored in Scored?.OrderBy(static s => s.Member, StringComparer.Ordinal) ?? Enumerable.Empty<ScoredMember>())
        {
            Add(text, scored.Member);
            Add(text, scored.Score.ToString("R", CultureInfo.InvariantCulture));
        }

        foreach (var entry in Entries ?? Enumerable.Empty<StreamEntryImage>())
        {
            Add(text, entry.Id);
            foreach (var field in entry.Fields)
            {
                Add(text, field.Name);
                Add(text, field.Value);
            }
        }

        return text.ToString();
    }

    /// <summary>Hash over the images of an ordered target list; a missing target counts as such.</summary>
    public static string Digest(IEnumerable<KeyImage?> images)
    {
        var text = new StringBuilder();
        foreach (var image in images)
        {
            Add(text, image?.Canonical() ?? "-");
        }

        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text.ToString())))[..16];
    }

    public static async Task<KeyImage?> CaptureAsync(IDatabase db, Target target, CancellationToken cancellationToken)
    {
        var key = target.Key;
        if (target.PartKind == "setmember")
        {
            return await db.SetContainsAsync(key, target.Part).WaitAsync(cancellationToken).ConfigureAwait(false)
                ? new KeyImage { Key = key, Part = target.Part, Type = "setmember", Value = target.Part }
                : null;
        }

        if (target.PartKind == "hashfield")
        {
            var value = await db.HashGetAsync(key, target.Part).WaitAsync(cancellationToken).ConfigureAwait(false);
            return value.HasValue ? new KeyImage { Key = key, Part = target.Part, Type = "hashfield", Value = value.ToString() } : null;
        }

        var type = await db.KeyTypeAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        switch (type)
        {
            case RedisType.String:
                {
                    var value = await db.StringGetAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
                    var ttl = await db.KeyTimeToLiveAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
                    if (!value.HasValue)
                    {
                        return null;
                    }

                    long? ttlMs = null;
                    if (ttl is { } remaining)
                    {
                        ttlMs = (long)remaining.TotalMilliseconds;
                    }

                    return new KeyImage { Key = key, Type = "string", Value = value.ToString(), TtlMs = ttlMs };
                }

            case RedisType.Hash:
                {
                    var entries = await db.HashGetAllAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
                    var pairs = entries.Select(static e => new Pair(e.Name.ToString(), e.Value.ToString())).ToList();
                    long? version = null;
                    if (pairs.FirstOrDefault(static p => p.Name == "v") is { } v && long.TryParse(v.Value, CultureInfo.InvariantCulture, out var parsed))
                    {
                        version = parsed;
                    }

                    return new KeyImage { Key = key, Type = "hash", Pairs = pairs, Version = version };
                }

            case RedisType.Set:
                {
                    var members = await db.SetMembersAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
                    return new KeyImage { Key = key, Type = "set", Members = [.. members.Select(static m => m.ToString()).Order(StringComparer.Ordinal)] };
                }

            case RedisType.SortedSet:
                {
                    var members = await db.SortedSetRangeByRankWithScoresAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
                    return new KeyImage { Key = key, Type = "zset", Scored = [.. members.Select(static m => new ScoredMember(m.Element.ToString(), m.Score))] };
                }

            case RedisType.Stream:
                {
                    var entries = await db.StreamRangeAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
                    return new KeyImage
                    {
                        Key = key,
                        Type = "stream",
                        Entries = [.. entries.Select(static e => new StreamEntryImage(e.Id.ToString(), [.. e.Values.Select(static v => new Pair(v.Name.ToString(), v.Value.ToString()))]))],
                    };
                }

            case RedisType.None:
                return null;
            default:
                throw new MaintenanceRefusedException($"Key {key} has the unsupported type {type}.");
        }
    }

    /// <summary>Queues the commands that write this image into a transaction; an existing whole key is replaced.</summary>
    /// <returns>The number of stream entries that get a new id.</returns>
    public int QueueRestore(ITransaction transaction, List<Task> tasks)
    {
        var readded = 0;
        switch (Type)
        {
            case "setmember":
                tasks.Add(transaction.SetAddAsync(Key, Part));
                return 0;
            case "hashfield":
                tasks.Add(transaction.HashSetAsync(Key, Part, Value));
                return 0;
        }

        tasks.Add(transaction.KeyDeleteAsync(Key));
        switch (Type)
        {
            case "string":
                tasks.Add(TtlMs is > 0
                    ? transaction.StringSetAsync(Key, Value, TimeSpan.FromMilliseconds(TtlMs.Value))
                    : transaction.StringSetAsync(Key, Value));
                break;
            case "hash":
                tasks.Add(transaction.HashSetAsync(Key, [.. (Pairs ?? []).Select(static p => new HashEntry(p.Name, p.Value))]));
                break;
            case "set":
                tasks.Add(transaction.SetAddAsync(Key, [.. (Members ?? []).Select(static m => (RedisValue)m)]));
                break;
            case "zset":
                tasks.Add(transaction.SortedSetAddAsync(Key, [.. (Scored ?? []).Select(static s => new SortedSetEntry(s.Member, s.Score))]));
                break;
            case "stream":
                foreach (var entry in Entries ?? [])
                {
                    tasks.Add(transaction.StreamAddAsync(Key, [.. entry.Fields.Select(static f => new NameValueEntry(f.Name, f.Value))]));
                    readded++;
                }

                break;
            default:
                throw new MaintenanceRefusedException($"The snapshot holds the unknown type '{Type}'.");
        }

        return readded;
    }

    private static void Add(StringBuilder text, string? value)
    {
        // Length first, so that the parts cannot run into each other.
        text.Append(value is null ? -1 : value.Length).Append(':').Append(value).Append('|');
    }
}

using System.Globalization;
using StackExchange.Redis;

namespace MicroDCS.Valkey.Documents;

/// <summary>Base of the document DAOs: reading the hash of D-41 and the common write arguments.</summary>
public abstract class DocumentDao
{
    private protected DocumentDao(ValkeyContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        Context = context;
    }

    private protected ValkeyContext Context { get; }

    private protected IDatabase Database => Context.Database;

    private protected KeySchema Keys => Context.Keys;

    private protected static async Task<StoredDocument?> ReadAsync(IDatabase database, string key, string id, CancellationToken cancellationToken)
    {
        var entries = await database.HashGetAllAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        return FromEntries(id, entries);
    }

    private protected static StoredDocument? FromEntries(string id, HashEntry[] entries)
    {
        if (entries.Length == 0)
        {
            return null;
        }

        string Field(string name) => entries.FirstOrDefault(e => e.Name == name).Value.ToString();
        _ = long.TryParse(Field("v"), CultureInfo.InvariantCulture, out var version);
        _ = double.TryParse(Field("score"), CultureInfo.InvariantCulture, out var score);
        return new StoredDocument(id, Field("scope"), version, Field("op"), Field("doc"), Field("state"), Field("order"), score);
    }

    private protected static RedisValue[] PutArguments(long? expectedVersion, string operation, string id, string scope, double score, string document, string changeType, string timestamp, string orderId, int changeStreamLength, string previousState, string state) =>
    [
        expectedVersion ?? -1,
        operation,
        id,
        scope,
        score.ToString("R", CultureInfo.InvariantCulture),
        document,
        changeType,
        timestamp,
        orderId,
        changeStreamLength,
        previousState,
        state,
    ];

    private protected static void RequireText(string value, string name)
    {
        if (string.IsNullOrEmpty(value))
        {
            throw new ArgumentException("A value is required.", name);
        }
    }

    private protected string Timestamp() => Context.Time.GetUtcNow().ToString("O", CultureInfo.InvariantCulture);

    private protected static string NewOperation(string? operation) => operation ?? Guid.NewGuid().ToString("N");
}

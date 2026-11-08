using Microsoft.Extensions.Logging;

namespace MicroDCS.Valkey;

/// <summary>Log messages of the Valkey layer. Identifiers and counts only, never document contents.</summary>
internal static partial class Log
{
    [LoggerMessage(EventId = 100, Level = LogLevel.Information, Message = "Valkey connected, key prefix {Prefix}")]
    public static partial void Connected(ILogger logger, string prefix);

    [LoggerMessage(EventId = 101, Level = LogLevel.Warning, Message = "Write to {Key} has an unknown outcome (attempt {Attempt}); checking before repeating")]
    public static partial void WriteOutcomeUnknown(ILogger logger, Exception exception, string key, int attempt);

    [LoggerMessage(EventId = 102, Level = LogLevel.Information, Message = "Write to {Key} had happened already; counted as success")]
    public static partial void WriteAlreadyApplied(ILogger logger, string key);

    [LoggerMessage(EventId = 103, Level = LogLevel.Warning, Message = "Stream {Stream} lost {Count} entries that were still pending: {Ids}")]
    public static partial void StreamEntriesLost(ILogger logger, string stream, int count, string ids);

    [LoggerMessage(EventId = 104, Level = LogLevel.Debug, Message = "Stream {Stream} trimmed by {Count} acknowledged entries")]
    public static partial void StreamTrimmed(ILogger logger, string stream, long count);

    [LoggerMessage(EventId = 105, Level = LogLevel.Information, Message = "Poller discovered scope {Scope}")]
    public static partial void ScopeDiscovered(ILogger logger, string scope);

    [LoggerMessage(EventId = 106, Level = LogLevel.Error, Message = "Poller step {Step} failed for scope {Scope}")]
    public static partial void PollFailed(ILogger logger, Exception exception, string step, string scope);

    [LoggerMessage(EventId = 107, Level = LogLevel.Warning, Message = "Index {Index} differs from the documents: {Kind} {Member}")]
    public static partial void IndexDifference(ILogger logger, string index, string kind, string member);

    [LoggerMessage(EventId = 108, Level = LogLevel.Information, Message = "Rebuilt {Indexes} indexes from {Documents} documents")]
    public static partial void IndexesRebuilt(ILogger logger, int indexes, int documents);
}

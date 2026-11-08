using Microsoft.Extensions.Logging;

namespace MicroDCS.Maintenance;

/// <summary>Audit lines of the maintenance operations (security category). Identifiers, counts and hashes only, never payloads.</summary>
internal static partial class Log
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "Maintenance plan by {Caller}: tool {Tool}, arguments {Arguments}, {Items} targets, digest {Digest}")]
    public static partial void Planned(ILogger logger, string caller, string tool, string arguments, int items, string digest);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "Maintenance apply by {Caller}: tool {Tool}, arguments {Arguments}, {Changed} changed, snapshot {Snapshot}, digest {Digest}")]
    public static partial void Applied(ILogger logger, string caller, string tool, string arguments, int changed, string snapshot, string digest);

    [LoggerMessage(EventId = 3, Level = LogLevel.Warning, Message = "Maintenance by {Caller} refused: {Tool}: {Reason}")]
    public static partial void Refused(ILogger logger, string caller, string tool, string reason);

    [LoggerMessage(EventId = 4, Level = LogLevel.Warning, Message = "Maintenance restore by {Caller}: {Count} stream entries were added again with new ids")]
    public static partial void StreamEntriesReadded(ILogger logger, string caller, int count);

    [LoggerMessage(EventId = 5, Level = LogLevel.Information, Message = "Maintenance snapshot {Snapshot} written: {Items} items, {Bytes} bytes; {Pruned} old snapshots removed")]
    public static partial void SnapshotWritten(ILogger logger, string snapshot, int items, long bytes, int pruned);
}

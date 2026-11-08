using Microsoft.Extensions.Logging;

namespace MicroDCS.Core.Hosting;

/// <summary>Log messages of the core. Identifiers and sizes only, never payloads or attribute values.</summary>
internal static partial class Log
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "Processor {Processor} initialised for binding {Binding}")]
    public static partial void ProcessorInitialised(ILogger logger, string processor, string binding);

    [LoggerMessage(EventId = 2, Level = LogLevel.Information, Message = "Transport {Transport} started")]
    public static partial void TransportStarted(ILogger logger, string transport);

    [LoggerMessage(EventId = 3, Level = LogLevel.Information, Message = "Shutdown phase {Phase}")]
    public static partial void ShutdownPhase(ILogger logger, string phase);

    [LoggerMessage(EventId = 4, Level = LogLevel.Warning, Message = "Dropped message {EventId} on binding {Binding}: {Reason}")]
    public static partial void MessageDropped(ILogger logger, string eventId, string binding, string reason);

    [LoggerMessage(EventId = 5, Level = LogLevel.Error, Message = "Handler failed for event {EventId} of type {Type} on binding {Binding}")]
    public static partial void HandlerFaulted(ILogger logger, Exception exception, string eventId, string type, string binding);

    [LoggerMessage(EventId = 6, Level = LogLevel.Debug, Message = "Duplicate event {EventId} on binding {Binding} acknowledged")]
    public static partial void DuplicateAcknowledged(ILogger logger, string eventId, string binding);

    [LoggerMessage(EventId = 7, Level = LogLevel.Information, Message = "Singleton start hook skipped: another instance holds the lease")]
    public static partial void StartLeaseHeldElsewhere(ILogger logger);
}

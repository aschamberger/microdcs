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

    [LoggerMessage(EventId = 9, Level = LogLevel.Error, Message = "Publishing failed on binding {Binding}")]
    public static partial void PublishFaulted(ILogger logger, Exception exception, string binding);

    [LoggerMessage(EventId = 10, Level = LogLevel.Error, Message = "Fault on binding {Binding}: stopping the host because transport {Transport} cannot redeliver")]
    public static partial void StoppingOnFault(ILogger logger, string binding, string transport);

    [LoggerMessage(EventId = 11, Level = LogLevel.Information, Message = "Command {CommandId} expired after {Interval} without a response")]
    public static partial void CommandExpired(ILogger logger, string commandId, TimeSpan interval);

    [LoggerMessage(EventId = 12, Level = LogLevel.Error, Message = "Expiry handling of command {CommandId} failed")]
    public static partial void ExpiryFaulted(ILogger logger, Exception exception, string commandId);

    [LoggerMessage(EventId = 13, Level = LogLevel.Warning, Message = "Scope rejected for event {EventId} on binding {Binding}")]
    public static partial void ScopeRejected(ILogger logger, string eventId, string binding);

    [LoggerMessage(EventId = 14, Level = LogLevel.Warning, Message = "Event {EventId} of type {Type} not published on binding {Binding}: filtered by the outgoing type filter")]
    public static partial void OutgoingFiltered(ILogger logger, string eventId, string type, string binding);

    [LoggerMessage(EventId = 15, Level = LogLevel.Debug, Message = "Published event {EventId} of type {Type} on binding {Binding}")]
    public static partial void Published(ILogger logger, string eventId, string type, string binding);

    [LoggerMessage(EventId = 16, Level = LogLevel.Warning, Message = "Follow-up events of the expired command {CommandId} were not published: the outgoing queues are closed")]
    public static partial void ExpiryFollowUpDropped(ILogger logger, string commandId);

    [LoggerMessage(EventId = 17, Level = LogLevel.Warning, Message = "Reply {EventId} on binding {Binding} not published: the request has no reply address and no address to answer on")]
    public static partial void ReplyHasNoDestination(ILogger logger, string eventId, string binding);

    [LoggerMessage(EventId = 18, Level = LogLevel.Warning, Message = "Releasing the dedupe lease of event {EventId} failed; it lapses by itself")]
    public static partial void ReleaseFailed(ILogger logger, Exception exception, string eventId);

    [LoggerMessage(EventId = 19, Level = LogLevel.Warning, Message = "Shutdown phase {Phase}: the grace period ended, the remaining phases run without waiting")]
    public static partial void GracePeriodEnded(ILogger logger, string phase);

    [LoggerMessage(EventId = 20, Level = LogLevel.Error, Message = "Dedupe store unavailable at the {Step} step for event {EventId} on binding {Binding}; the message is not acknowledged")]
    public static partial void DedupeStoreUnavailable(ILogger logger, Exception exception, string eventId, string binding, string step);
}

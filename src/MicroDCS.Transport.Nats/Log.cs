using Microsoft.Extensions.Logging;

namespace MicroDCS.Transport.Nats;

/// <summary>Log messages of the NATS transport. Identifiers and sizes only, never payloads, attribute values or credentials.</summary>
internal static partial class Log
{
    [LoggerMessage(EventId = 201, Level = LogLevel.Information, Message = "NATS connected to {Url} as {ClientName}")]
    public static partial void Connected(ILogger logger, string url, string clientName);

    [LoggerMessage(EventId = 202, Level = LogLevel.Information, Message = "NATS consumer {Consumer} on stream {Stream} for binding {Binding} ({Kind}), filters {Filters}")]
    public static partial void ConsumerReady(ILogger logger, string consumer, string stream, string binding, MicroDCS.Core.Transport.InboundKind kind, string filters);

    [LoggerMessage(EventId = 203, Level = LogLevel.Information, Message = "NATS reply subscription {Subject} for binding {Binding}")]
    public static partial void ReplySubscribed(ILogger logger, string subject, string binding);

    [LoggerMessage(EventId = 204, Level = LogLevel.Warning, Message = "NATS connection lost; the client library reconnects")]
    public static partial void ConnectionLost(ILogger logger);

    [LoggerMessage(EventId = 205, Level = LogLevel.Information, Message = "NATS connection restored")]
    public static partial void Reconnected(ILogger logger);

    [LoggerMessage(EventId = 206, Level = LogLevel.Warning, Message = "Sending NATS credentials over a connection without TLS")]
    public static partial void CredentialsWithoutTls(ILogger logger);

    [LoggerMessage(EventId = 207, Level = LogLevel.Warning, Message = "Dropped NATS message on subject {Subject} ({Size} bytes): {Reason}")]
    public static partial void Dropped(ILogger logger, string subject, int size, string reason);

    [LoggerMessage(EventId = 208, Level = LogLevel.Warning, Message = "Acknowledging NATS message on subject {Subject} failed")]
    public static partial void AcknowledgeFailed(ILogger logger, Exception exception, string subject);

    [LoggerMessage(EventId = 209, Level = LogLevel.Error, Message = "NATS worker failed on subject {Subject}")]
    public static partial void WorkerFaulted(ILogger logger, Exception exception, string subject);

    [LoggerMessage(EventId = 210, Level = LogLevel.Debug, Message = "Published to {Subject} ({Size} bytes, {Mode})")]
    public static partial void Published(ILogger logger, string subject, int size, string mode);

    [LoggerMessage(EventId = 211, Level = LogLevel.Warning, Message = "Publish to {Subject} failed while the connection is down; waiting")]
    public static partial void PublishWaiting(ILogger logger, string subject);

    [LoggerMessage(EventId = 212, Level = LogLevel.Warning, Message = "Message on {Subject} negatively acknowledged: the pipeline reported a fault (delivery {Delivery} of {MaxDeliver})")]
    public static partial void Nacked(ILogger logger, string subject, long delivery, int maxDeliver);

    [LoggerMessage(EventId = 213, Level = LogLevel.Error, Message = "Message on {Subject} terminated after {Delivery} failed deliveries (poison message)")]
    public static partial void Poisoned(ILogger logger, string subject, long delivery);

    [LoggerMessage(EventId = 214, Level = LogLevel.Warning, Message = "Rejected subject with an invalid scope segment on binding {Binding}")]
    public static partial void InvalidSubjectRejected(ILogger logger, string binding);

    [LoggerMessage(EventId = 215, Level = LogLevel.Warning, Message = "Consume loop of {Consumer} failed; restarting")]
    public static partial void ConsumeFailed(ILogger logger, Exception exception, string consumer);

    [LoggerMessage(EventId = 218, Level = LogLevel.Warning, Message = "Consumer {Consumer} does not answer pull requests ({Notification}); creating it again")]
    public static partial void ConsumerNotAnswering(ILogger logger, string consumer, string notification);

    [LoggerMessage(EventId = 216, Level = LogLevel.Warning, Message = "Binding {Binding} publishes with retain on NATS: the flag is ignored, use a state topic (IStateTopicPort)")]
    public static partial void RetainIgnored(ILogger logger, string binding);

    [LoggerMessage(EventId = 217, Level = LogLevel.Warning, Message = "Message on binding {Binding} dropped: its reply address was rejected ({Reason}, D-48)")]
    public static partial void ReplyAddressRejected(ILogger logger, string binding, string reason);

    [LoggerMessage(EventId = 219, Level = LogLevel.Warning, Message = "Message on {Subject} kept for a retry in {Delay}: a dependency of the pipeline is unavailable (not counted as a delivery)")]
    public static partial void RetryingUnavailable(ILogger logger, string subject, TimeSpan delay);
}

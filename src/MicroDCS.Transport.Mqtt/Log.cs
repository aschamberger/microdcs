using Microsoft.Extensions.Logging;

namespace MicroDCS.Transport.Mqtt;

/// <summary>Log messages of the MQTT transport. Identifiers and sizes only, never payloads, attribute values or credentials.</summary>
internal static partial class Log
{
    [LoggerMessage(EventId = 101, Level = LogLevel.Information, Message = "MQTT connected to {Host}:{Port} as {ClientId} (session present: {SessionPresent})")]
    public static partial void Connected(ILogger logger, string host, int port, string clientId, bool sessionPresent);

    [LoggerMessage(EventId = 102, Level = LogLevel.Information, Message = "MQTT subscribed to {Filter} for binding {Binding} ({Kind})")]
    public static partial void Subscribed(ILogger logger, string filter, string binding, MicroDCS.Core.Transport.InboundKind kind);

    [LoggerMessage(EventId = 103, Level = LogLevel.Warning, Message = "MQTT connection lost ({Reason}); reconnecting")]
    public static partial void ConnectionLost(ILogger logger, string reason);

    [LoggerMessage(EventId = 104, Level = LogLevel.Warning, Message = "MQTT reconnect attempt failed; next attempt in {Delay}")]
    public static partial void ReconnectFailed(ILogger logger, Exception exception, TimeSpan delay);

    [LoggerMessage(EventId = 105, Level = LogLevel.Warning, Message = "Sending MQTT credentials over a connection without TLS")]
    public static partial void CredentialsWithoutTls(ILogger logger);

    [LoggerMessage(EventId = 106, Level = LogLevel.Warning, Message = "Dropped MQTT message on topic {Topic} ({Size} bytes): {Reason}")]
    public static partial void Dropped(ILogger logger, string topic, int size, string reason);

    [LoggerMessage(EventId = 107, Level = LogLevel.Warning, Message = "Unsubscribe from {Filter} failed")]
    public static partial void UnsubscribeFailed(ILogger logger, Exception exception, string filter);

    [LoggerMessage(EventId = 108, Level = LogLevel.Warning, Message = "Acknowledging MQTT message on topic {Topic} failed")]
    public static partial void AcknowledgeFailed(ILogger logger, Exception exception, string topic);

    [LoggerMessage(EventId = 109, Level = LogLevel.Error, Message = "MQTT worker failed on topic {Topic}")]
    public static partial void WorkerFaulted(ILogger logger, Exception exception, string topic);

    [LoggerMessage(EventId = 110, Level = LogLevel.Debug, Message = "Published to {Topic} ({Size} bytes, QoS {Qos})")]
    public static partial void Published(ILogger logger, string topic, int size, int qos);

    [LoggerMessage(EventId = 111, Level = LogLevel.Warning, Message = "Publish to {Topic} found the connection down; waiting for reconnect")]
    public static partial void PublishWaiting(ILogger logger, string topic);

    [LoggerMessage(EventId = 112, Level = LogLevel.Warning, Message = "Message on {Topic} stays unacknowledged: the pipeline reported a fault (the session redelivers after reconnect)")]
    public static partial void LeftUnacknowledged(ILogger logger, string topic);

    [LoggerMessage(EventId = 113, Level = LogLevel.Warning, Message = "Rejected topic with an invalid scope segment on binding {Binding}")]
    public static partial void InvalidTopicRejected(ILogger logger, string binding);

    [LoggerMessage(EventId = 114, Level = LogLevel.Warning, Message = "Message on binding {Binding} dropped: its reply address was rejected ({Reason}, D-48)")]
    public static partial void ReplyAddressRejected(ILogger logger, string binding, string reason);
}

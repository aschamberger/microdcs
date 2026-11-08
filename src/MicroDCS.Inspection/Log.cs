using Microsoft.Extensions.Logging;

namespace MicroDCS.Inspection;

/// <summary>Log messages of the management surface. Identifiers and sizes only, never payloads or tokens.</summary>
internal static partial class Log
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "Management listener started on {Url} (authentication: {Authentication})")]
    public static partial void ListenerStarted(ILogger logger, string url, string authentication);

    [LoggerMessage(EventId = 2, Level = LogLevel.Information, Message = "Management listener not started (MicroDcs:Management:Enabled is false)")]
    public static partial void ListenerDisabled(ILogger logger);

    [LoggerMessage(EventId = 3, Level = LogLevel.Information, Message = "Inspection call by {Caller}: tool {Tool}, inspector {Inspector}, kind {Kind}, key {Key}, arguments {Arguments}")]
    public static partial void ToolCalled(ILogger logger, string caller, string tool, string inspector, string? kind, string? key, string arguments);

    [LoggerMessage(EventId = 4, Level = LogLevel.Warning, Message = "Inspection call by {Caller} refused: {Reason}")]
    public static partial void CallRefused(ILogger logger, string caller, string reason);

    [LoggerMessage(EventId = 5, Level = LogLevel.Warning, Message = "Management request rejected: {Reason}")]
    public static partial void RequestRejected(ILogger logger, string reason);

    [LoggerMessage(EventId = 6, Level = LogLevel.Warning, Message = "Event injected by {Caller} on binding {Binding}: id {EventId}, type {Type}, outcome {Outcome}")]
    public static partial void EventInjected(ILogger logger, string caller, string binding, string eventId, string type, string outcome);
}

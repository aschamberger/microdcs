using MicroDCS.JobControl.StateMachine;
using Microsoft.Extensions.Logging;

namespace MicroDCS.MachineryJobs;

/// <summary>Log messages. Identifiers and states only; comments and order contents stay out (no payloads in logs).</summary>
internal static partial class Log
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Applied {Operation} to job order {JobOrderId} in scope {Scope}, new state {State}")]
    public static partial void Applied(ILogger logger, JobOrderOperation operation, string jobOrderId, string scope, JobOrderState state);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Rejected {Operation} on job order {JobOrderId} in scope {Scope}: {Status}")]
    public static partial void Rejected(ILogger logger, JobOrderOperation operation, string jobOrderId, string scope, JobReturnStatus status);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Rejected the request {Operation}: a required field is missing")]
    public static partial void InvalidRequest(ILogger logger, JobOrderOperation operation);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Rejected the query {Call}: a required field is missing")]
    public static partial void InvalidQuery(ILogger logger, string call);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Action} {Kind} {Id} for scope {Scope}")]
    public static partial void ReferenceChanged(ILogger logger, string action, JobReferenceKind kind, string id, string scope);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not write job order {JobOrderId}: it kept changing")]
    public static partial void WriteContended(ILogger logger, string jobOrderId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Status event for job order {JobOrderId} was dropped: the outgoing queue is full")]
    public static partial void StatusEventDropped(ILogger logger, string jobOrderId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Job order {JobOrderId} rejected in scope {Scope}: the limit of {Limit} orders is reached")]
    public static partial void LimitReached(ILogger logger, string jobOrderId, string scope, long limit);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Job order {JobOrderId} rejected in scope {Scope}: {Kind} {Id} is not allowed")]
    public static partial void NotAllowed(ILogger logger, string jobOrderId, string scope, string kind, string id);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Action} {Kind} {Id} for scope {Scope}")]
    public static partial void ConfigChanged(ILogger logger, string action, string kind, string id, string scope);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Ignored Work Master {Id} for scope {Scope}: the id belongs to another scope")]
    public static partial void WorkMasterInOtherScope(ILogger logger, string id, string scope);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Ignored the acceptance limit {Limit} for scope {Scope}: it must not be negative")]
    public static partial void InvalidLimit(ILogger logger, int limit, string scope);
}

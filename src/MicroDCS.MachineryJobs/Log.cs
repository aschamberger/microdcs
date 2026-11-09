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

    [LoggerMessage(Level = LogLevel.Information, Message = "Job order publisher {Holder}: holds the lease = {Held}")]
    public static partial void PublisherLeaseChanged(ILogger logger, string holder, bool held);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Job order publisher could not reach its lease")]
    public static partial void PublisherLeaseFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Job order publisher failed for scope {Scope}; it will try again")]
    public static partial void PublisherScopeFailed(ILogger logger, Exception exception, string scope);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Job order publisher skipped scope {Scope}: no topic can carry it")]
    public static partial void PublisherScopeInvalid(ILogger logger, string scope);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Job order publisher skipped job order {JobOrderId} in scope {Scope}: no topic can carry the id")]
    public static partial void PublisherIdSkipped(ILogger logger, string jobOrderId, string scope);

    [LoggerMessage(Level = LogLevel.Error, Message = "Job order publisher skipped job order {JobOrderId} in scope {Scope}: the document cannot be decoded")]
    public static partial void PublisherOrderUndecodable(ILogger logger, string jobOrderId, string scope);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Change entries of scope {Scope} after {Cursor} may be lost (oldest is {First}); rebuilding its topics")]
    public static partial void PublisherGap(ILogger logger, string scope, string cursor, string first);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Published the state index of scope {Scope}, sequence {Sequence}, {Count} job orders")]
    public static partial void PublisherStateIndex(ILogger logger, string scope, long sequence, int count);
}

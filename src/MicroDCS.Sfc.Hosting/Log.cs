using MicroDCS.JobControl.StateMachine;
using Microsoft.Extensions.Logging;

namespace MicroDCS.Sfc.Hosting;

/// <summary>Log messages of the SFC host. Identifiers and states only, no parameters or responses.</summary>
internal static partial class Log
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Started the recipe of job {JobId} in scope {Scope}")]
    public static partial void RecipeStarted(ILogger logger, string jobId, string scope);

    [LoggerMessage(Level = LogLevel.Information, Message = "Skipped the start of job {JobId}: its order is not allowed to start ({State})")]
    public static partial void StartSkipped(ILogger logger, string jobId, JobOrderState state);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Refused the recipe of job {JobId}: {Reason}")]
    public static partial void RecipeRefused(ILogger logger, string jobId, string reason);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Event {Event} on job {JobId} changed nothing ({Disposition}): {Reason}")]
    public static partial void EventNotApplied(ILogger logger, string @event, string jobId, MicroDCS.Sfc.SfcDisposition disposition, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Event {Event} on job {JobId} was rejected: {Reason}")]
    public static partial void EventRejected(ILogger logger, string @event, string jobId, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Work entry {EntryId} of scope {Scope} is invalid and is dropped: {Reason}")]
    public static partial void InvalidWorkEntry(ILogger logger, string entryId, string scope, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Work entry {EntryId} of scope {Scope} failed and stays pending")]
    public static partial void WorkEntryFailed(ILogger logger, Exception exception, string entryId, string scope);

    [LoggerMessage(Level = LogLevel.Error, Message = "Work entry {EntryId} of scope {Scope} moved to the dead stream after {Deliveries} deliveries")]
    public static partial void WorkEntryDead(ILogger logger, string entryId, string scope, int deliveries);

    [LoggerMessage(Level = LogLevel.Information, Message = "Dispatched {Action} of job {JobId}, attempt {Attempt}")]
    public static partial void Dispatched(ILogger logger, string action, string jobId, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Skipped the dispatch of {Action} of job {JobId}: {Reason}")]
    public static partial void DispatchSkipped(ILogger logger, string action, string jobId, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not queue {Action} of job {JobId}: the outgoing queue is full. The deadline dispatches it again")]
    public static partial void DispatchQueueFull(ILogger logger, string action, string jobId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Response {EventId} names no pending command of this instance and is dropped (D-9)")]
    public static partial void ResponseNotPending(ILogger logger, string eventId);

    [LoggerMessage(Level = LogLevel.Error, Message = "Response {EventId} could not be applied to job {JobId}; its deadline will settle the action")]
    public static partial void ResponseNotApplied(ILogger logger, Exception exception, string jobId, string eventId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Job {JobId} has no SFC state; event {Event} is dropped")]
    public static partial void NoState(ILogger logger, string jobId, string @event);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Sweep of scope {Scope} found an outage of {OutageMs} ms; open deadlines are moved")]
    public static partial void OutageDetected(ILogger logger, string scope, long outageMs);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The job order of {JobId} no longer allows execution; ending its recipe")]
    public static partial void RecipeOrphaned(ILogger logger, string jobId);
}

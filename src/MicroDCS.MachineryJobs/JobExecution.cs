using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;
using MicroDCS.JobControl.Conversions;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace MicroDCS.MachineryJobs;

/// <summary>What a transition of the execution system did.</summary>
public enum JobExecutionStatus
{
    /// <summary>The transition was written.</summary>
    Applied,

    /// <summary>The state machine refuses it in the state the order is in.</summary>
    Refused,

    /// <summary>The scope has no such job order.</summary>
    Missing,
}

/// <summary>The result of a transition of the execution system.</summary>
/// <param name="Status">What happened.</param>
/// <param name="State">The state of the order afterwards; <see cref="JobOrderState.Removed"/> when it is missing.</param>
public sealed record JobExecutionResult(JobExecutionStatus Status, JobOrderState State);

/// <summary>
/// The transitions the execution system triggers on a job order, as opposed to the commands of the MES: the SFC host starts, completes and aborts
/// jobs here (S-10, S-26, S-27). Each write is version-guarded and idempotent through its operation id.
/// </summary>
public sealed class JobExecution
{
    private const int MaxWriteAttempts = 4;

    private readonly IJobStore _store;
    private readonly IEventPublisher _publisher;
    private readonly ILogger<JobExecution> _logger;
    private readonly TimeProvider _time;

    /// <summary>Initializes a new instance of the <see cref="JobExecution"/> class.</summary>
    /// <param name="store">The store.</param>
    /// <param name="publisher">Publishes the status events.</param>
    /// <param name="time">The clock.</param>
    /// <param name="logger">The logger.</param>
    public JobExecution(IJobStore store, IEventPublisher publisher, TimeProvider time, ILogger<JobExecution> logger)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(logger);
        _store = store;
        _publisher = publisher;
        _time = time;
        _logger = logger;
    }

    /// <summary>Applies a transition to a job order. <see cref="JobOrderOperation.Complete"/> writes the job response first, so the result is there when the state index says so.</summary>
    /// <param name="id">The job order id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="operation">The transition.</param>
    /// <param name="operationId">An id that makes a repeated write recognisable (R-4).</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    public ValueTask<JobExecutionResult> ApplyAsync(string id, string scope, JobOrderOperation operation, string operationId, CancellationToken cancellationToken) =>
        ApplyAsync(id, scope, operation, operationId, null, cancellationToken);

    /// <summary>Applies a transition to a job order; <paramref name="startedAt"/> becomes the start time of the job response that <see cref="JobOrderOperation.Complete"/> writes.</summary>
    /// <param name="id">The job order id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="operation">The transition.</param>
    /// <param name="operationId">An id that makes a repeated write recognisable (R-4).</param>
    /// <param name="startedAt">When the job started, if known; the response then sorts by it.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    public async ValueTask<JobExecutionResult> ApplyAsync(string id, string scope, JobOrderOperation operation, string operationId, DateTimeOffset? startedAt, CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < MaxWriteAttempts; attempt++)
        {
            var existing = await _store.GetOrderAsync(id, cancellationToken).ConfigureAwait(false);
            if (existing is null || !string.Equals(existing.Scope, scope, StringComparison.Ordinal))
            {
                return new JobExecutionResult(JobExecutionStatus.Missing, JobOrderState.Removed);
            }

            if (string.Equals(existing.Operation, operationId, StringComparison.Ordinal))
            {
                return new JobExecutionResult(JobExecutionStatus.Applied, existing.State);
            }

            var transition = JobStateMachine.Apply(JobControlVersion.V2, existing.State, operation);
            if (!transition.Accepted)
            {
                Log.ExecutionRefused(_logger, operation, id, existing.State);
                return new JobExecutionResult(JobExecutionStatus.Refused, existing.State);
            }

            if (operation == JobOrderOperation.Complete)
            {
                var now = (DateTimeUtc)_time.GetUtcNow().UtcDateTime;
                var response = new JobResponse { Id = id + "-response", JobOrderId = id, State = transition.State, StartTime = startedAt?.UtcDateTime ?? now, EndTime = now, ReceivedAt = now };
                await _store.SaveResponseAsync(response, scope, cancellationToken).ConfigureAwait(false);
            }

            var write = new JobOrderWrite(existing.Order, transition.State, scope, operation.ToString(), existing.Version, operationId, JobRecipeWork.None);
            if (await _store.SaveOrderAsync(write, cancellationToken).ConfigureAwait(false))
            {
                Log.Applied(_logger, operation, id, scope, transition.State);
                if (transition.Effect == JobOrderEffect.StatusEvent)
                {
                    await PublishStatusAsync(existing.Order, transition.State, scope, cancellationToken).ConfigureAwait(false);
                }

                return new JobExecutionResult(JobExecutionStatus.Applied, transition.State);
            }
        }

        throw new InvalidOperationException($"Job order '{id}' kept changing during the transition.");
    }

    private async ValueTask PublishStatusAsync(JobOrder order, JobOrderState state, string scope, CancellationToken cancellationToken)
    {
        try
        {
            await _publisher.PublishAsync(
                new JobOrderStatusEvent { JobOrder = JobControlConversions.ToV2Order(order), JobState = JobStateArray.ToV2(state) },
                Intent.Event,
                new PublishOptions { Subject = scope, CorrelationId = order.Id, BindingId = MachineryJobsProcessor.BindingId },
                cancellationToken).ConfigureAwait(false);
        }
        catch (OutboundQueueFullException)
        {
            // The transition is stored; the change stream repairs what the MES sees (S18).
            Log.StatusEventDropped(_logger, order.Id);
        }
    }
}

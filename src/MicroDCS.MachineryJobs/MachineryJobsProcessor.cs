using System.Collections.Immutable;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;
using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Conversions;
using MicroDCS.JobControl.Extension;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using Microsoft.Extensions.Logging;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.MachineryJobs;

/// <summary>
/// The Machinery Jobs processor (OPC 40001-3, Job Control V2): job order methods, job response queries and station configuration.
/// Every state change goes through the pure state machine and a version-guarded write.
/// </summary>
[Processor(ProcessorBinding.Northbound, Id = BindingId)]
public sealed partial class MachineryJobsProcessor
{
    /// <summary>The configuration identifier of the binding.</summary>
    public const string BindingId = "machinery_jobs";

    // Python paged the by-state query at 1000 (`paging(0, 1000)`); the transport also bounds the reply.
    private const int MaxResponsesByState = 1000;
    private const int MaxWriteAttempts = 4;
    private const string DeleteMethod = "DELETE";

    private static readonly JobOrderState[] _queryableStates =
    [
        .. Enum.GetValues<JobOrderState>().Where(static s => s is not (JobOrderState.Initial or JobOrderState.Removed or JobOrderState.V1Loaded or JobOrderState.V1Error)),
    ];

    private readonly IJobStore _store;
    private readonly JobAcceptanceCheck _acceptance;
    private readonly IEventPublisher _publisher;
    private readonly ILogger<MachineryJobsProcessor> _logger;

    /// <summary>Initializes a new instance of the <see cref="MachineryJobsProcessor"/> class.</summary>
    /// <param name="store">The store.</param>
    /// <param name="acceptance">The acceptance check.</param>
    /// <param name="publisher">The event publisher.</param>
    /// <param name="logger">The logger.</param>
    public MachineryJobsProcessor(IJobStore store, JobAcceptanceCheck acceptance, IEventPublisher publisher, ILogger<MachineryJobsProcessor> logger)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(acceptance);
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(logger);
        _store = store;
        _acceptance = acceptance;
        _publisher = publisher;
        _logger = logger;
    }

    /// <summary>Gets the descriptors of the payloads the processor publishes without a handler naming them.</summary>
    public static ImmutableArray<IPayloadDescriptor> PublishedPayloads { get; } =
        [MicroDcsPayloads.MicroDCS_MachineryJobs_JobOrderStatusEvent, MicroDcsPayloads.MicroDCS_MachineryJobs_JobMetadata];

    /// <summary>Publishes what the processor offers, retained for late subscribers. One replica does it.</summary>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    [OnStarted(Singleton = true)]
    public ValueTask PublishMetadataAsync(CancellationToken cancellationToken) =>
        _publisher.PublishAsync(
            new JobMetadata
            {
                SupportedCommands = ["Store", "StoreAndStart", "Abort", "Cancel", "Clear", "Pause", "Resume", "RevokeStart", "Start", "Stop", "Update", "RequestJobResponseByJobOrderID", "RequestJobResponseByJobOrderState"],
                EventTypes = ["ISA95JobOrderStatusEventType"],
            },
            Intent.Meta,
            new PublishOptions { Retain = true },
            cancellationToken);

    /// <summary>Handles the Store method.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<StoreResponse> OnStoreAsync(StoreCall call, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        Reply<StoreResponse>(await NewOrderAsync(JobOrderOperation.Store, call, scope, context, cancellationToken).ConfigureAwait(false));

    /// <summary>Handles the StoreAndStart method.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<StoreAndStartResponse> OnStoreAndStartAsync(StoreAndStartCall call, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        Reply<StoreAndStartResponse>(await NewOrderAsync(JobOrderOperation.StoreAndStart, call, scope, context, cancellationToken).ConfigureAwait(false));

    /// <summary>Handles the Update method: replaces the order data and keeps the state.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<UpdateResponse> OnUpdateAsync(UpdateCall call, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        Reply<UpdateResponse>(await UpdateOrderAsync(call, scope, context, cancellationToken).ConfigureAwait(false));

    /// <summary>Handles the Start method.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<StartResponse> OnStartAsync(StartCall call, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        Reply<StartResponse>(await ExistingOrderAsync(JobOrderOperation.Start, call, scope, context, cancellationToken).ConfigureAwait(false));

    /// <summary>Handles the Stop method.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<StopResponse> OnStopAsync(StopCall call, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        Reply<StopResponse>(await ExistingOrderAsync(JobOrderOperation.Stop, call, scope, context, cancellationToken).ConfigureAwait(false));

    /// <summary>Handles the Abort method.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<AbortResponse> OnAbortAsync(AbortCall call, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        Reply<AbortResponse>(await ExistingOrderAsync(JobOrderOperation.Abort, call, scope, context, cancellationToken).ConfigureAwait(false));

    /// <summary>Handles the Pause method.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<PauseResponse> OnPauseAsync(PauseCall call, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        Reply<PauseResponse>(await ExistingOrderAsync(JobOrderOperation.Pause, call, scope, context, cancellationToken).ConfigureAwait(false));

    /// <summary>Handles the Resume method.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<ResumeResponse> OnResumeAsync(ResumeCall call, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        Reply<ResumeResponse>(await ExistingOrderAsync(JobOrderOperation.Resume, call, scope, context, cancellationToken).ConfigureAwait(false));

    /// <summary>Handles the RevokeStart method.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<RevokeStartResponse> OnRevokeStartAsync(RevokeStartCall call, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        Reply<RevokeStartResponse>(await ExistingOrderAsync(JobOrderOperation.RevokeStart, call, scope, context, cancellationToken).ConfigureAwait(false));

    /// <summary>Handles the Cancel method.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<CancelResponse> OnCancelAsync(CancelCall call, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        Reply<CancelResponse>(await ExistingOrderAsync(JobOrderOperation.Cancel, call, scope, context, cancellationToken).ConfigureAwait(false));

    /// <summary>Handles the Clear method.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<ClearResponse> OnClearAsync(ClearCall call, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        Reply<ClearResponse>(await ExistingOrderAsync(JobOrderOperation.Clear, call, scope, context, cancellationToken).ConfigureAwait(false));

    /// <summary>Handles the RequestJobResponseByJobOrderID method.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<RequestJobResponseByJobOrderIdResponse> OnRequestJobResponseByIdAsync(RequestJobResponseByJobOrderIdCall call, [Scope] string scope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(call);
        if (string.IsNullOrEmpty(call.JobOrderID))
        {
            Log.InvalidQuery(_logger, nameof(RequestJobResponseByJobOrderIdCall));
            return new RequestJobResponseByJobOrderIdResponse { ReturnStatus = (ulong)JobReturnStatus.InvalidRequest };
        }

        var stored = await _store.GetResponseByOrderAsync(call.JobOrderID, cancellationToken).ConfigureAwait(false);
        if (stored is null || !string.Equals(stored.Scope, scope, StringComparison.Ordinal))
        {
            return new RequestJobResponseByJobOrderIdResponse { ReturnStatus = (ulong)JobReturnStatus.UnknownJobOrderId };
        }

        return new RequestJobResponseByJobOrderIdResponse
        {
            EncodingMask = (uint)JobResponseByIdResultDataTypeFields.JobResponse,
            JobResponse = JobControlConversions.ToV2Response(stored.Response),
            ReturnStatus = (ulong)JobReturnStatus.Success,
        };
    }

    /// <summary>Handles the RequestJobResponseByJobOrderState method. A query with only a top-level state matches all its sub-states.</summary>
    /// <param name="call">The call.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    [Incoming]
    public async ValueTask<RequestJobResponseByJobOrderStateResponse> OnRequestJobResponseByStateAsync(RequestJobResponseByJobOrderStateCall call, [Scope] string scope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(call);
        if (call.JobOrderState.IsEmpty)
        {
            Log.InvalidQuery(_logger, nameof(RequestJobResponseByJobOrderStateCall));
            return new RequestJobResponseByJobOrderStateResponse { ReturnStatus = (ulong)JobReturnStatus.InvalidRequest };
        }

        var states = _queryableStates.Where(state => JobStateArray.Matches(state, call.JobOrderState)).ToList();
        var responses = await _store.GetResponsesByStateAsync(scope, states, MaxResponsesByState, cancellationToken).ConfigureAwait(false);
        return new RequestJobResponseByJobOrderStateResponse
        {
            JobResponses = [.. responses.Select(JobControlConversions.ToV2Response)],
            ReturnStatus = (ulong)JobReturnStatus.Success,
        };
    }

    /// <summary>Adds or, with <c>DELETE</c>, removes equipment of a scope.</summary>
    /// <param name="config">The configuration.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    [Incoming]
    public ValueTask OnConfigEquipmentAsync(ConfigEquipment config, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        ApplyReferenceAsync(JobReferenceKind.Equipment, config.ID, scope, context, cancellationToken);

    /// <summary>Adds or, with <c>DELETE</c>, removes material class of a scope.</summary>
    /// <param name="config">The configuration.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    [Incoming]
    public ValueTask OnConfigMaterialClassAsync(ConfigMaterialClass config, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        ApplyReferenceAsync(JobReferenceKind.MaterialClass, config.MaterialClassID, scope, context, cancellationToken);

    /// <summary>Adds or, with <c>DELETE</c>, removes material definition of a scope.</summary>
    /// <param name="config">The configuration.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    [Incoming]
    public ValueTask OnConfigMaterialDefinitionAsync(ConfigMaterialDefinition config, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        ApplyReferenceAsync(JobReferenceKind.MaterialDefinition, config.MaterialDefinitionID, scope, context, cancellationToken);

    /// <summary>Adds or, with <c>DELETE</c>, removes personnel of a scope.</summary>
    /// <param name="config">The configuration.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    [Incoming]
    public ValueTask OnConfigPersonnelAsync(ConfigPersonnel config, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        ApplyReferenceAsync(JobReferenceKind.Personnel, config.ID, scope, context, cancellationToken);

    /// <summary>Adds or, with <c>DELETE</c>, removes physical asset of a scope.</summary>
    /// <param name="config">The configuration.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    [Incoming]
    public ValueTask OnConfigPhysicalAssetAsync(ConfigPhysicalAsset config, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        ApplyReferenceAsync(JobReferenceKind.PhysicalAsset, config.ID, scope, context, cancellationToken);

    /// <summary>Stores or, with <c>DELETE</c>, removes a standard Work Master of a scope.</summary>
    /// <param name="config">The Work Master.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    [Incoming]
    public ValueTask OnConfigWorkMasterAsync(ConfigWorkMaster config, [Scope] string scope, EventContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(config);
        return ApplyWorkMasterAsync(new ISA95WorkMasterExtDataType { ID = config.ID, Description = config.Description, Parameters = config.Parameters, EncodingMask = config.EncodingMask }, scope, context, cancellationToken);
    }

    /// <summary>Stores or, with <c>DELETE</c>, removes a Work Master with recipe content of a scope.</summary>
    /// <param name="config">The Work Master.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    [Incoming]
    public ValueTask OnConfigWorkMasterExtAsync(ConfigWorkMasterExt config, [Scope] string scope, EventContext context, CancellationToken cancellationToken) =>
        ApplyWorkMasterAsync(config, scope, context, cancellationToken);

    /// <summary>Sets or, with <c>DELETE</c>, removes the job acceptance limit of a scope.</summary>
    /// <param name="config">The limit.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    [Incoming]
    public async ValueTask OnConfigJobAcceptanceAsync(ConfigJobAcceptance config, [Scope] string scope, EventContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(context);
        if (IsDelete(context))
        {
            await _store.DeleteAcceptanceLimitAsync(scope, cancellationToken).ConfigureAwait(false);
            Log.ConfigChanged(_logger, "Removed", "acceptance limit", scope, scope);
            return;
        }

        if (config.MaxDownloadableJobOrders < 0)
        {
            Log.InvalidLimit(_logger, config.MaxDownloadableJobOrders, scope);
            return;
        }

        await _store.SetAcceptanceLimitAsync(scope, config.MaxDownloadableJobOrders, cancellationToken).ConfigureAwait(false);
        Log.ConfigChanged(_logger, "Stored", "acceptance limit", scope, scope);
    }

    private static T Reply<T>(JobReturnStatus status)
        where T : JobMethodResponseDataType, new() => new() { ReturnStatus = (ulong)status };

    private static bool IsDelete(EventContext context) => string.Equals(context.Method, DeleteMethod, StringComparison.Ordinal);

    private static bool IsAllowedToStart(JobOrderState state) =>
        state is JobOrderState.AllowedToStartWaiting or JobOrderState.AllowedToStartReady or JobOrderState.AllowedToStartLoaded;

    private static JobOrder? ToValidOrder(V2.ISA95JobOrderDataType? order)
    {
        if (order is null)
        {
            return null;
        }

        var neutral = JobControlConversions.FromV2Order(order);
        return JobControlValidation.Validate(neutral) == JobReturnStatus.Success ? neutral : null;
    }

    private async ValueTask<bool> IsRepeatAsync(string id, string scope, EventContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        var existing = await _store.GetOrderAsync(id, cancellationToken).ConfigureAwait(false);
        return existing is not null
            && string.Equals(existing.Scope, scope, StringComparison.Ordinal)
            && string.Equals(existing.Operation, context.EventId, StringComparison.Ordinal);
    }

    private async ValueTask<JobReturnStatus> NewOrderAsync(JobOrderOperation operation, JobOrderMethodCallDataType call, string scope, EventContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(call);
        if (ToValidOrder(call.JobOrder) is not { } order)
        {
            Log.InvalidRequest(_logger, operation);
            return JobReturnStatus.InvalidRequest;
        }

        // Before the acceptance check: a repeated command must not be refused because of its own write.
        if (await IsRepeatAsync(order.Id, scope, context, cancellationToken).ConfigureAwait(false))
        {
            return JobReturnStatus.Success;
        }

        return await _acceptance.IsAcceptableAsync(order, scope, addsOrder: true, cancellationToken).ConfigureAwait(false)
            ? await ApplyAsync(operation, order.Id, order, scope, context, cancellationToken).ConfigureAwait(false)
            : JobReturnStatus.UnableToAccept;
    }

    private async ValueTask<JobReturnStatus> UpdateOrderAsync(UpdateCall call, string scope, EventContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(call);
        if (ToValidOrder(call.JobOrder) is not { } order)
        {
            Log.InvalidRequest(_logger, JobOrderOperation.Update);
            return JobReturnStatus.InvalidRequest;
        }

        if (await IsRepeatAsync(order.Id, scope, context, cancellationToken).ConfigureAwait(false))
        {
            return JobReturnStatus.Success;
        }

        return await _acceptance.IsAcceptableAsync(order, scope, addsOrder: false, cancellationToken).ConfigureAwait(false)
            ? await ApplyAsync(JobOrderOperation.Update, order.Id, order, scope, context, cancellationToken).ConfigureAwait(false)
            : JobReturnStatus.UnableToAccept;
    }

    private async ValueTask<JobReturnStatus> ExistingOrderAsync(JobOrderOperation operation, JobOrderIdMethodCallDataType call, string scope, EventContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(call);
        if (string.IsNullOrEmpty(call.JobOrderID))
        {
            Log.InvalidRequest(_logger, operation);
            return JobReturnStatus.InvalidRequest;
        }

        return await ApplyAsync(operation, call.JobOrderID, replacement: null, scope, context, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<JobReturnStatus> ApplyAsync(JobOrderOperation operation, string id, JobOrder? replacement, string scope, EventContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        var creates = operation is JobOrderOperation.Store or JobOrderOperation.StoreAndStart;
        var removes = operation is JobOrderOperation.Cancel or JobOrderOperation.Clear;
        var conflicted = false;
        for (var attempt = 0; attempt < MaxWriteAttempts; attempt++)
        {
            var existing = await _store.GetOrderAsync(id, cancellationToken).ConfigureAwait(false);

            // An id belongs to one scope (D-42). Another scope learns nothing about the order, so this check comes first.
            if (existing is not null && !string.Equals(existing.Scope, scope, StringComparison.Ordinal))
            {
                var status = creates ? JobReturnStatus.UnableToAccept : JobReturnStatus.UnknownJobOrderId;
                Log.Rejected(_logger, operation, id, scope, status);
                return status;
            }

            // The same command again after its write happened (R-4): the outcome stands.
            if (existing is not null && string.Equals(existing.Operation, context.EventId, StringComparison.Ordinal))
            {
                return JobReturnStatus.Success;
            }

            // A removal leaves no document: a lost race or lost reply, or this command's own earlier removal.
            if (existing is null && removes && (conflicted || await _store.WasRemovedByAsync(id, scope, context.EventId, cancellationToken).ConfigureAwait(false)))
            {
                return JobReturnStatus.Success;
            }

            var state = existing?.State ?? JobOrderState.Initial;
            var transition = JobStateMachine.Apply(JobControlVersion.V2, state, operation);
            if (!transition.Accepted)
            {
                Log.Rejected(_logger, operation, id, scope, transition.Status);
                return transition.Status;
            }

            var order = replacement ?? existing?.Order ?? throw new InvalidOperationException("The state machine accepted an operation that has no order.");
            var version = existing?.Version ?? 0;
            var written = removes
                ? await _store.RemoveOrderAsync(id, scope, operation.ToString(), version, context.EventId, cancellationToken).ConfigureAwait(false)
                : await _store.SaveOrderAsync(new JobOrderWrite(order, transition.State, scope, operation.ToString(), version, context.EventId, IsAllowedToStart(transition.State) && !IsAllowedToStart(state)), cancellationToken).ConfigureAwait(false);
            if (written)
            {
                Log.Applied(_logger, operation, id, scope, transition.State);
                if (transition.Effect == JobOrderEffect.StatusEvent)
                {
                    await PublishStatusAsync(order, transition.State, scope, context, cancellationToken).ConfigureAwait(false);
                }

                return JobReturnStatus.Success;
            }

            conflicted = true;
        }

        Log.WriteContended(_logger, id);
        return creates ? JobReturnStatus.UnableToAccept : JobReturnStatus.InvalidStatus;
    }

    private async ValueTask PublishStatusAsync(JobOrder order, JobOrderState state, string scope, EventContext context, CancellationToken cancellationToken)
    {
        try
        {
            await _publisher.PublishAsync(
                new JobOrderStatusEvent { JobOrder = JobControlConversions.ToV2Order(order), JobState = JobStateArray.ToV2(state) },
                Intent.Event,
                new PublishOptions { Subject = scope, CorrelationId = context.CorrelationId, CausationId = context.EventId },
                cancellationToken).ConfigureAwait(false);
        }
        catch (OutboundQueueFullException)
        {
            // The transition is stored and the caller gets its result; a redelivery could not repair the event, the change stream can (S18).
            Log.StatusEventDropped(_logger, order.Id);
        }
    }

    private async ValueTask ApplyReferenceAsync(JobReferenceKind kind, string? id, string scope, EventContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (string.IsNullOrEmpty(id))
        {
            return;
        }

        if (IsDelete(context))
        {
            await _store.RemoveReferenceAsync(kind, id, scope, cancellationToken).ConfigureAwait(false);
            Log.ReferenceChanged(_logger, "Removed", kind, id, scope);
        }
        else
        {
            await _store.AddReferenceAsync(kind, id, scope, cancellationToken).ConfigureAwait(false);
            Log.ReferenceChanged(_logger, "Stored", kind, id, scope);
        }
    }

    private async ValueTask ApplyWorkMasterAsync(ISA95WorkMasterExtDataType config, string scope, EventContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (string.IsNullOrEmpty(config.ID))
        {
            return;
        }

        if (IsDelete(context))
        {
            await _store.DeleteWorkMasterAsync(config.ID, scope, cancellationToken).ConfigureAwait(false);
            Log.ConfigChanged(_logger, "Removed", "Work Master", config.ID, scope);
        }
        else if (await _store.SaveWorkMasterAsync(JobControlConversions.FromWorkMasterExt(config), scope, cancellationToken).ConfigureAwait(false))
        {
            Log.ConfigChanged(_logger, "Stored", "Work Master", config.ID, scope);
        }
        else
        {
            Log.WorkMasterInOtherScope(_logger, config.ID, scope);
        }
    }
}

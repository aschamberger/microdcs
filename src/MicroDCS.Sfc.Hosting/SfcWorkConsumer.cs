using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.MachineryJobs;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Sfc;
using MicroDCS.Valkey.Streams;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MicroDCS.Sfc.Hosting;

/// <summary>
/// Consumes the work stream of each scope through the poller (D-43). Entries are handled one after the other and acknowledged after their handler
/// succeeded; a failure leaves the entry pending, another pass takes it over, and after too many deliveries it goes to the dead stream (D-44). Nothing
/// here waits on a broker: a dispatch only queues the command (D-28, D-51).
/// </summary>
internal sealed class SfcWorkConsumer : IStreamConsumer
{
    private readonly ValkeyStreams _streams;
    private readonly KeySchema _keys;
    private readonly SfcStateDao _states;
    private readonly IJobStore _jobs;
    private readonly JobExecution _execution;
    private readonly SfcGraphs _graphs;
    private readonly SfcJobRunner _runner;
    private readonly SfcPendingCommands _pending;
    private readonly IOutgoingCommands _commands;
    private readonly SfcHostOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<SfcWorkConsumer> _logger;
    private readonly string _consumer;
    private readonly bool _enabled;
    private readonly ConcurrentDictionary<string, DateTimeOffset> _nextClaim = new(StringComparer.Ordinal);

    public SfcWorkConsumer(
        ValkeyStreams streams,
        ValkeyContext context,
        SfcStateDao states,
        IJobStore jobs,
        JobExecution execution,
        SfcGraphs graphs,
        SfcJobRunner runner,
        SfcPendingCommands pending,
        IOutgoingCommands commands,
        IOptions<SfcHostOptions> options,
        IOptions<MicroDcsOptions> core,
        TimeProvider time,
        ILogger<SfcWorkConsumer> logger)
    {
        _streams = streams;
        _keys = context.Keys;
        _states = states;
        _jobs = jobs;
        _execution = execution;
        _graphs = graphs;
        _runner = runner;
        _pending = pending;
        _commands = commands;
        _options = options.Value;
        _time = time;
        _logger = logger;
        _consumer = core.Value.InstanceId ?? throw new InvalidOperationException("The instance id is not set.");
        _enabled = _options.Enabled && core.Value.Roles.HasFlag(InstanceRoles.Processor);
    }

    public string Kind => SfcWork.NudgeKind;

    public async ValueTask ScopeDiscoveredAsync(string scope, CancellationToken cancellationToken)
    {
        if (!_enabled)
        {
            return;
        }

        await _streams.EnsureGroupAsync(_keys.SfcWork(scope), SfcWork.Group, cancellationToken).ConfigureAwait(false);

        // The recovery scan (S-29): every instance runs it when it first sees a scope. A resume with the scan mark changes nothing for live work, so several replicas can run it.
        try
        {
            var jobs = await _states.QueueRecoveryAsync(scope, cancellationToken).ConfigureAwait(false);
            if (jobs > 0)
            {
                Log.RecoveryScan(_logger, scope, jobs);
            }
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException)
        {
            // The poller does not repeat a discovery, and the other consumers still need theirs. The scan only repairs: the sweep owns every deadline.
            Log.RecoveryScanFailed(_logger, ex, scope);
        }
    }

    public async ValueTask<bool> PollAsync(string scope, CancellationToken cancellationToken)
    {
        if (!_enabled)
        {
            return false;
        }

        var stream = _keys.SfcWork(scope);
        try
        {
            var claimed = await ClaimOrphansAsync(scope, stream, cancellationToken).ConfigureAwait(false);
            var entries = await _streams.ReadAsync(stream, SfcWork.Group, _consumer, _options.ReadCount, cancellationToken).ConfigureAwait(false);
            await HandleAllAsync(scope, stream, entries, cancellationToken).ConfigureAwait(false);
            return claimed || entries.Count > 0;
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("NOGROUP", StringComparison.Ordinal))
        {
            // A purge removed the stream with its group.
            await _streams.EnsureGroupAsync(stream, SfcWork.Group, cancellationToken).ConfigureAwait(false);
            return false;
        }
    }

    private async ValueTask<bool> ClaimOrphansAsync(string scope, string stream, CancellationToken cancellationToken)
    {
        var now = _time.GetUtcNow();
        if (_nextClaim.TryGetValue(scope, out var next) && now < next)
        {
            return false;
        }

        _nextClaim[scope] = now + TimeSpan.FromTicks(Math.Max(_options.AutoclaimIdle.Ticks / 6, TimeSpan.TicksPerSecond));
        var claimed = await _streams.AutoClaimAsync(stream, SfcWork.Group, _consumer, _options.AutoclaimIdle, _options.ReadCount, cancellationToken).ConfigureAwait(false);
        if (claimed.Count == 0)
        {
            return false;
        }

        var deliveries = (await _streams.PendingAsync(stream, SfcWork.Group, claimed[0].Id, claimed[^1].Id, claimed.Count, _consumer, cancellationToken).ConfigureAwait(false))
            .ToDictionary(static p => p.MessageId, static p => p.DeliveryCount);
        var live = new List<StreamEntry>(claimed.Count);
        foreach (var entry in claimed)
        {
            var count = deliveries.GetValueOrDefault(entry.Id);
            if (count > _options.MaxDeliveries)
            {
                await _states.MoveToDeadAsync(scope, SfcWork.Group, entry, $"delivered {count} times", cancellationToken).ConfigureAwait(false);
                SfcTelemetry.Dead.Add(1);
                Log.WorkEntryDead(_logger, entry.Id.ToString(), scope, count);
            }
            else
            {
                live.Add(entry);
            }
        }

        await HandleAllAsync(scope, stream, live, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private async ValueTask HandleAllAsync(string scope, string stream, IReadOnlyList<StreamEntry> entries, CancellationToken cancellationToken)
    {
        var done = new List<RedisValue>(entries.Count);
        foreach (var entry in entries)
        {
            try
            {
                await HandleAsync(scope, entry, cancellationToken).ConfigureAwait(false);
                done.Add(entry.Id);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                Log.WorkEntryFailed(_logger, ex, entry.Id.ToString(), scope);
            }
        }

        await _streams.AcknowledgeAsync(stream, SfcWork.Group, done, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask HandleAsync(string scope, StreamEntry entry, CancellationToken cancellationToken)
    {
        var entryId = entry.Id.ToString();
        var jobId = entry[SfcWork.JobIdField].ToString();
        var action = entry[SfcWork.ActionField].ToString();
        if (action == SfcWork.Event)
        {
            await ReceiveEventAsync(scope, entry, entryId, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (string.IsNullOrEmpty(jobId) || string.IsNullOrEmpty(action))
        {
            Log.InvalidWorkEntry(_logger, entryId, scope, "job id or action missing");
            return;
        }

        var at = SfcWork.TimeOf(entryId);
        switch (action)
        {
            case SfcWork.Start:
                await StartAsync(jobId, scope, entryId, cancellationToken).ConfigureAwait(false);
                break;
            case SfcWork.Cancel:
                await _runner.RunAsync(jobId, new SfcCancel { At = at, Reason = entry[SfcWork.ReasonField].HasValue ? entry[SfcWork.ReasonField].ToString() : "job ended" }, entryId, null, cancellationToken).ConfigureAwait(false);
                break;
            case SfcWork.Tick:
                await TickAsync(jobId, scope, entry, entryId, at, cancellationToken).ConfigureAwait(false);
                break;
            case SfcWork.Resume:
                await ResumeAsync(jobId, scope, entry, entryId, at, cancellationToken).ConfigureAwait(false);
                break;
            case SfcWork.Job:
                await ApplyJobOperationsAsync(jobId, scope, entry, entryId, cancellationToken).ConfigureAwait(false);
                break;
            default:
                if (action.StartsWith(SfcWork.DispatchPrefix, StringComparison.Ordinal))
                {
                    await DispatchAsync(jobId, entry, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    Log.InvalidWorkEntry(_logger, entryId, scope, "unknown action");
                }

                break;
        }
    }

    private async ValueTask StartAsync(string jobId, string scope, string entryId, CancellationToken cancellationToken)
    {
        // RevokeStart and Cancel do not withdraw a start entry, and Start, RevokeStart, Start leaves two (D-50).
        var order = await _jobs.GetOrderAsync(jobId, cancellationToken).ConfigureAwait(false);
        if (order is null || !string.Equals(order.Scope, scope, StringComparison.Ordinal))
        {
            Log.StartSkipped(_logger, jobId, JobOrderState.Removed);
            return;
        }

        if (order.State is not (JobOrderState.AllowedToStartWaiting or JobOrderState.AllowedToStartReady or JobOrderState.AllowedToStartLoaded))
        {
            Log.StartSkipped(_logger, jobId, order.State);
            return;
        }

        var existing = await _states.GetAsync(jobId, cancellationToken).ConfigureAwait(false);
        if (existing is not null && !SfcJobRunner.Parse(existing).IsEnded)
        {
            return;
        }

        var built = await BuildRecipeAsync(order, scope, cancellationToken).ConfigureAwait(false);
        if (built.Graph is not { } graph)
        {
            Log.RecipeRefused(_logger, jobId, built.Reason);
            await _execution.ApplyAsync(jobId, scope, JobOrderOperation.Abort, entryId, cancellationToken).ConfigureAwait(false);
            return;
        }

        await _states.PutRecipeAsync(graph.RecipeHash, built.Recipe, cancellationToken).ConfigureAwait(false);
        var initial = SfcState.Create(graph, jobId, scope);

        // Not the time of the entry: an order can wait in the stream longer than its first timeout, and that wait must not cost an attempt. A repeated start is absorbed, so nothing replays this time (D-44).
        var now = await _states.ServerTimeAsync(cancellationToken).ConfigureAwait(false);
        if (await _runner.StartAsync(graph, initial, existing?.Version ?? 0, now, entryId, cancellationToken).ConfigureAwait(false))
        {
            Log.RecipeStarted(_logger, jobId, scope);
        }
    }

    private async ValueTask<(SfcGraph? Graph, string Recipe, string Reason)> BuildRecipeAsync(StoredJobOrder order, string scope, CancellationToken cancellationToken)
    {
        var workMasterId = order.Order.WorkMasters.ToArray()?.FirstOrDefault()?.Id;
        if (string.IsNullOrEmpty(workMasterId))
        {
            return (null, string.Empty, "the job order names no Work Master");
        }

        var workMaster = await _jobs.GetWorkMasterAsync(workMasterId, scope, cancellationToken).ConfigureAwait(false);
        if (workMaster is null)
        {
            return (null, string.Empty, $"Work Master '{workMasterId}' is not stored for this scope");
        }

        if (!string.Equals(workMaster.DataSchema, _graphs.Interpreter.DataSchema, StringComparison.Ordinal))
        {
            return (null, string.Empty, $"Work Master '{workMasterId}' does not carry a recipe this station can interpret");
        }

        if (string.IsNullOrEmpty(workMaster.Data))
        {
            return (null, string.Empty, $"Work Master '{workMasterId}' has no recipe");
        }

        var result = _graphs.Build(workMaster.Data);
        return result.Graph is { } graph
            ? (graph, workMaster.Data, string.Empty)
            : (null, string.Empty, string.Join("; ", result.Errors.Select(static e => $"{e.Code}: {e.Message}")));
    }

    private async ValueTask TickAsync(string jobId, string scope, StreamEntry entry, string entryId, long at, CancellationToken cancellationToken)
    {
        _ = long.TryParse(entry[SfcWork.OutageField].ToString(), CultureInfo.InvariantCulture, out var outage);
        var result = await _runner.RunAsync(jobId, new SfcTick { At = at, OutageMs = outage }, entryId, null, cancellationToken).ConfigureAwait(false);

        // The sweep took the job off the index, and an unchanged state writes nothing: without this a moved deadline would never fire (D-18).
        if (result is { Status: SfcRunStatus.Unchanged, State.NextDeadline: { } deadline })
        {
            await _states.SetDueAsync(scope, jobId, deadline, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask ResumeAsync(string jobId, string scope, StreamEntry entry, string entryId, long at, CancellationToken cancellationToken)
    {
        if (entry[SfcWork.ScanField] == "1")
        {
            await RecoverAsync(jobId, scope, entryId, at, cancellationToken).ConfigureAwait(false);
            return;
        }

        // With "only", every other open action counts as live, so the core re-issues just that one (operator tool sfc_redispatch).
        var only = entry[SfcWork.OnlyField].ToString();
        Func<SfcState, IReadOnlyList<string>>? live = string.IsNullOrEmpty(only)
            ? null
            : state => [.. state.Actions.Values.Where(a => !string.Equals(a.Name, only, StringComparison.Ordinal)).Select(a => ActionKey.Create(jobId, a.Name))];
        await _runner.RunAsync(jobId, new SfcResume { At = at }, entryId, live, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The recovery scan of one job. Every action with a deadline counts as live, because the sweep owns it: resending it would use up an attempt on every
    /// restart (D-9), and after an outage it would race the tick that moves the deadlines (D-18). So the scan re-issues only work that never got a
    /// deadline and puts back index entries that were lost. A repaired entry is not due before the outage threshold, so a tick already under way goes first.
    /// </summary>
    private async ValueTask RecoverAsync(string jobId, string scope, string entryId, long at, CancellationToken cancellationToken)
    {
        SfcRunResult result;
        try
        {
            result = await _runner.RunAsync(
                jobId,
                new SfcResume { At = at },
                entryId,
                state => [.. state.Actions.Values.Where(static a => a.IsOpen && a.Deadline is not null).Select(a => ActionKey.Create(jobId, a.Name))],
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is JsonException or (SfcStepFailedException and not SfcContentionException))
        {
            // A job that cannot be evaluated (recipe missing, state unreadable) would otherwise add a dead entry on every restart. Its deadline or an operator deals with it.
            Log.RecoveryNotPossible(_logger, ex, jobId);
            return;
        }

        if (result is not { Status: SfcRunStatus.Unchanged, State: { IsEnded: false } state })
        {
            return;
        }

        if (state.NextDeadline is { } deadline)
        {
            await _states.AddDueIfMissingAsync(scope, jobId, Math.Max(deadline, at + (long)_options.OutageThreshold.TotalMilliseconds), cancellationToken).ConfigureAwait(false);
        }

        foreach (var typeId in state.WaitingTypeIds)
        {
            await _states.AddWaitingAsync(scope, typeId, jobId, at, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Offers an event to the jobs waiting for its type, oldest wait first (S-16, R-1). The compare-and-set decides between replicas, and the core decides
    /// whether the selector matches. The first job that takes the event ends the search, so one event completes one action.
    /// </summary>
    private async ValueTask ReceiveEventAsync(string scope, StreamEntry entry, string entryId, CancellationToken cancellationToken)
    {
        var typeId = entry[SfcWork.TypeIdField].ToString();
        if (string.IsNullOrEmpty(typeId))
        {
            Log.InvalidWorkEntry(_logger, entryId, scope, "event without a type");
            return;
        }

        // The identity of the event, not of the entry: a broker redelivery of the event appends a new entry. An older entry has neither.
        var eventKey = NullIfMissing(entry[SfcWork.EventKeyField]) ?? entryId;

        // Applied before a crash or by another consumer: the job may have moved on, and the event must not complete another job.
        if (await _states.IsEventClaimedAsync(scope, eventKey, cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        var received = new SfcEventReceived
        {
            At = SfcWork.TimeOf(entryId),
            TypeId = typeId,
            Outcome = entry[SfcWork.OutcomeField].ToString(),
            Subject = NullIfMissing(entry[SfcWork.SubjectField]),
            CorrelationId = NullIfMissing(entry[SfcWork.CorrelationIdField]),
            Error = NullIfMissing(entry[SfcWork.ErrorField]),
        };

        foreach (var (jobId, _) in await _states.ListWaitingAsync(scope, typeId, int.MaxValue, cancellationToken).ConfigureAwait(false))
        {
            try
            {
                var result = await _runner.RunAsync(jobId, received, entryId, liveWork: null, claimedEvent: eventKey, cancellationToken).ConfigureAwait(false);
                if (result.Status == SfcRunStatus.AlreadyClaimed)
                {
                    return;
                }

                if (result.Status == SfcRunStatus.Applied)
                {
                    SfcTelemetry.EventsMatched.Add(1);
                    Log.PullEventMatched(_logger, entryId, typeId, jobId);
                    return;
                }

                // Only a job that cannot wait again: for a running one this read may be stale, and removing its entry would lose a live wait.
                if (result.State is null or { IsEnded: true })
                {
                    await _states.RemoveWaitingAsync(scope, typeId, jobId, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is JsonException or (SfcStepFailedException and not SfcContentionException))
            {
                // One broken job must not block the event for the others or send it to the dead stream.
                Log.PullEventNotApplied(_logger, ex, entryId, jobId);
            }
        }

        SfcTelemetry.EventsUnmatched.Add(1);
        Log.PullEventUnmatched(_logger, scope, entryId, typeId);
    }

    private static string? NullIfMissing(RedisValue value) => value.HasValue ? value.ToString() : null;

    private async ValueTask ApplyJobOperationsAsync(string jobId, string scope, StreamEntry entry, string entryId, CancellationToken cancellationToken)
    {
        foreach (var name in entry[SfcWork.OperationsField].ToString().Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            if (!Enum.TryParse<JobOrderOperation>(name, out var operation))
            {
                Log.InvalidWorkEntry(_logger, entryId, scope, "unknown job operation");
                continue;
            }

            // Entries of one step can reach two consumers: a recipe can end before the instance holding its start entry has run it.
            DateTimeOffset? startedAt = null;
            if (operation == JobOrderOperation.Complete)
            {
                await _execution.ApplyAsync(jobId, scope, JobOrderOperation.BeginExecution, entryId + ":begin", cancellationToken).ConfigureAwait(false);
                var document = await _states.GetAsync(jobId, cancellationToken).ConfigureAwait(false);
                if (document is not null && SfcJobRunner.Parse(document).StartedAt is { } started)
                {
                    startedAt = DateTimeOffset.FromUnixTimeMilliseconds(started);
                }
            }

            var result = await _execution.ApplyAsync(jobId, scope, operation, entryId + ":" + name, startedAt, cancellationToken).ConfigureAwait(false);
            if (operation == JobOrderOperation.BeginExecution && IsNoLongerStartable(result))
            {
                Log.RecipeOrphaned(_logger, jobId);
                await _streams.AppendAsync(_keys.SfcWork(scope), SfcWork.NudgeKind, scope, SfcWork.Entry(jobId, scope, SfcWork.Cancel, (SfcWork.ReasonField, "job order left AllowedToStart")), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // Refused while executing means the transition already happened. Any other refusal means the order left AllowedToStart before the recipe got going
    // (RevokeStart, or an Abort that found no recipe to cancel yet); a cancel for a recipe that has ended is absorbed.
    private static bool IsNoLongerStartable(JobExecutionResult result) => result.Status == JobExecutionStatus.Missing
        || (result.Status == JobExecutionStatus.Refused && result.State is not (JobOrderState.Running or JobOrderState.Held or JobOrderState.Suspended));

    private async ValueTask DispatchAsync(string jobId, StreamEntry entry, CancellationToken cancellationToken)
    {
        if (JsonSerializer.Deserialize(entry[SfcWork.CommandField].ToString(), SfcJsonContext.Default.SfcCommand) is not StartWork command)
        {
            Log.InvalidWorkEntry(_logger, entry.Id.ToString(), entry[SfcWork.ScopeField].ToString(), "no start command");
            return;
        }

        var document = await _states.GetAsync(jobId, cancellationToken).ConfigureAwait(false);
        var state = document is null ? null : SfcJobRunner.Parse(document);
        if (state is null || state.IsEnded || !state.Actions.TryGetValue(command.Action, out var action)
            || action.Status != SfcActionStatus.Dispatched || !string.Equals(action.CommandId, command.CommandId, StringComparison.Ordinal))
        {
            // A redelivered entry of an attempt that is over: the equipment must not get it again.
            Log.DispatchSkipped(_logger, command.Action, jobId, "the attempt is no longer open");
            return;
        }

        var now = await _states.ServerTimeAsync(cancellationToken).ConfigureAwait(false);
        var remaining = TimeSpan.FromMilliseconds(command.Deadline - now);
        if (remaining <= TimeSpan.Zero)
        {
            Log.DispatchSkipped(_logger, command.Action, jobId, "the deadline has passed");
            return;
        }

        // A timeout shorter than the grace must still reach the equipment, so the grace only applies where the time allows it.
        var expiry = remaining > _options.ExpiryGrace ? remaining - _options.ExpiryGrace : remaining;

        _pending.Add(command.CommandId, new PendingCommand(jobId, command.ActionKey, command.Round, command.Deadline));
        try
        {
            await _commands.DispatchAsync(
                command.TypeId,
                new CommandRequest
                {
                    Scope = state.Scope,
                    JobId = jobId,
                    ActionKey = command.ActionKey,
                    CommandId = command.CommandId,
                    CorrelationId = jobId,
                    Parameters = command.Parameters ?? new Dictionary<string, JsonElement>(StringComparer.Ordinal),
                    ExpiryInterval = expiry,
                },
                cancellationToken).ConfigureAwait(false);
            SfcTelemetry.Dispatched.Add(1);
            Log.Dispatched(_logger, command.Action, jobId, command.Attempt);
        }
        catch (OutboundQueueFullException)
        {
            // Backpressure, not a poison entry: the deadline dispatches again with the next attempt (D-28).
            _pending.Remove(command.CommandId);
            SfcTelemetry.DispatchDropped.Add(1);
            Log.DispatchQueueFull(_logger, command.Action, jobId);
        }
        catch
        {
            _pending.Remove(command.CommandId);
            throw;
        }
    }
}

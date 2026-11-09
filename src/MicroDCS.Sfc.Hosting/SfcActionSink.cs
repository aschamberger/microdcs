using System.Text.Json;
using MicroDCS.Core.Authoring;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Sfc;
using MicroDCS.Valkey.Streams;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace MicroDCS.Sfc.Hosting;

/// <summary>Feeds responses into the core. They do not pass through the work stream, so their time is one read of the server clock (D-44).</summary>
internal sealed class SfcActionSink : IActionSink
{
    private readonly SfcPendingCommands _pending;
    private readonly SfcJobRunner _runner;
    private readonly SfcStateDao _states;
    private readonly ValkeyStreams _streams;
    private readonly KeySchema _keys;
    private readonly ILogger<SfcActionSink> _logger;

    public SfcActionSink(SfcPendingCommands pending, SfcJobRunner runner, SfcStateDao states, ValkeyStreams streams, ValkeyContext context, ILogger<SfcActionSink> logger)
    {
        _streams = streams;
        _keys = context.Keys;
        _pending = pending;
        _runner = runner;
        _states = states;
        _logger = logger;
    }

    public async ValueTask CompleteAsync(ActionResponse response, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(response);

        // A response is matched by action key: the map finds it for a responder that only sets the causation id (D-44).
        _pending.TryGet(response.CommandId, out var pending);
        var actionKey = pending?.ActionKey ?? response.ActionKey;
        if (!ActionKey.TryParse(actionKey, out var jobId, out _))
        {
            Log.ResponseNotPending(_logger, response.EventId);
            return;
        }

        var at = await _states.ServerTimeAsync(cancellationToken).ConfigureAwait(false);
        SfcEvent input = response.Outcome.IsFailure
            ? new SfcActionFailed { At = at, ActionKey = actionKey!, Error = response.Outcome.Error, Round = pending?.Round }
            : new SfcActionCompleted { At = at, ActionKey = actionKey!, Outcome = response.Outcome.Name, Round = pending?.Round };

        // The type id of a response is not the type id of its command, so it is not passed for the contradiction check.
        try
        {
            await _runner.RunAsync(jobId, input, response.EventId, liveWork: null, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is SfcStepFailedException or JsonException)
        {
            // Not a broker or Valkey fault, so a redelivery would fail the same way; on a transport that cannot redeliver it would stop the host. The deadline settles the action.
            Log.ResponseNotApplied(_logger, ex, jobId, response.EventId);
        }
    }

    /// <summary>
    /// Queues an event that a job waits for; any replica may take it, because the wait is state (R-1). Every incoming event passes here, so the index
    /// is read first. An event that arrives before its wait exists is dropped, as in Python (S-16); the action then ends with <c>timeout</c>.
    /// </summary>
    public async ValueTask EventReceivedAsync(EventContext context, ActionOutcome outcome, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(outcome);
        if (context.Address?.Scope is not { Length: > 0 } scope)
        {
            return;
        }

        try
        {
            if (!await _states.HasWaitingAsync(scope, context.Type, cancellationToken).ConfigureAwait(false))
            {
                return;
            }
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException)
        {
            // Every incoming event passes here, most of them no recipe waits for. Faulting would stop the host over one of those; a job that did wait times out.
            SfcTelemetry.EventsUnmatched.Add(1);
            Log.WaitCheckFailed(_logger, ex, context.EventId);
            return;
        }

        var fields = new List<(string, string)>
        {
            (SfcWork.EventKeyField, context.Source + "|" + context.EventId),
            (SfcWork.TypeIdField, context.Type),
            (SfcWork.OutcomeField, outcome.IsFailure ? SfcOutcome.Error : outcome.Name),
        };
        AddIfSet(fields, SfcWork.SubjectField, context.Subject);
        AddIfSet(fields, SfcWork.CorrelationIdField, context.CorrelationId);
        AddIfSet(fields, SfcWork.ErrorField, outcome.Error);
        await _streams.AppendAsync(_keys.SfcWork(scope), SfcWork.NudgeKind, scope, SfcWork.Entry(string.Empty, scope, SfcWork.Event, [.. fields]), cancellationToken).ConfigureAwait(false);
    }

    private static void AddIfSet(List<(string, string)> fields, string name, string? value)
    {
        if (value is not null)
        {
            fields.Add((name, value));
        }
    }
}

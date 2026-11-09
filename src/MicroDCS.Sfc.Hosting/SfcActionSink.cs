using System.Text.Json;
using MicroDCS.Core.Authoring;
using MicroDCS.Valkey.Sfc;
using Microsoft.Extensions.Logging;

namespace MicroDCS.Sfc.Hosting;

/// <summary>Feeds responses into the core. They do not pass through the work stream, so their time is one read of the server clock (D-44).</summary>
internal sealed class SfcActionSink : IActionSink
{
    private readonly SfcPendingCommands _pending;
    private readonly SfcJobRunner _runner;
    private readonly SfcStateDao _states;
    private readonly ILogger<SfcActionSink> _logger;

    public SfcActionSink(SfcPendingCommands pending, SfcJobRunner runner, SfcStateDao states, ILogger<SfcActionSink> logger)
    {
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

    // Pull events follow in S20.
    public ValueTask EventReceivedAsync(EventContext context, CancellationToken cancellationToken) => ValueTask.CompletedTask;
}

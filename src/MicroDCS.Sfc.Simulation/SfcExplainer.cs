using System.Globalization;
using System.Text;

namespace MicroDCS.Sfc.Simulation;

/// <summary>Describes a graph, and optionally the state of a job on it, as plain text for people and agents (read-only).</summary>
public static class SfcExplainer
{
    /// <summary>Describes the structure of a recipe: steps, actions, divergences and the routes between them.</summary>
    /// <param name="graph">The graph.</param>
    /// <returns>The description.</returns>
    public static string Describe(SfcGraph graph)
    {
        ArgumentNullException.ThrowIfNull(graph);
        var text = new StringBuilder();
        text.Append(CultureInfo.InvariantCulture, $"recipe {graph.RecipeHash}; initial step '{graph.InitialStep}'\n");
        foreach (string stepName in graph.Recipe.Steps.Select(static s => s.Name))
        {
            SfcSequencePosition? position = graph.SequenceOf(stepName);
            string where = position is { } p ? $" (sequence {p.Sequence + 1}, position {p.Index + 1} of divergence '{p.Divergence.Name}')" : string.Empty;
            text.Append(CultureInfo.InvariantCulture, $"step '{stepName}'{where}\n");
            foreach (SfcActionBlock action in graph.ActionsOf(stepName))
            {
                string attempts = action.Interaction == SfcNames.PushCommand ? $", max attempts {(action.MaxAttempts?.ToString(CultureInfo.InvariantCulture) ?? "host default")}" : string.Empty;
                text.Append(CultureInfo.InvariantCulture, $"  action '{action.Name}': {action.Interaction} {action.TypeId}, timeout {action.TimeoutSeconds}s{attempts}\n");
                if (action.Hold is { } hold && action.Restart is { } restart)
                {
                    text.Append(CultureInfo.InvariantCulture, $"    on pause: hold {hold.TypeId} (timeout {hold.TimeoutSeconds}s), on resume: restart {restart.TypeId} (timeout {restart.TimeoutSeconds}s)\n");
                }
            }

            AppendTransitions(text, graph.Outgoing(stepName));
        }

        foreach (SfcDivergence divergence in graph.Recipe.Divergences ?? [])
        {
            text.Append(CultureInfo.InvariantCulture, $"divergence '{divergence.Name}' ({divergence.Kind})\n");
            for (int i = 0; i < divergence.Sequences.Count; i++)
            {
                text.Append(CultureInfo.InvariantCulture, $"  sequence {i + 1}: {string.Join(" -> ", divergence.Sequences[i])}\n");
            }

            text.Append("  entries:\n");
            AppendTransitions(text, graph.SelectionTransitions(divergence));
            text.Append("  exits:\n");
            AppendTransitions(text, graph.ConvergenceTransitions(divergence));
        }

        return text.ToString();
    }

    /// <summary>Describes where a job is and what it waits for.</summary>
    /// <param name="graph">The graph of the job's recipe.</param>
    /// <param name="state">The job state.</param>
    /// <returns>The description.</returns>
    public static string Describe(SfcGraph graph, SfcState state)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(state);
        var text = new StringBuilder();
        text.Append(CultureInfo.InvariantCulture, $"job '{state.JobId}' in scope '{state.Scope}': {state.Status}");
        if (!string.Equals(state.RecipeHash, graph.RecipeHash, StringComparison.Ordinal))
        {
            text.Append("\nWARNING: the state is pinned to a different recipe than the one given.");
        }

        if (state.Current is not null)
        {
            text.Append(CultureInfo.InvariantCulture, $", at '{state.Current}', active steps [{string.Join(", ", state.ActiveSteps)}]");
        }

        text.Append('\n');
        if (state.FaultCode is not null)
        {
            text.Append(CultureInfo.InvariantCulture, $"fault {state.FaultCode}: {state.FaultMessage}\n");
        }

        foreach (SfcActionState action in state.Actions.Values)
        {
            text.Append(CultureInfo.InvariantCulture, $"  action '{action.Name}' ({action.TypeId}): {action.Status}");
            text.Append(CultureInfo.InvariantCulture, $", attempt {action.Attempt}");
            if (action.Deadline is long deadline && action.IsOpen)
            {
                text.Append(CultureInfo.InvariantCulture, $", deadline {deadline}");
            }

            if (action.Outcome is not null)
            {
                text.Append(CultureInfo.InvariantCulture, $", outcome '{action.Outcome}'");
            }

            text.Append(action.Hold switch
            {
                SfcHoldStatus.Holding => string.Create(CultureInfo.InvariantCulture, $", hold sent (answer due {action.HoldDeadline})"),
                SfcHoldStatus.Held => ", held by the equipment (restarted on release)",
                SfcHoldStatus.NotHeld => ", could not be held (runs to its end)",
                _ => action.Restarted ? ", restarted" : string.Empty,
            });

            text.Append('\n');
        }

        if (state.IsHeld)
        {
            text.Append(CultureInfo.InvariantCulture, $"held since {state.HeldAt} (MES Pause): no transition fires and nothing but hold commands is dispatched; deadlines move by the length of the hold on release\n");
        }
        else
        {
            text.Append(state.NextDeadline is long next
                ? string.Create(CultureInfo.InvariantCulture, $"next deadline {next} (the sweep wakes the job then)\n")
                : "no open deadline\n");
        }
        foreach (string step in state.ActiveSteps)
        {
            string waiting = string.Join(", ", graph.ActionsOf(step).Where(a => state.Actions.TryGetValue(a.Name, out SfcActionState? s) && s.IsOpen).Select(static a => $"'{a.Name}'"));
            if (waiting.Length > 0)
            {
                text.Append(CultureInfo.InvariantCulture, $"step '{step}' waits for {waiting}\n");
            }
        }

        return text.ToString();
    }

    private static void AppendTransitions(StringBuilder text, IReadOnlyList<SfcTransition> transitions)
    {
        foreach (SfcTransition t in transitions)
        {
            text.Append(CultureInfo.InvariantCulture, $"  -> '{t.Target}' when {(t.Condition == SfcOutcome.Default ? "default" : $"outcome '{t.Condition}'")} (priority {t.Priority})\n");
        }
    }
}

namespace MicroDCS.Sfc;

/// <summary>Build-time validation of a recipe (D-44). Messages name steps, actions and types, never parameter values.</summary>
internal static class SfcGraphValidator
{
    internal static List<SfcBuildError> Validate(SfcRecipe recipe, SfcCapabilities caps, out string? initialStep)
    {
        var errors = new List<SfcBuildError>();
        initialStep = null;
        IReadOnlyList<SfcDivergence> divergences = recipe.Divergences ?? [];

        // Names. Later phases index by name, so duplicates end the validation here.
        Duplicates(recipe.Steps.Select(static s => s.Name), "step", errors);
        Duplicates(divergences.Select(static b => b.Name), "divergence", errors);
        Duplicates(recipe.Actions.Select(static a => a.Name), "action", errors);
        foreach (string name in divergences.Select(static b => b.Name).Intersect(recipe.Steps.Select(static s => s.Name), StringComparer.Ordinal))
        {
            errors.Add(new("SFC002", $"'{name}' is both a step and a divergence."));
        }

        List<SfcStep> initial = [.. recipe.Steps.Where(static s => s.Initial)];
        if (initial.Count != 1)
        {
            errors.Add(new("SFC001", initial.Count == 0 ? "The recipe has no initial step." : $"The recipe has {initial.Count} initial steps; exactly one is required."));
        }

        if (errors.Count > 0)
        {
            return errors;
        }

        var steps = recipe.Steps.Select(static s => s.Name).ToHashSet(StringComparer.Ordinal);
        var divergenceNames = divergences.Select(static b => b.Name).ToHashSet(StringComparer.Ordinal);
        var sequenceSteps = new Dictionary<string, SfcDivergence>(StringComparer.Ordinal);

        foreach (SfcActionBlock action in recipe.Actions.Where(a => !steps.Contains(a.Step)))
        {
            errors.Add(new("SFC003", $"Action '{action.Name}' belongs to unknown step '{action.Step}'."));
        }

        foreach (SfcDivergence divergence in divergences)
        {
            ValidateBranch(divergence, steps, sequenceSteps, errors);
        }

        foreach (SfcTransition t in recipe.Transitions)
        {
            ValidateTransition(t, steps, divergenceNames, sequenceSteps, errors);
        }

        if (sequenceSteps.ContainsKey(initial[0].Name))
        {
            errors.Add(new("SFC006", $"The initial step '{initial[0].Name}' must not be inside a divergence sequence."));
        }

        if (errors.Count > 0)
        {
            return errors;
        }

        ValidateActions(recipe, caps, errors);
        ValidateDecisions(recipe, divergences, caps, errors);
        ValidateReachability(recipe, divergences, initial[0].Name, errors);
        ValidateCycles(recipe, divergences, errors);
        ValidateSelectors(recipe, divergences, errors);
        initialStep = initial[0].Name;
        return errors;
    }

    private static void Duplicates(IEnumerable<string> names, string what, List<SfcBuildError> errors)
    {
        foreach (string name in names.GroupBy(static n => n, StringComparer.Ordinal).Where(static g => g.Count() > 1).Select(static g => g.Key))
        {
            errors.Add(new("SFC002", $"Duplicate {what} name '{name}'."));
        }
    }

    private static void ValidateBranch(SfcDivergence divergence, HashSet<string> steps, Dictionary<string, SfcDivergence> sequenceSteps, List<SfcBuildError> errors)
    {
        if (divergence.Kind is not (SfcNames.Selection or SfcNames.Simultaneous))
        {
            errors.Add(new("SFC004", $"Divergence '{divergence.Name}' has the unknown type '{divergence.Kind}'."));
        }

        if (divergence.Sequences.Count == 0 || divergence.Sequences.Any(static p => p.Count == 0))
        {
            errors.Add(new("SFC004", $"Divergence '{divergence.Name}' has no sequences or an empty sequence."));
        }

        foreach (string step in divergence.Sequences.SelectMany(static p => p))
        {
            if (!steps.Contains(step))
            {
                errors.Add(new("SFC003", $"Divergence '{divergence.Name}' names unknown step '{step}'."));
            }
            else if (!sequenceSteps.TryAdd(step, divergence))
            {
                errors.Add(new("SFC005", $"Step '{step}' is in more than one divergence sequence."));
            }
        }
    }

    private static void ValidateTransition(SfcTransition t, HashSet<string> steps, HashSet<string> divergenceNames, Dictionary<string, SfcDivergence> sequenceSteps, List<SfcBuildError> errors)
    {
        bool sourceKnown = steps.Contains(t.Source) || divergenceNames.Contains(t.Source);
        bool targetKnown = steps.Contains(t.Target) || divergenceNames.Contains(t.Target);
        if (!sourceKnown || !targetKnown)
        {
            errors.Add(new("SFC003", $"Transition '{t.Source}' -> '{t.Target}' names an unknown step or divergence."));
            return;
        }

        if (sequenceSteps.ContainsKey(t.Source))
        {
            errors.Add(new("SFC006", $"Step '{t.Source}' is inside a divergence sequence and cannot have transitions; the order of the sequence is its flow."));
        }

        if (sequenceSteps.TryGetValue(t.Target, out SfcDivergence? owner)
            && !(string.Equals(owner.Name, t.Source, StringComparison.Ordinal) && SfcGraph.IsFirstStep(owner, t.Target)))
        {
            errors.Add(new("SFC006", $"Transition '{t.Source}' -> '{t.Target}' enters the middle of divergence '{owner.Name}'; only the divergence itself may lead to the first step of a sequence."));
        }
    }

    private static void ValidateActions(SfcRecipe recipe, SfcCapabilities caps, List<SfcBuildError> errors)
    {
        foreach (SfcActionBlock a in recipe.Actions)
        {
            if (!SfcActionQualifiers.TryParse(a.Qualifier, out SfcActionQualifier qualifier))
            {
                errors.Add(new("SFC010", $"Action '{a.Name}' has the unknown qualifier '{a.Qualifier}'; IEC 61131-3 defines {string.Join(", ", SfcActionQualifiers.Codes)}."));
            }
            else if (!SfcCapabilities.Supports(qualifier))
            {
                errors.Add(new("SFC010", $"Action '{a.Name}' uses qualifier '{a.Qualifier}' ({qualifier}); only 'N' ({SfcActionQualifier.NonStored}) is supported."));
            }

            if (a.Interaction == SfcNames.PushCommand)
            {
                RequireFeature(caps, SfcFeature.PushCommand, $"Action '{a.Name}' is a push command, which this station does not support.", errors);
            }
            else if (a.Interaction == SfcNames.PullEvent)
            {
                RequireFeature(caps, SfcFeature.PullEvent, $"Action '{a.Name}' is a pull event, which this station does not support.", errors);
            }
            else
            {
                errors.Add(new("SFC009", $"Action '{a.Name}' has the unknown interaction '{a.Interaction}'."));
            }

            if (a.TimeoutSeconds <= 0)
            {
                errors.Add(new("SFC009", $"Action '{a.Name}' needs TimeoutSeconds above zero."));
            }

            if (a.MaxAttempts is < 1)
            {
                errors.Add(new("SFC009", $"Action '{a.Name}' needs MaxAttempts of at least 1."));
            }

            if (!caps.Outcomes.ContainsKey(a.TypeId))
            {
                errors.Add(new("SFC014", $"No processor of this station declares the type '{a.TypeId}' (action '{a.Name}')."));
            }
        }

        foreach (SfcDivergence b in recipe.Divergences ?? [])
        {
            RequireFeature(
                caps,
                b.Kind == SfcNames.Selection ? SfcFeature.SelectionDivergence : SfcFeature.SimultaneousDivergence,
                $"Divergence '{b.Name}' is {b.Kind}, which this station does not support.",
                errors);
        }
    }

    private static void RequireFeature(SfcCapabilities caps, SfcFeature feature, string message, List<SfcBuildError> errors)
    {
        if ((caps.Features & feature) == 0)
        {
            errors.Add(new("SFC015", message));
        }
    }

    /// <summary>Priorities, defaults and outcome names of every decision point: a step, the exits of a divergence, the entries of a selection.</summary>
    private static void ValidateDecisions(SfcRecipe recipe, IReadOnlyList<SfcDivergence> divergences, SfcCapabilities caps, List<SfcBuildError> errors)
    {
        var divergenceByName = divergences.ToDictionary(static b => b.Name, StringComparer.Ordinal);
        var topLevel = recipe.Steps.Select(static s => s.Name).Concat(divergences.Select(static b => b.Name)).ToList();
        foreach (string node in topLevel)
        {
            List<SfcTransition> outgoing = [.. recipe.Transitions.Where(t => t.Source == node)];
            if (divergenceByName.TryGetValue(node, out SfcDivergence? divergence))
            {
                List<SfcTransition> entries = [.. outgoing.Where(t => SfcGraph.IsFirstStep(divergence, t.Target))];
                List<SfcTransition> exits = [.. outgoing.Where(t => !SfcGraph.IsFirstStep(divergence, t.Target))];
                if (divergence.Kind == SfcNames.Selection)
                {
                    CheckDecision($"selection divergence '{node}'", entries, requireDefault: true, errors);
                    foreach (string first in divergence.Sequences.Where(static p => p.Count > 0).Select(static p => p[0]).Where(f => !entries.Any(t => t.Target == f)))
                    {
                        errors.Add(new("SFC007", $"Sequence starting with '{first}' of selection divergence '{node}' has no entry transition and is never taken."));
                    }

                    CheckOutcomes(entries, recipe, divergences, caps, errors, entering: true);
                }

                CheckDecision($"exit of divergence '{node}'", exits, requireDefault: exits.Count > 1, errors);
                CheckOutcomes(exits, recipe, divergences, caps, errors, entering: false);
            }
            else
            {
                CheckDecision($"step '{node}'", outgoing, requireDefault: outgoing.Count > 1, errors);
                CheckOutcomes(outgoing, recipe, divergences, caps, errors, entering: false);
            }
        }
    }

    private static void CheckDecision(string where, List<SfcTransition> transitions, bool requireDefault, List<SfcBuildError> errors)
    {
        if (transitions.Count == 0)
        {
            return;
        }

        if (transitions.Select(static t => t.Priority).Distinct().Count() != transitions.Count)
        {
            errors.Add(new("SFC008", $"The transitions of {where} need unique priorities."));
        }

        if (requireDefault && !transitions.Any(static t => t.Condition == SfcOutcome.Default))
        {
            errors.Add(new("SFC007", $"The transitions of {where} need a default transition with condition '{SfcOutcome.Default}'."));
        }
    }

    private static void CheckOutcomes(List<SfcTransition> transitions, SfcRecipe recipe, IReadOnlyList<SfcDivergence> divergences, SfcCapabilities caps, List<SfcBuildError> errors, bool entering)
    {
        foreach (SfcTransition t in transitions.Where(static t => t.Condition != SfcOutcome.Default))
        {
            HashSet<string> types = entering
                ? [.. recipe.Transitions.Where(p => p.Target == t.Source).SelectMany(p => TypesOf(p.Source, recipe, divergences))]
                : [.. TypesOf(t.Source, recipe, divergences)];
            if (!types.Any(type => caps.IsOutcomeValid(type, t.Condition)))
            {
                errors.Add(new("SFC016", $"Transition '{t.Source}' -> '{t.Target}' names the outcome '{t.Condition}', which no action type involved declares."));
            }
        }
    }

    /// <summary>The action types whose outcomes decide the transitions out of a step or divergence.</summary>
    private static IEnumerable<string> TypesOf(string node, SfcRecipe recipe, IReadOnlyList<SfcDivergence> divergences)
    {
        SfcDivergence? divergence = divergences.FirstOrDefault(b => b.Name == node);
        IEnumerable<string> steps = divergence is null ? [node] : divergence.Sequences.SelectMany(static p => p);
        return recipe.Actions.Where(a => steps.Contains(a.Step)).Select(static a => a.TypeId);
    }

    private static void ValidateReachability(SfcRecipe recipe, IReadOnlyList<SfcDivergence> divergences, string initial, List<SfcBuildError> errors)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal) { initial };
        var queue = new Queue<string>([initial]);
        while (queue.Count > 0)
        {
            string node = queue.Dequeue();
            IEnumerable<string> next = recipe.Transitions.Where(t => t.Source == node).Select(static t => t.Target);
            SfcDivergence? divergence = divergences.FirstOrDefault(b => b.Name == node);
            if (divergence is not null)
            {
                next = next.Concat(divergence.Sequences.SelectMany(static p => p));
            }

            foreach (string target in next.Where(seen.Add))
            {
                queue.Enqueue(target);
            }
        }

        foreach (string name in recipe.Steps.Select(static s => s.Name).Concat(divergences.Select(static b => b.Name)).Where(n => !seen.Contains(n)))
        {
            errors.Add(new("SFC012", $"'{name}' cannot be reached from the initial step."));
        }
    }

    /// <summary>A cycle through steps without actions would spin inside one evolution, so it is refused ("a cycle must wait").</summary>
    private static void ValidateCycles(SfcRecipe recipe, IReadOnlyList<SfcDivergence> divergences, List<SfcBuildError> errors)
    {
        var instant = new HashSet<string>(StringComparer.Ordinal);
        var withActions = recipe.Actions.Select(static a => a.Step).ToHashSet(StringComparer.Ordinal);
        instant.UnionWith(recipe.Steps.Where(s => !withActions.Contains(s.Name)).Select(static s => s.Name));

        instant.UnionWith(divergences.Where(b => b.Sequences.SelectMany(static p => p).All(s => !withActions.Contains(s))).Select(static b => b.Name));

        IEnumerable<string> Edges(string node)
        {
            SfcDivergence? divergence = divergences.FirstOrDefault(b => b.Name == node);
            return recipe.Transitions
                .Where(t => t.Source == node && (divergence is null || !SfcGraph.IsFirstStep(divergence, t.Target)))
                .Select(static t => t.Target)
                .Where(instant.Contains);
        }

        var onPath = new HashSet<string>(StringComparer.Ordinal);
        var finished = new HashSet<string>(StringComparer.Ordinal);
        bool Visit(string node)
        {
            onPath.Add(node);
            foreach (string next in Edges(node))
            {
                if (onPath.Contains(next) || (!finished.Contains(next) && Visit(next)))
                {
                    return true;
                }
            }

            onPath.Remove(node);
            finished.Add(node);
            return false;
        }

        string? cyclic = instant.Order(StringComparer.Ordinal).Where(n => !finished.Contains(n)).FirstOrDefault(Visit);
        if (cyclic is not null)
        {
            errors.Add(new("SFC013", $"A cycle through '{cyclic}' has no action to wait for and would spin."));
        }
    }

    /// <summary>Two pull-event actions of one type that can wait at the same time and carry the same selector cannot be told apart (R-1).</summary>
    private static void ValidateSelectors(SfcRecipe recipe, IReadOnlyList<SfcDivergence> divergences, List<SfcBuildError> errors)
    {
        List<SfcActionBlock> pulls = [.. recipe.Actions.Where(static a => a.Interaction == SfcNames.PullEvent)];
        for (int i = 0; i < pulls.Count; i++)
        {
            for (int j = i + 1; j < pulls.Count; j++)
            {
                SfcActionBlock a = pulls[i], b = pulls[j];
                if (a.TypeId == b.TypeId && SameSelector(a, b) && CanWaitTogether(a.Step, b.Step, divergences))
                {
                    errors.Add(new("SFC011", $"Pull-event actions '{a.Name}' and '{b.Name}' can wait at the same time for type '{a.TypeId}' with the same parameters; an event could not be matched."));
                }
            }
        }
    }

    private static bool SameSelector(SfcActionBlock a, SfcActionBlock b)
    {
        string Key(SfcActionBlock x) => string.Join(
            ';',
            (x.Parameters ?? new Dictionary<string, System.Text.Json.JsonElement>()).OrderBy(static p => p.Key, StringComparer.Ordinal).Select(static p => p.Key + "=" + p.Value.GetRawText()));
        return Key(a) == Key(b);
    }

    private static bool CanWaitTogether(string stepA, string stepB, IReadOnlyList<SfcDivergence> divergences)
    {
        if (stepA == stepB)
        {
            return true;
        }

        return divergences.Any(b => b.Kind == SfcNames.Simultaneous
            && b.Sequences.Select((sequence, index) => (sequence, index)).Any(x => x.sequence.Contains(stepA)
                && b.Sequences.Select((sequence, index) => (sequence, index)).Any(y => y.index != x.index && y.sequence.Contains(stepB))));
    }
}

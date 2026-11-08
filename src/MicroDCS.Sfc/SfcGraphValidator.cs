namespace MicroDCS.Sfc;

/// <summary>Build-time validation of a recipe (D-44). Messages name steps, actions and types, never parameter values.</summary>
internal static class SfcGraphValidator
{
    /// <summary>The largest number of steps, transitions, actions or divergences accepted; bounds the work of a build.</summary>
    internal const int MaxElements = 20_000;

    /// <summary>The number of errors after which a validation phase stops reporting.</summary>
    private const int MaxReportedErrors = 100;

    /// <summary>
    /// Tells whether a collection of the recipe holds a JSON <c>null</c> entry. The serializer checks members but not the elements of
    /// lists, so this is the one place that turns such a document into a build error.
    /// </summary>
    internal static bool ContainsNull(SfcRecipe recipe) =>
        Any(recipe.Steps) || Any(recipe.Transitions) || Any(recipe.Actions)
        || (recipe.Divergences is { } divergences
            && (Any(divergences) || divergences.Any(static d => Any(d.Sequences) || d.Sequences.Any(static s => Any(s)))));

    private static bool Any<T>(IEnumerable<T?> items)
        where T : class => items.Any(static i => i is null);

    internal static List<SfcBuildError> Validate(SfcRecipe recipe, SfcCapabilities caps, out string? initialStep)
    {
        var errors = new List<SfcBuildError>();
        initialStep = null;
        IReadOnlyList<SfcDivergence> divergences = recipe.Divergences ?? [];

        if (recipe.Steps.Count > MaxElements || recipe.Transitions.Count > MaxElements || recipe.Actions.Count > MaxElements || divergences.Count > MaxElements)
        {
            errors.Add(new("SFC018", $"The recipe has more than {MaxElements} steps, transitions, actions or divergences."));
            return errors;
        }

        // Names. Later phases index by name, so duplicates and empty names end the validation here.
        Duplicates(recipe.Steps.Select(static s => s.Name), "step", errors);
        Duplicates(divergences.Select(static b => b.Name), "divergence", errors);
        Duplicates(recipe.Actions.Select(static a => a.Name), "action", errors);
        foreach (string name in divergences.Select(static b => b.Name).Intersect(recipe.Steps.Select(static s => s.Name), StringComparer.Ordinal))
        {
            errors.Add(new("SFC002", $"'{name}' is both a step and a divergence."));
        }

        if (recipe.Steps.Any(static s => s.Name.Length == 0) || divergences.Any(static d => d.Name.Length == 0) || recipe.Actions.Any(static a => a.Name.Length == 0))
        {
            errors.Add(new("SFC017", "Step, divergence and action names must not be empty."));
        }

        foreach (SfcActionBlock action in recipe.Actions.Where(static a => a.Name.Contains(':', StringComparison.Ordinal)))
        {
            errors.Add(new("SFC017", $"Action name '{action.Name}' must not contain ':' (the action key is '{{jobId}}:{{action}}')."));
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
            ValidateDivergence(divergence, steps, sequenceSteps, errors);
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

        var index = new RecipeIndex(recipe, divergences);
        ValidateActions(recipe, caps, errors);
        ValidateDecisions(recipe, index, caps, errors);
        ValidateReachability(recipe, index, initial[0].Name, errors);
        ValidateCycles(recipe, index, errors);
        ValidateSelectors(recipe, index, errors);
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

    private static void ValidateDivergence(SfcDivergence divergence, HashSet<string> steps, Dictionary<string, SfcDivergence> sequenceSteps, List<SfcBuildError> errors)
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
    private static void ValidateDecisions(SfcRecipe recipe, RecipeIndex index, SfcCapabilities caps, List<SfcBuildError> errors)
    {
        foreach (string node in recipe.Steps.Select(static s => s.Name).Concat(index.Divergences.Keys))
        {
            List<SfcTransition> outgoing = [.. index.BySource[node]];
            if (index.Divergences.TryGetValue(node, out SfcDivergence? divergence))
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

                    CheckOutcomes(entries, index, caps, errors, entering: true);
                }

                CheckDecision($"exit of divergence '{node}'", exits, requireDefault: exits.Count > 1, errors);
                CheckOutcomes(exits, index, caps, errors, entering: false);
            }
            else
            {
                CheckDecision($"step '{node}'", outgoing, requireDefault: outgoing.Count > 1, errors);
                CheckOutcomes(outgoing, index, caps, errors, entering: false);
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

    private static void CheckOutcomes(List<SfcTransition> transitions, RecipeIndex index, SfcCapabilities caps, List<SfcBuildError> errors, bool entering)
    {
        foreach (SfcTransition t in transitions.Where(static t => t.Condition != SfcOutcome.Default))
        {
            HashSet<string> types = entering
                ? [.. index.ByTarget[t.Source].SelectMany(p => index.TypesOf(p.Source))]
                : [.. index.TypesOf(t.Source)];
            if (!types.Any(type => caps.IsOutcomeValid(type, t.Condition)))
            {
                errors.Add(new("SFC016", $"Transition '{t.Source}' -> '{t.Target}' names the outcome '{t.Condition}', which no action type involved declares."));
            }
        }
    }

    private static void ValidateReachability(SfcRecipe recipe, RecipeIndex index, string initial, List<SfcBuildError> errors)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal) { initial };
        var queue = new Queue<string>([initial]);
        while (queue.Count > 0)
        {
            string node = queue.Dequeue();
            IEnumerable<string> next = index.BySource[node].Select(static t => t.Target);
            if (index.Divergences.TryGetValue(node, out SfcDivergence? divergence))
            {
                next = next.Concat(divergence.Sequences.SelectMany(static p => p));
            }

            foreach (string target in next.Where(seen.Add))
            {
                queue.Enqueue(target);
            }
        }

        foreach (string name in recipe.Steps.Select(static s => s.Name).Concat(index.Divergences.Keys).Where(n => !seen.Contains(n)).Take(MaxReportedErrors))
        {
            errors.Add(new("SFC012", $"'{name}' cannot be reached from the initial step."));
        }
    }

    /// <summary>
    /// A cycle through steps without actions would spin inside one evolution, so it is refused ("a cycle must wait"). A selection
    /// divergence counts as instant when any of its sequences has no action, because that one can be taken. The search is
    /// iterative: a long chain of steps must not exhaust the stack.
    /// </summary>
    private static void ValidateCycles(SfcRecipe recipe, RecipeIndex index, List<SfcBuildError> errors)
    {
        var withActions = recipe.Actions.Select(static a => a.Step).ToHashSet(StringComparer.Ordinal);
        var instant = new HashSet<string>(StringComparer.Ordinal);
        instant.UnionWith(recipe.Steps.Where(s => !withActions.Contains(s.Name)).Select(static s => s.Name));
        foreach (SfcDivergence d in index.Divergences.Values)
        {
            bool Waits(IEnumerable<string> sequence) => sequence.Any(withActions.Contains);
            if (d.Kind == SfcNames.Selection ? d.Sequences.Any(s => !Waits(s)) : !d.Sequences.Any(Waits))
            {
                instant.Add(d.Name);
            }
        }

        IEnumerable<string> Edges(string node)
        {
            index.Divergences.TryGetValue(node, out SfcDivergence? divergence);
            return index.BySource[node]
                .Where(t => divergence is null || !SfcGraph.IsFirstStep(divergence, t.Target))
                .Select(static t => t.Target)
                .Where(instant.Contains);
        }

        var color = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (string start in instant.Order(StringComparer.Ordinal).Where(n => !color.ContainsKey(n)))
        {
            var stack = new Stack<(string Node, IEnumerator<string> Edges)>();
            color[start] = 1;
            stack.Push((start, Edges(start).GetEnumerator()));
            while (stack.Count > 0)
            {
                (string node, IEnumerator<string> edges) = stack.Peek();
                if (!edges.MoveNext())
                {
                    color[node] = 2;
                    stack.Pop();
                    continue;
                }

                string next = edges.Current;
                int state = color.GetValueOrDefault(next);
                if (state == 1)
                {
                    errors.Add(new("SFC013", $"A cycle through '{next}' has no action to wait for and would spin."));
                    return;
                }

                if (state == 0)
                {
                    color[next] = 1;
                    stack.Push((next, Edges(next).GetEnumerator()));
                }
            }
        }
    }

    /// <summary>
    /// Two pull-event actions of one type that can wait at the same time cannot be told apart by the core (R-1): an event carries
    /// the type only. Matching by parameters (a selector) comes with the host in S20.
    /// </summary>
    private static void ValidateSelectors(SfcRecipe recipe, RecipeIndex index, List<SfcBuildError> errors)
    {
        foreach (IGrouping<string, SfcActionBlock> sameType in recipe.Actions.Where(static a => a.Interaction == SfcNames.PullEvent).GroupBy(static a => a.TypeId, StringComparer.Ordinal))
        {
            List<SfcActionBlock> pulls = [.. sameType];
            for (int i = 0; i < pulls.Count; i++)
            {
                for (int j = i + 1; j < pulls.Count; j++)
                {
                    if (errors.Count >= MaxReportedErrors)
                    {
                        return;
                    }

                    if (index.CanWaitTogether(pulls[i].Step, pulls[j].Step))
                    {
                        errors.Add(new("SFC011", $"Pull-event actions '{pulls[i].Name}' and '{pulls[j].Name}' can wait at the same time for type '{sameType.Key}'; an event could not be matched."));
                    }
                }
            }
        }
    }

    /// <summary>Lookups built once so that no phase scans the whole recipe per node.</summary>
    private sealed class RecipeIndex
    {
        private readonly ILookup<string, SfcActionBlock> _actionsByStep;
        private readonly Dictionary<string, (SfcDivergence Divergence, int Sequence)> _sequenceOf = new(StringComparer.Ordinal);

        internal RecipeIndex(SfcRecipe recipe, IReadOnlyList<SfcDivergence> divergences)
        {
            BySource = recipe.Transitions.ToLookup(static t => t.Source, StringComparer.Ordinal);
            ByTarget = recipe.Transitions.ToLookup(static t => t.Target, StringComparer.Ordinal);
            _actionsByStep = recipe.Actions.ToLookup(static a => a.Step, StringComparer.Ordinal);
            Divergences = divergences.ToDictionary(static d => d.Name, StringComparer.Ordinal);
            foreach (SfcDivergence divergence in divergences)
            {
                for (int s = 0; s < divergence.Sequences.Count; s++)
                {
                    foreach (string step in divergence.Sequences[s])
                    {
                        _sequenceOf[step] = (divergence, s);
                    }
                }
            }
        }

        internal ILookup<string, SfcTransition> BySource { get; }

        internal ILookup<string, SfcTransition> ByTarget { get; }

        internal Dictionary<string, SfcDivergence> Divergences { get; }

        /// <summary>The action types whose outcomes decide the transitions out of a step or divergence.</summary>
        internal IEnumerable<string> TypesOf(string node)
        {
            IEnumerable<string> steps = Divergences.TryGetValue(node, out SfcDivergence? divergence)
                ? divergence.Sequences.SelectMany(static p => p)
                : [node];
            return steps.SelectMany(step => _actionsByStep[step]).Select(static a => a.TypeId);
        }

        internal bool CanWaitTogether(string stepA, string stepB) =>
            stepA == stepB
            || (_sequenceOf.TryGetValue(stepA, out (SfcDivergence Divergence, int Sequence) a)
                && _sequenceOf.TryGetValue(stepB, out (SfcDivergence Divergence, int Sequence) b)
                && a.Divergence == b.Divergence
                && a.Divergence.Kind == SfcNames.Simultaneous
                && a.Sequence != b.Sequence);
    }
}

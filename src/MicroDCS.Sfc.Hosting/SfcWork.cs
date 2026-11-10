using System.Globalization;
using System.Text.Json;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.Valkey.Sfc;

namespace MicroDCS.Sfc.Hosting;

/// <summary>The work stream vocabulary: entry fields and the entries a step of the core produces (D-44).</summary>
internal static class SfcWork
{
    public const string Group = SfcStateDao.ConsumerGroup;
    public const string NudgeKind = "sfc";

    public const string JobIdField = "job_id";
    public const string ActionField = "action";
    public const string ScopeField = "scope";
    public const string CommandField = "cmd";
    public const string OperationsField = "ops";
    public const string ReasonField = "reason";
    public const string OutageField = "outage_ms";
    public const string OnlyField = "only";
    public const string ScanField = "scan";
    public const string TypeIdField = "type_id";
    public const string SubjectField = "subject";
    public const string CorrelationIdField = "correlation_id";
    public const string OutcomeField = "outcome";
    public const string ErrorField = "error";
    public const string EventKeyField = "event_key";

    public const string Start = "start";
    public const string Cancel = "cancel";
    public const string Hold = "hold";
    public const string Release = "release";
    public const string Tick = "tick";
    public const string Resume = "resume";
    public const string Job = "job";
    public const string Event = "event";
    public const string DispatchPrefix = "dispatch:";

    /// <summary>The time of an entry: the millisecond part of its id, so all replicas read one clock and a redelivery keeps its time (D-44).</summary>
    public static long TimeOf(string entryId)
    {
        var dash = entryId.IndexOf('-', StringComparison.Ordinal);
        return long.Parse(dash < 0 ? entryId : entryId[..dash], CultureInfo.InvariantCulture);
    }

    public static KeyValuePair<string, string>[] Entry(string jobId, string scope, string action, params (string Name, string Value)[] extra) =>
    [
        new(JobIdField, jobId),
        new(ActionField, action),
        new(ScopeField, scope),
        .. extra.Select(static e => new KeyValuePair<string, string>(e.Name, e.Value)),
    ];

    /// <summary>The operations a continuation asks of the job order; <paramref name="started"/> adds the start of execution (S-10).</summary>
    public static IReadOnlyList<JobOrderOperation> JobOperations(SfcContinuation continuation, bool started)
    {
        var operations = new List<JobOrderOperation>(2);
        if (started)
        {
            operations.Add(JobOrderOperation.BeginExecution);
        }

        switch (continuation)
        {
            case SfcComplete:
                operations.Add(JobOrderOperation.Complete);
                break;
            case SfcFault:
            case SfcCancelled:
                operations.Add(JobOrderOperation.Abort);
                break;
            default:
                break;
        }

        return operations;
    }

    /// <summary>
    /// The entries a step appends together with its state: the job order transitions first, then one dispatch per push command. A wait is state and a
    /// withdrawn attempt only concerns the instance that sent it, so neither becomes an entry (S-15, D-9).
    /// </summary>
    public static List<SfcWorkEntry> EntriesOf(SfcStepResult result, bool started)
    {
        ArgumentNullException.ThrowIfNull(result);
        var state = result.State;
        var entries = new List<SfcWorkEntry>(result.Commands.Count + 1);
        var operations = JobOperations(result.Continuation, started);
        if (operations.Count > 0)
        {
            var extra = new List<(string, string)> { (OperationsField, string.Join(',', operations)) };
            if (result.Continuation is SfcFault fault)
            {
                extra.Add((ReasonField, fault.Code));
            }

            entries.Add(new SfcWorkEntry(Entry(state.JobId, state.Scope, Job, [.. extra])));
        }

        foreach (var command in result.Commands.OfType<StartWork>().Where(static c => c.Work == SfcWorkKind.PushCommand))
        {
            entries.Add(new SfcWorkEntry(Entry(state.JobId, state.Scope, DispatchPrefix + command.Action, (CommandField, JsonSerializer.Serialize<SfcCommand>(command, SfcJsonContext.Default.SfcCommand)))));
        }

        return entries;
    }
}

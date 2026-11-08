using System.Globalization;
using System.Text;

namespace MicroDCS.Sfc.Simulation;

/// <summary>One evolution of the core, as the simulation saw it.</summary>
/// <param name="Index">The running number, from 1.</param>
/// <param name="At">The virtual time in milliseconds.</param>
/// <param name="Input">A one-line description of the event.</param>
/// <param name="Disposition">What the core did with it.</param>
/// <param name="Reason">The reason of an absorbed or rejected event.</param>
/// <param name="Commands">One line per command.</param>
/// <param name="Continuation">The continuation kind and its data.</param>
/// <param name="Status">The job status after the evolution.</param>
/// <param name="ActiveSteps">The active steps after the evolution.</param>
public sealed record SfcTranscriptEntry(
    int Index,
    long At,
    string Input,
    SfcDisposition Disposition,
    string? Reason,
    IReadOnlyList<string> Commands,
    string Continuation,
    SfcJobStatus Status,
    IReadOnlyList<string> ActiveSteps);

/// <summary>The record of every evolution of a simulation; also the audit trail of a dry run.</summary>
public sealed class SfcTranscript
{
    private readonly List<SfcTranscriptEntry> _entries = [];

    /// <summary>Gets the entries in order.</summary>
    public IReadOnlyList<SfcTranscriptEntry> Entries => _entries;

    internal void Add(long at, SfcEvent input, SfcStepResult result)
    {
        _entries.Add(new SfcTranscriptEntry(
            _entries.Count + 1,
            at,
            Describe(input),
            result.Disposition,
            result.Reason,
            [.. result.Commands.Select(Describe)],
            Describe(result.Continuation),
            result.State.Status,
            result.State.ActiveSteps));
    }

    /// <summary>Renders the transcript as text, one block per evolution.</summary>
    /// <returns>The text.</returns>
    public string ToText()
    {
        var text = new StringBuilder();
        foreach (SfcTranscriptEntry e in _entries)
        {
            text.Append(CultureInfo.InvariantCulture, $"#{e.Index} t={e.At} {e.Input} -> {e.Disposition}");
            if (e.Reason is not null)
            {
                text.Append(CultureInfo.InvariantCulture, $" ({e.Reason})");
            }

            text.Append(CultureInfo.InvariantCulture, $"; {e.Status} [{string.Join(", ", e.ActiveSteps)}]; {e.Continuation}\n");
            foreach (string command in e.Commands)
            {
                text.Append("    ").Append(command).Append('\n');
            }
        }

        return text.ToString();
    }

    internal static string Describe(SfcEvent input) => input switch
    {
        SfcStart => "start",
        SfcActionCompleted c => $"response {c.ActionKey} outcome={c.Outcome}",
        SfcActionFailed f => $"failure {f.ActionKey}",
        SfcEventReceived e => $"event {e.TypeId} outcome={e.Outcome}",
        SfcTick t => t.OutageMs > 0 ? $"tick after-outage={t.OutageMs}ms" : "tick",
        SfcResume => "resume",
        SfcCancel c => $"cancel {c.Reason}".TrimEnd(),
        _ => input.GetType().Name,
    };

    internal static string Describe(SfcCommand command) => command switch
    {
        StartWork s => $"start_work {(s.Work == SfcWorkKind.PushCommand ? "push" : "await")} {s.ActionKey} type={s.TypeId} attempt={s.Attempt} deadline={s.Deadline} id={s.CommandId}",
        CancelWork c => $"cancel_work {c.ActionKey} id={c.CommandId} reason={c.Reason}",
        _ => command.GetType().Name,
    };

    internal static string Describe(SfcContinuation continuation) => continuation switch
    {
        SfcComplete c => $"complete({c.Outcome})",
        SfcFault f => $"fault({f.Code}: {f.Message})",
        SfcCancelled => "cancelled",
        _ => "defer",
    };
}

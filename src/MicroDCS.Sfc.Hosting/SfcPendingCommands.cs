using System.Collections.Concurrent;

namespace MicroDCS.Sfc.Hosting;

/// <summary>A command this instance sent and waits for.</summary>
/// <param name="JobId">The job.</param>
/// <param name="ActionKey">The action key.</param>
/// <param name="Round">The pass through the step.</param>
/// <param name="DeadlineMs">The deadline as Unix milliseconds.</param>
internal sealed record PendingCommand(string JobId, string ActionKey, int Round, long DeadlineMs);

/// <summary>
/// The routing map of the instance that sent a command (D-9): from the command id on a response to the action. Not persisted and never decides
/// anything; a lost entry only means the deadline re-dispatches. An entry stays after a response or a re-dispatch and runs out by time, because it holds
/// the round: a late answer to an earlier pass of a loop has to be told apart from an answer to the current one (S15).
/// </summary>
internal sealed class SfcPendingCommands
{
    // A response in flight at the deadline is still accepted for this long, then the entry is dropped.
    private const long RetainMs = 60_000;

    private readonly ConcurrentDictionary<string, PendingCommand> _pending = new(StringComparer.Ordinal);

    public int Count => _pending.Count;

    public void Add(string commandId, PendingCommand command) => _pending[commandId] = command;

    public bool TryGet(string commandId, out PendingCommand? command) => _pending.TryGetValue(commandId, out command);

    public void Remove(string commandId) => _pending.TryRemove(commandId, out _);

    public void Purge(long nowMs)
    {
        foreach (var (id, command) in _pending)
        {
            if (nowMs > command.DeadlineMs + RetainMs)
            {
                _pending.TryRemove(id, out _);
            }
        }
    }
}

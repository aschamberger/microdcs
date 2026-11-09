using StackExchange.Redis;

namespace MicroDCS.Valkey.Documents;

/// <summary>Job order documents (R-8 to R-11). The document text is produced by the caller's codec.</summary>
public sealed class JobOrderDao : DocumentDao
{
    private const int RemovalScanLength = 1000;

    /// <summary>Initializes a new instance of the <see cref="JobOrderDao"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public JobOrderDao(ValkeyContext context)
        : base(context)
    {
    }

    /// <summary>Stores a job order, adds it to the scope list and appends a change entry, atomically.</summary>
    /// <param name="id">The job order id.</param>
    /// <param name="scope">The scope; fixed for the life of the id.</param>
    /// <param name="document">The serialized document.</param>
    /// <param name="priority">The list score.</param>
    /// <param name="changeType">The change type written to the change stream.</param>
    /// <param name="expectedVersion">The version this write builds on, 0 to create; <see langword="null"/> overwrites unconditionally.</param>
    /// <param name="operation">An id that makes a repeated logical write recognisable (R-4); generated when absent.</param>
    /// <param name="recipeWork">The SFC work action the same step appends for this job (<c>start</c> or <c>cancel</c>); <see langword="null"/> for none.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    public Task<WriteResult> SaveAsync(string id, string scope, string document, double priority, string changeType, long? expectedVersion = null, string? operation = null, string? recipeWork = null, CancellationToken cancellationToken = default)
    {
        RequireText(id, nameof(id));
        RequireText(scope, nameof(scope));
        RequireText(document, nameof(document));
        var op = NewOperation(operation);
        var key = Keys.JobOrder(id);
        RedisKey[] keys = recipeWork is not null
            ? [key, Keys.JobOrderList(scope), Keys.Changes(scope), Keys.Scopes(), Keys.Nudge("changes"), Keys.SfcWork(scope), Keys.Nudge("sfc")]
            : [key, Keys.JobOrderList(scope), Keys.Changes(scope), Keys.Scopes(), Keys.Nudge("changes")];
        var script = recipeWork is not null ? DocumentScripts.PutJobOrderAndWork : DocumentScripts.PutJobOrder;
        var arguments = PutArguments(expectedVersion, op, id, scope, priority, document, changeType, Timestamp(), id, Context.Options.ChangeStreamMaxLength, string.Empty, string.Empty);
        RedisValue[] values = recipeWork is not null ? [.. arguments, recipeWork] : arguments;
        return Context.WriteAsync(key, op, async _ => await Database.ScriptEvaluateAsync(script, keys, values).ConfigureAwait(false), cancellationToken);
    }

    /// <summary>Removes a job order and its list entry and appends a change entry, atomically and guarded by the version (D-34).</summary>
    /// <param name="id">The job order id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="changeType">The change type written to the change stream.</param>
    /// <param name="expectedVersion">The version this removal builds on.</param>
    /// <param name="operation">An id that makes a repeated logical write recognisable (R-4); generated when absent.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result; not written when the version differs.</returns>
    public Task<WriteResult> RemoveAsync(string id, string scope, string changeType, long expectedVersion, string? operation = null, CancellationToken cancellationToken = default)
    {
        RequireText(id, nameof(id));
        RequireText(scope, nameof(scope));
        var op = NewOperation(operation);
        var key = Keys.JobOrder(id);
        return Context.WriteAsync(key, op, async _ => await Database.ScriptEvaluateAsync(
            DocumentScripts.RemoveJobOrder,
            [key, Keys.JobOrderList(scope), Keys.Changes(scope), Keys.Nudge("changes")],
            PutArguments(expectedVersion, op, id, scope, 0, string.Empty, changeType, Timestamp(), id, Context.Options.ChangeStreamMaxLength, string.Empty, string.Empty)).ConfigureAwait(false), cancellationToken);
    }

    /// <summary>Checks whether an operation removed a job order, from the change entry of the removal; the hash is gone by then.</summary>
    /// <param name="id">The job order id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="operation">The operation id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when a recent change entry records it.</returns>
    public async Task<bool> WasRemovedByAsync(string id, string scope, string operation, CancellationToken cancellationToken = default)
    {
        var entries = await Database.StreamRangeAsync(Keys.Changes(scope), "-", "+", RemovalScanLength, Order.Descending).WaitAsync(cancellationToken).ConfigureAwait(false);
        return entries.Any(e => e["job_order_id"] == id && e["op"] == operation);
    }

    /// <summary>Counts the job orders of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The count.</returns>
    public async Task<long> CountAsync(string scope, CancellationToken cancellationToken = default) =>
        await Database.SortedSetLengthAsync(Keys.JobOrderList(scope)).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Reads a job order.</summary>
    /// <param name="id">The id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The document, or <see langword="null"/> when it does not exist.</returns>
    public Task<StoredDocument?> GetAsync(string id, CancellationToken cancellationToken = default)
    {
        RequireText(id, nameof(id));
        return ReadAsync(Database, Keys.JobOrder(id), id, cancellationToken);
    }

    /// <summary>Deletes a job order and its list entry.</summary>
    /// <param name="id">The id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
        RequireText(id, nameof(id));
        var key = Keys.JobOrder(id);
        for (var attempt = 0; attempt < Context.Options.WriteAttempts; attempt++)
        {
            var scope = (string?)await Database.HashGetAsync(key, "scope").WaitAsync(cancellationToken).ConfigureAwait(false);
            if (scope is null)
            {
                return;
            }

            var status = (int)await Database.ScriptEvaluateAsync(DocumentScripts.DeleteJobOrder, [key, Keys.JobOrderList(scope)], [id, scope]).WaitAsync(cancellationToken).ConfigureAwait(false);
            if (status == ValkeyContext.StatusWritten)
            {
                return;
            }
        }

        throw new InvalidOperationException("The job order kept changing during the delete.");
    }

    /// <summary>Removes only the list entry (R-10).</summary>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task RemoveFromListAsync(string id, string scope, CancellationToken cancellationToken = default) =>
        await Database.SortedSetRemoveAsync(Keys.JobOrderList(scope), id).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Lists job order ids of a scope by priority (R-11).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="start">First index.</param>
    /// <param name="count">Number of ids; -1 for all.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The ids.</returns>
    public async Task<IReadOnlyList<string>> ListAsync(string scope, long start = 0, long count = -1, CancellationToken cancellationToken = default)
    {
        var values = await Database.SortedSetRangeByRankAsync(Keys.JobOrderList(scope), start, count < 0 ? -1 : start + count - 1).WaitAsync(cancellationToken).ConfigureAwait(false);
        return [.. values.Select(static v => v.ToString())];
    }
}

namespace MicroDCS.Valkey.Documents;

/// <summary>Job response documents with the by-order and by-state indexes (R-12 to R-15, D-41).</summary>
public sealed class JobResponseDao : DocumentDao
{
    /// <summary>Initializes a new instance of the <see cref="JobResponseDao"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public JobResponseDao(ValkeyContext context)
        : base(context)
    {
    }

    /// <summary>Stores a job response, moves it between the state indexes, sets the by-order index and appends a change entry, atomically.</summary>
    /// <param name="id">The job response id.</param>
    /// <param name="scope">The scope; fixed for the life of the id.</param>
    /// <param name="document">The serialized document.</param>
    /// <param name="startTime">The list score.</param>
    /// <param name="jobOrderId">The job order the response belongs to; empty for none. Fixed for the life of the id.</param>
    /// <param name="state">The normalized state (<see cref="KeySchema.NormalizeState"/>); empty is not indexed (R-13).</param>
    /// <param name="expectedVersion">The version this write builds on, 0 to create; <see langword="null"/> overwrites unconditionally.</param>
    /// <param name="operation">An id that makes a repeated logical write recognisable (R-4); generated when absent.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    public Task<WriteResult> SaveAsync(string id, string scope, string document, double startTime, string jobOrderId, string state, long? expectedVersion = null, string? operation = null, CancellationToken cancellationToken = default)
    {
        RequireText(id, nameof(id));
        RequireText(scope, nameof(scope));
        RequireText(document, nameof(document));
        ArgumentNullException.ThrowIfNull(jobOrderId);
        ArgumentNullException.ThrowIfNull(state);
        var op = NewOperation(operation);
        var key = Keys.JobResponse(id);
        return Context.WriteAsync(key, op, async ct =>
        {
            var previous = (string?)await Database.HashGetAsync(key, "state").WaitAsync(ct).ConfigureAwait(false) ?? string.Empty;
            return await Database.ScriptEvaluateAsync(
                DocumentScripts.PutJobResponse,
                [key, Keys.JobResponseList(scope), Keys.Changes(scope), Keys.Scopes(), Keys.Nudge("changes"), Keys.JobResponseByOrder(jobOrderId), Keys.JobResponseByState(scope, previous), Keys.JobResponseByState(scope, state)],
                PutArguments(expectedVersion, op, id, scope, startTime, document, "ResultUpdate", Timestamp(), jobOrderId, Context.Options.ChangeStreamMaxLength, previous, state)).ConfigureAwait(false);
        }, cancellationToken);
    }

    /// <summary>Reads a job response.</summary>
    /// <param name="id">The id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The document, or <see langword="null"/>.</returns>
    public Task<StoredDocument?> GetAsync(string id, CancellationToken cancellationToken = default)
    {
        RequireText(id, nameof(id));
        return ReadAsync(Database, Keys.JobResponse(id), id, cancellationToken);
    }

    /// <summary>Reads the job response of a job order (R-14).</summary>
    /// <param name="jobOrderId">The job order id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The document, or <see langword="null"/>.</returns>
    public async Task<StoredDocument?> GetByJobOrderAsync(string jobOrderId, CancellationToken cancellationToken = default)
    {
        RequireText(jobOrderId, nameof(jobOrderId));
        var id = (string?)await Database.StringGetAsync(Keys.JobResponseByOrder(jobOrderId)).WaitAsync(cancellationToken).ConfigureAwait(false);
        return id is null ? null : await ReadAsync(Database, Keys.JobResponse(id), id, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Enumerates the ids of the job responses in a normalized state (R-15); an empty state yields nothing.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="state">The normalized state.</param>
    /// <param name="pageSize">The scan page size.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The ids.</returns>
    public async IAsyncEnumerable<string> IdsByStateAsync(string scope, string state, int pageSize = 250, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        RequireText(scope, nameof(scope));
        if (string.IsNullOrEmpty(state))
        {
            yield break;
        }

        await foreach (var value in Database.SetScanAsync(Keys.JobResponseByState(scope, state), pageSize: pageSize).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return value.ToString();
        }
    }

    /// <summary>Deletes a job response with its list entry and index entries.</summary>
    /// <param name="id">The id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
        RequireText(id, nameof(id));
        var key = Keys.JobResponse(id);
        for (var attempt = 0; attempt < Context.Options.WriteAttempts; attempt++)
        {
            var values = await Database.HashGetAsync(key, ["scope", "state", "order"]).WaitAsync(cancellationToken).ConfigureAwait(false);
            if (values[0].IsNull)
            {
                return;
            }

            string scope = values[0].ToString(), state = values[1].ToString(), order = values[2].ToString();
            var status = (int)await Database.ScriptEvaluateAsync(
                DocumentScripts.DeleteJobResponse,
                [key, Keys.JobResponseList(scope), Keys.JobResponseByOrder(order), Keys.JobResponseByState(scope, state)],
                [id, scope, state, order]).WaitAsync(cancellationToken).ConfigureAwait(false);
            if (status == ValkeyContext.StatusWritten)
            {
                return;
            }
        }

        throw new InvalidOperationException("The job response kept changing during the delete.");
    }

    /// <summary>Removes only the list entry (R-10).</summary>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task RemoveFromListAsync(string id, string scope, CancellationToken cancellationToken = default) =>
        await Database.SortedSetRemoveAsync(Keys.JobResponseList(scope), id).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Lists job response ids of a scope by start time (R-11).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="start">First index.</param>
    /// <param name="count">Number of ids; -1 for all.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The ids.</returns>
    public async Task<IReadOnlyList<string>> ListAsync(string scope, long start = 0, long count = -1, CancellationToken cancellationToken = default)
    {
        var values = await Database.SortedSetRangeByRankAsync(Keys.JobResponseList(scope), start, count < 0 ? -1 : start + count - 1).WaitAsync(cancellationToken).ConfigureAwait(false);
        return [.. values.Select(static v => v.ToString())];
    }
}

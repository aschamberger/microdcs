namespace MicroDCS.Valkey.Documents;

/// <summary>Work master documents (R-19).</summary>
public sealed class WorkMasterDao : DocumentDao
{
    /// <summary>Initializes a new instance of the <see cref="WorkMasterDao"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public WorkMasterDao(ValkeyContext context)
        : base(context)
    {
    }

    /// <summary>Stores a work master, adds it to the scope set and registers the scope, atomically.</summary>
    /// <param name="id">The work master id.</param>
    /// <param name="scope">The scope; fixed for the life of the id.</param>
    /// <param name="document">The serialized document.</param>
    /// <param name="expectedVersion">The version this write builds on, 0 to create; <see langword="null"/> overwrites unconditionally.</param>
    /// <param name="operation">An id that makes a repeated logical write recognisable (R-4); generated when absent.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    public Task<WriteResult> SaveAsync(string id, string scope, string document, long? expectedVersion = null, string? operation = null, CancellationToken cancellationToken = default)
    {
        RequireText(id, nameof(id));
        RequireText(scope, nameof(scope));
        RequireText(document, nameof(document));
        var op = NewOperation(operation);
        var key = Keys.WorkMaster(id);
        return Context.WriteAsync(key, op, async _ => await Database.ScriptEvaluateAsync(
            DocumentScripts.PutWorkMaster,
            [key, Keys.WorkMasterList(scope), Keys.Scopes()],
            PutArguments(expectedVersion, op, id, scope, 0, document, string.Empty, string.Empty, string.Empty, 0, string.Empty, string.Empty)).ConfigureAwait(false), cancellationToken);
    }

    /// <summary>Reads a work master.</summary>
    /// <param name="id">The id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The document, or <see langword="null"/>.</returns>
    public Task<StoredDocument?> GetAsync(string id, CancellationToken cancellationToken = default)
    {
        RequireText(id, nameof(id));
        return ReadAsync(Database, Keys.WorkMaster(id), id, cancellationToken);
    }

    /// <summary>Deletes a work master and its set entry.</summary>
    /// <param name="id">The id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
        RequireText(id, nameof(id));
        var key = Keys.WorkMaster(id);
        for (var attempt = 0; attempt < Context.Options.WriteAttempts; attempt++)
        {
            var scope = (string?)await Database.HashGetAsync(key, "scope").WaitAsync(cancellationToken).ConfigureAwait(false);
            if (scope is null)
            {
                return;
            }

            var status = (int)await Database.ScriptEvaluateAsync(DocumentScripts.DeleteWorkMaster, [key, Keys.WorkMasterList(scope)], [id, scope]).WaitAsync(cancellationToken).ConfigureAwait(false);
            if (status == ValkeyContext.StatusWritten)
            {
                return;
            }
        }

        throw new InvalidOperationException("The work master kept changing during the delete.");
    }

    /// <summary>Lists the work master ids of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The ids, sorted.</returns>
    public async Task<IReadOnlyList<string>> ListAsync(string scope, CancellationToken cancellationToken = default)
    {
        var values = await Database.SetMembersAsync(Keys.WorkMasterList(scope)).WaitAsync(cancellationToken).ConfigureAwait(false);
        return [.. values.Select(static v => v.ToString()).Order(StringComparer.Ordinal)];
    }

    /// <summary>Checks whether a work master is in the scope set.</summary>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when it is a member.</returns>
    public async Task<bool> IsMemberAsync(string id, string scope, CancellationToken cancellationToken = default) =>
        await Database.SetContainsAsync(Keys.WorkMasterList(scope), id).WaitAsync(cancellationToken).ConfigureAwait(false);
}

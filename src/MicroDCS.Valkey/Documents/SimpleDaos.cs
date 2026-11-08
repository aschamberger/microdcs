namespace MicroDCS.Valkey.Documents;

/// <summary>The five reference lists of a scope (R-20): one generic set per kind.</summary>
public sealed class ReferenceListDao
{
    /// <summary>The list kinds of the key schema.</summary>
    public static readonly IReadOnlyList<string> Kinds = ["equipment", "materialclass", "personnel", "physicalasset", "materialdefinition"];

    private readonly ValkeyContext _context;

    /// <summary>Initializes a new instance of the <see cref="ReferenceListDao"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public ReferenceListDao(ValkeyContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _context = context;
    }

    /// <summary>Adds an id.</summary>
    /// <param name="kind">One of <see cref="Kinds"/>.</param>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task AddAsync(string kind, string id, string scope, CancellationToken cancellationToken = default) =>
        await _context.Database.SetAddAsync(Key(kind, scope), id).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Removes an id.</summary>
    /// <param name="kind">One of <see cref="Kinds"/>.</param>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task RemoveAsync(string kind, string id, string scope, CancellationToken cancellationToken = default) =>
        await _context.Database.SetRemoveAsync(Key(kind, scope), id).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Checks membership.</summary>
    /// <param name="kind">One of <see cref="Kinds"/>.</param>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when the id is in the list.</returns>
    public async Task<bool> IsMemberAsync(string kind, string id, string scope, CancellationToken cancellationToken = default) =>
        await _context.Database.SetContainsAsync(Key(kind, scope), id).WaitAsync(cancellationToken).ConfigureAwait(false);

    private string Key(string kind, string scope)
    {
        if (!Kinds.Contains(kind, StringComparer.Ordinal))
        {
            throw new ArgumentException($"Unknown reference list kind '{kind}'.", nameof(kind));
        }

        return _context.Keys.Reference(kind, scope);
    }
}

/// <summary>The job acceptance limit per scope (R-21).</summary>
public sealed class JobAcceptanceDao
{
    private readonly ValkeyContext _context;

    /// <summary>Initializes a new instance of the <see cref="JobAcceptanceDao"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public JobAcceptanceDao(ValkeyContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _context = context;
    }

    /// <summary>Sets the limit.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="maxDownloadableJobOrders">The limit.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task SaveAsync(string scope, int maxDownloadableJobOrders, CancellationToken cancellationToken = default) =>
        await _context.Database.StringSetAsync(_context.Keys.JobAcceptance(scope), maxDownloadableJobOrders).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Reads the limit.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The limit, or <see langword="null"/> when none is set.</returns>
    public async Task<int?> GetAsync(string scope, CancellationToken cancellationToken = default)
    {
        var value = await _context.Database.StringGetAsync(_context.Keys.JobAcceptance(scope)).WaitAsync(cancellationToken).ConfigureAwait(false);
        return value.HasValue && value.TryParse(out int limit) ? limit : null;
    }

    /// <summary>Removes the limit.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task DeleteAsync(string scope, CancellationToken cancellationToken = default) =>
        await _context.Database.KeyDeleteAsync(_context.Keys.JobAcceptance(scope)).WaitAsync(cancellationToken).ConfigureAwait(false);
}

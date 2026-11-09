using System.Globalization;
using MicroDCS.Core.Inspection;
using MicroDCS.Core.Transport;

namespace MicroDCS.MachineryJobs.Publishing;

/// <summary>
/// Inspector <c>publisher</c> (D-19): lease, cursor, last sequence and the state topics of a scope. Read-only.
/// Kind: <c>publisher.status</c> (argument <c>scope</c>). The topics are what the publisher keeps; a broker cannot be asked which retained topics exist.
/// </summary>
public sealed class JobPublisherInspector : IInspector
{
    private const int MaxTopicsShown = 200;

    private readonly JobOrderPublisher _publisher;
    private readonly IJobPublisherStore _state;
    private readonly IStateTopicCatalog _catalog;

    /// <summary>Initializes a new instance of the <see cref="JobPublisherInspector"/> class.</summary>
    /// <param name="publisher">The publisher of this instance.</param>
    /// <param name="state">The publisher state.</param>
    /// <param name="catalog">The state topics.</param>
    public JobPublisherInspector(JobOrderPublisher publisher, IJobPublisherStore state, IStateTopicCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(catalog);
        _publisher = publisher;
        _state = state;
        _catalog = catalog;
    }

    /// <inheritdoc />
    public string Name => "publisher";

    /// <inheritdoc />
    public async ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        if (!string.Equals(query.Kind, "publisher.status", StringComparison.Ordinal))
        {
            throw new ArgumentException("Kind must be publisher.status.", nameof(query));
        }

        var scope = query.Argument("scope") ?? throw new ArgumentException("Argument 'scope' is required.", nameof(query));
        var topics = await _catalog.ListAsync(scope, null, cancellationToken).ConfigureAwait(false);
        var fields = new Dictionary<string, InspectionValue>(StringComparer.Ordinal)
        {
            ["active"] = new(_publisher.IsActive ? "true" : "false", Sensitivity.Public),
            ["leaseHolder"] = new(await _state.GetLeaseHolderAsync(cancellationToken).ConfigureAwait(false) ?? string.Empty, Sensitivity.Identifier),
            ["cursor"] = new(await _state.GetCursorAsync(scope, cancellationToken).ConfigureAwait(false) ?? string.Empty, Sensitivity.Public),
            ["lastChange"] = new(await _state.LastChangeIdAsync(scope, cancellationToken).ConfigureAwait(false) ?? string.Empty, Sensitivity.Public),
            ["sequence"] = new((await _state.GetSequenceAsync(scope, cancellationToken).ConfigureAwait(false)).ToString(CultureInfo.InvariantCulture), Sensitivity.Public),
            ["topicCount"] = new(topics.Count.ToString(CultureInfo.InvariantCulture), Sensitivity.Public),
            ["topics"] = new(string.Join(' ', topics.Take(MaxTopicsShown)), Sensitivity.Identifier),
        };
        return new InspectionPage([new InspectionEntry("publisher.status", scope, fields)], null);
    }
}

using System.Collections.Immutable;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Options;

namespace MicroDCS.MachineryJobs.Publishing;

/// <summary>
/// Where the job state topics live: the address of a scope on the transport of the Machinery Jobs binding, and the topic names
/// <c>state-index</c>, <c>order/{id}</c> and <c>result/{id}</c> (D-51).
/// </summary>
public sealed class JobStateTopics : IStateTopicCatalog
{
    /// <summary>The name of the state index topic.</summary>
    public const string StateIndex = "state-index";

    private readonly IJobStore _store;
    private readonly IReadOnlyList<ITransport> _transports;
    private readonly IOptions<MicroDcsOptions> _options;

    /// <summary>Initializes a new instance of the <see cref="JobStateTopics"/> class.</summary>
    /// <param name="store">The job store.</param>
    /// <param name="transports">The transports.</param>
    /// <param name="options">The core options, for the binding.</param>
    public JobStateTopics(IJobStore store, IEnumerable<ITransport> transports, IOptions<MicroDcsOptions> options)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(transports);
        ArgumentNullException.ThrowIfNull(options);
        _store = store;
        _transports = [.. transports];
        _options = options;
    }

    private BindingOptions Binding => _options.Value.Bindings[MachineryJobsProcessor.BindingId];

    /// <summary>The name of the topic that carries a job order.</summary>
    /// <param name="id">The job order id.</param>
    /// <returns>The name.</returns>
    public static string OrderName(string id) => "order/" + id;

    /// <summary>The name of the topic that carries a job result.</summary>
    /// <param name="id">The job order id.</param>
    /// <returns>The name.</returns>
    public static string ResultName(string id) => "result/" + id;

    /// <summary>The port of the transport the binding names. Any transport without state topics ends the start (D-27).</summary>
    /// <returns>The port.</returns>
    /// <exception cref="InvalidOperationException">The transport has no state topics.</exception>
    public IStateTopicPort ResolvePort()
    {
        var name = Binding.Transport;
        return _transports.FirstOrDefault(t => string.Equals(t.Name, name, StringComparison.Ordinal)) is { Profile.StateTopics: true } and IStateTopicPort port
            ? port
            : throw new InvalidOperationException($"The job order publisher needs state topics, and the transport '{name}' of binding '{MachineryJobsProcessor.BindingId}' has none.");
    }

    /// <summary>Builds the address of a scope.</summary>
    /// <param name="scope">The scope, dotted form.</param>
    /// <returns>The address.</returns>
    public LogicalAddress AddressOf(string scope)
    {
        ArgumentException.ThrowIfNullOrEmpty(scope);
        var binding = Binding;
        return new LogicalAddress(binding.Prefix!, [.. scope.Split('.')], binding.Discriminator, Intent.Data);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<string>> ListAsync(string scope, string? id, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(scope);

        // "a/b" would render the same topics as the scope a.b, and a wildcard is no topic at all.
        if (!scope.Split('.').All(LogicalAddress.IsValidSegment))
        {
            return [];
        }

        if (id is not null)
        {
            return LogicalAddress.IsValidSegment(id) ? [OrderName(id), ResultName(id)] : [];
        }

        var ids = await _store.ListOrderIdsAsync(scope, 0, int.MaxValue, cancellationToken).ConfigureAwait(false);
        return [StateIndex, .. ids.Where(LogicalAddress.IsValidSegment).SelectMany(static i => (ImmutableArray<string>)[OrderName(i), ResultName(i)])];
    }

    /// <inheritdoc />
    public async ValueTask DeleteAsync(string scope, string name, CancellationToken cancellationToken) =>
        await ResolvePort().DeleteAsync(AddressOf(scope), name, cancellationToken).ConfigureAwait(false);
}

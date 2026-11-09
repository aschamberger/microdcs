using System.Globalization;
using MicroDCS.Core.Inspection;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.Valkey;
using StackExchange.Redis;

namespace MicroDCS.MachineryJobs;

/// <summary>
/// Inspector <c>jobs</c> (D-19): the job orders of a scope and one order with its response and recent changes. Read-only.
/// Kinds: <c>jobs.orders</c> (argument <c>scope</c>, optional <c>state</c>), <c>jobs.order</c> (argument <c>id</c>).
/// </summary>
public sealed class JobsInspector : IInspector
{
    private const int ChangesScanned = 500;
    private const int OrderPageSize = 200;

    private readonly IJobStore _store;
    private readonly ValkeyContext _context;

    /// <summary>Initializes a new instance of the <see cref="JobsInspector"/> class.</summary>
    /// <param name="store">The store.</param>
    /// <param name="context">The Valkey context, for the change stream.</param>
    public JobsInspector(IJobStore store, ValkeyContext context)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(context);
        _store = store;
        _context = context;
    }

    /// <inheritdoc />
    public string Name => "jobs";

    /// <inheritdoc />
    public async ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var entries = query.Kind switch
        {
            "jobs.orders" => await OrdersAsync(query, cancellationToken).ConfigureAwait(false),
            "jobs.order" => await OrderAsync(query, cancellationToken).ConfigureAwait(false),
            _ => throw new ArgumentException("Kind must be jobs.orders or jobs.order.", nameof(query)),
        };
        return new InspectionPage(entries, null);
    }

    private static string Required(InspectionQuery query, string name) =>
        query.Argument(name) ?? throw new ArgumentException($"Argument '{name}' is required.", nameof(query));

    private static InspectionValue Public(string text) => new(text, Sensitivity.Public);

    private static InspectionValue Identifier(string text) => new(text, Sensitivity.Identifier);

    private static Dictionary<string, InspectionValue> Fields(StoredJobOrder order) => new(StringComparer.Ordinal)
    {
        ["scope"] = Identifier(order.Scope),
        ["state"] = Public(order.State.ToString()),
        ["version"] = Public(order.Version.ToString(CultureInfo.InvariantCulture)),
        ["priority"] = Public(order.Order.Priority.ToString(CultureInfo.InvariantCulture)),
        ["operation"] = Identifier(order.Operation),
    };

    private async ValueTask<IReadOnlyList<InspectionEntry>> OrdersAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        var scope = Required(query, "scope");
        var wanted = query.Argument("state");
        var limit = Math.Max(query.Limit, 1);
        var entries = new List<InspectionEntry>(limit);

        // The state filter runs on the documents, so keep reading pages until the limit is filled or the list ends.
        var start = 0;
        while (entries.Count < limit)
        {
            var ids = await _store.ListOrderIdsAsync(scope, start, OrderPageSize, cancellationToken).ConfigureAwait(false);
            foreach (var id in ids)
            {
                var order = await _store.GetOrderAsync(id, cancellationToken).ConfigureAwait(false);
                if (order is not null && entries.Count < limit && (wanted is null || string.Equals(order.State.ToString(), wanted, StringComparison.OrdinalIgnoreCase)))
                {
                    entries.Add(new InspectionEntry("jobs.orders", id, Fields(order)));
                }
            }

            if (ids.Count < OrderPageSize)
            {
                break;
            }

            start += OrderPageSize;
        }

        return entries;
    }

    private async ValueTask<IReadOnlyList<InspectionEntry>> OrderAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        var id = Required(query, "id");
        var order = await _store.GetOrderAsync(id, cancellationToken).ConfigureAwait(false);
        if (order is null)
        {
            return [];
        }

        var fields = Fields(order);
        var response = await _store.GetResponseByOrderAsync(id, cancellationToken).ConfigureAwait(false);
        fields["responseId"] = Identifier(response?.Response.Id ?? string.Empty);
        fields["responseState"] = Public(response?.Response.State.ToString() ?? nameof(JobOrderState.Initial));
        fields["changes"] = Public(await RecentChangesAsync(id, order.Scope, cancellationToken).ConfigureAwait(false));
        return [new InspectionEntry("jobs.order", id, fields)];
    }

    private async ValueTask<string> RecentChangesAsync(string id, string scope, CancellationToken cancellationToken)
    {
        var entries = await _context.Multiplexer.GetDatabase()
            .StreamRangeAsync(_context.Keys.Changes(scope), "-", "+", ChangesScanned, Order.Descending)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        return string.Join(
            "; ",
            entries.Where(e => e["job_order_id"] == id).Select(static e => $"{e["ts"]} {e["change_type"]}"));
    }
}

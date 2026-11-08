using MicroDCS.Core.Inspection;

namespace MicroDCS.Transport.Nats;

/// <summary>Inspector <c>transport.nats</c> (D-19): connection state, counters, streams and consumers. No credentials and no payloads.</summary>
public sealed class NatsInspector : IInspector
{
    private readonly NatsTransport _transport;

    /// <summary>Initializes a new instance of the <see cref="NatsInspector"/> class.</summary>
    /// <param name="transport">The transport.</param>
    public NatsInspector(NatsTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _transport = transport;
    }

    /// <inheritdoc />
    public string Name => "transport.nats";

    /// <inheritdoc />
    public ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var status = _transport.Status;
        var entries = new List<InspectionEntry>();
        if (InspectionEntries.Wants(query, "nats.status"))
        {
            entries.Add(InspectionEntries.Create(
                "nats.status",
                "nats",
                ("server", status.Url, Sensitivity.Identifier),
                ("clientName", status.ClientName, Sensitivity.Identifier),
                ("connected", status.Connected ? "true" : "false", Sensitivity.Public),
                ("receiving", status.Receiving ? "true" : "false", Sensitivity.Public),
                ("received", InspectionEntries.Number(status.Received), Sensitivity.Public),
                ("acknowledged", InspectionEntries.Number(status.Acknowledged), Sensitivity.Public),
                ("redelivered", InspectionEntries.Number(status.Nacked), Sensitivity.Public),
                ("poisoned", InspectionEntries.Number(status.Poisoned), Sensitivity.Public),
                ("dropped", InspectionEntries.Number(status.Dropped), Sensitivity.Public),
                ("published", InspectionEntries.Number(status.Published), Sensitivity.Public),
                ("reconnects", InspectionEntries.Number(status.Reconnects), Sensitivity.Public)));
        }

        if (InspectionEntries.Wants(query, "nats.consumer"))
        {
            entries.AddRange(status.Consumers
                .Where(c => query.Key is null || c.BindingId == query.Key)
                .Select(static c => InspectionEntries.Create(
                    "nats.consumer",
                    c.BindingId,
                    ("stream", c.Stream, Sensitivity.Identifier),
                    ("consumer", c.Consumer, Sensitivity.Identifier),
                    ("filters", c.Filters, Sensitivity.Identifier),
                    ("kind", c.Kind.ToString(), Sensitivity.Public))));
        }

        return ValueTask.FromResult(new InspectionPage([.. entries.Take(Math.Max(query.Limit, 0))], null));
    }
}

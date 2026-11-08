using MicroDCS.Core.Inspection;

namespace MicroDCS.Transport.Mqtt;

/// <summary>Inspector <c>transport.mqtt</c> (D-19): connection state, counters and the subscriptions. No credentials and no payloads.</summary>
public sealed class MqttInspector : IInspector
{
    private readonly MqttTransport _transport;

    /// <summary>Initializes a new instance of the <see cref="MqttInspector"/> class.</summary>
    /// <param name="transport">The transport.</param>
    public MqttInspector(MqttTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _transport = transport;
    }

    /// <inheritdoc />
    public string Name => "transport.mqtt";

    /// <inheritdoc />
    public ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var status = _transport.Status;
        var entries = new List<InspectionEntry>();
        if (InspectionEntries.Wants(query, "mqtt.status"))
        {
            entries.Add(InspectionEntries.Create(
                "mqtt.status",
                "mqtt",
                ("broker", status.Host + ":" + InspectionEntries.Number(status.Port), Sensitivity.Public),
                ("clientId", status.ClientId, Sensitivity.Identifier),
                ("connected", status.Connected ? "true" : "false", Sensitivity.Public),
                ("receiving", status.Receiving ? "true" : "false", Sensitivity.Public),
                ("received", InspectionEntries.Number(status.Received), Sensitivity.Public),
                ("acknowledged", InspectionEntries.Number(status.Acknowledged), Sensitivity.Public),
                ("dropped", InspectionEntries.Number(status.Dropped), Sensitivity.Public),
                ("published", InspectionEntries.Number(status.Published), Sensitivity.Public),
                ("reconnects", InspectionEntries.Number(status.Reconnects), Sensitivity.Public),
                ("lastDisconnect", status.LastDisconnect, Sensitivity.Public)));
        }

        if (InspectionEntries.Wants(query, "mqtt.subscription"))
        {
            entries.AddRange(status.Subscriptions
                .Where(s => query.Key is null || s.BindingId == query.Key)
                .Select(static s => InspectionEntries.Create(
                    "mqtt.subscription",
                    s.BindingId,
                    ("filter", s.Filter, Sensitivity.Identifier),
                    ("kind", s.Kind.ToString(), Sensitivity.Public))));
        }

        return ValueTask.FromResult(new InspectionPage([.. entries.Take(Math.Max(query.Limit, 0))], null));
    }
}

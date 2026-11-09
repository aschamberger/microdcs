namespace MicroDCS.Core.Transport;

/// <summary>
/// Last-value publishing (A-4, D-10): MQTT retained messages, NATS KV or a last-per-subject stream, Zenoh storage.
/// AMQP 1.0 has no standard equivalent; its transport reports <see cref="DeliveryProfile.StateTopics"/> as false.
/// Publishers (job order state topics) depend on this port, not on a transport class.
/// </summary>
public interface IStateTopicPort
{
    /// <summary>Publishes the current value of a state topic.</summary>
    /// <param name="address">The logical address of the topic.</param>
    /// <param name="name">The topic name below the address, for example <c>order/{id}</c>.</param>
    /// <param name="payload">The encoded value.</param>
    /// <param name="timeToLive">How long the value stays when the publisher disappears.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask PublishAsync(LogicalAddress address, string name, ReadOnlyMemory<byte> payload, TimeSpan timeToLive, CancellationToken cancellationToken);

    /// <summary>Removes a state topic (MQTT: zero-byte retained message; NATS: KV delete/purge).</summary>
    /// <param name="address">The logical address of the topic.</param>
    /// <param name="name">The topic name below the address.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask DeleteAsync(LogicalAddress address, string name, CancellationToken cancellationToken);
}

/// <summary>
/// Knows which state topics a component keeps, for maintenance: a broker cannot be asked which retained topics exist.
/// </summary>
public interface IStateTopicCatalog
{
    /// <summary>Lists the names of the state topics of a scope.</summary>
    /// <param name="scope">The scope, dotted form.</param>
    /// <param name="id">Only the topics of this item; <see langword="null"/> for the whole scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The topic names below the address of the scope.</returns>
    ValueTask<IReadOnlyList<string>> ListAsync(string scope, string? id, CancellationToken cancellationToken);

    /// <summary>Removes one state topic.</summary>
    /// <param name="scope">The scope, dotted form.</param>
    /// <param name="name">The topic name.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask DeleteAsync(string scope, string name, CancellationToken cancellationToken);
}

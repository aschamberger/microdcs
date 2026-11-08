using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Transport;

/// <summary>What a transport guarantees natively (A-5, D-10). The pipeline adapts to it; it does not emulate missing features.</summary>
public enum DeliveryGuarantee
{
    /// <summary>A message can be lost, never redelivered (Zenoh, NATS core, MQTT QoS 0). No dedupe claim.</summary>
    AtMostOnce,

    /// <summary>A message is redelivered until acknowledged (MQTT QoS 1, JetStream, AMQP). The dedupe claim applies.</summary>
    AtLeastOnce,
}

/// <summary>Capabilities of a transport binding, declared once.</summary>
/// <param name="Guarantee">The native delivery guarantee.</param>
/// <param name="NativeReply">The transport has a native reply mechanism (response topic, inbox, reply-to, query/reply).</param>
/// <param name="NativeExpiry">The broker drops expired messages.</param>
/// <param name="StateTopics">The transport supports last-value state topics (A-4).</param>
/// <param name="SharedConsumption">The transport supports competing consumers.</param>
/// <param name="CanRedeliver">The transport can nack a delivery and have the broker redeliver it in the same session. When it cannot, a handler fault ends the process and the session redelivers on reconnect (D-27, D-37).</param>
public sealed record DeliveryProfile(
    DeliveryGuarantee Guarantee,
    bool NativeReply,
    bool NativeExpiry,
    bool StateTopics,
    bool SharedConsumption,
    bool CanRedeliver = false);

/// <summary>What the pipeline tells a transport to do with a received message.</summary>
public enum DeliveryOutcome
{
    /// <summary>Processed, or a duplicate: acknowledge.</summary>
    Ack,

    /// <summary>Permanently unprocessable (undecodable, unknown type, scope rejected): acknowledge so it is not redelivered. The pipeline has logged and counted it.</summary>
    Drop,

    /// <summary>The handler failed. Do not acknowledge; the dedupe lease lapses and the message is redelivered (D-27: the host stops when the transport cannot nack).</summary>
    Fault,
}

/// <summary>A reply address in transport-native form, opaque to the pipeline (A-6). Per instance (D-9).</summary>
/// <param name="Value">The address as the transport renders it.</param>
public readonly record struct ReplyAddress(string Value);

/// <summary>A decoded message handed from a transport to the pipeline (A-1, A-2).</summary>
public sealed record InboundMessage
{
    /// <summary>The binding whose subscription received the message. The pipeline dispatches to this binding's processor only.</summary>
    public required string BindingId { get; init; }

    /// <summary>Whether the message arrived on a request subscription or on the instance's reply subscription.</summary>
    public required InboundKind Kind { get; init; }

    /// <summary>The envelope. Decoding failures never reach the pipeline as messages; the transport reports them with <see cref="DeliveryOutcome.Drop"/>.</summary>
    public required EventEnvelope Envelope { get; init; }

    /// <summary>The address the message arrived on, parsed. <see langword="null"/> when the transport has no address structure.</summary>
    public LogicalAddress? Address { get; init; }

    /// <summary>Where a response should go, if the sender asked for one.</summary>
    public ReplyAddress? ReplyTo { get; init; }

    /// <summary>The transport redelivered this message.</summary>
    public bool Redelivered { get; init; }
}

/// <summary>Kind of subscription a message arrived on.</summary>
public enum InboundKind
{
    /// <summary>A request subscription of a binding.</summary>
    Request,

    /// <summary>The reply subscription of this instance (D-9).</summary>
    Response,
}

/// <summary>A message the pipeline hands to a transport.</summary>
public sealed record OutboundMessage
{
    /// <summary>The binding that publishes.</summary>
    public required string BindingId { get; init; }

    /// <summary>The envelope.</summary>
    public required EventEnvelope Envelope { get; init; }

    /// <summary>The logical address, or <see langword="null"/> for a reply that goes to <see cref="ReplyTo"/>.</summary>
    public LogicalAddress? Address { get; init; }

    /// <summary>Send to this reply address instead of <see cref="Address"/>.</summary>
    public ReplyAddress? SendTo { get; init; }

    /// <summary>The address on which the sender expects a response (this instance's, D-9).</summary>
    public ReplyAddress? ReplyTo { get; init; }

    /// <summary>A hint to keep the message as last value where the transport has a per-message retain flag (MQTT). Other transports ignore it and log a warning; durable last values go through <see cref="IStateTopicPort"/> (D-47).</summary>
    public bool Retain { get; init; }
}

/// <summary>The pipeline side that a transport calls for every received message.</summary>
public interface IInboundHandler
{
    /// <summary>Processes one message completely: claim, dispatch, publish responses, mark done. Returns what to do with the delivery.</summary>
    /// <param name="message">The message.</param>
    /// <param name="cancellationToken">Cancelled only when the grace period ends; a stopping host lets in-flight messages finish.</param>
    /// <returns>The delivery outcome.</returns>
    ValueTask<DeliveryOutcome> HandleAsync(InboundMessage message, CancellationToken cancellationToken);
}

/// <summary>
/// A transport binding. It subscribes, decodes into <see cref="InboundMessage"/>, publishes
/// <see cref="OutboundMessage"/> and owns its connection and reconnect handling. Everything else is the pipeline.
/// </summary>
public interface ITransport
{
    /// <summary>The transport name used in configuration and telemetry, for example <c>mqtt</c>.</summary>
    string Name { get; }

    /// <summary>The native capabilities.</summary>
    DeliveryProfile Profile { get; }

    /// <summary>The address renderer.</summary>
    IAddressRenderer Addresses { get; }

    /// <summary>Whether the transport is connected and can publish. Feeds readiness.</summary>
    bool IsConnected { get; }

    /// <summary>The address on which this instance receives responses to the commands of a binding (D-9), or <see langword="null"/> when the transport has no native reply mechanism.</summary>
    /// <param name="bindingId">The binding.</param>
    /// <param name="instanceId">The instance id.</param>
    /// <returns>The reply address in the transport's own form.</returns>
    ReplyAddress? GetReplyAddress(string bindingId, string instanceId);

    /// <summary>Connects and subscribes. A failure ends the process (D-27).</summary>
    /// <param name="handler">The pipeline.</param>
    /// <param name="cancellationToken">Startup cancellation.</param>
    /// <returns>A task that completes when the transport is ready.</returns>
    ValueTask StartAsync(IInboundHandler handler, CancellationToken cancellationToken);

    /// <summary>Shutdown phase 1: stops receiving (unsubscribe). In-flight messages still complete.</summary>
    /// <param name="cancellationToken">Cancelled when the grace period ends.</param>
    /// <returns>A task.</returns>
    ValueTask StopReceivingAsync(CancellationToken cancellationToken);

    /// <summary>Publishes a message. Waits while the transport reconnects; throws when the transport is stopped.</summary>
    /// <param name="message">The message.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task that completes when the transport accepted the message.</returns>
    ValueTask PublishAsync(OutboundMessage message, CancellationToken cancellationToken);

    /// <summary>Shutdown phase 4: disconnects.</summary>
    /// <param name="cancellationToken">Cancelled when the grace period ends.</param>
    /// <returns>A task.</returns>
    ValueTask StopAsync(CancellationToken cancellationToken);
}

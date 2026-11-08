using System.Collections.Immutable;
using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Authoring;

/// <summary>A reply produced by a handler: the payload plus optional overrides.</summary>
/// <param name="Payload">The payload object; its type has <see cref="CloudEventTypeAttribute"/>.</param>
/// <param name="Options">Overrides, or <see langword="null"/> to reply with defaults (correlation and causation from the request, P-6).</param>
public sealed record Reply(object Payload, PublishOptions? Options = null);

/// <summary>A list of replies of different payload types.</summary>
public sealed class Replies : List<Reply>
{
    /// <summary>Adds a reply with default options.</summary>
    /// <param name="payload">The payload.</param>
    /// <returns>This list.</returns>
    public Replies With(object payload)
    {
        Add(new Reply(payload));
        return this;
    }
}

/// <summary>The kinds of handler a processor can declare.</summary>
public enum HandlerKind
{
    /// <summary><see cref="IncomingAttribute"/>.</summary>
    Incoming,

    /// <summary><see cref="OutgoingAttribute"/>.</summary>
    Outgoing,

    /// <summary><see cref="OnResponseAttribute"/>.</summary>
    Response,

    /// <summary><see cref="OnExpiredAttribute"/>.</summary>
    Expired,

    /// <summary><see cref="FallbackAttribute"/>.</summary>
    Fallback,
}

/// <summary>Invokes a handler. The generator emits one per method; parameter binding is compiled code, not reflection.</summary>
/// <param name="services">The service provider of the processor scope.</param>
/// <param name="context">The event context.</param>
/// <param name="input">The decoded payload, the <see cref="CommandRequest"/> or the raw envelope, by kind.</param>
/// <param name="cancellationToken">Cancellation.</param>
/// <returns>The replies, possibly empty.</returns>
public delegate ValueTask<IReadOnlyList<Reply>> HandlerInvoker(
    IServiceProvider services,
    EventContext context,
    object? input,
    CancellationToken cancellationToken);

/// <summary>A handler of a processor.</summary>
/// <param name="Kind">The kind.</param>
/// <param name="TypeId">The CloudEvent type id the handler is keyed by; <see langword="null"/> for <see cref="HandlerKind.Fallback"/>.</param>
/// <param name="Payload">The payload descriptor for the type id.</param>
/// <param name="Invoke">The invoker.</param>
public sealed record HandlerDescriptor(HandlerKind Kind, string? TypeId, IPayloadDescriptor? Payload, HandlerInvoker Invoke);

/// <summary>Lifecycle hooks (P-14).</summary>
/// <param name="Initialize">Before transports start.</param>
/// <param name="Started">After transports started.</param>
/// <param name="StartedSingleton">Whether <paramref name="Started"/> runs on one replica only.</param>
/// <param name="Shutdown">After transports stopped.</param>
public sealed record LifecycleDescriptor(
    Func<IServiceProvider, CancellationToken, ValueTask>? Initialize,
    Func<IServiceProvider, CancellationToken, ValueTask>? Started,
    bool StartedSingleton,
    Func<IServiceProvider, CancellationToken, ValueTask>? Shutdown);

/// <summary>
/// What the pipeline knows about a processor. Generated from the attributes (S08); explicit registration builds the same
/// object by hand, so the generator is an optimisation of authoring, not a requirement.
/// </summary>
/// <param name="ProcessorType">The processor class.</param>
/// <param name="BindingId">The configuration identifier of the binding section.</param>
/// <param name="Binding">The direction.</param>
/// <param name="SubscribeIntents">Intents to subscribe to.</param>
/// <param name="PublishIntents">Intents the processor may publish.</param>
/// <param name="Handlers">The handlers.</param>
/// <param name="Lifecycle">The lifecycle hooks.</param>
/// <param name="ReplyPayloads">Descriptors of the payload types the handlers return, so replies can be encoded.</param>
public sealed record ProcessorDescriptor(
    Type ProcessorType,
    string BindingId,
    ProcessorBinding Binding,
    ImmutableArray<Intent> SubscribeIntents,
    ImmutableArray<Intent> PublishIntents,
    ImmutableArray<HandlerDescriptor> Handlers,
    LifecycleDescriptor Lifecycle,
    ImmutableArray<IPayloadDescriptor> ReplyPayloads = default);

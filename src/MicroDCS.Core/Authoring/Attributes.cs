using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Authoring;

/// <summary>
/// Marks a payload record and gives it its CloudEvent type id (D-25). The source generator (S08) emits the
/// <see cref="IPayloadDescriptor"/> and the codec registration, including <c>JsonSerializerContext</c> entries.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class CloudEventTypeAttribute : Attribute
{
    /// <summary>Creates the attribute.</summary>
    /// <param name="typeId">The CloudEvent type id.</param>
    public CloudEventTypeAttribute(string typeId) => Id = typeId;

    /// <summary>The CloudEvent type id.</summary>
    public string Id { get; }

    /// <summary>The schema URI written to <c>dataschema</c>.</summary>
    public string? Schema { get; init; }

    /// <summary>How the payload is encoded.</summary>
    public PayloadCodec Codec { get; init; } = PayloadCodec.Json;
}

/// <summary>A payload property that is not part of the data but travels as a CloudEvent extension attribute (E-4) and is restored on the response.</summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
public sealed class CarriedAsAttributeAttribute : Attribute
{
    /// <summary>Creates the attribute.</summary>
    /// <param name="name">The attribute name; lowercase letters, digits and hyphens.</param>
    public CarriedAsAttributeAttribute(string name) => Name = name;

    /// <summary>The attribute name.</summary>
    public string Name { get; }
}

/// <summary>Marks a <c>partial</c> class as a processor. The generator emits its <see cref="ProcessorDescriptor"/> (S08).</summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class ProcessorAttribute : Attribute
{
    /// <summary>Creates the attribute.</summary>
    /// <param name="binding">The binding direction; it defines the default subscribe and publish intents (P-2).</param>
    public ProcessorAttribute(ProcessorBinding binding) => Binding = binding;

    /// <summary>The binding direction.</summary>
    public ProcessorBinding Binding { get; }

    /// <summary>The configuration identifier of the binding section (<c>Bindings:{Id}</c>). Defaults to the class name without the <c>Processor</c> suffix, kebab-cased.</summary>
    public string? Id { get; init; }

    /// <summary>Overrides the subscribe intents; empty keeps the default of the binding.</summary>
    public Intent[] SubscribeIntents { get; init; } = [];

    /// <summary>Overrides the publish intents; empty keeps the default of the binding.</summary>
    public Intent[] PublishIntents { get; init; } = [];
}

/// <summary>
/// A method that handles an incoming event. The payload type is the first parameter. Allowed parameters: the
/// payload, <see cref="EventContext"/>, <see cref="ScopeAttribute"/>/<see cref="SubjectPartAttribute"/> strings, a
/// <see cref="CancellationToken"/> and services. Returns: a payload, <c>IEnumerable</c>/<c>IAsyncEnumerable</c> of payloads,
/// a <see cref="Replies"/>, <c>Task</c>/<c>ValueTask</c> of those, <c>void</c> or <see langword="null"/>.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class IncomingAttribute : Attribute;

/// <summary>
/// A type-id keyed payload factory (P-13). Used where the caller knows only a type id and free-form parameters, in
/// particular for SFC <c>push_command</c> actions. The first parameter is a <see cref="CommandRequest"/>; the method
/// returns the payload. The pipeline sets ids, subject, action key and correlation.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class OutgoingAttribute : Attribute
{
    /// <summary>Creates the attribute.</summary>
    /// <param name="payloadType">The payload type the method produces (must have <see cref="CloudEventTypeAttribute"/>).</param>
    public OutgoingAttribute(Type payloadType) => PayloadType = payloadType;

    /// <summary>The payload type.</summary>
    public Type PayloadType { get; }
}

/// <summary>A method that handles a response to a command this instance sent (P-11). Arrives on the instance's reply address (D-9).</summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class OnResponseAttribute : Attribute;

/// <summary>A method called when a command with a reply address expired without a response (P-12). The first parameter is the original payload. The timer is local to the instance (D-9).</summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class OnExpiredAttribute : Attribute;

/// <summary>A method that receives events no <see cref="IncomingAttribute"/> handler claims (P-10). Takes an <see cref="EventEnvelope"/>.</summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class FallbackAttribute : Attribute;

/// <summary>Called once before transports start (P-14).</summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class OnInitializeAttribute : Attribute;

/// <summary>Called once after transports started. With <see cref="Singleton"/> only one replica runs it (a lease in the Valkey layer).</summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class OnStartedAttribute : Attribute
{
    /// <summary>Run on one replica only.</summary>
    public bool Singleton { get; init; }
}

/// <summary>Called during shutdown after transports stopped and before shared infrastructure closes (P-14).</summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class OnShutdownAttribute : Attribute;

/// <summary>Binds a <see cref="string"/> parameter to the scope of the subject (the part before the first <c>/</c>), P-4.</summary>
[AttributeUsage(AttributeTargets.Parameter, AllowMultiple = false, Inherited = false)]
public sealed class ScopeAttribute : Attribute;

/// <summary>Binds a parameter to a <c>/</c> separated part of the subject, optionally parsed by a static <c>Parse</c> method of the parameter type (P-4).</summary>
[AttributeUsage(AttributeTargets.Parameter, AllowMultiple = false, Inherited = false)]
public sealed class SubjectPartAttribute : Attribute
{
    /// <summary>Creates the attribute.</summary>
    /// <param name="index">Zero based index of the part.</param>
    public SubjectPartAttribute(int index) => Index = index;

    /// <summary>The index of the part.</summary>
    public int Index { get; }
}

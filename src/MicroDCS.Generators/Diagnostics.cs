using Microsoft.CodeAnalysis;

namespace MicroDCS.Generators;

/// <summary>The MDCS diagnostics reported by the generator (D-35).</summary>
internal static class Diagnostics
{
    private const string Category = "MicroDCS";

    public static readonly DiagnosticDescriptor ProcessorNotPartial = Error("MDCS001", "Processor class must be partial", "Processor '{0}' must be declared partial and not nested or generic");

    public static readonly DiagnosticDescriptor NoPayloadParameter = Error("MDCS002", "Handler without payload parameter", "Handler '{0}' must take {1} as its first parameter");

    public static readonly DiagnosticDescriptor DuplicateHandler = Error("MDCS003", "Duplicate handler", "Processor '{0}' has more than one {1} for '{2}'");

    public static readonly DiagnosticDescriptor UnsupportedParameter = Error("MDCS004", "Unsupported parameter", "Parameter '{0}' of '{1}' is not supported: use the payload, EventContext, CancellationToken, [Scope]/[SubjectPart] string or a service");

    public static readonly DiagnosticDescriptor UnsupportedReturn = Error("MDCS005", "Unsupported return type", "Return type of '{0}' is not supported: use a payload class, IEnumerable/IAsyncEnumerable of payloads, Replies, void or Task/ValueTask of these");

    public static readonly DiagnosticDescriptor PayloadWithoutAttribute = Error("MDCS006", "Payload without [CloudEventType]", "Type '{0}' is used as a payload but has no [CloudEventType] attribute");

    public static readonly DiagnosticDescriptor InvalidCarriedAs = Error("MDCS007", "Invalid [CarriedAs]", "Property '{0}': [CarriedAs] needs a string property with a settable accessor, [JsonIgnore], and a lowercase alphanumeric name of at most 20 characters");

    public static readonly DiagnosticDescriptor DuplicateTypeId = Error("MDCS008", "Duplicate type id", "The CloudEvent type id '{0}' is declared by more than one payload type");

    public static readonly DiagnosticDescriptor NotInJsonContext = Error("MDCS009", "Payload not in a JsonSerializerContext", "Payload '{0}' must be listed with [JsonSerializable] in a JsonSerializerContext of this assembly");

    public static readonly DiagnosticDescriptor CodecNotGenerated = Error("MDCS010", "Codec has no generated descriptor", "Payload '{0}' uses a codec without a generated descriptor; handlers need a handwritten descriptor and explicit registration");

    public static readonly DiagnosticDescriptor PayloadNotSupported = Error("MDCS011", "Payload type not supported", "Payload '{0}' must be a non-generic reference type declared in this assembly");

    public static readonly DiagnosticDescriptor InvalidProcessorAttribute = Error("MDCS012", "Invalid [Processor] attribute", "The [Processor] attribute of '{0}' needs a resolvable ProcessorBinding argument");

    private static DiagnosticDescriptor Error(string id, string title, string message) =>
        new(id, title, message, Category, DiagnosticSeverity.Error, isEnabledByDefault: true);
}

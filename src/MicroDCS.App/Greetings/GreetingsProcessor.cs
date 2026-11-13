using System.Text.Json.Serialization;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;
using Microsoft.Extensions.Logging;

namespace MicroDCS.App.Greetings;

/// <summary>Greets someone. The demo payload of the framework (Python parity: <c>models/greetings.py</c>).</summary>
/// <param name="Name">Who is greeted.</param>
[CloudEventType("com.github.aschamberger.microdcs.greetings.hello.v1", Schema = GreetingsSchema.Document + "#/$defs/Hello")]
public sealed record Hello(string Name)
{
    /// <summary>Demonstrates a payload field that travels as a CloudEvent attribute and comes back on the response (E-4).</summary>
    [CarriedAs("hiddenstr")]
    [JsonIgnore]
    public string? HiddenStr { get; init; }
}

/// <summary>Says goodbye. Sent as a command by the southbound side.</summary>
/// <param name="Name">Who is told goodbye.</param>
[CloudEventType("com.github.aschamberger.microdcs.greetings.bye.v1", Schema = GreetingsSchema.Document + "#/$defs/Bye")]
public sealed record Bye(string Name);

/// <summary>The schema document of the greetings payloads, served by the docs site (D-58); <c>docs/schemas/greetings/</c>.</summary>
internal static class GreetingsSchema
{
    /// <summary>The document URL; each payload adds its <c>$defs</c> fragment.</summary>
    public const string Document = "https://aschamberger.github.io/microdcs/schemas/greetings/v1.0.0.json";
}

/// <summary>JSON metadata of the greetings payloads (source generated, AOT).</summary>
[JsonSerializable(typeof(Hello))]
[JsonSerializable(typeof(Bye))]
internal sealed partial class GreetingsJsonContext : JsonSerializerContext;

/// <summary>
/// The demo processor: answers a hello with two hellos, produces bye commands on request and echoes events nobody handles.
/// It is the first end-to-end test of the processor model (S08).
/// </summary>
/// <param name="logger">The logger.</param>
[Processor(ProcessorBinding.Southbound, Id = "greetings")]
public sealed partial class GreetingsProcessor(ILogger<GreetingsProcessor> logger)
{
    private const string RawTypeId = "com.github.aschamberger.microdcs.greetings.raw.v1";

    /// <summary>Replies with the greeting itself (keeping the carried attribute) and a hello to Alice.</summary>
    /// <param name="hello">The request.</param>
    /// <returns>Two replies.</returns>
    [Incoming]
    public IEnumerable<Hello> OnHello(Hello hello)
    {
        logger.HelloReceived();
        yield return hello with { };
        yield return new Hello("Alice");
    }

    /// <summary>The factory for bye commands, used when a recipe pushes this type id (P-13).</summary>
    /// <param name="request">The command request.</param>
    /// <returns>Bye for the scope.</returns>
    [Outgoing(typeof(Bye))]
    public static Bye CreateBye(CommandRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        return new Bye(request.Scope);
    }

    /// <summary>Takes the equipment's answer to a bye command; the SFC action of the command ends with the default outcome (D-44).</summary>
    /// <param name="response">The answer.</param>
    /// <returns>The outcome.</returns>
    [OnResponse]
    public static ActionOutcome OnByeAnswered(Hello response)
    {
        ArgumentNullException.ThrowIfNull(response);
        return ActionOutcome.Done;
    }

    /// <summary>Echoes events without a handler as raw events (P-10).</summary>
    /// <param name="raw">The received event.</param>
    /// <returns>The echo, same data and content type.</returns>
    [Fallback]
    public static EventEnvelope OnUnknown(EventEnvelope raw)
    {
        ArgumentNullException.ThrowIfNull(raw);
        return new EventEnvelope
        {
            Id = string.Empty,
            Source = string.Empty,
            Type = RawTypeId,
            DataSchema = GreetingsSchema.Document + "#/$defs/Raw",
            DataContentType = raw.DataContentType,
            Data = raw.Data,
        };
    }
}

internal static partial class GreetingsLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "Received hello")]
    public static partial void HelloReceived(this ILogger logger);
}

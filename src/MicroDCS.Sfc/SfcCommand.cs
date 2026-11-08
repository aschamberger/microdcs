using System.Text.Json;
using System.Text.Json.Serialization;

namespace MicroDCS.Sfc;

/// <summary>What a started work item is.</summary>
public enum SfcWorkKind
{
    /// <summary>Send a command to the equipment.</summary>
    PushCommand,

    /// <summary>Wait for an incoming event; the wait itself is state, the command is for registration and inspection.</summary>
    AwaitEvent,
}

/// <summary>Inert data returned by the step function; the host applies commands in order and idempotently by action key.</summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "kind")]
[JsonDerivedType(typeof(StartWork), "start_work")]
[JsonDerivedType(typeof(CancelWork), "cancel_work")]
public abstract record SfcCommand
{
    /// <summary>Gets the action key, the work token id (D-44).</summary>
    [JsonPropertyName("actionKey")]
    public required string ActionKey { get; init; }

    /// <summary>Gets the command event id of the attempt.</summary>
    [JsonPropertyName("commandId")]
    public required string CommandId { get; init; }
}

/// <summary>Start one attempt of an action.</summary>
public sealed record StartWork : SfcCommand
{
    /// <summary>Gets the kind of work.</summary>
    [JsonPropertyName("work")]
    public required SfcWorkKind Work { get; init; }

    /// <summary>Gets the action name.</summary>
    [JsonPropertyName("action")]
    public required string Action { get; init; }

    /// <summary>Gets the type id of the command or the awaited event.</summary>
    [JsonPropertyName("typeId")]
    public required string TypeId { get; init; }

    /// <summary>Gets the attempt number, starting at 1.</summary>
    [JsonPropertyName("attempt")]
    public required int Attempt { get; init; }

    /// <summary>Gets the absolute deadline in epoch milliseconds; the host derives the wire expiry from it (D-18).</summary>
    [JsonPropertyName("deadline")]
    public required long Deadline { get; init; }

    /// <summary>Gets the recipe parameters.</summary>
    [JsonPropertyName("parameters")]
    public IReadOnlyDictionary<string, JsonElement>? Parameters { get; init; }
}

/// <summary>Withdraw an attempt: re-dispatch, divergence teardown, fault or cancel.</summary>
public sealed record CancelWork : SfcCommand
{
    /// <summary>Gets the reason: <c>redispatch</c>, <c>teardown</c> or the cancel reason.</summary>
    [JsonPropertyName("reason")]
    public required string Reason { get; init; }
}

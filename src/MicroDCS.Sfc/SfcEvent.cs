using System.Text.Json.Serialization;

namespace MicroDCS.Sfc;

/// <summary>
/// An input of the step function. <see cref="At"/> is the host's time in epoch milliseconds (the stream entry id time, D-44); the
/// core reads no clock.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "kind")]
[JsonDerivedType(typeof(SfcStart), "start")]
[JsonDerivedType(typeof(SfcActionCompleted), "completed")]
[JsonDerivedType(typeof(SfcActionFailed), "failed")]
[JsonDerivedType(typeof(SfcEventReceived), "event")]
[JsonDerivedType(typeof(SfcTick), "tick")]
[JsonDerivedType(typeof(SfcResume), "resume")]
[JsonDerivedType(typeof(SfcCancel), "cancel")]
[JsonDerivedType(typeof(SfcHold), "hold")]
[JsonDerivedType(typeof(SfcRelease), "release")]
public abstract record SfcEvent
{
    /// <summary>Gets the event time in epoch milliseconds.</summary>
    [JsonPropertyName("at")]
    public required long At { get; init; }
}

/// <summary>Starts the job: activates the initial step. A second start is absorbed (idempotent, S-8).</summary>
public sealed record SfcStart : SfcEvent;

/// <summary>A response to a push command. It is matched by action key, never by command id (D-44).</summary>
public sealed record SfcActionCompleted : SfcEvent
{
    /// <summary>Gets the action key <c>{jobId}:{actionName}</c>.</summary>
    [JsonPropertyName("actionKey")]
    public required string ActionKey { get; init; }

    /// <summary>Gets the outcome the response handler reported.</summary>
    [JsonPropertyName("outcome")]
    public required string Outcome { get; init; }

    /// <summary>Gets the type id of the response when known; a contradiction with the open action rejects the response.</summary>
    [JsonPropertyName("typeId")]
    public string? TypeId { get; init; }

    /// <summary>Gets the round of the command this answers (<see cref="StartWork.Round"/>); a response of an earlier pass is absorbed. Absent means: match by action key only.</summary>
    [JsonPropertyName("round")]
    public int? Round { get; init; }
}

/// <summary>The response handler or the equipment reported a failure; the action ends with the reserved outcome <c>error</c>.</summary>
public sealed record SfcActionFailed : SfcEvent
{
    /// <summary>Gets the action key.</summary>
    [JsonPropertyName("actionKey")]
    public required string ActionKey { get; init; }

    /// <summary>Gets the error text; kept as data on the action record.</summary>
    [JsonPropertyName("error")]
    public string? Error { get; init; }

    /// <summary>Gets the round of the command this answers; absent means: match by action key only.</summary>
    [JsonPropertyName("round")]
    public int? Round { get; init; }
}

/// <summary>An incoming event for a pull-event action. The host picks the job (see <see cref="SfcState.IsWaitingFor"/>).</summary>
public sealed record SfcEventReceived : SfcEvent
{
    /// <summary>Gets the event type id.</summary>
    [JsonPropertyName("typeId")]
    public required string TypeId { get; init; }

    /// <summary>Gets the outcome the event carries.</summary>
    [JsonPropertyName("outcome")]
    public required string Outcome { get; init; }

    /// <summary>Gets the event subject, for the selector of the action (R-1).</summary>
    [JsonPropertyName("subject")]
    public string? Subject { get; init; }

    /// <summary>Gets the event correlation id, for the selector of the action (R-1).</summary>
    [JsonPropertyName("correlationId")]
    public string? CorrelationId { get; init; }

    /// <summary>Gets the error text of an <c>error</c> outcome; kept as data on the action record.</summary>
    [JsonPropertyName("error")]
    public string? Error { get; init; }
}

/// <summary>The sweep found the job due. Expires open deadlines; <see cref="OutageMs"/> above zero extends them first (D-18).</summary>
public sealed record SfcTick : SfcEvent
{
    /// <summary>Gets the length of a detected platform outage in milliseconds; open deadlines are moved by this amount.</summary>
    [JsonPropertyName("outageMs")]
    public long OutageMs { get; init; }
}

/// <summary>Recovery: re-emits the work of open actions that the snapshot's live work does not contain.</summary>
public sealed record SfcResume : SfcEvent;

/// <summary>Ends the job on request and tears down open work.</summary>
public sealed record SfcCancel : SfcEvent
{
    /// <summary>Gets the reason, for the cancel commands.</summary>
    [JsonPropertyName("reason")]
    public string? Reason { get; init; }
}

/// <summary>
/// Holds the job at the next step boundary (MES Pause, D-52). While held no transition fires and nothing is dispatched; responses and events for work
/// already out are recorded, and open deadlines do not expire. A second hold is absorbed.
/// </summary>
public sealed record SfcHold : SfcEvent;

/// <summary>
/// Releases a held job (MES Resume, D-52): open deadlines move by the length of the hold, push commands of open attempts are issued again with their command
/// id (the host sends each command id once), and transitions that became possible fire. A release of a job that is not held is absorbed.
/// </summary>
public sealed record SfcRelease : SfcEvent;

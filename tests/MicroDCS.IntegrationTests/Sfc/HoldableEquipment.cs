using System.Text.Json.Serialization;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;

namespace MicroDCS.IntegrationTests.Sfc;

/// <summary>Tells the equipment to hold the work of an action (D-57).</summary>
/// <param name="ActionKey">The action key of the work.</param>
[CloudEventType("com.github.aschamberger.microdcs.test.hold.v1")]
public sealed record HoldWork(string ActionKey);

/// <summary>Tells the equipment to restart held work (D-57).</summary>
/// <param name="ActionKey">The action key of the work.</param>
[CloudEventType("com.github.aschamberger.microdcs.test.restart.v1")]
public sealed record RestartWork(string ActionKey);

/// <summary>The equipment's answer to a hold or restart command.</summary>
/// <param name="Outcome">The outcome name.</param>
[CloudEventType("com.github.aschamberger.microdcs.test.answer.v1")]
public sealed record EquipmentAnswer(string Outcome);

/// <summary>JSON metadata of the test equipment payloads.</summary>
[JsonSerializable(typeof(HoldWork))]
[JsonSerializable(typeof(RestartWork))]
[JsonSerializable(typeof(EquipmentAnswer))]
internal sealed partial class HoldableEquipmentJsonContext : JsonSerializerContext;

/// <summary>Equipment that can hold and restart its work: the hold and restart types of the S27 compose tests.</summary>
[Processor(ProcessorBinding.Southbound, Id = "holdable")]
public sealed partial class HoldableEquipmentProcessor
{
    /// <summary>The binding id.</summary>
    public const string BindingId = "holdable";

    /// <summary>The hold command; its answer is <c>held</c> or <c>not_held</c>.</summary>
    /// <param name="request">The command request.</param>
    /// <returns>The command.</returns>
    [Outgoing(typeof(HoldWork), Outcomes = ["held", "not_held"])]
    public static HoldWork CreateHold(CommandRequest request) => new(request.ActionKey);

    /// <summary>The restart command; its answer completes the action.</summary>
    /// <param name="request">The command request.</param>
    /// <returns>The command.</returns>
    [Outgoing(typeof(RestartWork))]
    public static RestartWork CreateRestart(CommandRequest request) => new(request.ActionKey);

    /// <summary>Gets how many answers this instance handed to the recipe; makes the processor an instance type, as the host creates it.</summary>
    public int Answers { get; private set; }

    /// <summary>Hands the equipment's outcome to the recipe.</summary>
    /// <param name="answer">The answer.</param>
    /// <returns>The outcome.</returns>
    [OnResponse]
    public ActionOutcome OnAnswer(EquipmentAnswer answer)
    {
        Answers++;
        return new(answer.Outcome);
    }
}

using System.Text.Json;
using System.Text.Json.Serialization;

namespace MicroDCS.Sfc;

/// <summary>Source-generated JSON for the recipe, state, events and commands (AOT, explicit wire names).</summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.Unspecified,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    UseStringEnumConverter = true,
    RespectNullableAnnotations = true)]
[JsonSerializable(typeof(SfcRecipe))]
[JsonSerializable(typeof(SfcState))]
[JsonSerializable(typeof(SfcEvent))]
[JsonSerializable(typeof(SfcCommand))]
[JsonSerializable(typeof(SfcStepResult))]
[JsonSerializable(typeof(SfcSnapshot))]
public sealed partial class SfcJsonContext : JsonSerializerContext
{
    /// <summary>Gets the options of the context, for hosts that need a <see cref="JsonSerializerOptions"/>.</summary>
    public static JsonSerializerOptions Compact => Default.Options;
}

using System.Text.Json;
using System.Text.Json.Serialization;

namespace MicroDCS.Mcp;

/// <summary>Source-generated JSON for the tool parameters and results (AOT, no reflection serializer).</summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(int?))]
internal sealed partial class McpJsonContext : JsonSerializerContext
{
    /// <summary>The serializer options handed to the tool registration.</summary>
    internal static JsonSerializerOptions ToolOptions => Default.Options;
}

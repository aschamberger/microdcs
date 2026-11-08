using System.Text.Json;
using System.Text.Json.Serialization;
using MicroDCS.Core.Inspection;

namespace MicroDCS.Inspection;

/// <summary>Compact JSON form of an inspection page: kind, key and plain field texts.</summary>
/// <param name="Kind">The entry kind.</param>
/// <param name="Key">The entry key.</param>
/// <param name="Fields">Field name to text.</param>
public sealed record EntryDto(string Kind, string Key, Dictionary<string, string> Fields);

/// <summary>Compact JSON form of a page.</summary>
/// <param name="Entries">The entries.</param>
/// <param name="Next">The cursor for the next page, or <see langword="null"/>.</param>
public sealed record PageDto(List<EntryDto> Entries, string? Next);

/// <summary>Source-generated JSON for inspection results (AOT).</summary>
[JsonSourceGenerationOptions(WriteIndented = false, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull, PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(PageDto))]
public sealed partial class InspectionJsonContext : JsonSerializerContext
{
    /// <summary>Renders a page as compact JSON.</summary>
    /// <param name="page">The page.</param>
    /// <returns>The JSON text.</returns>
    public static string Render(InspectionPage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        var dto = new PageDto(
            [.. page.Entries.Select(static e => new EntryDto(e.Kind, e.Key, e.Fields.ToDictionary(static f => f.Key, static f => f.Value.Text, StringComparer.Ordinal)))],
            page.NextCursor);
        return JsonSerializer.Serialize(dto, Default.PageDto);
    }
}

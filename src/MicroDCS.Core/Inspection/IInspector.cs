namespace MicroDCS.Core.Inspection;

/// <summary>How sensitive a value is. The redactor drops <see cref="Payload"/> values unless payloads are enabled (D-19).</summary>
public enum Sensitivity
{
    /// <summary>Counts, states, timestamps, sizes, versions.</summary>
    Public,

    /// <summary>Ids, subjects, types, keys, addresses. Shown by default, never secrets.</summary>
    Identifier,

    /// <summary>Message or document content. Redacted by default.</summary>
    Payload,
}

/// <summary>One value of an inspection entry.</summary>
/// <param name="Text">The value as text.</param>
/// <param name="Sensitivity">The classification, set by the inspector at the source.</param>
public readonly record struct InspectionValue(string Text, Sensitivity Sensitivity);

/// <summary>One thing an inspector reports: a connection, a pipeline decision, a stream, a job.</summary>
/// <param name="Kind">What it is, for example <c>pipeline.decision</c>.</param>
/// <param name="Key">Its identity within the kind.</param>
/// <param name="Fields">Field name to value.</param>
public sealed record InspectionEntry(string Kind, string Key, IReadOnlyDictionary<string, InspectionValue> Fields);

/// <summary>A bounded, cursor based query. Results are capped; tools return compact output.</summary>
/// <param name="Kind">Restrict to a kind, or <see langword="null"/> for all.</param>
/// <param name="Key">Restrict to a key, or <see langword="null"/>.</param>
/// <param name="Limit">Maximum entries (hard cap in the host).</param>
/// <param name="Cursor">Continuation from a previous result.</param>
public sealed record InspectionQuery(string? Kind = null, string? Key = null, int Limit = 50, string? Cursor = null);

/// <summary>A page of entries.</summary>
/// <param name="Entries">The entries.</param>
/// <param name="NextCursor">The cursor for the next page, or <see langword="null"/> at the end.</param>
public sealed record InspectionPage(IReadOnlyList<InspectionEntry> Entries, string? NextCursor);

/// <summary>
/// Read-only view of a stateful component (D-19). Each component registers its own inspector. No transport types, no HTTP
/// types; REST and MCP are adapters over this contract. Inspectors must not affect hot paths (fixed-size buffers).
/// </summary>
public interface IInspector
{
    /// <summary>The inspector name, for example <c>pipeline</c>, <c>transport.mqtt</c>.</summary>
    string Name { get; }

    /// <summary>Returns a page of entries.</summary>
    /// <param name="query">The query.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The page.</returns>
    ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken);
}

/// <summary>Applies the redaction policy to a page before an adapter returns it.</summary>
public static class Redaction
{
    /// <summary>Replaces <see cref="Sensitivity.Payload"/> values with a size marker unless payloads are included.</summary>
    /// <param name="page">The page.</param>
    /// <param name="includePayloads">Whether payload values may be shown.</param>
    /// <returns>The redacted page.</returns>
    public static InspectionPage Apply(InspectionPage page, bool includePayloads)
    {
        ArgumentNullException.ThrowIfNull(page);
        if (includePayloads)
        {
            return page;
        }

        var entries = page.Entries.Select(static e => e with
        {
            Fields = e.Fields.ToDictionary(
                static f => f.Key,
                static f => f.Value.Sensitivity == Sensitivity.Payload
                    ? new InspectionValue($"<redacted {f.Value.Text.Length} chars>", Sensitivity.Public)
                    : f.Value),
        }).ToList();
        return page with { Entries = entries };
    }
}

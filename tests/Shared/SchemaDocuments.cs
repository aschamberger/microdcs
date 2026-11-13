using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Schema;

namespace MicroDCS.Tests.Shared;

/// <summary>
/// The schema documents in <c>docs/schemas/</c> that the payloads name in <c>dataschema</c> and the docs site serves (D-58). They are the golden
/// files of the wire format (Q-TEST-1): each test project regenerates the documents of its models and compares them with the checked-in ones.
/// Run the tests with <c>UPDATE_SCHEMAS=1</c> to rewrite them after a model change; a non-additive change needs a new version and a new file.
/// Linked into the test projects that own a document.
/// </summary>
internal static class SchemaDocuments
{
    /// <summary>The URL under which the docs site serves <c>docs/schemas/</c>.</summary>
    public const string BaseUrl = "https://aschamberger.github.io/microdcs/schemas/";

    private static readonly JsonSerializerOptions _indented = new() { WriteIndented = true };

    /// <summary>A path in the repository.</summary>
    /// <param name="relative">The path relative to the repository root.</param>
    /// <returns>The absolute path.</returns>
    public static string RepoFile(string relative)
    {
        var directory = AppContext.BaseDirectory;
        while (directory is not null && !File.Exists(Path.Combine(directory, "MicroDCS.slnx")))
        {
            directory = Path.GetDirectoryName(directory);
        }

        directory.ShouldNotBeNull();
        return Path.Combine(directory, relative);
    }

    /// <summary>Compares the checked-in document with the generated one, or rewrites it with <c>UPDATE_SCHEMAS=1</c>.</summary>
    /// <param name="relative">The document's path relative to the repository root.</param>
    /// <param name="generated">The generated document.</param>
    public static void AssertCheckedIn(string relative, string generated)
    {
        var path = RepoFile(relative);
        if (Environment.GetEnvironmentVariable("UPDATE_SCHEMAS") == "1")
        {
            Directory.CreateDirectory(Path.GetDirectoryName(path)!); // A repository file path always has a directory.
            File.WriteAllText(path, generated);
        }

        File.Exists(path).ShouldBeTrue($"{relative} is missing; run the tests with UPDATE_SCHEMAS=1.");
        File.ReadAllText(path).ReplaceLineEndings("\n").ShouldBe(generated, $"{relative} is out of date; run the tests with UPDATE_SCHEMAS=1.");
    }

    /// <summary>A document with one <c>$defs</c> entry per System.Text.Json payload type.</summary>
    /// <param name="options">The source-generated options of the payloads.</param>
    /// <param name="id">The document URL.</param>
    /// <param name="title">The title.</param>
    /// <param name="definitions">The definition names (the <c>dataschema</c> fragments) and their types; a null type allows any JSON.</param>
    /// <returns>The document text.</returns>
    public static string FromTypes(JsonSerializerOptions options, string id, string title, params (string Name, Type? Type)[] definitions)
    {
        var defs = new JsonObject();
        foreach (var (name, type) in definitions)
        {
            defs[name] = type is null
                ? new JsonObject { ["description"] = "Any JSON value." }
                : JsonSchemaExporter.GetJsonSchemaAsNode(options, type, new JsonSchemaExporterOptions { TreatNullObliviousAsNonNullable = true });
        }

        return Format(new JsonObject
        {
            ["$schema"] = "https://json-schema.org/draft/2020-12/schema",
            ["$id"] = id,
            ["title"] = title,
            ["$defs"] = defs,
        });
    }

    /// <summary>Sets the <c>$id</c> of a generated document and formats it like the others.</summary>
    /// <param name="document">The generated document.</param>
    /// <param name="id">The document URL.</param>
    /// <param name="title">The title.</param>
    /// <returns>The document text.</returns>
    public static string WithId(string document, string id, string title)
    {
        var root = JsonNode.Parse(document).ShouldBeOfType<JsonObject>();
        var result = new JsonObject
        {
            ["$schema"] = "https://json-schema.org/draft/2020-12/schema",
            ["$id"] = id,
            ["title"] = title,
        };
        foreach (var property in root.ToList())
        {
            if (property.Key is "$schema" or "$id" or "title")
            {
                continue;
            }

            root.Remove(property.Key);
            result[property.Key] = property.Value;
        }

        // The OPC UA generator emits the definitions in the order it first meets them, which differs between runs; sorted, the file is stable.
        if (result["$defs"] is JsonObject defs)
        {
            var sorted = new JsonObject();
            foreach (var definition in defs.OrderBy(static d => d.Key, StringComparer.Ordinal).ToList())
            {
                defs.Remove(definition.Key);
                sorted[definition.Key] = definition.Value;
            }

            result["$defs"] = sorted;
        }

        return Format(result);
    }

    private static string Format(JsonObject document) => document.ToJsonString(_indented).ReplaceLineEndings("\n") + "\n";
}

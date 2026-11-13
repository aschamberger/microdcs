using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Schema;
using System.Text.Json.Serialization;
using static MicroDCS.Tests.Shared.SchemaDocuments;

namespace MicroDCS.Sfc.Tests;

/// <summary>
/// The C# model is the source of the JSON Schemas (D-44). The checked-in files in <c>docs/schemas/</c> (served by the docs site, D-58) are regenerated here and compared;
/// run with <c>UPDATE_SCHEMAS=1</c> to rewrite them after a model change (bump the format number for non-additive changes).
/// </summary>
public class SchemaTests
{
    private static readonly JsonSerializerOptions _indented = new(SfcJsonContext.Default.Options) { WriteIndented = true };

    private static string Generate(Type type, string id, string title)
    {
        JsonNode schema = JsonSchemaExporter.GetJsonSchemaAsNode(SfcJsonContext.Default.Options, type, new JsonSchemaExporterOptions { TreatNullObliviousAsNonNullable = true });
        var root = (JsonObject)schema;
        var result = new JsonObject
        {
            ["$schema"] = "https://json-schema.org/draft/2020-12/schema",
            ["$id"] = id,
            ["title"] = title,
        };
        foreach (KeyValuePair<string, JsonNode?> property in root.ToList())
        {
            root.Remove(property.Key);
            result[property.Key] = property.Value;
        }

        return result.ToJsonString(_indented).ReplaceLineEndings("\n") + "\n";
    }

    [Fact]
    public void TheRecipeSchemaIsCurrent() =>
        AssertCheckedIn("docs/schemas/sfc-recipe/v1.1.0.json", Generate(typeof(SfcRecipe), SfcInterpreter.RecipeDataSchema, "SFC Recipe"));

    [Fact]
    public void TheStateSchemaIsCurrent() =>
        AssertCheckedIn("docs/schemas/sfc-state/v1.0.0.json", Generate(typeof(SfcState), "https://aschamberger.github.io/microdcs/schemas/sfc-state/v1.0.0.json", "SFC Execution State"));

    [Fact]
    public void TheFrozenRecipeSchemaOfFormatV10IsServedUnderItsDataSchema()
    {
        // v1.0.0 recipes are still read (D-57); the document is frozen and no longer generated.
        using var document = JsonDocument.Parse(File.ReadAllText(RepoFile("docs/schemas/sfc-recipe/v1.0.0.json")));
        document.RootElement.GetProperty("$id").GetString().ShouldBe(SfcInterpreter.RecipeDataSchemaV10);
    }

    [Fact]
    public void EveryDocumentPropertyHasAnExplicitWireName()
    {
        var notSerialized = new HashSet<Type> { typeof(SfcBuildError), typeof(SfcBuildResult), typeof(SfcCapabilities), typeof(SfcSequencePosition) };
        Type[] documents = [.. typeof(SfcRecipe).Assembly.GetExportedTypes()
            .Where(static t => t.IsClass && !t.IsAbstract || t.IsAbstract && !t.IsSealed)
            .Where(static t => t.GetMethod("<Clone>$") is not null)
            .Where(t => !notSerialized.Contains(t))];

        documents.ShouldNotBeEmpty();
        foreach (Type type in documents)
        {
            foreach (PropertyInfo property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                         .Where(static p => p.GetCustomAttribute<JsonIgnoreAttribute>() is null && p.Name != "EqualityContract"))
            {
                property.GetCustomAttribute<JsonPropertyNameAttribute>().ShouldNotBeNull($"{type.Name}.{property.Name} needs [JsonPropertyName]");
            }
        }
    }

    [Fact]
    public void ThePolymorphicTypesDeclareAllTheirDerivedTypes()
    {
        foreach (Type root in new[] { typeof(SfcEvent), typeof(SfcCommand), typeof(SfcContinuation) })
        {
            int declared = root.GetCustomAttributes<JsonDerivedTypeAttribute>().Count();
            int actual = typeof(SfcEvent).Assembly.GetExportedTypes().Count(t => t.IsSubclassOf(root));
            declared.ShouldBe(actual, root.Name);
        }
    }

    [Fact]
    public void EventsCommandsAndResultsRoundTripWithDiscriminators()
    {
        SfcEvent input = new SfcActionCompleted { At = 5, ActionKey = "j:a", Outcome = "ok" };

        string json = JsonSerializer.Serialize(input, SfcJsonContext.Default.SfcEvent);
        SfcEvent back = JsonSerializer.Deserialize(json, SfcJsonContext.Default.SfcEvent)!;

        json.ShouldContain("\"kind\":\"completed\"");
        back.ShouldBe(input);
    }
}

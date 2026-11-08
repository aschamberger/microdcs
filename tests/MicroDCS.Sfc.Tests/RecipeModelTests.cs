using System.Text;
using System.Text.Json;
using static MicroDCS.Sfc.Tests.Rec;

namespace MicroDCS.Sfc.Tests;

/// <summary>Ports of <c>test_sfc_recipe.py</c>: the recipe wire format round trips and keeps the Python names.</summary>
public class RecipeModelTests
{
    private const string FullRecipe = """
        {
          "Steps": [
            {"Name": "Init", "Initial": true}, {"Name": "Positioning"}, {"Name": "TightenBolts"},
            {"Name": "QaCheck"}, {"Name": "VerifyTorque"}, {"Name": "Complete"}
          ],
          "Branches": [
            {"Name": "FitAndQa", "Type": "simultaneous", "Branches": [["Positioning", "TightenBolts"], ["QaCheck"]]}
          ],
          "Transitions": [
            {"Source": "Init", "Target": "FitAndQa", "Condition": "true"},
            {"Source": "FitAndQa", "Target": "VerifyTorque", "Condition": "true"},
            {"Source": "VerifyTorque", "Target": "Complete", "Condition": "torque_ok"}
          ],
          "Actions": [
            {"Name": "position_axle", "Step": "Positioning", "Qualifier": "N", "Interaction": "push_command", "TypeId": "com.example.station.position.v1", "TimeoutSeconds": 30},
            {"Name": "tighten", "Step": "TightenBolts", "Qualifier": "N", "Interaction": "push_command", "TypeId": "com.example.station.tighten.v1", "TimeoutSeconds": 60, "MaxAttempts": 2, "Parameters": {"torque_spec": "85Nm"}},
            {"Name": "camera_qa", "Step": "QaCheck", "Qualifier": "N", "Interaction": "pull_event", "TypeId": "com.example.station.qa_result.v1", "TimeoutSeconds": 45},
            {"Name": "verify", "Step": "VerifyTorque", "Qualifier": "N", "Interaction": "pull_event", "TypeId": "com.example.station.torque_result.v1", "TimeoutSeconds": 15},
            {"Name": "finish", "Step": "Complete", "Qualifier": "N", "Interaction": "push_command", "TypeId": "com.example.station.finish.v1", "TimeoutSeconds": 10}
          ]
        }
        """;

    private static SfcRecipe Parse(string json) => JsonSerializer.Deserialize(json, SfcJsonContext.Default.SfcRecipe)!;

    [Fact]
    public void DataSchemaUriMatchesTheSchemaId()
    {
        SfcInterpreter.RecipeDataSchema.ShouldBe("https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/");
    }

    [Fact]
    public void TheFullRecipeParsesWithPascalCaseNames()
    {
        SfcRecipe recipe = Parse(FullRecipe);

        recipe.Steps.Count.ShouldBe(6);
        recipe.Steps[0].Initial.ShouldBeTrue();
        recipe.Steps[1].Initial.ShouldBeFalse();
        recipe.Divergences.ShouldNotBeNull().Single().Kind.ShouldBe(SfcNames.Simultaneous);
        recipe.Divergences[0].Sequences[0].ShouldBe(["Positioning", "TightenBolts"]);
        recipe.Transitions[2].Condition.ShouldBe("torque_ok");
        recipe.Transitions[0].Priority.ShouldBe(0);
        SfcActionBlock tighten = recipe.Actions[1];
        tighten.Qualifier.ShouldBe("N");
        tighten.Interaction.ShouldBe(SfcNames.PushCommand);
        tighten.MaxAttempts.ShouldBe(2);
        tighten.Parameters!["torque_spec"].GetString().ShouldBe("85Nm");
    }

    [Fact]
    public void TheFullRecipeBuildsWithItsDeclaredOutcomes()
    {
        SfcRecipe recipe = Parse(FullRecipe);

        SfcBuildResult result = SfcGraph.Build(Encoding.UTF8.GetBytes(FullRecipe), Caps(recipe, "torque_ok"));

        result.Errors.ShouldBeEmpty();
        result.Graph!.FindDivergence("FitAndQa").ShouldNotBeNull();
    }

    [Fact]
    public void ARecipeSurvivesASerializationRoundTrip()
    {
        SfcRecipe recipe = Parse(FullRecipe);

        string json = JsonSerializer.Serialize(recipe, SfcJsonContext.Default.SfcRecipe);
        SfcRecipe again = Parse(json);

        again.Steps.Select(static s => s.Name).ShouldBe(recipe.Steps.Select(static s => s.Name));
        again.Actions.Select(static a => a.TypeId).ShouldBe(recipe.Actions.Select(static a => a.TypeId));
        json.ShouldContain("\"TimeoutSeconds\":30");
        json.ShouldContain("\"TypeId\"");
    }

    [Fact]
    public void OptionalPartsMayBeLeftOut()
    {
        SfcRecipe recipe = Parse("""{"Steps":[{"Name":"S1","Initial":true}],"Transitions":[],"Actions":[{"Name":"act","Step":"S1","Qualifier":"N","Interaction":"pull_event","TypeId":"e","TimeoutSeconds":10}]}""");

        recipe.Divergences.ShouldBeNull();
        recipe.Actions[0].Parameters.ShouldBeNull();
        recipe.Actions[0].MaxAttempts.ShouldBeNull();
        JsonSerializer.Serialize(recipe, SfcJsonContext.Default.SfcRecipe).ShouldNotContain("Branches");
    }

    [Fact]
    public void RequiredFieldsAreEnforced()
    {
        Should.Throw<JsonException>(() => Parse("""{"Steps":[{"Initial":true}],"Transitions":[],"Actions":[]}"""));
    }

    [Fact]
    public void EnumeratedValuesAreTheNamesOfTheSchema()
    {
        SfcNames.PushCommand.ShouldBe("push_command");
        SfcNames.PullEvent.ShouldBe("pull_event");
        SfcNames.Selection.ShouldBe("selection");
        SfcNames.Simultaneous.ShouldBe("simultaneous");
        SfcCapabilities.AcceptedQualifier.ShouldBe("N");
    }
}

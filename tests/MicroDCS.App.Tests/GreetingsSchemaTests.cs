using MicroDCS.App.Greetings;
using static MicroDCS.Tests.Shared.SchemaDocuments;

namespace MicroDCS.App.Tests;

/// <summary>The greetings schema document that the payloads name in <c>dataschema</c> (D-58).</summary>
public sealed class GreetingsSchemaTests
{
    [Fact]
    public void The_greetings_schema_document_is_current() =>
        AssertCheckedIn(
            "docs/schemas/greetings/v1.0.0.json",
            FromTypes(GreetingsJsonContext.Default.Options, GreetingsSchema.Document, "MicroDCS greetings", ("Hello", typeof(Hello)), ("Bye", typeof(Bye)), ("Raw", null)));

    [Fact]
    public void The_document_is_served_under_the_docs_site() =>
        GreetingsSchema.Document.ShouldStartWith(BaseUrl);
}

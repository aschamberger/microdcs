using System.Collections.Immutable;
using MicroDCS.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace MicroDCS.Generators.Tests;

public sealed class DiagnosticTests
{
    private const string Header = """
        using System.Text.Json.Serialization;
        using MicroDCS.Core.Authoring;
        using MicroDCS.Core.Messaging;

        [CloudEventType("a.v1")] public sealed record A(string X);
        [CloudEventType("b.v1")] public sealed record B(string X);
        [JsonSerializable(typeof(A))] [JsonSerializable(typeof(B))] internal sealed partial class Ctx : JsonSerializerContext;
        """;

    private static ImmutableArray<Diagnostic> Run(string source)
    {
        var references = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!.ToString()!
            .Split(Path.PathSeparator)
            .Append(typeof(MicroDCS.Core.Authoring.ProcessorAttribute).Assembly.Location)
            .Select(path => MetadataReference.CreateFromFile(path));
        var compilation = CSharpCompilation.Create(
            "TestAssembly",
            [CSharpSyntaxTree.ParseText(Header + "\n" + source)],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));

        var driver = CSharpGeneratorDriver.Create(new MicroDcsGenerator()).RunGenerators(compilation);
        return driver.GetRunResult().Diagnostics;
    }

    private static string[] Ids(string source) => [.. Run(source).Select(d => d.Id).Order()];

    [Fact]
    public void Valid_processor_has_no_diagnostics() =>
        Ids("[Processor(ProcessorBinding.Southbound)] public partial class P { [Incoming] public A On(A a) => a; }").ShouldBeEmpty();

    [Fact]
    public void Processor_must_be_partial() =>
        Ids("[Processor(ProcessorBinding.Southbound)] public class P { [Incoming] public A On(A a) => a; }").ShouldBe(["MDCS001"]);

    [Fact]
    public void Handler_without_parameters_is_reported() =>
        Ids("[Processor(ProcessorBinding.Southbound)] public partial class P { [Incoming] public void On() { } }").ShouldBe(["MDCS002"]);

    [Fact]
    public void Duplicate_handler_for_a_type_id_is_reported() =>
        Ids("[Processor(ProcessorBinding.Southbound)] public partial class P { [Incoming] public void On(A a) { } [Incoming] public void On2(A a) { } }").ShouldBe(["MDCS003"]);

    [Fact]
    public void Value_type_parameter_is_unsupported() =>
        Ids("[Processor(ProcessorBinding.Southbound)] public partial class P { [Incoming] public void On(A a, int n) { } }").ShouldBe(["MDCS004"]);

    [Fact]
    public void Return_type_that_is_not_a_payload_is_unsupported() =>
        Ids("[Processor(ProcessorBinding.Southbound)] public partial class P { [Incoming] public string On(A a) => \"\"; }").ShouldBe(["MDCS005"]);

    [Fact]
    public void Payload_without_attribute_is_reported() =>
        Ids("public sealed record C(string X); [Processor(ProcessorBinding.Southbound)] public partial class P { [Incoming] public void On(C c) { } }").ShouldBe(["MDCS006"]);

    [Fact]
    public void Carried_property_needs_json_ignore() =>
        Ids("[CloudEventType(\"c.v1\")] public sealed record C(string X) { [CarriedAs(\"hid\")] public string? H { get; init; } } [JsonSerializable(typeof(C))] internal sealed partial class Ctx2 : JsonSerializerContext;").ShouldContain("MDCS007");

    [Fact]
    public void Duplicate_type_id_is_reported() =>
        Ids("[CloudEventType(\"a.v1\")] public sealed record A2(string X);").ShouldContain("MDCS008");

    [Fact]
    public void Payload_missing_from_the_json_context_is_reported() =>
        Ids("[CloudEventType(\"d.v1\")] public sealed record D(string X);").ShouldBe(["MDCS009"]);

    [Fact]
    public void Non_json_codec_has_no_generated_descriptor() =>
        Ids("[CloudEventType(\"e.v1\", Codec = PayloadCodec.Binary)] public sealed record E(string X);").ShouldBe(["MDCS010"]);

    [Fact]
    public void Generic_payload_is_not_supported() =>
        Ids("[CloudEventType(\"g.v1\")] public sealed record G<T>(T X);").ShouldBe(["MDCS011"]);
}

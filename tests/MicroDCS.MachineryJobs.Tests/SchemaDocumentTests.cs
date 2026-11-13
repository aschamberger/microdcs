using System.Reflection;
using System.Text.Json;
using MicroDCS.Core.Authoring;
using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Extension;
using static MicroDCS.Tests.Shared.SchemaDocuments;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.MachineryJobs.Tests;

/// <summary>The schema documents of the Machinery Jobs payloads (D-58): generated from the models, served by the docs site, and complete.</summary>
public sealed class SchemaDocumentTests
{
    private static string PathOf(string documentUrl)
    {
        documentUrl.ShouldStartWith(BaseUrl);
        return "docs/schemas/" + documentUrl[BaseUrl.Length..];
    }

    [Fact]
    public void The_extension_document_is_current() =>
        AssertCheckedIn(PathOf(JobSchemas.Extension), WithId(JobControlSchemas.GenerateNamespaceJson<JobOrderMethodCallDataType>(), JobSchemas.Extension, "MicroDCS Job Control extension (urn:microdcs:jobcontrol)"));

    [Fact]
    public void The_isa95_v2_document_is_current() =>
        AssertCheckedIn(PathOf(JobSchemas.Isa95V2), WithId(JobControlSchemas.GenerateNamespaceJson<V2.ISA95JobOrderDataType>(), JobSchemas.Isa95V2, "ISA-95 Job Control V2 (http://opcfoundation.org/UA/ISA95-JOBCONTROL_V2/)"));

    [Fact]
    public void The_plain_json_document_is_current() =>
        AssertCheckedIn(
            PathOf(JobSchemas.MachineryJobs),
            FromTypes(MachineryJobsJsonContext.Default.Options, JobSchemas.MachineryJobs, "MicroDCS Machinery Jobs", ("ConfigJobAcceptance", typeof(ConfigJobAcceptance)), ("JobMetadata", typeof(JobMetadata))));

    [Fact]
    public void Every_payload_names_a_definition_that_its_document_has()
    {
        var payloads = typeof(MachineryJobsProcessor).Assembly.GetTypes()
            .Select(static t => (Type: t, Attribute: t.GetCustomAttribute<CloudEventTypeAttribute>()))
            .Where(static p => p.Attribute is not null)
            .ToList();
        payloads.Count.ShouldBeGreaterThan(30);

        foreach (var (type, attribute) in payloads)
        {
            var schema = attribute!.Schema.ShouldNotBeNull(type.Name);
            var parts = schema.Split("#/$defs/");
            parts.Length.ShouldBe(2, $"{type.Name}: {schema}");
            using var document = JsonDocument.Parse(File.ReadAllText(RepoFile(PathOf(parts[0]))));
            document.RootElement.GetProperty("$defs").TryGetProperty(parts[1], out _).ShouldBeTrue($"{type.Name}: {schema}");
        }
    }
}

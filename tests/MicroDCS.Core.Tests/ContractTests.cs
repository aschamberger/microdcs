using System.Collections.Immutable;
using MicroDCS.Core.Inspection;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;

namespace MicroDCS.Core.Tests;

/// <summary>Pure rules of the S06 contracts: the cases that Python covered in test_mqtt.py (scope check, discriminator) and the new R-6 rule.</summary>
public class ContractTests
{
    private static LogicalAddress Address(params string[] scope) =>
        new("dcs/jobs", [.. scope], null, Intent.Command);

    [Theory]
    [InlineData("data", Intent.Data)]
    [InlineData("events", Intent.Event)]
    [InlineData("commands", Intent.Command)]
    [InlineData("metadata", Intent.Meta)]
    public void IntentSegmentsRoundTrip(string segment, Intent intent)
    {
        IntentNames.TryParse(segment, out var parsed).ShouldBeTrue();
        parsed.ShouldBe(intent);
        IntentNames.ToSegment(intent).ShouldBe(segment);
    }

    [Fact]
    public void UnknownIntentSegmentIsRejected() => IntentNames.TryParse("other", out _).ShouldBeFalse();

    [Theory]
    [InlineData("plant1", true)]
    [InlineData("line_2-a", true)]
    [InlineData("", false)]
    [InlineData("a/b", false)]
    [InlineData("a+b", false)]
    [InlineData("a#", false)]
    [InlineData("a*", false)]
    [InlineData("a b", false)]
    [InlineData("a.b", false)]
    public void SegmentRuleExcludesEverySeparatorAndWildcard(string segment, bool valid) =>
        LogicalAddress.IsValidSegment(segment).ShouldBe(valid);

    [Fact]
    public void ScopePathComesFromTheScopePartOfTheSubject()
    {
        LogicalAddress.TryGetScopePath("plant1.line2/job-7", out var path).ShouldBeTrue();
        path.ShouldBe(["plant1", "line2"]);
    }

    [Fact]
    public void SubjectWithInvalidScopeSegmentHasNoPath() =>
        LogicalAddress.TryGetScopePath("plant+1", out _).ShouldBeFalse();

    [Fact]
    public void EnforcedPolicyAcceptsMatchingScope() =>
        ScopePolicy.Allows(ScopeCheck.Enforce, Level0Subjects.Reject, Address("plant1", "line2"), "plant1.line2/job-7").ShouldBeTrue();

    [Fact]
    public void EnforcedPolicyRejectsOtherScope() =>
        ScopePolicy.Allows(ScopeCheck.Enforce, Level0Subjects.Reject, Address("plant1"), "plant2/job-7").ShouldBeFalse();

    [Fact]
    public void NothingToVerifyWithoutSubjectOrAddress()
    {
        ScopePolicy.Allows(ScopeCheck.Enforce, Level0Subjects.Reject, Address("plant1"), null).ShouldBeTrue();
        ScopePolicy.Allows(ScopeCheck.Enforce, Level0Subjects.Reject, null, "plant1").ShouldBeTrue();
    }

    [Fact]
    public void CheckCanBeDisabled() =>
        ScopePolicy.Allows(ScopeCheck.Off, Level0Subjects.Reject, Address("plant1"), "plant2").ShouldBeTrue();

    [Fact]
    public void Level0AddressRejectsSubjectsByDefault() =>
        ScopePolicy.Allows(ScopeCheck.Enforce, Level0Subjects.Reject, Address(), "plant1").ShouldBeFalse();

    [Fact]
    public void Level0AddressAcceptsSubjectsWhenDeclared() =>
        ScopePolicy.Allows(ScopeCheck.Enforce, Level0Subjects.Accept, Address(), "plant1").ShouldBeTrue();

    [Fact]
    public void RedactionHidesPayloadValuesOnly()
    {
        var entry = new InspectionEntry("pipeline.decision", "e1", new Dictionary<string, InspectionValue>
        {
            ["id"] = new("e1", Sensitivity.Identifier),
            ["size"] = new("12", Sensitivity.Public),
            ["data"] = new("secret-value", Sensitivity.Payload),
        });
        var page = new InspectionPage([entry], null);

        var redacted = Redaction.Apply(page, includePayloads: false).Entries[0].Fields;

        redacted["id"].Text.ShouldBe("e1");
        redacted["size"].Text.ShouldBe("12");
        redacted["data"].Text.ShouldNotContain("secret");
        Redaction.Apply(page, includePayloads: true).Entries[0].Fields["data"].Text.ShouldBe("secret-value");
    }
}

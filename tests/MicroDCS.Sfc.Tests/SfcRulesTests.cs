namespace MicroDCS.Sfc.Tests;

/// <summary>Pure rules of D-44: action keys, reserved outcomes, qualifiers, declared outcomes.</summary>
public class SfcRulesTests
{
    private static readonly SfcCapabilities _capabilities = new(
        SfcFeature.PushCommand,
        new Dictionary<string, IReadOnlySet<string>> { ["greet"] = new HashSet<string> { "ok", "rejected" } });

    [Fact]
    public void ActionKeyRoundTrips()
    {
        string key = ActionKey.Create("job-1", "heat");

        ActionKey.TryParse(key, out string jobId, out string action).ShouldBeTrue();
        jobId.ShouldBe("job-1");
        action.ShouldBe("heat");
    }

    [Fact]
    public void ActionKeySplitsAtTheLastColon()
    {
        ActionKey.TryParse("urn:job:7:heat", out string jobId, out string action).ShouldBeTrue();
        jobId.ShouldBe("urn:job:7");
        action.ShouldBe("heat");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("nocolon")]
    [InlineData(":heat")]
    [InlineData("job:")]
    public void ActionKeyRejectsMalformedKeys(string? key) =>
        ActionKey.TryParse(key, out _, out _).ShouldBeFalse();

    [Fact]
    public void ActionKeyRequiresBothParts()
    {
        Should.Throw<ArgumentException>(() => ActionKey.Create("", "a"));
        Should.Throw<ArgumentException>(() => ActionKey.Create("j", ""));
    }

    [Theory]
    [InlineData("timeout", true)]
    [InlineData("error", true)]
    [InlineData("ok", false)]
    [InlineData("true", false)]
    public void ReservedOutcomesAreTimeoutAndError(string name, bool reserved) =>
        SfcOutcome.IsReserved(name).ShouldBe(reserved);

    [Theory]
    [InlineData("N", true)]
    [InlineData("P", false)]
    [InlineData("S", false)]
    [InlineData("R", false)]
    [InlineData("L", false)]
    [InlineData("D", false)]
    [InlineData("n", false)]
    [InlineData(null, false)]
    public void OnlyQualifierNIsAccepted(string? qualifier, bool accepted) =>
        SfcCapabilities.AcceptsQualifier(qualifier).ShouldBe(accepted);

    [Fact]
    public void OutcomesAreDeclaredPerTypeAndReservedOnesAlwaysHold()
    {
        _capabilities.IsOutcomeValid("greet", "ok").ShouldBeTrue();
        _capabilities.IsOutcomeValid("greet", "timeout").ShouldBeTrue();
        _capabilities.IsOutcomeValid("greet", "error").ShouldBeTrue();
        _capabilities.IsOutcomeValid("greet", "missing").ShouldBeFalse();
        _capabilities.IsOutcomeValid("unknown", "ok").ShouldBeFalse();
        _capabilities.IsOutcomeValid("unknown", "error").ShouldBeTrue();
    }

    [Fact]
    public void BuildResultSucceedsOnlyWithAGraphAndNoErrors()
    {
        new SfcBuildResult(new object(), []).Succeeded.ShouldBeTrue();
        new SfcBuildResult(null, [new SfcBuildError("SFC001", "no initial step")]).Succeeded.ShouldBeFalse();
        new SfcBuildResult(new object(), [new SfcBuildError("SFC001", "x")]).Succeeded.ShouldBeFalse();
    }
}

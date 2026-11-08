namespace MicroDCS.Valkey.Tests;

/// <summary>Key names of D-42: prefix, shapes, dedupe hash.</summary>
public class KeySchemaTests
{
    private readonly KeySchema _keys = new("mdcs");

    [Fact]
    public void EveryKeyStartsWithThePrefix()
    {
        string[] all =
        [
            _keys.CloudEventDedupe("s", "i"), _keys.PostStartLock("c"), _keys.JobOrder("j"), _keys.JobOrderList("sc"),
            _keys.JobResponse("r"), _keys.JobResponseList("sc"), _keys.JobResponseByOrder("j"),
            _keys.JobResponseByState("sc", "Ended_Completed"), _keys.WorkMaster("w"), _keys.WorkMasterList("sc"),
            _keys.Reference("equipment", "sc"), _keys.JobAcceptance("sc"), _keys.Scopes(), _keys.Changes("sc"),
            _keys.PublishSequence("sc"), _keys.PublisherCursors(), _keys.SfcState("j"), _keys.SfcWork("sc"),
            _keys.SfcDead("sc"), _keys.SfcActive("sc"), _keys.SfcDue("sc"), _keys.SfcHeartbeat(), _keys.SfcRecipe("h"),
            _keys.Nudge("sfc"),
        ];

        all.ShouldAllBe(k => k.StartsWith("mdcs:", StringComparison.Ordinal));
        all.Distinct().Count().ShouldBe(all.Length);
    }

    [Fact]
    public void KeysHaveTheDocumentedShape()
    {
        _keys.JobOrder("j1").ShouldBe("mdcs:joborder:j1");
        _keys.JobResponseByState("line1", "Ended_Completed").ShouldBe("mdcs:jobresponse:bystate:line1:Ended_Completed");
        _keys.Reference("equipment", "line1").ShouldBe("mdcs:ref:equipment:line1");
        _keys.SfcWork("line1").ShouldBe("mdcs:sfc:work:line1");
        _keys.Scopes().ShouldBe("mdcs:scopes");
    }

    [Fact]
    public void DedupeKeyIsA32CharHexHashOfSourceAndId()
    {
        string key = _keys.CloudEventDedupe("urn:a", "42");

        key.ShouldStartWith("mdcs:ce:dedupe:");
        key["mdcs:ce:dedupe:".Length..].ShouldMatch("^[0-9a-f]{32}$");
        key.ShouldBe(_keys.CloudEventDedupe("urn:a", "42"));
        key.ShouldNotBe(_keys.CloudEventDedupe("urn:a", "43"));
        key.ShouldNotBe(_keys.CloudEventDedupe("urn:b", "42"));
    }

    [Fact]
    public void DifferentPrefixesNeverShareKeys() =>
        new KeySchema("a").JobOrder("1").ShouldNotBe(new KeySchema("b").JobOrder("1"));

    [Fact]
    public void PrefixIsRequired() => Should.Throw<ArgumentException>(() => new KeySchema(""));

    [Fact]
    public void NormalizeStateJoinsNonEmptyTextsWithUnderscore()
    {
        KeySchema.NormalizeState(["AllowedToStart", "Ready"]).ShouldBe("AllowedToStart_Ready");
        KeySchema.NormalizeState(["Ended", null, ""]).ShouldBe("Ended");
        KeySchema.NormalizeState([]).ShouldBeEmpty();
    }
}

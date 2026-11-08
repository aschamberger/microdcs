namespace MicroDCS.Maintenance.Tests;

public sealed class KeyImageTests
{
    [Fact]
    public void EqualContentGivesEqualDigestIndependentOfFieldOrderAndTtl()
    {
        var a = new KeyImage { Key = "k", Type = "hash", Pairs = [new Pair("a", "1"), new Pair("b", "2")], TtlMs = 5 };
        var b = new KeyImage { Key = "k", Type = "hash", Pairs = [new Pair("b", "2"), new Pair("a", "1")], TtlMs = 99 };

        KeyImage.Digest([a]).ShouldBe(KeyImage.Digest([b]));
    }

    [Fact]
    public void ADifferentValueChangesTheDigest()
    {
        var a = new KeyImage { Key = "k", Type = "hash", Pairs = [new Pair("v", "1")] };
        var b = new KeyImage { Key = "k", Type = "hash", Pairs = [new Pair("v", "2")] };

        KeyImage.Digest([a]).ShouldNotBe(KeyImage.Digest([b]));
    }

    [Fact]
    public void AMissingTargetIsPartOfTheDigest()
    {
        var image = new KeyImage { Key = "k", Type = "string", Value = "x" };

        KeyImage.Digest([image, null]).ShouldNotBe(KeyImage.Digest([image]));
        KeyImage.Digest([null, image]).ShouldNotBe(KeyImage.Digest([image, null]));
    }

    [Fact]
    public void PartsCannotRunIntoEachOther()
    {
        var a = new KeyImage { Key = "k", Type = "hash", Pairs = [new Pair("ab", "c")] };
        var b = new KeyImage { Key = "k", Type = "hash", Pairs = [new Pair("a", "bc")] };

        KeyImage.Digest([a]).ShouldNotBe(KeyImage.Digest([b]));
    }
}

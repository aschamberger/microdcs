namespace MicroDCS.Core.Tests;

/// <summary>Proves that the test project builds, references the core and runs.</summary>
public class ScaffoldingTests
{
    [Fact]
    public void CoreAssemblyIsLoadable()
    {
        MicroDCS.Core.AssemblyMarker.Assembly.GetName().Name.ShouldBe("MicroDCS.Core");
    }
}

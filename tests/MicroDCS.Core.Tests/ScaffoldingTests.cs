namespace MicroDCS.Core.Tests;

/// <summary>Proves that the test project builds, references the core and runs.</summary>
public class ScaffoldingTests
{
    [Fact]
    public void CoreAssemblyIsLoadable()
    {
        typeof(MicroDCS.Core.AssemblyMarker).Assembly.GetName().Name.ShouldBe("MicroDCS.Core");
    }
}

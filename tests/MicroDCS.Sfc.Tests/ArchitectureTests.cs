namespace MicroDCS.Sfc.Tests;

/// <summary>The SFC core stays dependency-free (D-17, D-44): no other MicroDCS project, no package, only the base class library.</summary>
public class ArchitectureTests
{
    private static string RepoFile(string relative)
    {
        string? directory = AppContext.BaseDirectory;
        while (directory is not null && !File.Exists(Path.Combine(directory, "MicroDCS.slnx")))
        {
            directory = Path.GetDirectoryName(directory);
        }

        directory.ShouldNotBeNull();
        return Path.Combine(directory, relative);
    }

    [Fact]
    public void TheCoreProjectFileHasNoReferences()
    {
        string project = File.ReadAllText(RepoFile("src/MicroDCS.Sfc/MicroDCS.Sfc.csproj"));

        project.ShouldNotContain("ProjectReference");
        project.ShouldNotContain("PackageReference");
    }

    [Fact]
    public void TheCoreAssemblyReferencesOnlyTheBaseClassLibrary()
    {
        string[] references = [.. typeof(SfcGraph).Assembly.GetReferencedAssemblies().Select(static a => a.Name!)];

        references.ShouldNotBeEmpty();
        references.ShouldAllBe(static name => name == "System.Runtime" || name.StartsWith("System.", StringComparison.Ordinal) || name == "netstandard");
    }

    [Fact]
    public void TheSimulationDependsOnTheCoreOnly()
    {
        string project = File.ReadAllText(RepoFile("src/MicroDCS.Sfc.Simulation/MicroDCS.Sfc.Simulation.csproj"));

        project.ShouldNotContain("PackageReference");
        project.Split("ProjectReference").Length.ShouldBe(2);
        project.ShouldContain("MicroDCS.Sfc.csproj");
    }
}

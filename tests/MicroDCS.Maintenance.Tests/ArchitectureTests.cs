using System.Reflection;

namespace MicroDCS.Maintenance.Tests;

/// <summary>
/// Structural rule of D-19: the destructive maintenance contract lives in a dev-only assembly that the inspection package and the
/// REST adapter cannot reference. The MCP adapter (dev only) is the one adapter that does.
/// </summary>
public sealed class ArchitectureTests
{
    private static readonly string _maintenanceName = typeof(IMaintenanceOperations).Assembly.GetName().Name!;

    private static IEnumerable<string> References(Assembly assembly) => assembly.GetReferencedAssemblies().Select(static a => a.Name!);

    [Fact]
    public void InspectionDoesNotReferenceMaintenance() =>
        References(typeof(MicroDCS.Inspection.InspectionService).Assembly).ShouldNotContain(_maintenanceName);

    [Fact]
    public void MaintenanceDoesNotReferenceTheAdapters()
    {
        var references = References(typeof(IMaintenanceOperations).Assembly).ToList();

        references.ShouldNotContain("MicroDCS.Inspection");
        references.ShouldNotContain("MicroDCS.Mcp");
        references.ShouldNotContain(static name => name.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal));
    }

    [Fact]
    public void OnlyTheMcpAdapterAndTheAppReferenceMaintenance()
    {
        var srcAssemblies = new[]
        {
            typeof(MicroDCS.Core.Hosting.MicroDcsOptions).Assembly,
            typeof(MicroDCS.Valkey.ValkeyContext).Assembly,
            typeof(MicroDCS.Inspection.InspectionService).Assembly,
        };

        foreach (var assembly in srcAssemblies)
        {
            References(assembly).ShouldNotContain(_maintenanceName, assembly.GetName().Name);
        }
    }
}

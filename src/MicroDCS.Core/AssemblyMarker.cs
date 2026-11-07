using System.Reflection;

namespace MicroDCS.Core;

/// <summary>
/// Placeholder type so the assembly can be referenced before the core contracts exist (S06).
/// </summary>
public static class AssemblyMarker
{
    /// <summary>The MicroDCS.Core assembly.</summary>
    public static Assembly Assembly => typeof(AssemblyMarker).Assembly;
}

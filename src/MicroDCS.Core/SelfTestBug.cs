#nullable disable
namespace MicroDCS.Core;

/// <summary>S03 gate self-test: a deliberate null dereference for SonarQube Cloud. Never merged.</summary>
public static class SelfTestBug
{
    /// <summary>Returns the length of <paramref name="value"/>, wrongly.</summary>
    public static int Length(string value)
    {
        if (value == null)
        {
            return value.Length;
        }

        return value.Length > 3 ? value.Length : 3;
    }
}

namespace MicroDCS.Core.Transport;

/// <summary>
/// Reads a credential from a mounted file (D-49). A transport calls this on every connect attempt, never once at start, so a rotated
/// secret is used by the next reconnect.
/// </summary>
public static class SecretFile
{
    /// <summary>Reads the file as it is now; one trailing line break is removed.</summary>
    /// <param name="path">The file.</param>
    /// <returns>The secret.</returns>
    public static string Read(string path)
    {
        var text = File.ReadAllText(path);
        if (text.EndsWith("\r\n", StringComparison.Ordinal))
        {
            return text[..^2];
        }

        return text.EndsWith('\n') ? text[..^1] : text;
    }
}

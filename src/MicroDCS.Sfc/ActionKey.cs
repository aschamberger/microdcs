namespace MicroDCS.Sfc;

/// <summary>
/// The work token id of an action: <c>{jobId}:{actionName}</c> (D-44). A response is matched by this key,
/// never by the instance-local command event id.
/// </summary>
public static class ActionKey
{
    /// <summary>Builds the key for an action of a job.</summary>
    /// <param name="jobId">The job id; must not be empty.</param>
    /// <param name="actionName">The action name from the recipe; must not be empty.</param>
    /// <returns>The action key.</returns>
    public static string Create(string jobId, string actionName)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        ArgumentException.ThrowIfNullOrEmpty(actionName);
        return string.Concat(jobId, ":", actionName);
    }

    /// <summary>Splits a key at its last colon, so job ids may contain colons but action names may not.</summary>
    /// <param name="key">The action key.</param>
    /// <param name="jobId">The job id when the key is valid.</param>
    /// <param name="actionName">The action name when the key is valid.</param>
    /// <returns><see langword="true"/> when both parts are non-empty.</returns>
    public static bool TryParse(string? key, out string jobId, out string actionName)
    {
        jobId = actionName = string.Empty;
        if (key is null)
        {
            return false;
        }

        int index = key.LastIndexOf(':');
        if (index <= 0 || index == key.Length - 1)
        {
            return false;
        }

        jobId = key[..index];
        actionName = key[(index + 1)..];
        return true;
    }
}

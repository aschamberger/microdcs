namespace MicroDCS.Inspection;

/// <summary>A call was refused by policy: rate limit, unknown inspector or a disabled group. The message is safe to show to the caller.</summary>
public sealed class InspectionRefusedException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="InspectionRefusedException"/> class.</summary>
    /// <param name="message">What was refused and why.</param>
    public InspectionRefusedException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="InspectionRefusedException"/> class.</summary>
    /// <param name="message">What was refused and why.</param>
    /// <param name="rateLimited">Whether the rate limit refused the call.</param>
    public InspectionRefusedException(string message, bool rateLimited)
        : base(message)
    {
        RateLimited = rateLimited;
    }

    /// <summary>Gets a value indicating whether the rate limit refused the call (REST answers 429).</summary>
    public bool RateLimited { get; }
}

using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;

namespace MicroDCS.JobControl.Codec;

/// <summary>Required-field checks. The OPC UA JSON decoder accepts a payload with missing fields, so these are ours (D-34).</summary>
public static class JobControlValidation
{
    /// <summary>Checks a job order.</summary>
    /// <param name="order">The order.</param>
    /// <returns><see cref="JobReturnStatus.Success"/>, or <see cref="JobReturnStatus.InvalidRequest"/> without an id.</returns>
    public static JobReturnStatus Validate(JobOrder order)
    {
        ArgumentNullException.ThrowIfNull(order);

        return string.IsNullOrEmpty(order.Id) ? JobReturnStatus.InvalidRequest : JobReturnStatus.Success;
    }

    /// <summary>Checks a job response.</summary>
    /// <param name="response">The response.</param>
    /// <returns><see cref="JobReturnStatus.Success"/>, or <see cref="JobReturnStatus.InvalidRequest"/> without both ids.</returns>
    public static JobReturnStatus Validate(JobResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);

        return string.IsNullOrEmpty(response.Id) || string.IsNullOrEmpty(response.JobOrderId) ? JobReturnStatus.InvalidRequest : JobReturnStatus.Success;
    }
}

using System.ComponentModel;
using MicroDCS.Inspection;
using MicroDCS.Sfc.Hosting;
using ModelContextProtocol;
using ModelContextProtocol.Server;

namespace MicroDCS.Mcp;

/// <summary>Operator actions on running recipes, part of the write group (D-19). Each queues a work entry that the normal path carries out.</summary>
[McpServerToolType]
internal sealed class SfcWriteTools
{
    private const string Caller = "mcp";

    // Tools are static methods with their services as parameters; the class is never instantiated.
    private SfcWriteTools()
    {
    }

    [McpServerTool(Name = "sfc_tick", ReadOnly = false, Destructive = false, Idempotent = true)]
    [Description("Queues a tick for a job, as if the sweep had found it due: expired deadlines fire, others stay. Confirm with the user before calling.")]
    public static Task<string> SfcTick(
        InspectionService service,
        ISfcOperations operations,
        [Description("The job id.")] string id,
        CancellationToken cancellationToken = default) =>
        Run(service, "sfc_tick", id, () => operations.TickAsync(id, cancellationToken));

    [McpServerTool(Name = "sfc_redispatch", ReadOnly = false, Destructive = false, Idempotent = false)]
    [Description("Queues a new dispatch of one open action of a job: the equipment gets the command again under the same action key. Confirm with the user before calling.")]
    public static Task<string> SfcRedispatch(
        InspectionService service,
        ISfcOperations operations,
        [Description("The job id.")] string id,
        [Description("The action name, see sfc_job.")] string action,
        CancellationToken cancellationToken = default) =>
        Run(service, "sfc_redispatch", id, () => operations.RedispatchAsync(id, action, cancellationToken));

    [McpServerTool(Name = "sfc_abort", ReadOnly = false, Destructive = true, Idempotent = true)]
    [Description("Ends the recipe of a job and aborts its job order. Open work is withdrawn. Confirm with the user before calling.")]
    public static Task<string> SfcAbort(
        InspectionService service,
        ISfcOperations operations,
        [Description("The job id.")] string id,
        [Description("Why, for the job's record.")] string? reason = null,
        CancellationToken cancellationToken = default) =>
        Run(service, "sfc_abort", id, () => operations.AbortAsync(id, reason, cancellationToken));

    private static async Task<string> Run(InspectionService service, string tool, string id, Func<ValueTask<bool>> call)
    {
        try
        {
            service.Admit(Caller, tool);
            return await call().ConfigureAwait(false) ? "Queued." : $"Job '{id}' has no SFC state.";
        }
        catch (InspectionRefusedException ex)
        {
            throw new McpException(ex.Message, ex);
        }
    }
}

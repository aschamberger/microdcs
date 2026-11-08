using System.ComponentModel;
using MicroDCS.Inspection;
using ModelContextProtocol;
using ModelContextProtocol.Server;

namespace MicroDCS.Mcp;

/// <summary>The write group (D-19): only registered with <c>MicroDcs:Management:WriteEnabled</c>. Every call goes through the pipeline.</summary>
[McpServerToolType]
internal sealed class WriteTools
{
    // Tools are static methods with their services as parameters; the class is never instantiated.
    private WriteTools()
    {
    }

    [McpServerTool(Name = "inject_event", ReadOnly = false, Destructive = false, Idempotent = false)]
    [Description("Hands a request event to the pipeline of a binding as if its transport had received it, for testing. Handlers run and replies are published. Confirm with the user before calling.")]
    public static async Task<string> InjectEvent(
        InspectionService service,
        EventInjector injector,
        [Description("Binding id (see processors_list).")] string binding,
        [Description("CloudEvent type id the binding handles.")] string type,
        [Description("Subject: scope segments joined by '.', optionally '/resource'. The segment count must match the binding's scope levels.")] string subject,
        [Description("Event data as JSON text, or empty.")] string? dataJson = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            service.Admit("mcp", "inject_event");
            var outcome = await injector.InjectAsync("mcp", binding, type, subject, string.IsNullOrWhiteSpace(dataJson) ? null : dataJson, cancellationToken).ConfigureAwait(false);
            return outcome.ToString();
        }
        catch (InspectionRefusedException ex)
        {
            throw new McpException(ex.Message, ex);
        }
    }
}

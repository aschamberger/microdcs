using System.ComponentModel;
using System.Globalization;
using System.Text;
using MicroDCS.Inspection;
using MicroDCS.Maintenance;
using ModelContextProtocol;
using ModelContextProtocol.Server;

namespace MicroDCS.Mcp;

/// <summary>
/// The maintenance group (D-19, D-45): only registered with <c>MicroDcs:Maintenance:Enabled</c> in Development or Test. Every change is a
/// plan followed by <c>maint_apply</c>; the plan text is what the client shows for confirmation. No other adapter offers these.
/// </summary>
[McpServerToolType]
internal sealed class MaintenanceTools
{
    private const string Caller = "mcp";

    // Tools are static methods with their services as parameters; the class is never instantiated.
    private MaintenanceTools()
    {
    }

    [McpServerTool(Name = "maint_dedupe_release_plan", ReadOnly = true, Destructive = false, Idempotent = false)]
    [Description("Plans the release of one dedupe entry (lease or done marker), so the event can be processed again. Changes nothing; run maint_apply with the returned token to execute.")]
    public static Task<string> DedupeReleasePlan(
        InspectionService service,
        IMaintenanceOperations operations,
        [Description("CloudEvent source.")] string source,
        [Description("CloudEvent id.")] string id,
        CancellationToken cancellationToken = default) =>
        Run(service, "maint_dedupe_release_plan", async () => Format(await operations.PlanDedupeReleaseAsync(Caller, source, id, cancellationToken).ConfigureAwait(false)));

    [McpServerTool(Name = "maint_purge_scope_plan", ReadOnly = true, Destructive = false, Idempotent = false)]
    [Description("Plans the removal of everything stored for one scope (documents, indexes, streams, due-index, registry entry). Bypasses the OPC UA state machine; the MES is not told. Changes nothing; run maint_apply with the returned token to execute.")]
    public static Task<string> PurgeScopePlan(
        InspectionService service,
        IMaintenanceOperations operations,
        [Description("The scope, as in valkey_keys.")] string scope,
        [Description("Allow a plan above the item cap.")] bool allowLarge = false,
        CancellationToken cancellationToken = default) =>
        Run(service, "maint_purge_scope_plan", async () => Format(await operations.PlanPurgeScopeAsync(Caller, scope, allowLarge, cancellationToken).ConfigureAwait(false)));

    [McpServerTool(Name = "maint_restore_plan", ReadOnly = true, Destructive = false, Idempotent = false)]
    [Description("Plans putting the content of a snapshot back (see maint_snapshots_list). Refuses when a target exists with different content unless overwrite is set. Changes nothing; run maint_apply with the returned token to execute.")]
    public static Task<string> RestorePlan(
        InspectionService service,
        IMaintenanceOperations operations,
        [Description("Snapshot id.")] string snapshotId,
        [Description("Replace targets that exist with different content; they are snapshotted first.")] bool overwrite = false,
        CancellationToken cancellationToken = default) =>
        Run(service, "maint_restore_plan", async () => Format(await operations.PlanRestoreAsync(Caller, snapshotId, overwrite, cancellationToken).ConfigureAwait(false)));

    [McpServerTool(Name = "maint_apply", ReadOnly = false, Destructive = true, Idempotent = false)]
    [Description("Executes a plan from a maint_*_plan tool. Deletes or overwrites stored data after writing a snapshot. The token works once and fails when the targets changed since the plan. Confirm with the user before calling.")]
    public static Task<string> Apply(
        InspectionService service,
        IMaintenanceOperations operations,
        [Description("The token from the plan.")] string token,
        CancellationToken cancellationToken = default) =>
        Run(service, "maint_apply", async () => Format(await operations.ApplyAsync(Caller, token, cancellationToken).ConfigureAwait(false)));

    [McpServerTool(Name = "maint_snapshots_list", ReadOnly = true, Destructive = false, Idempotent = true)]
    [Description("Lists the snapshots written before earlier applies, newest first, without their content.")]
    public static Task<string> SnapshotsList(
        InspectionService service,
        IMaintenanceOperations operations,
        CancellationToken cancellationToken = default) =>
        Run(service, "maint_snapshots_list", async () =>
        {
            var snapshots = await operations.ListSnapshotsAsync(Caller, cancellationToken).ConfigureAwait(false);
            return snapshots.Count == 0
                ? "No snapshots."
                : string.Join('\n', snapshots.Select(static s => string.Create(CultureInfo.InvariantCulture, $"{s.Id}  {s.CreatedAt:u}  {s.Tool}  {s.Arguments}  {s.Items} items  {s.Bytes} bytes")));
        });

    private static async Task<string> Run(InspectionService service, string tool, Func<Task<string>> call)
    {
        try
        {
            service.Admit(Caller, tool);
            return await call().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is InspectionRefusedException or MaintenanceRefusedException)
        {
            throw new McpException(ex.Message, ex);
        }
    }

    private static string Format(MaintenancePlan plan)
    {
        var text = new StringBuilder();
        text.AppendLine(CultureInfo.InvariantCulture, $"PLAN {plan.Tool} ({plan.Arguments})");
        text.AppendLine(plan.Summary);
        foreach (var warning in plan.Warnings)
        {
            text.AppendLine(CultureInfo.InvariantCulture, $"WARNING: {warning}");
        }

        text.AppendLine(CultureInfo.InvariantCulture, $"{plan.Items.Count} targets:");
        foreach (var item in plan.Items)
        {
            text.AppendLine(CultureInfo.InvariantCulture, $"  {item.Action,-9} {item.Key}  ({item.Detail})");
        }

        text.AppendLine(CultureInfo.InvariantCulture, $"State digest {plan.Digest}. Token (valid until {plan.ExpiresAt:u}, works once): {plan.Token}");
        text.Append("Run maint_apply with this token to execute. Nothing has changed yet.");
        return text.ToString();
    }

    private static string Format(MaintenanceResult result)
    {
        var text = new StringBuilder();
        text.AppendLine(CultureInfo.InvariantCulture, $"APPLIED {result.Tool}: {result.Changed} structures changed.");
        text.AppendLine(result.SnapshotId is null ? "No snapshot was needed." : $"Snapshot {result.SnapshotId} holds the previous content; maint_restore_plan puts it back.");
        foreach (var note in result.Notes)
        {
            text.AppendLine(note);
        }

        return text.ToString().TrimEnd();
    }
}

using System.Text.Json.Serialization;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;
using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Extension;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.MachineryJobs;

/// <summary>The dataschema URIs of the job payloads.</summary>
internal static class JobSchemas
{
    public const string JobControl = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/";
    public const string MachineryJobs = "https://aschamberger.github.com/schemas/UA/Machinery/Jobs/v1.0.1/";
}

/// <summary>The Store method call.</summary>
[CloudEventType(JobControlTypes.Store, Schema = JobSchemas.JobControl + "#/$defs/StoreCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class StoreCall : JobOrderMethodCallDataType;

/// <summary>The result of the Store method.</summary>
[CloudEventType(JobControlTypes.StoreResponse, Schema = JobSchemas.JobControl + "#/$defs/StoreResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class StoreResponse : JobMethodResponseDataType;

/// <summary>The StoreAndStart method call.</summary>
[CloudEventType(JobControlTypes.StoreAndStart, Schema = JobSchemas.JobControl + "#/$defs/StoreAndStartCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class StoreAndStartCall : JobOrderMethodCallDataType;

/// <summary>The result of the StoreAndStart method.</summary>
[CloudEventType(JobControlTypes.StoreAndStartResponse, Schema = JobSchemas.JobControl + "#/$defs/StoreAndStartResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class StoreAndStartResponse : JobMethodResponseDataType;

/// <summary>The Update method call.</summary>
[CloudEventType(JobControlTypes.Update, Schema = JobSchemas.JobControl + "#/$defs/UpdateCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class UpdateCall : JobOrderMethodCallDataType;

/// <summary>The result of the Update method.</summary>
[CloudEventType(JobControlTypes.UpdateResponse, Schema = JobSchemas.JobControl + "#/$defs/UpdateResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class UpdateResponse : JobMethodResponseDataType;

/// <summary>The Start method call.</summary>
[CloudEventType(JobControlTypes.Start, Schema = JobSchemas.JobControl + "#/$defs/StartCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class StartCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Start method.</summary>
[CloudEventType(JobControlTypes.StartResponse, Schema = JobSchemas.JobControl + "#/$defs/StartResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class StartResponse : JobMethodResponseDataType;

/// <summary>The Stop method call.</summary>
[CloudEventType(JobControlTypes.Stop, Schema = JobSchemas.JobControl + "#/$defs/StopCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class StopCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Stop method.</summary>
[CloudEventType(JobControlTypes.StopResponse, Schema = JobSchemas.JobControl + "#/$defs/StopResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class StopResponse : JobMethodResponseDataType;

/// <summary>The Abort method call.</summary>
[CloudEventType(JobControlTypes.Abort, Schema = JobSchemas.JobControl + "#/$defs/AbortCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class AbortCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Abort method.</summary>
[CloudEventType(JobControlTypes.AbortResponse, Schema = JobSchemas.JobControl + "#/$defs/AbortResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class AbortResponse : JobMethodResponseDataType;

/// <summary>The Pause method call.</summary>
[CloudEventType(JobControlTypes.Pause, Schema = JobSchemas.JobControl + "#/$defs/PauseCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class PauseCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Pause method.</summary>
[CloudEventType(JobControlTypes.PauseResponse, Schema = JobSchemas.JobControl + "#/$defs/PauseResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class PauseResponse : JobMethodResponseDataType;

/// <summary>The Resume method call.</summary>
[CloudEventType(JobControlTypes.Resume, Schema = JobSchemas.JobControl + "#/$defs/ResumeCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class ResumeCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Resume method.</summary>
[CloudEventType(JobControlTypes.ResumeResponse, Schema = JobSchemas.JobControl + "#/$defs/ResumeResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class ResumeResponse : JobMethodResponseDataType;

/// <summary>The RevokeStart method call.</summary>
[CloudEventType(JobControlTypes.RevokeStart, Schema = JobSchemas.JobControl + "#/$defs/RevokeStartCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class RevokeStartCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the RevokeStart method.</summary>
[CloudEventType(JobControlTypes.RevokeStartResponse, Schema = JobSchemas.JobControl + "#/$defs/RevokeStartResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class RevokeStartResponse : JobMethodResponseDataType;

/// <summary>The Cancel method call.</summary>
[CloudEventType(JobControlTypes.Cancel, Schema = JobSchemas.JobControl + "#/$defs/CancelCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class CancelCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Cancel method.</summary>
[CloudEventType(JobControlTypes.CancelResponse, Schema = JobSchemas.JobControl + "#/$defs/CancelResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class CancelResponse : JobMethodResponseDataType;

/// <summary>The Clear method call.</summary>
[CloudEventType(JobControlTypes.Clear, Schema = JobSchemas.JobControl + "#/$defs/ClearCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class ClearCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Clear method.</summary>
[CloudEventType(JobControlTypes.ClearResponse, Schema = JobSchemas.JobControl + "#/$defs/ClearResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class ClearResponse : JobMethodResponseDataType;

/// <summary>The RequestJobResponseByJobOrderID method call.</summary>
[CloudEventType(JobControlTypes.RequestJobResponseByJobOrderId, Schema = JobSchemas.MachineryJobs + "#/$defs/RequestJobResponseByJobOrderIDCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class RequestJobResponseByJobOrderIdCall : JobResponseByIdCallDataType;

/// <summary>The result of the RequestJobResponseByJobOrderID method.</summary>
[CloudEventType(JobControlTypes.RequestJobResponseByJobOrderIdResponse, Schema = JobSchemas.MachineryJobs + "#/$defs/RequestJobResponseByJobOrderIDResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class RequestJobResponseByJobOrderIdResponse : JobResponseByIdResultDataType;

/// <summary>The RequestJobResponseByJobOrderState method call.</summary>
[CloudEventType(JobControlTypes.RequestJobResponseByJobOrderState, Schema = JobSchemas.MachineryJobs + "#/$defs/RequestJobResponseByJobOrderStateCall", Codec = PayloadCodec.OpcUaJson)]
public sealed class RequestJobResponseByJobOrderStateCall : JobResponseByStateCallDataType;

/// <summary>The result of the RequestJobResponseByJobOrderState method.</summary>
[CloudEventType(JobControlTypes.RequestJobResponseByJobOrderStateResponse, Schema = JobSchemas.MachineryJobs + "#/$defs/RequestJobResponseByJobOrderStateResponse", Codec = PayloadCodec.OpcUaJson)]
public sealed class RequestJobResponseByJobOrderStateResponse : JobResponseByStateResultDataType;

/// <summary>The status event of a job order (<c>ISA95JobOrderStatusEventType</c>).</summary>
[CloudEventType(JobControlTypes.JobOrderStatusEvent, Schema = JobSchemas.JobControl + "#/$defs/ISA95JobOrderStatusEventType", Codec = PayloadCodec.OpcUaJson)]
public sealed class JobOrderStatusEvent : JobOrderStatusEventDataType;

/// <summary>Equipment a station may use. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.ConfigEquipment, Schema = JobSchemas.JobControl + "ConfigEquipment/", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigEquipment : V2.ISA95EquipmentDataType;

/// <summary>Material class a station may use. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.ConfigMaterialClass, Schema = JobSchemas.JobControl + "ConfigMaterialClass/", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigMaterialClass : V2.ISA95MaterialDataType;

/// <summary>Material definition a station may use. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.ConfigMaterialDefinition, Schema = JobSchemas.JobControl + "ConfigMaterialDefinition/", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigMaterialDefinition : V2.ISA95MaterialDataType;

/// <summary>Personnel a station may use. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.ConfigPersonnel, Schema = JobSchemas.JobControl + "ConfigPersonnel/", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigPersonnel : V2.ISA95PersonnelDataType;

/// <summary>Physical asset a station may use. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.ConfigPhysicalAsset, Schema = JobSchemas.JobControl + "ConfigPhysicalAsset/", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigPhysicalAsset : V2.ISA95PhysicalAssetDataType;

/// <summary>A standard Work Master for a station. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.WorkMaster, Schema = JobSchemas.JobControl + "ConfigWorkMaster/", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigWorkMaster : V2.ISA95WorkMasterDataType;

/// <summary>A Work Master with recipe content for a station. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.WorkMasterExt, Schema = JobSchemas.JobControl + "ConfigWorkMasterExt/", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigWorkMasterExt : ISA95WorkMasterExtDataType;

/// <summary>The job acceptance limit of a scope. <c>DELETE</c> removes it.</summary>
/// <param name="MaxDownloadableJobOrders">How many job orders the scope may hold. 0 accepts no further order.</param>
[CloudEventType(JobControlTypes.ConfigJobAcceptance, Schema = JobSchemas.JobControl + "ConfigJobAcceptance/")]
public sealed record ConfigJobAcceptance([property: JsonPropertyName("max_downloadable_job_orders"), JsonRequired] int MaxDownloadableJobOrders);

/// <summary>What the processor offers, published once and retained for late subscribers.</summary>
[CloudEventType(JobControlTypes.Metadata, Schema = JobSchemas.MachineryJobs + "Meta/")]
public sealed record JobMetadata
{
    /// <summary>Gets the companion specification URI.</summary>
    [JsonPropertyName("modelUri")]
#pragma warning disable S5332 // A namespace URI of the OPC UA companion specification, an identifier that is never fetched.
    public string ModelUri { get; init; } = "http://opcfoundation.org/UA/Machinery/Jobs/";
#pragma warning restore S5332

    /// <summary>Gets the companion specification version.</summary>
    [JsonPropertyName("modelVersion")]
    public string ModelVersion { get; init; } = "1.0.1";

    /// <summary>Gets the publication date of the companion specification.</summary>
    [JsonPropertyName("modelDate")]
    public string ModelDate { get; init; } = "2024-05-01T00:00:00";

    /// <summary>Gets the methods the processor answers.</summary>
    [JsonPropertyName("supportedCommands")]
    public IReadOnlyList<string> SupportedCommands { get; init; } = [];

    /// <summary>Gets the events the processor publishes.</summary>
    [JsonPropertyName("eventTypes")]
    public IReadOnlyList<string> EventTypes { get; init; } = [];
}

/// <summary>JSON metadata of the plain JSON payloads (source generated, AOT).</summary>
[JsonSerializable(typeof(ConfigJobAcceptance))]
[JsonSerializable(typeof(JobMetadata))]
internal sealed partial class MachineryJobsJsonContext : JsonSerializerContext;

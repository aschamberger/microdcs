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
    /// <summary>The wire shapes of the extension NodeSet <c>urn:microdcs:jobcontrol</c> (method calls and results, status event, state index).</summary>
    public const string Extension = "https://aschamberger.github.io/microdcs/schemas/microdcs-jobcontrol/v1.2.0.json";

    /// <summary>The ISA-95 Job Control V2 data types, for the station configuration.</summary>
    public const string Isa95V2 = "https://aschamberger.github.io/microdcs/schemas/isa95-jobcontrol-v2/v2.0.0.json";

    /// <summary>The payloads of the processor that are plain JSON (job acceptance limit, metadata).</summary>
    public const string MachineryJobs = "https://aschamberger.github.io/microdcs/schemas/machinery-jobs/v1.0.0.json";
}

/// <summary>The Store method call.</summary>
[CloudEventType(JobControlTypes.Store, Schema = JobSchemas.Extension + "#/$defs/JobOrderMethodCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class StoreCall : JobOrderMethodCallDataType;

/// <summary>The result of the Store method.</summary>
[CloudEventType(JobControlTypes.StoreResponse, Schema = JobSchemas.Extension + "#/$defs/JobMethodResponseDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class StoreResponse : JobMethodResponseDataType;

/// <summary>The StoreAndStart method call.</summary>
[CloudEventType(JobControlTypes.StoreAndStart, Schema = JobSchemas.Extension + "#/$defs/JobOrderMethodCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class StoreAndStartCall : JobOrderMethodCallDataType;

/// <summary>The result of the StoreAndStart method.</summary>
[CloudEventType(JobControlTypes.StoreAndStartResponse, Schema = JobSchemas.Extension + "#/$defs/JobMethodResponseDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class StoreAndStartResponse : JobMethodResponseDataType;

/// <summary>The Update method call.</summary>
[CloudEventType(JobControlTypes.Update, Schema = JobSchemas.Extension + "#/$defs/JobOrderMethodCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class UpdateCall : JobOrderMethodCallDataType;

/// <summary>The result of the Update method.</summary>
[CloudEventType(JobControlTypes.UpdateResponse, Schema = JobSchemas.Extension + "#/$defs/JobMethodResponseDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class UpdateResponse : JobMethodResponseDataType;

/// <summary>The Start method call.</summary>
[CloudEventType(JobControlTypes.Start, Schema = JobSchemas.Extension + "#/$defs/JobOrderIdMethodCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class StartCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Start method.</summary>
[CloudEventType(JobControlTypes.StartResponse, Schema = JobSchemas.Extension + "#/$defs/JobMethodResponseDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class StartResponse : JobMethodResponseDataType;

/// <summary>The Stop method call.</summary>
[CloudEventType(JobControlTypes.Stop, Schema = JobSchemas.Extension + "#/$defs/JobOrderIdMethodCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class StopCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Stop method.</summary>
[CloudEventType(JobControlTypes.StopResponse, Schema = JobSchemas.Extension + "#/$defs/JobMethodResponseDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class StopResponse : JobMethodResponseDataType;

/// <summary>The Abort method call.</summary>
[CloudEventType(JobControlTypes.Abort, Schema = JobSchemas.Extension + "#/$defs/JobOrderIdMethodCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class AbortCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Abort method.</summary>
[CloudEventType(JobControlTypes.AbortResponse, Schema = JobSchemas.Extension + "#/$defs/JobMethodResponseDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class AbortResponse : JobMethodResponseDataType;

/// <summary>The Pause method call.</summary>
[CloudEventType(JobControlTypes.Pause, Schema = JobSchemas.Extension + "#/$defs/JobOrderIdMethodCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class PauseCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Pause method.</summary>
[CloudEventType(JobControlTypes.PauseResponse, Schema = JobSchemas.Extension + "#/$defs/JobMethodResponseDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class PauseResponse : JobMethodResponseDataType;

/// <summary>The Resume method call.</summary>
[CloudEventType(JobControlTypes.Resume, Schema = JobSchemas.Extension + "#/$defs/JobOrderIdMethodCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class ResumeCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Resume method.</summary>
[CloudEventType(JobControlTypes.ResumeResponse, Schema = JobSchemas.Extension + "#/$defs/JobMethodResponseDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class ResumeResponse : JobMethodResponseDataType;

/// <summary>The RevokeStart method call.</summary>
[CloudEventType(JobControlTypes.RevokeStart, Schema = JobSchemas.Extension + "#/$defs/JobOrderIdMethodCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class RevokeStartCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the RevokeStart method.</summary>
[CloudEventType(JobControlTypes.RevokeStartResponse, Schema = JobSchemas.Extension + "#/$defs/JobMethodResponseDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class RevokeStartResponse : JobMethodResponseDataType;

/// <summary>The Cancel method call.</summary>
[CloudEventType(JobControlTypes.Cancel, Schema = JobSchemas.Extension + "#/$defs/JobOrderIdMethodCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class CancelCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Cancel method.</summary>
[CloudEventType(JobControlTypes.CancelResponse, Schema = JobSchemas.Extension + "#/$defs/JobMethodResponseDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class CancelResponse : JobMethodResponseDataType;

/// <summary>The Clear method call.</summary>
[CloudEventType(JobControlTypes.Clear, Schema = JobSchemas.Extension + "#/$defs/JobOrderIdMethodCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class ClearCall : JobOrderIdMethodCallDataType;

/// <summary>The result of the Clear method.</summary>
[CloudEventType(JobControlTypes.ClearResponse, Schema = JobSchemas.Extension + "#/$defs/JobMethodResponseDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class ClearResponse : JobMethodResponseDataType;

/// <summary>The RequestJobResponseByJobOrderID method call.</summary>
[CloudEventType(JobControlTypes.RequestJobResponseByJobOrderId, Schema = JobSchemas.Extension + "#/$defs/JobResponseByIdCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class RequestJobResponseByJobOrderIdCall : JobResponseByIdCallDataType;

/// <summary>The result of the RequestJobResponseByJobOrderID method.</summary>
[CloudEventType(JobControlTypes.RequestJobResponseByJobOrderIdResponse, Schema = JobSchemas.Extension + "#/$defs/JobResponseByIdResultDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class RequestJobResponseByJobOrderIdResponse : JobResponseByIdResultDataType;

/// <summary>The RequestJobResponseByJobOrderState method call.</summary>
[CloudEventType(JobControlTypes.RequestJobResponseByJobOrderState, Schema = JobSchemas.Extension + "#/$defs/JobResponseByStateCallDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class RequestJobResponseByJobOrderStateCall : JobResponseByStateCallDataType;

/// <summary>The result of the RequestJobResponseByJobOrderState method.</summary>
[CloudEventType(JobControlTypes.RequestJobResponseByJobOrderStateResponse, Schema = JobSchemas.Extension + "#/$defs/JobResponseByStateResultDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class RequestJobResponseByJobOrderStateResponse : JobResponseByStateResultDataType;

/// <summary>The status event of a job order (<c>ISA95JobOrderStatusEventType</c>).</summary>
[CloudEventType(JobControlTypes.JobOrderStatusEvent, Schema = JobSchemas.Extension + "#/$defs/JobOrderStatusEventDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class JobOrderStatusEvent : JobOrderStatusEventDataType;

/// <summary>Equipment a station may use. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.ConfigEquipment, Schema = JobSchemas.Isa95V2 + "#/$defs/ISA95EquipmentDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigEquipment : V2.ISA95EquipmentDataType;

/// <summary>Material class a station may use. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.ConfigMaterialClass, Schema = JobSchemas.Isa95V2 + "#/$defs/ISA95MaterialDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigMaterialClass : V2.ISA95MaterialDataType;

/// <summary>Material definition a station may use. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.ConfigMaterialDefinition, Schema = JobSchemas.Isa95V2 + "#/$defs/ISA95MaterialDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigMaterialDefinition : V2.ISA95MaterialDataType;

/// <summary>Personnel a station may use. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.ConfigPersonnel, Schema = JobSchemas.Isa95V2 + "#/$defs/ISA95PersonnelDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigPersonnel : V2.ISA95PersonnelDataType;

/// <summary>Physical asset a station may use. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.ConfigPhysicalAsset, Schema = JobSchemas.Isa95V2 + "#/$defs/ISA95PhysicalAssetDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigPhysicalAsset : V2.ISA95PhysicalAssetDataType;

/// <summary>A standard Work Master for a station. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.WorkMaster, Schema = JobSchemas.Isa95V2 + "#/$defs/ISA95WorkMasterDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigWorkMaster : V2.ISA95WorkMasterDataType;

/// <summary>A Work Master with recipe content for a station. <c>DELETE</c> removes it.</summary>
[CloudEventType(JobControlTypes.WorkMasterExt, Schema = JobSchemas.Extension + "#/$defs/ISA95WorkMasterExtDataType", Codec = PayloadCodec.OpcUaJson)]
public sealed class ConfigWorkMasterExt : ISA95WorkMasterExtDataType;

/// <summary>The job acceptance limit of a scope. <c>DELETE</c> removes it.</summary>
/// <param name="MaxDownloadableJobOrders">How many job orders the scope may hold. 0 accepts no further order.</param>
[CloudEventType(JobControlTypes.ConfigJobAcceptance, Schema = JobSchemas.MachineryJobs + "#/$defs/ConfigJobAcceptance")]
public sealed record ConfigJobAcceptance([property: JsonPropertyName("max_downloadable_job_orders"), JsonRequired] int MaxDownloadableJobOrders);

/// <summary>What the processor offers, published once and retained for late subscribers.</summary>
[CloudEventType(JobControlTypes.Metadata, Schema = JobSchemas.MachineryJobs + "#/$defs/JobMetadata")]
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

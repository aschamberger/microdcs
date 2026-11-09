namespace MicroDCS.JobControl.Codec;

/// <summary>CloudEvent type ids of the job models (D-34, D-50). A response has its own id, the call id with <c>.response</c> before the version.</summary>
public static class JobControlTypes
{
    /// <summary>The standard Work Master configuration.</summary>
    public const string WorkMaster = "com.github.aschamberger.ISA95-JOBCONTROL_V2.config.workmaster.v1";

    /// <summary>The extended Work Master: the receiver decodes the subtype directly, so no discriminator is needed in the JSON.</summary>
    public const string WorkMasterExt = "com.github.aschamberger.ISA95-JOBCONTROL_V2.config.workmaster.ext.v1";

    private const string JobControl = "org.opcfoundation.ISA95-JOBCONTROL_V2.";
    private const string MachineryJobs = "org.opcfoundation.Machinery.Jobs.";
    private const string Config = "com.github.aschamberger.ISA95-JOBCONTROL_V2.config.";

    /// <summary>The Store method.</summary>
    public const string Store = JobControl + "Store.v2";

    /// <summary>The response of the Store method.</summary>
    public const string StoreResponse = JobControl + "Store.response.v2";

    /// <summary>The StoreAndStart method.</summary>
    public const string StoreAndStart = JobControl + "StoreAndStart.v2";

    /// <summary>The response of the StoreAndStart method.</summary>
    public const string StoreAndStartResponse = JobControl + "StoreAndStart.response.v2";

    /// <summary>The Update method.</summary>
    public const string Update = JobControl + "Update.v2";

    /// <summary>The response of the Update method.</summary>
    public const string UpdateResponse = JobControl + "Update.response.v2";

    /// <summary>The Start method.</summary>
    public const string Start = JobControl + "Start.v2";

    /// <summary>The response of the Start method.</summary>
    public const string StartResponse = JobControl + "Start.response.v2";

    /// <summary>The Stop method.</summary>
    public const string Stop = JobControl + "Stop.v2";

    /// <summary>The response of the Stop method.</summary>
    public const string StopResponse = JobControl + "Stop.response.v2";

    /// <summary>The Abort method.</summary>
    public const string Abort = JobControl + "Abort.v2";

    /// <summary>The response of the Abort method.</summary>
    public const string AbortResponse = JobControl + "Abort.response.v2";

    /// <summary>The Pause method.</summary>
    public const string Pause = JobControl + "Pause.v2";

    /// <summary>The response of the Pause method.</summary>
    public const string PauseResponse = JobControl + "Pause.response.v2";

    /// <summary>The Resume method.</summary>
    public const string Resume = JobControl + "Resume.v2";

    /// <summary>The response of the Resume method.</summary>
    public const string ResumeResponse = JobControl + "Resume.response.v2";

    /// <summary>The RevokeStart method.</summary>
    public const string RevokeStart = JobControl + "RevokeStart.v2";

    /// <summary>The response of the RevokeStart method.</summary>
    public const string RevokeStartResponse = JobControl + "RevokeStart.response.v2";

    /// <summary>The Cancel method.</summary>
    public const string Cancel = JobControl + "Cancel.v2";

    /// <summary>The response of the Cancel method.</summary>
    public const string CancelResponse = JobControl + "Cancel.response.v2";

    /// <summary>The Clear method.</summary>
    public const string Clear = JobControl + "Clear.v2";

    /// <summary>The response of the Clear method.</summary>
    public const string ClearResponse = JobControl + "Clear.response.v2";

    /// <summary>The RequestJobResponseByJobOrderID method.</summary>
    public const string RequestJobResponseByJobOrderId = MachineryJobs + "RequestJobResponseByJobOrderID.v1";

    /// <summary>The response of the RequestJobResponseByJobOrderID method.</summary>
    public const string RequestJobResponseByJobOrderIdResponse = MachineryJobs + "RequestJobResponseByJobOrderID.response.v1";

    /// <summary>The RequestJobResponseByJobOrderState method.</summary>
    public const string RequestJobResponseByJobOrderState = MachineryJobs + "RequestJobResponseByJobOrderState.v1";

    /// <summary>The response of the RequestJobResponseByJobOrderState method.</summary>
    public const string RequestJobResponseByJobOrderStateResponse = MachineryJobs + "RequestJobResponseByJobOrderState.response.v1";

    /// <summary>The status event of a job order.</summary>
    public const string JobOrderStatusEvent = JobControl + "ISA95JobOrderStatusEventType.v2";

    /// <summary>The metadata event of the Machinery Jobs processor.</summary>
    public const string Metadata = MachineryJobs + "Meta.v1";

    /// <summary>The equipment configuration of a station.</summary>
    public const string ConfigEquipment = Config + "equipment.v1";

    /// <summary>The materialclass configuration of a station.</summary>
    public const string ConfigMaterialClass = Config + "materialclass.v1";

    /// <summary>The materialdefinition configuration of a station.</summary>
    public const string ConfigMaterialDefinition = Config + "materialdefinition.v1";

    /// <summary>The personnel configuration of a station.</summary>
    public const string ConfigPersonnel = Config + "personnel.v1";

    /// <summary>The physicalasset configuration of a station.</summary>
    public const string ConfigPhysicalAsset = Config + "physicalasset.v1";

    /// <summary>The jobacceptance configuration of a station.</summary>
    public const string ConfigJobAcceptance = Config + "jobacceptance.v1";
}

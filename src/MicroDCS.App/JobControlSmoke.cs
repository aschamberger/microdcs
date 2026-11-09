using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Conversions;
using MicroDCS.JobControl.Extension;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.MachineryJobs;
using Opc.Ua;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.App;

/// <summary>
/// Native AOT smoke check of the job models (S16): the OPC UA JSON round trip of a job order, the generated Work Master extension,
/// runtime JSON Schema generation and the state machine table (D-34).
/// </summary>
internal static class JobControlSmoke
{
    public static int Run(TextWriter output)
    {
        var order = JobControlConversions.ToV2Order(new JobOrder { Id = "smoke", Priority = 1 });
        var decoded = OpcUaJsonCodec.Decode<V2.ISA95JobOrderDataType>(OpcUaJsonCodec.Encode(order));

        var workMaster = JobControlConversions.ToWorkMasterExt(new JobWorkMaster { Id = "wm", DataSchema = "urn:smoke", Data = "{}" });
        var decodedWorkMaster = OpcUaJsonCodec.Decode<ISA95WorkMasterExtDataType>(OpcUaJsonCodec.Encode(workMaster));

        var call = OpcUaJsonCodec.Decode<StoreCall>(OpcUaJsonCodec.Encode(new StoreCall { JobOrder = order }));

        var index = OpcUaJsonCodec.Decode<JobOrderStateIndexDataType>(OpcUaJsonCodec.Encode(new JobOrderStateIndexDataType
        {
            Sequence = 7,
            Scope = "site.area",
            PublishedAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            Jobs = new[] { new JobOrderStateIndexEntryDataType { JobOrderID = "smoke", State = JobStateArray.ToV2(JobOrderState.Running), HasResult = true } }.ToArrayOf(),
        }));

        var schema = JobControlSchemas.GenerateJson<V2.ISA95JobOrderDataType>();
        var transition = JobStateMachine.Apply(JobControlVersion.V2, JobOrderState.Initial, JobOrderOperation.StoreAndStart);

        var ok = decoded.JobOrderID == "smoke"
            && call.JobOrder.JobOrderID == "smoke"
            && decodedWorkMaster.Data == "{}"
            && index.Sequence == 7
            && index.Jobs.Count == 1
            && index.Jobs[0].HasResult
            && schema.Contains("JobOrderID", StringComparison.Ordinal)
            && transition.State == JobOrderState.AllowedToStartReady;
        output.WriteLine(ok ? "jobcontrol smoke ok" : "jobcontrol smoke FAILED");
        return ok ? 0 : 1;
    }
}

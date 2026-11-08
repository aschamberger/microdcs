using MicroDCS.JobControl.Extension;
using Opc.Ua;
using Opc.Ua.ISA95;
using Opc.Ua.ISA95.JobControl.V1;
using Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.JobControl.Codec;

/// <summary>The OPC UA message context with the ISA-95 Common Model, Job Control V1 and V2 and the MicroDCS extension.</summary>
public static class JobControlContext
{
    /// <summary>Gets the shared message context. It is immutable after initialisation.</summary>
    public static IServiceMessageContext Shared { get; } = Create();

    private static ServiceMessageContext Create()
    {
        var context = ServiceMessageContext.CreateEmpty(DefaultTelemetry.Create(_ => { }));
        context.NamespaceUris.GetIndexOrAppend(Opc.Ua.ISA95.Namespaces.ISA95);
        context.NamespaceUris.GetIndexOrAppend(Opc.Ua.ISA95.JobControl.V1.Namespaces.ISA95JobControlV1);
        context.NamespaceUris.GetIndexOrAppend(Opc.Ua.ISA95.JobControl.V2.Namespaces.ISA95JobControlV2);
        context.NamespaceUris.GetIndexOrAppend(Extension.Namespaces.jobcontrol);

        var builder = context.Factory.Builder;
        builder.AddOpcUaISA95();
        builder.AddOpcUaISA95JobControlV1();
        builder.AddOpcUaISA95JobControlV2();
        builder.AddMicroDCSJobControlExtension();
        builder.Commit();
        return context;
    }
}

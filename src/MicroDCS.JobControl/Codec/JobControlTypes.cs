namespace MicroDCS.JobControl.Codec;

/// <summary>CloudEvent type ids of the job models (D-34). S17 adds the command types with the processor.</summary>
public static class JobControlTypes
{
    /// <summary>The standard Work Master configuration.</summary>
    public const string WorkMaster = "com.github.aschamberger.ISA95-JOBCONTROL_V2.config.workmaster.v1";

    /// <summary>The extended Work Master: the receiver decodes the subtype directly, so no discriminator is needed in the JSON.</summary>
    public const string WorkMasterExt = "com.github.aschamberger.ISA95-JOBCONTROL_V2.config.workmaster.ext.v1";
}

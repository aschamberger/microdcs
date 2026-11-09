namespace MicroDCS.MachineryJobs;

/// <summary>What a job order must satisfy to be accepted, bound from <c>MicroDcs:JobAcceptance</c>.</summary>
public sealed class JobAcceptanceOptions
{
    /// <summary>The configuration section name.</summary>
    public const string SectionName = "MicroDcs:JobAcceptance";

    /// <summary>Gets or sets a value indicating whether every equipment requirement must be in the scope's equipment list.</summary>
    public bool CheckEquipmentId { get; set; } = true;

    /// <summary>Gets or sets a value indicating whether every material class must be in the scope's list.</summary>
    public bool CheckMaterialClassId { get; set; } = true;

    /// <summary>Gets or sets a value indicating whether every material definition must be in the scope's list.</summary>
    public bool CheckMaterialDefinitionId { get; set; } = true;

    /// <summary>Gets or sets a value indicating whether every personnel requirement must be in the scope's list.</summary>
    public bool CheckPersonnelId { get; set; } = true;

    /// <summary>Gets or sets a value indicating whether every physical asset requirement must be in the scope's list.</summary>
    public bool CheckPhysicalAssetId { get; set; } = true;

    /// <summary>Gets or sets a value indicating whether every Work Master must be known in the scope.</summary>
    public bool CheckWorkMaster { get; set; } = true;

    /// <summary>Gets or sets a value indicating whether the number of job orders of a scope is limited.</summary>
    public bool CheckMaxDownloadableJobOrders { get; set; } = true;

    /// <summary>Gets or sets the limit for a scope that has none of its own; 0 means no limit.</summary>
    public int MaxDownloadableJobOrders { get; set; }
}

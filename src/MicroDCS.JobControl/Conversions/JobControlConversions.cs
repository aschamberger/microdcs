/* ========================================================================
 * Copyright (c) 2005-2026 The OPC Foundation, Inc. All rights reserved.
 *
 * OPC Foundation MIT License 1.00
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * The complete license agreement can be found here:
 * http://opcfoundation.org/License/MIT/1.00/
 * ======================================================================*/

// Adapted from src/Opc.Ua.ISA95.Server/Providers/Internal/Isa95JobControlConversions.cs of OPC Foundation UA-.NETStandard, tag 2.0.1 (commit 07e62f7, D-34).
using MicroDCS.JobControl.Extension;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using Opc.Ua;
using V1 = Opc.Ua.ISA95.JobControl.V1;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.JobControl.Conversions;

/// <summary>
/// Conversions between the version-specific generated Job Control V1/V2 types
/// and the version-neutral records. This type is the only boundary at
/// which generated types are handled.
/// Every standard job-order and job-response field is round-tripped, including
/// the work master, personnel, equipment, physical-asset and material
/// collections, so that an order or response survives a same-version
/// round-trip unchanged. Cross-version conversions are loss-aware only where
/// the two models genuinely diverge (for example the V1 single-string
/// descriptions and the V1-only <c>Loaded</c>/<c>Error</c> response states).
/// </summary>
public static class JobControlConversions
{
    /// <summary>Maps a neutral state to the Job Control V1 state (D-34: Waiting is NotAllowedToStart, Ready is AllowedToStart).</summary>
    /// <param name="state">The neutral state.</param>
    /// <returns>The V1 state.</returns>
    public static V1.ISA95JobOrderStateEnum ToV1State(JobOrderState state)
    {
        return state switch
        {
            JobOrderState.NotAllowedToStartWaiting or JobOrderState.NotAllowedToStartReady or JobOrderState.NotAllowedToStartLoaded => V1.ISA95JobOrderStateEnum.Waiting,
            JobOrderState.AllowedToStartWaiting or JobOrderState.AllowedToStartReady or JobOrderState.AllowedToStartLoaded => V1.ISA95JobOrderStateEnum.Ready,
            JobOrderState.V1Loaded => V1.ISA95JobOrderStateEnum.Loaded,
            JobOrderState.Running => V1.ISA95JobOrderStateEnum.Running,
            JobOrderState.Held => V1.ISA95JobOrderStateEnum.Held,
            JobOrderState.Suspended => V1.ISA95JobOrderStateEnum.Suspended,
            JobOrderState.Completed => V1.ISA95JobOrderStateEnum.Completed,
            JobOrderState.Aborted => V1.ISA95JobOrderStateEnum.Aborted,
            JobOrderState.Closed => V1.ISA95JobOrderStateEnum.Closed,
            JobOrderState.V1Error => V1.ISA95JobOrderStateEnum.Error,
            _ => V1.ISA95JobOrderStateEnum.Undefined
        };
    }

    /// <summary>Maps a Job Control V1 state to a neutral state. <c>Loaded</c> and <c>Error</c> are kept so a V1 response round-trips.</summary>
    /// <param name="state">The V1 state.</param>
    /// <returns>The neutral state; <see cref="JobOrderState.Initial"/> for <c>Undefined</c>.</returns>
    public static JobOrderState FromV1State(V1.ISA95JobOrderStateEnum state)
    {
        return state switch
        {
            V1.ISA95JobOrderStateEnum.Waiting => JobOrderState.NotAllowedToStartWaiting,
            V1.ISA95JobOrderStateEnum.Ready => JobOrderState.AllowedToStartReady,
            V1.ISA95JobOrderStateEnum.Loaded => JobOrderState.V1Loaded,
            V1.ISA95JobOrderStateEnum.Running => JobOrderState.Running,
            V1.ISA95JobOrderStateEnum.Held => JobOrderState.Held,
            V1.ISA95JobOrderStateEnum.Suspended => JobOrderState.Suspended,
            V1.ISA95JobOrderStateEnum.Completed => JobOrderState.Completed,
            V1.ISA95JobOrderStateEnum.Aborted => JobOrderState.Aborted,
            V1.ISA95JobOrderStateEnum.Closed => JobOrderState.Closed,
            V1.ISA95JobOrderStateEnum.Error => JobOrderState.V1Error,
            _ => JobOrderState.Initial
        };
    }

    /// <summary>Maps a Job Control V1 command to an operation.</summary>
    /// <param name="command">The V1 command.</param>
    /// <param name="operation">The operation.</param>
    /// <returns><see langword="false"/> for the undefined or an unrecognized command.</returns>
    public static bool TryMapV1Command(V1.ISA95JobOrderCommandEnum command, out JobOrderOperation operation)
    {
        switch (command)
        {
            case V1.ISA95JobOrderCommandEnum.Store:
                operation = JobOrderOperation.Store;
                return true;
            case V1.ISA95JobOrderCommandEnum.StoreAndStart:
                operation = JobOrderOperation.StoreAndStart;
                return true;
            case V1.ISA95JobOrderCommandEnum.Start:
                operation = JobOrderOperation.Start;
                return true;
            case V1.ISA95JobOrderCommandEnum.Update:
                operation = JobOrderOperation.Update;
                return true;
            case V1.ISA95JobOrderCommandEnum.Stop:
                operation = JobOrderOperation.Stop;
                return true;
            case V1.ISA95JobOrderCommandEnum.Cancel:
                operation = JobOrderOperation.Cancel;
                return true;
            case V1.ISA95JobOrderCommandEnum.Clear:
                operation = JobOrderOperation.Clear;
                return true;
            default:
                operation = default;
                return false;
        }
    }

    /// <summary>Projects an extended Work Master onto a neutral Work Master.</summary>
    /// <param name="workMaster">The extended Work Master.</param>
    /// <returns>The neutral Work Master.</returns>
    public static JobWorkMaster FromWorkMasterExt(ISA95WorkMasterExtDataType workMaster)
    {
        ArgumentNullException.ThrowIfNull(workMaster);

        return new JobWorkMaster
        {
            Id = workMaster.ID,
            Description = FromLocalizedTextDescription(workMaster.Description),
            Parameters = FromV2Parameters(workMaster.Parameters),
            DataSchema = workMaster.DataSchema,
            Data = workMaster.Data,
        };
    }

    /// <summary>Materializes a neutral Work Master as an extended Work Master, with its encoding mask.</summary>
    /// <param name="workMaster">The neutral Work Master.</param>
    /// <returns>The extended Work Master.</returns>
    public static ISA95WorkMasterExtDataType ToWorkMasterExt(JobWorkMaster workMaster)
    {
        ArgumentNullException.ThrowIfNull(workMaster);

        var result = new ISA95WorkMasterExtDataType
        {
            ID = workMaster.Id,
            Description = FirstLocalizedText(workMaster.Description),
            Parameters = ToV2Parameters(workMaster.Parameters),
            DataSchema = workMaster.DataSchema,
            Data = workMaster.Data,
        };
        uint mask = 0;
        if (workMaster.DataSchema is not null)
        {
            mask |= (uint)ISA95WorkMasterExtDataTypeFields.DataSchema;
        }

        if (workMaster.Data is not null)
        {
            mask |= (uint)ISA95WorkMasterExtDataTypeFields.Data;
        }

        result.EncodingMask = mask;
        return result;
    }

    /// <summary>
    /// Projects a Job Control V1 job order onto a neutral job order.
    /// </summary>
    public static JobOrder FromV1Order(V1.ISA95JobOrderDataType order)
    {
        return new JobOrder
        {
            Id = order.ID ?? string.Empty,
            Description = FromStringDescription(order.Description),
            WorkMasters = FromV1WorkMasters(order.WorkMasterID),
            Priority = order.Priority,
            StartTime = order.StartTime,
            EndTime = order.EndTime,
            Parameters = FromV1Parameters(order.JobOrderParameters),
            PersonnelRequirements = FromV1Personnel(order.PersonnelRequirements),
            EquipmentRequirements = FromV1Equipment(order.EquipmentRequirements),
            PhysicalAssetRequirements = FromV1PhysicalAssets(order.PhysicalAssetRequirements),
            MaterialRequirements = FromV1Materials(order.MaterialRequirements)
        };
    }

    /// <summary>
    /// Projects a Job Control V2 job order onto a neutral job order.
    /// </summary>
    public static JobOrder FromV2Order(V2.ISA95JobOrderDataType order)
    {
        return new JobOrder
        {
            Id = order.JobOrderID ?? string.Empty,
            Description = order.Description,
            WorkMasters = FromV2WorkMasters(order.WorkMasterID),
            Priority = order.Priority,
            StartTime = order.StartTime,
            EndTime = order.EndTime,
            Parameters = FromV2Parameters(order.JobOrderParameters),
            PersonnelRequirements = FromV2Personnel(order.PersonnelRequirements),
            EquipmentRequirements = FromV2Equipment(order.EquipmentRequirements),
            PhysicalAssetRequirements = FromV2PhysicalAssets(order.PhysicalAssetRequirements),
            MaterialRequirements = FromV2Materials(order.MaterialRequirements)
        };
    }

    /// <summary>
    /// Projects a Job Control V1 job response onto a neutral job response. The
    /// <see cref="JobResponse.ReceivedAt"/> field is left at its default
    ///.
    /// </summary>
    public static JobResponse FromV1Response(V1.ISA95JobResponseDataType response)
    {
        return new JobResponse
        {
            Id = response.ID ?? string.Empty,
            JobOrderId = response.JobOrderID ?? string.Empty,
            Description = FromStringDescription(response.Description),
            StartTime = response.StartTime,
            EndTime = response.EndTime,
            State = FromV1State(response.JobState),
            ResponseData = FromV1Parameters(response.JobResponseData),
            PersonnelActuals = FromV1Personnel(response.PersonnelActuals),
            EquipmentActuals = FromV1Equipment(response.EquipmentActuals),
            PhysicalAssetActuals = FromV1PhysicalAssets(response.PhysicalAssetActuals),
            MaterialActuals = FromV1Materials(response.MaterialActuals)
        };
    }

    /// <summary>
    /// Projects a Job Control V2 job response onto a neutral job response. The
    /// <see cref="JobResponse.ReceivedAt"/> field is left at its default
    ///.
    /// </summary>
    public static JobResponse FromV2Response(V2.ISA95JobResponseDataType response)
    {
        return new JobResponse
        {
            Id = response.JobResponseID ?? string.Empty,
            JobOrderId = response.JobOrderID ?? string.Empty,
            Description = FromLocalizedTextDescription(response.Description),
            StartTime = response.StartTime,
            EndTime = response.EndTime,
            State = JobStateArray.FromV2(response.JobState),
            ResponseData = FromV2Parameters(response.JobResponseData),
            PersonnelActuals = FromV2Personnel(response.PersonnelActuals),
            EquipmentActuals = FromV2Equipment(response.EquipmentActuals),
            PhysicalAssetActuals = FromV2PhysicalAssets(response.PhysicalAssetActuals),
            MaterialActuals = FromV2Materials(response.MaterialActuals)
        };
    }

    /// <summary>
    /// Materializes a neutral job response as a Job Control V1 job response.
    /// </summary>
    public static V1.ISA95JobResponseDataType ToV1Response(JobResponse response)
    {
        return new V1.ISA95JobResponseDataType
        {
            ID = response.Id,
            JobOrderID = response.JobOrderId,
            Description = FirstText(response.Description),
            StartTime = response.StartTime,
            EndTime = response.EndTime,
            JobState = ToV1State(response.State),
            JobResponseData = ToV1Parameters(response.ResponseData),
            PersonnelActuals = ToV1Personnel(response.PersonnelActuals),
            EquipmentActuals = ToV1Equipment(response.EquipmentActuals),
            PhysicalAssetActuals = ToV1PhysicalAssets(response.PhysicalAssetActuals),
            MaterialActuals = ToV1Materials(response.MaterialActuals)
        };
    }

    /// <summary>
    /// Materializes a neutral job response as a Job Control V2 job response.
    /// </summary>
    public static V2.ISA95JobResponseDataType ToV2Response(JobResponse response)
    {
        var result = new V2.ISA95JobResponseDataType
        {
            JobResponseID = response.Id,
            JobOrderID = response.JobOrderId,
            Description = FirstLocalizedText(response.Description),
            StartTime = response.StartTime,
            EndTime = response.EndTime,
            JobState = JobStateArray.ToV2(response.State),
            JobResponseData = ToV2Parameters(response.ResponseData),
            PersonnelActuals = ToV2Personnel(response.PersonnelActuals),
            EquipmentActuals = ToV2Equipment(response.EquipmentActuals),
            PhysicalAssetActuals = ToV2PhysicalAssets(response.PhysicalAssetActuals),
            MaterialActuals = ToV2Materials(response.MaterialActuals)
        };
        uint mask = 0;
        if (response.Description.Count > 0)
        {
            mask |= (uint)V2.ISA95JobResponseDataTypeFields.Description;
        }
        if (!response.StartTime.Equals(DateTimeUtc.MinValue))
        {
            mask |= (uint)V2.ISA95JobResponseDataTypeFields.StartTime;
        }
        if (!response.EndTime.Equals(DateTimeUtc.MinValue))
        {
            mask |= (uint)V2.ISA95JobResponseDataTypeFields.EndTime;
        }
        if (response.ResponseData.Count > 0)
        {
            mask |= (uint)V2.ISA95JobResponseDataTypeFields.JobResponseData;
        }
        if (response.PersonnelActuals.Count > 0)
        {
            mask |= (uint)V2.ISA95JobResponseDataTypeFields.PersonnelActuals;
        }
        if (response.EquipmentActuals.Count > 0)
        {
            mask |= (uint)V2.ISA95JobResponseDataTypeFields.EquipmentActuals;
        }
        if (response.PhysicalAssetActuals.Count > 0)
        {
            mask |= (uint)V2.ISA95JobResponseDataTypeFields.PhysicalAssetActuals;
        }
        if (response.MaterialActuals.Count > 0)
        {
            mask |= (uint)V2.ISA95JobResponseDataTypeFields.MaterialActuals;
        }
        result.EncodingMask = mask;
        return result;
    }

    /// <summary>
    /// Materializes a neutral job order as a Job Control V2 job order.
    /// </summary>
    public static V2.ISA95JobOrderDataType ToV2Order(JobOrder order)
    {
        var result = new V2.ISA95JobOrderDataType
        {
            JobOrderID = order.Id,
            Description = order.Description,
            WorkMasterID = ToV2WorkMasters(order.WorkMasters),
            Priority = order.Priority,
            StartTime = order.StartTime,
            EndTime = order.EndTime,
            JobOrderParameters = ToV2Parameters(order.Parameters),
            PersonnelRequirements = ToV2Personnel(order.PersonnelRequirements),
            EquipmentRequirements = ToV2Equipment(order.EquipmentRequirements),
            PhysicalAssetRequirements = ToV2PhysicalAssets(order.PhysicalAssetRequirements),
            MaterialRequirements = ToV2Materials(order.MaterialRequirements)
        };
        uint mask = 0;
        if (order.Description.Count > 0)
        {
            mask |= (uint)V2.ISA95JobOrderDataTypeFields.Description;
        }
        if (order.WorkMasters.Count > 0)
        {
            mask |= (uint)V2.ISA95JobOrderDataTypeFields.WorkMasterID;
        }
        if (!order.StartTime.Equals(DateTimeUtc.MinValue))
        {
            mask |= (uint)V2.ISA95JobOrderDataTypeFields.StartTime;
        }
        if (!order.EndTime.Equals(DateTimeUtc.MinValue))
        {
            mask |= (uint)V2.ISA95JobOrderDataTypeFields.EndTime;
        }
        if (order.Priority != 0)
        {
            mask |= (uint)V2.ISA95JobOrderDataTypeFields.Priority;
        }
        if (order.Parameters.Count > 0)
        {
            mask |= (uint)V2.ISA95JobOrderDataTypeFields.JobOrderParameters;
        }
        if (order.PersonnelRequirements.Count > 0)
        {
            mask |= (uint)V2.ISA95JobOrderDataTypeFields.PersonnelRequirements;
        }
        if (order.EquipmentRequirements.Count > 0)
        {
            mask |= (uint)V2.ISA95JobOrderDataTypeFields.EquipmentRequirements;
        }
        if (order.PhysicalAssetRequirements.Count > 0)
        {
            mask |= (uint)V2.ISA95JobOrderDataTypeFields.PhysicalAssetRequirements;
        }
        if (order.MaterialRequirements.Count > 0)
        {
            mask |= (uint)V2.ISA95JobOrderDataTypeFields.MaterialRequirements;
        }
        result.EncodingMask = mask;
        return result;
    }

    /// <summary>
    /// Materializes a neutral job order as a Job Control V1 job order.
    /// </summary>
    public static V1.ISA95JobOrderDataType ToV1Order(JobOrder order)
    {
        return new V1.ISA95JobOrderDataType
        {
            ID = order.Id,
            Description = FirstText(order.Description),
            WorkMasterID = ToV1WorkMasters(order.WorkMasters),
            Priority = order.Priority,
            StartTime = order.StartTime,
            EndTime = order.EndTime,
            JobOrderParameters = ToV1Parameters(order.Parameters),
            PersonnelRequirements = ToV1Personnel(order.PersonnelRequirements),
            EquipmentRequirements = ToV1Equipment(order.EquipmentRequirements),
            PhysicalAssetRequirements = ToV1PhysicalAssets(order.PhysicalAssetRequirements),
            MaterialRequirements = ToV1Materials(order.MaterialRequirements)
        };
    }

    private static ArrayOf<JobParameter> FromV1Parameters(ArrayOf<V1.ISA95ParameterDataType> parameters)
    {
        if (parameters.Count == 0)
        {
            return [];
        }
        var result = new List<JobParameter>(parameters.Count);
        foreach (V1.ISA95ParameterDataType parameter in parameters)
        {
            result.Add(new JobParameter
            {
                Id = parameter.ID,
                Value = parameter.Value,
                Description = FromStringDescription(parameter.Description),
                EngineeringUnits = FromV1Unit(parameter.UoM),
                Subparameters = FromV1Parameters(parameter.Subparameters)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobParameter> FromV2Parameters(ArrayOf<V2.ISA95ParameterDataType> parameters)
    {
        if (parameters.Count == 0)
        {
            return [];
        }
        var result = new List<JobParameter>(parameters.Count);
        foreach (V2.ISA95ParameterDataType parameter in parameters)
        {
            result.Add(new JobParameter
            {
                Id = parameter.ID,
                Value = parameter.Value,
                Description = parameter.Description,
                EngineeringUnits = FromV2Unit(parameter.EngineeringUnits),
                Subparameters = FromV2Parameters(parameter.Subparameters)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V1.ISA95ParameterDataType> ToV1Parameters(ArrayOf<JobParameter> parameters)
    {
        if (parameters.Count == 0)
        {
            return [];
        }
        var result = new List<V1.ISA95ParameterDataType>(parameters.Count);
        foreach (JobParameter parameter in parameters)
        {
            result.Add(new V1.ISA95ParameterDataType
            {
                ID = parameter.Id,
                Value = parameter.Value,
                Description = FirstText(parameter.Description),
                UoM = ToV1Unit(parameter.EngineeringUnits),
                Subparameters = ToV1Parameters(parameter.Subparameters)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V2.ISA95ParameterDataType> ToV2Parameters(ArrayOf<JobParameter> parameters)
    {
        if (parameters.Count == 0)
        {
            return [];
        }
        var result = new List<V2.ISA95ParameterDataType>(parameters.Count);
        foreach (JobParameter parameter in parameters)
        {
            uint mask = 0;
            if (parameter.Description.Count > 0)
            {
                mask |= (uint)V2.ISA95ParameterDataTypeFields.Description;
            }
            if (!IsEmptyUnit(parameter.EngineeringUnits))
            {
                mask |= (uint)V2.ISA95ParameterDataTypeFields.EngineeringUnits;
            }
            if (parameter.Subparameters.Count > 0)
            {
                mask |= (uint)V2.ISA95ParameterDataTypeFields.Subparameters;
            }
            result.Add(new V2.ISA95ParameterDataType
            {
                ID = parameter.Id,
                Value = parameter.Value,
                Description = parameter.Description,
                EngineeringUnits = ToV2Unit(parameter.EngineeringUnits),
                Subparameters = ToV2Parameters(parameter.Subparameters),
                EncodingMask = mask
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobProperty> FromV1Properties(ArrayOf<V1.ISA95PropertyDataType> properties)
    {
        if (properties.Count == 0)
        {
            return [];
        }
        var result = new List<JobProperty>(properties.Count);
        foreach (V1.ISA95PropertyDataType property in properties)
        {
            result.Add(new JobProperty
            {
                Id = property.ID,
                Value = property.Value,
                Description = FromStringDescription(property.Description),
                EngineeringUnits = FromV1Unit(property.UoM),
                Subproperties = FromV1Properties(property.Subproperties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobProperty> FromV2Properties(ArrayOf<V2.ISA95PropertyDataType> properties)
    {
        if (properties.Count == 0)
        {
            return [];
        }
        var result = new List<JobProperty>(properties.Count);
        foreach (V2.ISA95PropertyDataType property in properties)
        {
            result.Add(new JobProperty
            {
                Id = property.ID,
                Value = property.Value,
                Description = property.Description,
                EngineeringUnits = FromV2Unit(property.EngineeringUnits),
                Subproperties = FromV2Properties(property.Subproperties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V1.ISA95PropertyDataType> ToV1Properties(ArrayOf<JobProperty> properties)
    {
        if (properties.Count == 0)
        {
            return [];
        }
        var result = new List<V1.ISA95PropertyDataType>(properties.Count);
        foreach (JobProperty property in properties)
        {
            result.Add(new V1.ISA95PropertyDataType
            {
                ID = property.Id,
                Value = property.Value,
                Description = FirstText(property.Description),
                UoM = ToV1Unit(property.EngineeringUnits),
                Subproperties = ToV1Properties(property.Subproperties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V2.ISA95PropertyDataType> ToV2Properties(ArrayOf<JobProperty> properties)
    {
        if (properties.Count == 0)
        {
            return [];
        }
        var result = new List<V2.ISA95PropertyDataType>(properties.Count);
        foreach (JobProperty property in properties)
        {
            uint mask = 0;
            if (property.Description.Count > 0)
            {
                mask |= (uint)V2.ISA95PropertyDataTypeFields.Description;
            }
            if (!IsEmptyUnit(property.EngineeringUnits))
            {
                mask |= (uint)V2.ISA95PropertyDataTypeFields.EngineeringUnits;
            }
            if (property.Subproperties.Count > 0)
            {
                mask |= (uint)V2.ISA95PropertyDataTypeFields.Subproperties;
            }
            result.Add(new V2.ISA95PropertyDataType
            {
                ID = property.Id,
                Value = property.Value,
                Description = property.Description,
                EngineeringUnits = ToV2Unit(property.EngineeringUnits),
                Subproperties = ToV2Properties(property.Subproperties),
                EncodingMask = mask
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobWorkMaster> FromV1WorkMasters(ArrayOf<V1.ISA95WorkMasterDataType> workMasters)
    {
        if (workMasters.Count == 0)
        {
            return [];
        }
        var result = new List<JobWorkMaster>(workMasters.Count);
        foreach (V1.ISA95WorkMasterDataType workMaster in workMasters)
        {
            result.Add(new JobWorkMaster
            {
                Id = workMaster.ID,
                Description = FromStringDescription(workMaster.Description),
                Parameters = FromV1Parameters(workMaster.Parameters)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobWorkMaster> FromV2WorkMasters(ArrayOf<V2.ISA95WorkMasterDataType> workMasters)
    {
        if (workMasters.Count == 0)
        {
            return [];
        }
        var result = new List<JobWorkMaster>(workMasters.Count);
        foreach (V2.ISA95WorkMasterDataType workMaster in workMasters)
        {
            result.Add(new JobWorkMaster
            {
                Id = workMaster.ID,
                Description = FromLocalizedTextDescription(workMaster.Description),
                Parameters = FromV2Parameters(workMaster.Parameters)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V1.ISA95WorkMasterDataType> ToV1WorkMasters(ArrayOf<JobWorkMaster> workMasters)
    {
        if (workMasters.Count == 0)
        {
            return [];
        }
        var result = new List<V1.ISA95WorkMasterDataType>(workMasters.Count);
        foreach (JobWorkMaster workMaster in workMasters)
        {
            result.Add(new V1.ISA95WorkMasterDataType
            {
                ID = workMaster.Id,
                Description = FirstText(workMaster.Description),
                Parameters = ToV1Parameters(workMaster.Parameters)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V2.ISA95WorkMasterDataType> ToV2WorkMasters(ArrayOf<JobWorkMaster> workMasters)
    {
        if (workMasters.Count == 0)
        {
            return [];
        }
        var result = new List<V2.ISA95WorkMasterDataType>(workMasters.Count);
        foreach (JobWorkMaster workMaster in workMasters)
        {
            uint mask = 0;
            if (workMaster.Description.Count > 0)
            {
                mask |= (uint)V2.ISA95WorkMasterDataTypeFields.Description;
            }
            if (workMaster.Parameters.Count > 0)
            {
                mask |= (uint)V2.ISA95WorkMasterDataTypeFields.Parameters;
            }
            result.Add(new V2.ISA95WorkMasterDataType
            {
                ID = workMaster.Id,
                Description = FirstLocalizedText(workMaster.Description),
                Parameters = ToV2Parameters(workMaster.Parameters),
                EncodingMask = mask
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobResource> FromV1Personnel(ArrayOf<V1.ISA95PersonnelDataType> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<JobResource>(resources.Count);
        foreach (V1.ISA95PersonnelDataType resource in resources)
        {
            result.Add(new JobResource
            {
                Id = resource.ID,
                Description = FromStringDescription(resource.Description),
                Use = resource.PersonnelUse,
                Quantity = resource.Quantity,
                EngineeringUnits = FromV1Unit(resource.UoM),
                Properties = FromV1Properties(resource.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobResource> FromV2Personnel(ArrayOf<V2.ISA95PersonnelDataType> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<JobResource>(resources.Count);
        foreach (V2.ISA95PersonnelDataType resource in resources)
        {
            result.Add(new JobResource
            {
                Id = resource.ID,
                Description = resource.Description,
                Use = resource.PersonnelUse,
                Quantity = resource.Quantity,
                EngineeringUnits = FromV2Unit(resource.EngineeringUnits),
                Properties = FromV2Properties(resource.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V1.ISA95PersonnelDataType> ToV1Personnel(ArrayOf<JobResource> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<V1.ISA95PersonnelDataType>(resources.Count);
        foreach (JobResource resource in resources)
        {
            result.Add(new V1.ISA95PersonnelDataType
            {
                ID = resource.Id,
                Description = FirstText(resource.Description),
                PersonnelUse = resource.Use,
                Quantity = resource.Quantity,
                UoM = ToV1Unit(resource.EngineeringUnits),
                Properties = ToV1Properties(resource.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V2.ISA95PersonnelDataType> ToV2Personnel(ArrayOf<JobResource> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<V2.ISA95PersonnelDataType>(resources.Count);
        foreach (JobResource resource in resources)
        {
            uint mask = 0;
            if (resource.Description.Count > 0)
            {
                mask |= (uint)V2.ISA95PersonnelDataTypeFields.Description;
            }
            if (!string.IsNullOrEmpty(resource.Use))
            {
                mask |= (uint)V2.ISA95PersonnelDataTypeFields.PersonnelUse;
            }
            if (!string.IsNullOrEmpty(resource.Quantity))
            {
                mask |= (uint)V2.ISA95PersonnelDataTypeFields.Quantity;
            }
            if (!IsEmptyUnit(resource.EngineeringUnits))
            {
                mask |= (uint)V2.ISA95PersonnelDataTypeFields.EngineeringUnits;
            }
            if (resource.Properties.Count > 0)
            {
                mask |= (uint)V2.ISA95PersonnelDataTypeFields.Properties;
            }
            result.Add(new V2.ISA95PersonnelDataType
            {
                ID = resource.Id,
                Description = resource.Description,
                PersonnelUse = resource.Use,
                Quantity = resource.Quantity,
                EngineeringUnits = ToV2Unit(resource.EngineeringUnits),
                Properties = ToV2Properties(resource.Properties),
                EncodingMask = mask
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobResource> FromV1Equipment(ArrayOf<V1.ISA95EquipmentDataType> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<JobResource>(resources.Count);
        foreach (V1.ISA95EquipmentDataType resource in resources)
        {
            result.Add(new JobResource
            {
                Id = resource.ID,
                Description = FromStringDescription(resource.Description),
                Use = resource.EquipmentUse,
                Quantity = resource.Quantity,
                EngineeringUnits = FromV1Unit(resource.UoM),
                Properties = FromV1Properties(resource.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobResource> FromV2Equipment(ArrayOf<V2.ISA95EquipmentDataType> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<JobResource>(resources.Count);
        foreach (V2.ISA95EquipmentDataType resource in resources)
        {
            result.Add(new JobResource
            {
                Id = resource.ID,
                Description = resource.Description,
                Use = resource.EquipmentUse,
                Quantity = resource.Quantity,
                EngineeringUnits = FromV2Unit(resource.EngineeringUnits),
                Properties = FromV2Properties(resource.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V1.ISA95EquipmentDataType> ToV1Equipment(ArrayOf<JobResource> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<V1.ISA95EquipmentDataType>(resources.Count);
        foreach (JobResource resource in resources)
        {
            result.Add(new V1.ISA95EquipmentDataType
            {
                ID = resource.Id,
                Description = FirstText(resource.Description),
                EquipmentUse = resource.Use,
                Quantity = resource.Quantity,
                UoM = ToV1Unit(resource.EngineeringUnits),
                Properties = ToV1Properties(resource.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V2.ISA95EquipmentDataType> ToV2Equipment(ArrayOf<JobResource> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<V2.ISA95EquipmentDataType>(resources.Count);
        foreach (JobResource resource in resources)
        {
            uint mask = 0;
            if (resource.Description.Count > 0)
            {
                mask |= (uint)V2.ISA95EquipmentDataTypeFields.Description;
            }
            if (!string.IsNullOrEmpty(resource.Use))
            {
                mask |= (uint)V2.ISA95EquipmentDataTypeFields.EquipmentUse;
            }
            if (!string.IsNullOrEmpty(resource.Quantity))
            {
                mask |= (uint)V2.ISA95EquipmentDataTypeFields.Quantity;
            }
            if (!IsEmptyUnit(resource.EngineeringUnits))
            {
                mask |= (uint)V2.ISA95EquipmentDataTypeFields.EngineeringUnits;
            }
            if (resource.Properties.Count > 0)
            {
                mask |= (uint)V2.ISA95EquipmentDataTypeFields.Properties;
            }
            result.Add(new V2.ISA95EquipmentDataType
            {
                ID = resource.Id,
                Description = resource.Description,
                EquipmentUse = resource.Use,
                Quantity = resource.Quantity,
                EngineeringUnits = ToV2Unit(resource.EngineeringUnits),
                Properties = ToV2Properties(resource.Properties),
                EncodingMask = mask
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobResource> FromV1PhysicalAssets(
        ArrayOf<V1.ISA95PhysicalAssetDataType> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<JobResource>(resources.Count);
        foreach (V1.ISA95PhysicalAssetDataType resource in resources)
        {
            result.Add(new JobResource
            {
                Id = resource.ID,
                Description = FromStringDescription(resource.Description),
                Use = resource.PhysicalAssetUse,
                Quantity = resource.Quantity,
                EngineeringUnits = FromV1Unit(resource.UoM),
                Properties = FromV1Properties(resource.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobResource> FromV2PhysicalAssets(
        ArrayOf<V2.ISA95PhysicalAssetDataType> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<JobResource>(resources.Count);
        foreach (V2.ISA95PhysicalAssetDataType resource in resources)
        {
            result.Add(new JobResource
            {
                Id = resource.ID,
                Description = resource.Description,
                Use = resource.PhysicalAssetUse,
                Quantity = resource.Quantity,
                EngineeringUnits = FromV2Unit(resource.EngineeringUnits),
                Properties = FromV2Properties(resource.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V1.ISA95PhysicalAssetDataType> ToV1PhysicalAssets(
        ArrayOf<JobResource> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<V1.ISA95PhysicalAssetDataType>(resources.Count);
        foreach (JobResource resource in resources)
        {
            result.Add(new V1.ISA95PhysicalAssetDataType
            {
                ID = resource.Id,
                Description = FirstText(resource.Description),
                PhysicalAssetUse = resource.Use,
                Quantity = resource.Quantity,
                UoM = ToV1Unit(resource.EngineeringUnits),
                Properties = ToV1Properties(resource.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V2.ISA95PhysicalAssetDataType> ToV2PhysicalAssets(
        ArrayOf<JobResource> resources)
    {
        if (resources.Count == 0)
        {
            return [];
        }
        var result = new List<V2.ISA95PhysicalAssetDataType>(resources.Count);
        foreach (JobResource resource in resources)
        {
            uint mask = 0;
            if (resource.Description.Count > 0)
            {
                mask |= (uint)V2.ISA95PhysicalAssetDataTypeFields.Description;
            }
            if (!string.IsNullOrEmpty(resource.Use))
            {
                mask |= (uint)V2.ISA95PhysicalAssetDataTypeFields.PhysicalAssetUse;
            }
            if (!string.IsNullOrEmpty(resource.Quantity))
            {
                mask |= (uint)V2.ISA95PhysicalAssetDataTypeFields.Quantity;
            }
            if (!IsEmptyUnit(resource.EngineeringUnits))
            {
                mask |= (uint)V2.ISA95PhysicalAssetDataTypeFields.EngineeringUnits;
            }
            if (resource.Properties.Count > 0)
            {
                mask |= (uint)V2.ISA95PhysicalAssetDataTypeFields.Properties;
            }
            result.Add(new V2.ISA95PhysicalAssetDataType
            {
                ID = resource.Id,
                Description = resource.Description,
                PhysicalAssetUse = resource.Use,
                Quantity = resource.Quantity,
                EngineeringUnits = ToV2Unit(resource.EngineeringUnits),
                Properties = ToV2Properties(resource.Properties),
                EncodingMask = mask
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobMaterial> FromV1Materials(ArrayOf<V1.ISA95MaterialDataType> materials)
    {
        if (materials.Count == 0)
        {
            return [];
        }
        var result = new List<JobMaterial>(materials.Count);
        foreach (V1.ISA95MaterialDataType material in materials)
        {
            result.Add(new JobMaterial
            {
                MaterialClassId = material.MaterialClassID,
                MaterialDefinitionId = material.MaterialDefinitionID,
                MaterialLotId = material.MaterialLotID,
                MaterialSublotId = material.MaterialSublotID,
                Description = FromStringDescription(material.Description),
                Use = material.MaterialUse,
                Quantity = material.Quantity,
                EngineeringUnits = FromV1Unit(material.UoM),
                Properties = FromV1Properties(material.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<JobMaterial> FromV2Materials(ArrayOf<V2.ISA95MaterialDataType> materials)
    {
        if (materials.Count == 0)
        {
            return [];
        }
        var result = new List<JobMaterial>(materials.Count);
        foreach (V2.ISA95MaterialDataType material in materials)
        {
            result.Add(new JobMaterial
            {
                MaterialClassId = material.MaterialClassID,
                MaterialDefinitionId = material.MaterialDefinitionID,
                MaterialLotId = material.MaterialLotID,
                MaterialSublotId = material.MaterialSublotID,
                Description = material.Description,
                Use = material.MaterialUse,
                Quantity = material.Quantity,
                EngineeringUnits = FromV2Unit(material.EngineeringUnits),
                Properties = FromV2Properties(material.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V1.ISA95MaterialDataType> ToV1Materials(ArrayOf<JobMaterial> materials)
    {
        if (materials.Count == 0)
        {
            return [];
        }
        var result = new List<V1.ISA95MaterialDataType>(materials.Count);
        foreach (JobMaterial material in materials)
        {
            result.Add(new V1.ISA95MaterialDataType
            {
                MaterialClassID = material.MaterialClassId,
                MaterialDefinitionID = material.MaterialDefinitionId,
                MaterialLotID = material.MaterialLotId,
                MaterialSublotID = material.MaterialSublotId,
                Description = FirstText(material.Description),
                MaterialUse = material.Use,
                Quantity = material.Quantity,
                UoM = ToV1Unit(material.EngineeringUnits),
                Properties = ToV1Properties(material.Properties)
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<V2.ISA95MaterialDataType> ToV2Materials(ArrayOf<JobMaterial> materials)
    {
        if (materials.Count == 0)
        {
            return [];
        }
        var result = new List<V2.ISA95MaterialDataType>(materials.Count);
        foreach (JobMaterial material in materials)
        {
            uint mask = 0;
            if (!string.IsNullOrEmpty(material.MaterialClassId))
            {
                mask |= (uint)V2.ISA95MaterialDataTypeFields.MaterialClassID;
            }
            if (!string.IsNullOrEmpty(material.MaterialDefinitionId))
            {
                mask |= (uint)V2.ISA95MaterialDataTypeFields.MaterialDefinitionID;
            }
            if (!string.IsNullOrEmpty(material.MaterialLotId))
            {
                mask |= (uint)V2.ISA95MaterialDataTypeFields.MaterialLotID;
            }
            if (!string.IsNullOrEmpty(material.MaterialSublotId))
            {
                mask |= (uint)V2.ISA95MaterialDataTypeFields.MaterialSublotID;
            }
            if (material.Description.Count > 0)
            {
                mask |= (uint)V2.ISA95MaterialDataTypeFields.Description;
            }
            if (!string.IsNullOrEmpty(material.Use))
            {
                mask |= (uint)V2.ISA95MaterialDataTypeFields.MaterialUse;
            }
            if (!string.IsNullOrEmpty(material.Quantity))
            {
                mask |= (uint)V2.ISA95MaterialDataTypeFields.Quantity;
            }
            if (!IsEmptyUnit(material.EngineeringUnits))
            {
                mask |= (uint)V2.ISA95MaterialDataTypeFields.EngineeringUnits;
            }
            if (material.Properties.Count > 0)
            {
                mask |= (uint)V2.ISA95MaterialDataTypeFields.Properties;
            }
            result.Add(new V2.ISA95MaterialDataType
            {
                MaterialClassID = material.MaterialClassId,
                MaterialDefinitionID = material.MaterialDefinitionId,
                MaterialLotID = material.MaterialLotId,
                MaterialSublotID = material.MaterialSublotId,
                Description = material.Description,
                MaterialUse = material.Use,
                Quantity = material.Quantity,
                EngineeringUnits = ToV2Unit(material.EngineeringUnits),
                Properties = ToV2Properties(material.Properties),
                EncodingMask = mask
            });
        }
        return result.ToArrayOf();
    }

    private static ArrayOf<LocalizedText> FromStringDescription(string? description)
    {
        if (string.IsNullOrEmpty(description))
        {
            return [];
        }
        return new[] { new LocalizedText(description) }.ToArrayOf();
    }

    private static ArrayOf<LocalizedText> FromLocalizedTextDescription(LocalizedText description)
    {
        if (description.IsNull)
        {
            return [];
        }
        return new[] { description }.ToArrayOf();
    }

    private static string? FirstText(ArrayOf<LocalizedText> description)
    {
        return description.Count == 0 ? null : description[0].Text;
    }

    private static LocalizedText FirstLocalizedText(ArrayOf<LocalizedText> description)
    {
        return description.Count == 0 ? LocalizedText.Null : description[0];
    }

    private static EUInformation? FromV1Unit(string? unit)
    {
        if (string.IsNullOrEmpty(unit))
        {
            return null;
        }
        return new EUInformation { DisplayName = new LocalizedText(unit) };
    }

    private static EUInformation? FromV2Unit(EUInformation engineeringUnits)
    {
        return IsEmptyUnit(engineeringUnits) ? null : engineeringUnits;
    }

    private static string? ToV1Unit(EUInformation? engineeringUnits)
    {
        if (engineeringUnits == null || engineeringUnits.DisplayName.IsNull)
        {
            return null;
        }
        return engineeringUnits.DisplayName.Text;
    }

    private static EUInformation ToV2Unit(EUInformation? engineeringUnits)
    {
        return engineeringUnits ?? new EUInformation();
    }

    private static bool IsEmptyUnit(EUInformation? engineeringUnits)
    {
        return engineeringUnits == null ||
            (string.IsNullOrEmpty(engineeringUnits.NamespaceUri) &&
                engineeringUnits.UnitId == 0 &&
                engineeringUnits.DisplayName.IsNull &&
                engineeringUnits.Description.IsNull);
    }
}

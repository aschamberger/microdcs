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

// Adapted from src/Opc.Ua.ISA95.Server/Providers/Internal/Isa95JobResponse.cs of OPC Foundation UA-.NETStandard, tag 2.0.1 (commit 07e62f7, D-34).
using MicroDCS.JobControl.StateMachine;
using Opc.Ua;

namespace MicroDCS.JobControl.Model;

/// <summary>
/// A version-neutral job response of the version-neutral model. Every
/// standard Job Control V1/V2 job-response field is captured, including the
/// personnel, equipment, physical-asset and material actual collections, so
/// that responses round-trip losslessly within a version.
/// </summary>
public sealed record JobResponse
{
    /// <summary>The identifier of the job response.</summary>
    public required string Id { get; init; }
    /// <summary>The identifier of the job order this response belongs to.</summary>
    public required string JobOrderId { get; init; }
    /// <summary>The localized descriptions of the job response.</summary>
    public ArrayOf<LocalizedText> Description { get; init; } = [];
    /// <summary>The actual start time.</summary>
    public DateTimeUtc StartTime { get; init; }
    /// <summary>The actual end time.</summary>
    public DateTimeUtc EndTime { get; init; }
    /// <summary>The state of the job order.</summary>
    public JobOrderState State { get; init; }
    /// <summary>The job response data.</summary>
    public ArrayOf<JobParameter> ResponseData { get; init; } = [];
    /// <summary>The personnel actuals.</summary>
    public ArrayOf<JobResource> PersonnelActuals { get; init; } = [];
    /// <summary>The equipment actuals.</summary>
    public ArrayOf<JobResource> EquipmentActuals { get; init; } = [];
    /// <summary>The physical-asset actuals.</summary>
    public ArrayOf<JobResource> PhysicalAssetActuals { get; init; } = [];
    /// <summary>The material actuals.</summary>
    public ArrayOf<JobMaterial> MaterialActuals { get; init; } = [];
    /// <summary>The time the response was received; set by the caller, not part of the specification types.</summary>
    public DateTimeUtc ReceivedAt { get; init; }
}

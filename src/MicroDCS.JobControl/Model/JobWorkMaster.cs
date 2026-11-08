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

// Adapted from src/Opc.Ua.ISA95.Server/Providers/Internal/Isa95WorkMaster.cs of OPC Foundation UA-.NETStandard, tag 2.0.1 (commit 07e62f7, D-34).
using Opc.Ua;

namespace MicroDCS.JobControl.Model;

/// <summary>
/// A version-neutral work master reference of the version-neutral model.
/// Work masters are referenced by a job order in both Job Control V1 and V2.
/// V1 carries a single string description; V2 carries a single localized text,
/// both unified onto <see cref="Description"/>.
/// </summary>
public sealed record JobWorkMaster
{
    /// <summary>
    /// The identifier of the work master.
    /// </summary>
    public string? Id { get; init; }

    /// <summary>
    /// The localized descriptions of the work master.
    /// </summary>
    public ArrayOf<LocalizedText> Description { get; init; } = [];

    /// <summary>
    /// The parameters of the work master.
    /// </summary>
    public ArrayOf<JobParameter> Parameters { get; init; } = [];

    /// <summary>
    /// Identifies the structure of <see cref="Data"/>. Set only on an extended Work Master, which has its own CloudEvent type (D-34).
    /// </summary>
    public string? DataSchema { get; init; }

    /// <summary>The opaque recipe content of an extended Work Master, as text.</summary>
    public string? Data { get; init; }
}

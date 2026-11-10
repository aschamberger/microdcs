using System.Text.Json.Serialization;

namespace MicroDCS.Inspection;

/// <summary>One job order in a list (<c>GET /api/v1/jobs</c>).</summary>
/// <param name="Id">The job order id.</param>
/// <param name="Scope">The scope.</param>
/// <param name="State">The job order state (ISA-95 Job Control V2 name, for example <c>Running</c>).</param>
/// <param name="Version">The document version; it grows with every change.</param>
/// <param name="Priority">The priority.</param>
public sealed record JobSummary(string Id, string Scope, string State, long Version, int Priority);

/// <summary>The answer of <c>GET /api/v1/jobs</c>.</summary>
/// <param name="Jobs">The job orders, at most the requested limit.</param>
public sealed record JobList(IReadOnlyList<JobSummary> Jobs);

/// <summary>The recipe state of a job (present when the SFC host runs and the job has a recipe).</summary>
/// <param name="Status">The recipe status, for example <c>Running</c> or <c>Completed</c>.</param>
/// <param name="Current">The last step that became active.</param>
/// <param name="ActiveSteps">The active steps.</param>
/// <param name="NextDeadline">The next deadline (ISO 8601), if any.</param>
/// <param name="HeldSince">Since when the recipe is held (ISO 8601), if it is.</param>
/// <param name="FaultCode">The fault code of a failed recipe.</param>
public sealed record RecipeState(string Status, string? Current, IReadOnlyList<string> ActiveSteps, string? NextDeadline, string? HeldSince, string? FaultCode);

/// <summary>The answer of <c>GET /api/v1/jobs/{id}</c>.</summary>
/// <param name="Id">The job order id.</param>
/// <param name="Scope">The scope.</param>
/// <param name="State">The job order state.</param>
/// <param name="Version">The document version.</param>
/// <param name="Priority">The priority.</param>
/// <param name="ResponseState">The state of the job response, if there is one.</param>
/// <param name="Recipe">The recipe state, if any.</param>
public sealed record JobDetail(string Id, string Scope, string State, long Version, int Priority, string? ResponseState, RecipeState? Recipe);

/// <summary>An RFC 9457 problem.</summary>
/// <param name="Type">The problem type.</param>
/// <param name="Title">A short summary.</param>
/// <param name="Status">The HTTP status.</param>
/// <param name="Detail">What went wrong; safe to show.</param>
public sealed record Problem(string Type, string Title, int Status, string Detail);

/// <summary>Source-generated JSON for the REST API (AOT).</summary>
[JsonSourceGenerationOptions(WriteIndented = false, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull, PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(JobList))]
[JsonSerializable(typeof(JobDetail))]
[JsonSerializable(typeof(Problem))]
internal sealed partial class RestJsonContext : JsonSerializerContext;

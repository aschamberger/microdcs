using System.Globalization;
using System.Text.Json;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MicroDCS.Inspection;

/// <summary>
/// The read-only REST API under <c>/api/v1</c> (D-54, R-19): job orders by scope and one job with its response and recipe
/// state. A thin adapter over <see cref="InspectionService"/>, so audit, result cap, redaction and the rate limit apply; the
/// DTOs are the stable contract, not the inspector fields. On when <c>MicroDcs:Management:ApiEnabled</c> is true; off
/// loopback it needs the bearer token like every other route.
/// </summary>
internal sealed class RestApiModule : IManagementModule
{
    /// <summary>The audit caller and the rate-limit window of the REST API.</summary>
    internal const string Caller = "rest";

    internal const string JobsRoute = "/api/v1/jobs";
    internal const string JobRoute = "/api/v1/jobs/{id}";
    internal const string OpenApiRoute = "/api/v1/openapi.json";

    private const string JobsInspector = "jobs";
    private const string SfcInspector = "sfc";

    public bool IsEnabled(IServiceProvider application) =>
        application.GetRequiredService<IOptions<MicroDcsOptions>>().Value.Management.ApiEnabled;

    public void ConfigureServices(IServiceCollection management, IServiceProvider application)
    {
        // The listener copies InspectionService and the options into its host; nothing else is needed.
    }

    public void Map(IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        endpoints.MapGet(OpenApiRoute, static context => WriteOpenApiAsync(context));
        endpoints.MapGet(JobsRoute, static context => GuardAsync(context, ListJobsAsync));
        endpoints.MapGet(JobRoute, static context => GuardAsync(context, GetJobAsync));
    }

    internal static JobSummary ToSummary(InspectionEntry entry) => new(
        entry.Key,
        Text(entry, "scope"),
        Text(entry, "state"),
        Number(entry, "version"),
        (int)Number(entry, "priority"));

    internal static JobDetail ToDetail(InspectionEntry order, InspectionEntry? recipe) => new(
        order.Key,
        Text(order, "scope"),
        Text(order, "state"),
        Number(order, "version"),
        (int)Number(order, "priority"),
        // The inspector shows Initial when there is no response; the contract says null (no response yet).
        Text(order, "responseId").Length == 0 ? null : NullIfEmpty(Text(order, "responseState")),
        recipe is null ? null : new RecipeState(
            Text(recipe, "status"),
            NullIfEmpty(Text(recipe, "current")),
            [.. Text(recipe, "activeSteps").Split(',', StringSplitOptions.RemoveEmptyEntries)],
            NullIfEmpty(Text(recipe, "nextDeadline")),
            NullIfEmpty(Text(recipe, "heldSince")),
            NullIfEmpty(Text(recipe, "faultCode"))));

    private static string Text(InspectionEntry entry, string field) =>
        entry.Fields.TryGetValue(field, out var value) ? value.Text : string.Empty;

    private static long Number(InspectionEntry entry, string field) =>
        long.TryParse(Text(entry, field), NumberStyles.Integer, CultureInfo.InvariantCulture, out var number) ? number : 0;

    private static string? NullIfEmpty(string text) => text.Length == 0 ? null : text;

    private static async Task GuardAsync(HttpContext context, Func<HttpContext, InspectionService, Task> handler)
    {
        var service = context.RequestServices.GetRequiredService<InspectionService>();
        if (!service.InspectorNames.Contains(JobsInspector))
        {
            await WriteProblemAsync(context, StatusCodes.Status404NotFound, "Not available", "This instance does not run the Machinery Jobs processor.").ConfigureAwait(false);
            return;
        }

        try
        {
            await handler(context, service).ConfigureAwait(false);
        }
        catch (InspectionRefusedException ex) when (ex.RateLimited)
        {
            context.Response.Headers.RetryAfter = "60";
            await WriteProblemAsync(context, StatusCodes.Status429TooManyRequests, "Rate limit", ex.Message).ConfigureAwait(false);
        }
        catch (ArgumentException ex)
        {
            await WriteProblemAsync(context, StatusCodes.Status400BadRequest, "Invalid request", ex.Message).ConfigureAwait(false);
        }
    }

    private static async Task ListJobsAsync(HttpContext context, InspectionService service)
    {
        var request = context.Request.Query;
        var scope = request["scope"].ToString();
        if (scope.Length == 0)
        {
            throw new ArgumentException("The query parameter 'scope' is required.");
        }

        var max = context.RequestServices.GetRequiredService<IOptions<MicroDcsOptions>>().Value.Management.MaxResults;
        var limit = max;
        if (request.TryGetValue("limit", out var text) && (!int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out limit) || limit < 1 || limit > max))
        {
            throw new ArgumentException($"The query parameter 'limit' must be a number from 1 to {max}.");
        }

        var arguments = new Dictionary<string, string>(StringComparer.Ordinal) { ["scope"] = scope };
        if (request["state"].ToString() is { Length: > 0 } state)
        {
            arguments["state"] = state;
        }

        var page = await service.QueryTypedAsync(Caller, "GET " + JobsRoute, JobsInspector, new InspectionQuery("jobs.orders", Limit: limit, Arguments: arguments), acquire: true, context.RequestAborted).ConfigureAwait(false);
        var list = new JobList([.. page.Entries.Select(ToSummary)]);
        await WriteJsonAsync(context, StatusCodes.Status200OK, JsonSerializer.Serialize(list, RestJsonContext.Default.JobList), "application/json").ConfigureAwait(false);
    }

    private static async Task GetJobAsync(HttpContext context, InspectionService service)
    {
        var id = context.Request.RouteValues["id"] as string ?? string.Empty;
        // The change history is not part of the contract; skipping it saves a stream range read per request.
        var orderArguments = new Dictionary<string, string>(StringComparer.Ordinal) { ["id"] = id, ["changes"] = "false" };
        var orders = await service.QueryTypedAsync(Caller, "GET " + JobRoute, JobsInspector, new InspectionQuery("jobs.order", Limit: 1, Arguments: orderArguments), acquire: true, context.RequestAborted).ConfigureAwait(false);
        if (orders.Entries.Count == 0)
        {
            await WriteProblemAsync(context, StatusCodes.Status404NotFound, "Not found", "No job order with this id.").ConfigureAwait(false);
            return;
        }

        InspectionEntry? recipe = null;
        if (service.InspectorNames.Contains(SfcInspector))
        {
            // One HTTP request takes one rate-limit token (D-54).
            var arguments = new Dictionary<string, string>(StringComparer.Ordinal) { ["id"] = id };
            var states = await service.QueryTypedAsync(Caller, "GET " + JobRoute, SfcInspector, new InspectionQuery("sfc.job", Limit: 1, Arguments: arguments), acquire: false, context.RequestAborted).ConfigureAwait(false);
            recipe = states.Entries.Count == 0 ? null : states.Entries[0];
        }

        var detail = ToDetail(orders.Entries[0], recipe);
        await WriteJsonAsync(context, StatusCodes.Status200OK, JsonSerializer.Serialize(detail, RestJsonContext.Default.JobDetail), "application/json").ConfigureAwait(false);
    }

    private static Task WriteProblemAsync(HttpContext context, int status, string title, string detail) =>
        WriteJsonAsync(context, status, JsonSerializer.Serialize(new Problem("about:blank", title, status, detail), RestJsonContext.Default.Problem), "application/problem+json");

    private static Task WriteJsonAsync(HttpContext context, int status, string json, string contentType)
    {
        context.Response.StatusCode = status;
        context.Response.ContentType = contentType;
        return context.Response.WriteAsync(json, context.RequestAborted);
    }

    private static async Task WriteOpenApiAsync(HttpContext context)
    {
        var stream = OpenApiDocument();
        await using var _ = stream.ConfigureAwait(false);
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "application/json";
        await stream.CopyToAsync(context.Response.Body, context.RequestAborted).ConfigureAwait(false);
    }

    /// <summary>The checked-in OpenAPI document (<c>src/MicroDCS.Inspection/openapi.json</c>), embedded.</summary>
    internal static Stream OpenApiDocument() =>
        typeof(RestApiModule).Assembly.GetManifestResourceStream("MicroDCS.Inspection.openapi.json")
        ?? throw new InvalidOperationException("The OpenAPI document is not embedded.");
}

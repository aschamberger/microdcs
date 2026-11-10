using System.Net;
using System.Text.Json;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Inspection.Tests;

public sealed class RestApiTests
{
    private static readonly InspectionEntry _order = new("jobs.orders", "job-1", new Dictionary<string, InspectionValue>(StringComparer.Ordinal)
    {
        ["scope"] = new("plant/line1", Sensitivity.Identifier),
        ["state"] = new("Running", Sensitivity.Public),
        ["version"] = new("7", Sensitivity.Public),
        ["priority"] = new("3", Sensitivity.Public),
        ["operation"] = new("op-9", Sensitivity.Identifier),
        ["responseId"] = new("resp-1", Sensitivity.Identifier),
        ["responseState"] = new("Running", Sensitivity.Public),
        ["changes"] = new("1 created", Sensitivity.Public),
    });

    private static readonly InspectionEntry _recipe = new("sfc.job", "job-1", new Dictionary<string, InspectionValue>(StringComparer.Ordinal)
    {
        ["status"] = new("Running", Sensitivity.Public),
        ["current"] = new("Fill", Sensitivity.Public),
        ["activeSteps"] = new("Fill,Heat", Sensitivity.Public),
        ["nextDeadline"] = new("2026-11-10T10:00:00.000Z", Sensitivity.Public),
        ["heldSince"] = new(string.Empty, Sensitivity.Public),
        ["faultCode"] = new(string.Empty, Sensitivity.Public),
        ["errors"] = new("secret payload", Sensitivity.Payload),
    });

    private static (ManagementServer Server, KindInspector Jobs) Create(Action<ManagementOptions> configure, bool withJobs = true, bool healthy = true)
    {
        var options = new MicroDcsOptions();
        options.Management.Enabled = true;
        options.Management.Url = "http://127.0.0.1:0";
        configure(options.Management);
        var jobs = new KindInspector("jobs", new() { ["jobs.orders"] = [_order], ["jobs.order"] = [_order] });
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Options.Create(options));
        services.AddSingleton(TimeProvider.System);
        if (withJobs)
        {
            services.AddSingleton<IInspector>(jobs);
            services.AddSingleton<IInspector>(new KindInspector("sfc", new() { ["sfc.job"] = [_recipe] }));
        }

        services.AddSingleton<InspectionService>();
        services.AddSingleton<IManagementModule, RestApiModule>();
        services.AddHealthChecks().AddCheck("transports", () => healthy ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy("down", new InvalidOperationException("secret detail")));
        var provider = services.BuildServiceProvider();
        return (new ManagementServer(provider, provider.GetRequiredService<IOptions<MicroDcsOptions>>(), provider.GetRequiredService<ILoggerFactory>()), jobs);
    }

    private static async Task WithClient(ManagementServer server, Func<HttpClient, CancellationToken, Task> test)
    {
        var ct = TestContext.Current.CancellationToken;
        await server.StartAsync(ct);
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(server.GetAddresses()[0]) };
            await test(client, ct);
        }
        finally
        {
            await server.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ApiIsOffByDefault()
    {
        var (server, _) = Create(static _ => { });

        await WithClient(server, static async (client, ct) =>
            (await client.GetAsync("/api/v1/jobs?scope=plant", ct)).StatusCode.ShouldBe(HttpStatusCode.NotFound));
    }

    [Fact]
    public async Task ListsTheJobsOfAScope()
    {
        var (server, jobs) = Create(static m => m.ApiEnabled = true);

        await WithClient(server, async (client, ct) =>
        {
            var response = await client.GetAsync("/api/v1/jobs?scope=plant/line1&state=Running&limit=5", ct);

            response.StatusCode.ShouldBe(HttpStatusCode.OK);
            var list = JsonSerializer.Deserialize(await response.Content.ReadAsStringAsync(ct), RestJsonContext.Default.JobList)!;
            list.Jobs.ShouldBe([new JobSummary("job-1", "plant/line1", "Running", 7, 3)]);
            jobs.LastQuery!.Kind.ShouldBe("jobs.orders");
            jobs.LastQuery.Limit.ShouldBe(5);
            jobs.LastQuery.Argument("scope").ShouldBe("plant/line1");
            jobs.LastQuery.Argument("state").ShouldBe("Running");
        });
    }

    [Fact]
    public async Task ShowsOneJobWithItsRecipeAndWithoutPayloads()
    {
        var (server, _) = Create(static m => m.ApiEnabled = true);

        await WithClient(server, static async (client, ct) =>
        {
            var json = await client.GetStringAsync("/api/v1/jobs/job-1", ct);

            json.ShouldNotContain("secret");
            json.ShouldNotContain("op-9");
            var detail = JsonSerializer.Deserialize(json, RestJsonContext.Default.JobDetail)!;
            detail.Id.ShouldBe("job-1");
            detail.ResponseState.ShouldBe("Running");
            detail.Recipe!.Status.ShouldBe("Running");
            detail.Recipe.ActiveSteps.ShouldBe(["Fill", "Heat"]);
            detail.Recipe.HeldSince.ShouldBeNull();
        });
    }

    [Fact]
    public async Task AnswersProblemsForBadRequestsUnknownJobsAndTheRateLimit()
    {
        var (server, jobs) = Create(static m =>
        {
            m.ApiEnabled = true;
            m.RequestsPerMinute = 3;
        });

        await WithClient(server, async (client, ct) =>
        {
            var missing = await client.GetAsync("/api/v1/jobs", ct);
            missing.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
            missing.Content.Headers.ContentType!.MediaType.ShouldBe("application/problem+json");
            (await client.GetAsync("/api/v1/jobs?scope=plant&limit=0", ct)).StatusCode.ShouldBe(HttpStatusCode.BadRequest);

            jobs.Entries["jobs.order"] = [];
            (await client.GetAsync("/api/v1/jobs/nope", ct)).StatusCode.ShouldBe(HttpStatusCode.NotFound);

            (await client.GetAsync("/api/v1/jobs?scope=plant", ct)).StatusCode.ShouldBe(HttpStatusCode.OK);
            (await client.GetAsync("/api/v1/jobs?scope=plant", ct)).StatusCode.ShouldBe(HttpStatusCode.OK);
            var limited = await client.GetAsync("/api/v1/jobs?scope=plant", ct);
            limited.StatusCode.ShouldBe(HttpStatusCode.TooManyRequests);
            limited.Headers.RetryAfter.ShouldNotBeNull();
        });
    }

    [Fact]
    public async Task WithoutTheJobsProcessorTheRoutesAnswerNotFound()
    {
        var (server, _) = Create(static m => m.ApiEnabled = true, withJobs: false);

        await WithClient(server, static async (client, ct) =>
        {
            (await client.GetAsync("/api/v1/jobs/job-1", ct)).StatusCode.ShouldBe(HttpStatusCode.NotFound);
            (await client.GetAsync("/api/v1/openapi.json", ct)).StatusCode.ShouldBe(HttpStatusCode.OK);
        });
    }

    [Fact]
    public async Task ReadinessNamesTheFailingChecksButNotTheirDetails()
    {
        var (server, _) = Create(static _ => { }, healthy: false);

        await WithClient(server, static async (client, ct) =>
        {
            var response = await client.GetAsync("/health/ready", ct);

            response.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
            (await response.Content.ReadAsStringAsync(ct)).ShouldBe("not ready: transports");
            (await client.GetAsync("/health/live", ct)).StatusCode.ShouldBe(HttpStatusCode.OK);
        });
    }

    [Fact]
    public async Task JobDetailTakesOneRateLimitTokenAndSkipsTheChangeHistory()
    {
        var (server, jobs) = Create(static m =>
        {
            m.ApiEnabled = true;
            m.RequestsPerMinute = 2;
        });

        await WithClient(server, async (client, ct) =>
        {
            // Two queries per request (jobs and sfc) used to take two tokens: the second request was refused.
            (await client.GetAsync("/api/v1/jobs/job-1", ct)).StatusCode.ShouldBe(HttpStatusCode.OK);
            (await client.GetAsync("/api/v1/jobs/job-1", ct)).StatusCode.ShouldBe(HttpStatusCode.OK);
            jobs.LastQuery!.Argument("changes").ShouldBe("false");
        });
    }

    [Fact]
    public void JobWithoutAResponseHasNoResponseState()
    {
        // The inspector shows Initial for an order without a response; the contract says null.
        var order = _order with { Fields = new Dictionary<string, InspectionValue>(_order.Fields, StringComparer.Ordinal) { ["responseId"] = new(string.Empty, Sensitivity.Identifier), ["responseState"] = new("Initial", Sensitivity.Public) } };

        RestApiModule.ToDetail(order, null).ResponseState.ShouldBeNull();
        RestApiModule.ToDetail(_order, null).ResponseState.ShouldBe("Running");
    }

    [Fact]
    public async Task LongActiveStepListsAreNotCut()
    {
        var steps = Enumerable.Range(0, 300).Select(static i => "Step_number_" + i.ToString(System.Globalization.CultureInfo.InvariantCulture)).ToList();
        var recipe = _recipe with { Fields = new Dictionary<string, InspectionValue>(_recipe.Fields, StringComparer.Ordinal) { ["activeSteps"] = new(string.Join(',', steps), Sensitivity.Public) } };
        var server = RebuildWithRecipe(recipe);

        await WithClient(server, async (client, ct) =>
        {
            var detail = JsonSerializer.Deserialize(await client.GetStringAsync("/api/v1/jobs/job-1", ct), RestJsonContext.Default.JobDetail)!;

            detail.Recipe!.ActiveSteps.ShouldBe(steps);
        });
    }

    private static ManagementServer RebuildWithRecipe(InspectionEntry recipe)
    {
        var options = new MicroDcsOptions();
        options.Management.Enabled = true;
        options.Management.ApiEnabled = true;
        options.Management.Url = "http://127.0.0.1:0";
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Options.Create(options));
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<IInspector>(new KindInspector("jobs", new() { ["jobs.order"] = [_order] }));
        services.AddSingleton<IInspector>(new KindInspector("sfc", new() { ["sfc.job"] = [recipe] }));
        services.AddSingleton<InspectionService>();
        services.AddSingleton<IManagementModule, RestApiModule>();
        var provider = services.BuildServiceProvider();
        return new ManagementServer(provider, provider.GetRequiredService<IOptions<MicroDcsOptions>>(), provider.GetRequiredService<ILoggerFactory>());
    }

    [Fact]
    public void OpenApiDocumentDescribesEveryRoute()
    {
        using var stream = RestApiModule.OpenApiDocument();
        using var document = JsonDocument.Parse(stream);

        var paths = document.RootElement.GetProperty("paths").EnumerateObject().Select(static p => p.Name).Order(StringComparer.Ordinal).ToList();

        paths.ShouldBe(new[] { "/health/live", "/health/ready", RestApiModule.OpenApiRoute, RestApiModule.JobsRoute, RestApiModule.JobRoute }.Order(StringComparer.Ordinal).ToList());
    }

    /// <summary>An inspector that answers per kind.</summary>
    private sealed class KindInspector(string name, Dictionary<string, InspectionEntry[]> entries) : IInspector
    {
        public string Name => name;

        public Dictionary<string, InspectionEntry[]> Entries => entries;

        public InspectionQuery? LastQuery { get; private set; }

        public ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken)
        {
            LastQuery = query;
            return ValueTask.FromResult(new InspectionPage(entries.GetValueOrDefault(query.Kind ?? string.Empty) ?? [], null));
        }
    }
}

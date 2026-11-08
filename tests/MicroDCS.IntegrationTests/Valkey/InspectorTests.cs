using MicroDCS.Core.Inspection;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Documents;

namespace MicroDCS.IntegrationTests.Valkey;

[Trait("Category", "Integration")]
public sealed class InspectorTests
{
    private static InspectionQuery Query(string kind, params (string Name, string Value)[] arguments) =>
        new(kind, Arguments: arguments.ToDictionary(static a => a.Name, static a => a.Value));

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task ReadsKeysDocumentsDedupeAndStreamsThroughTheKeySchema()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var inspector = new ValkeyInspector(env.Context);
        var dao = new JobOrderDao(env.Context);
        await dao.SaveAsync("j1", "plant", "{\"secret\":1}", 1, "Create", 0, cancellationToken: ct);
        await new ValkeyDedupeStore(env.Context, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1)).MarkDoneAsync("urn:s", "e1", ct);

        var scopes = (await inspector.InspectAsync(Query("valkey.keys", ("kind", "scopes")), ct)).Entries.Single();
        var orders = (await inspector.InspectAsync(Query("valkey.keys", ("kind", "joborder"), ("scope", "plant")), ct)).Entries.Single();
        var document = (await inspector.InspectAsync(Query("valkey.document", ("kind", "joborder"), ("id", "j1")), ct)).Entries.Single();
        var missing = (await inspector.InspectAsync(Query("valkey.document", ("kind", "joborder"), ("id", "nope")), ct)).Entries.Single();
        var dedupe = (await inspector.InspectAsync(Query("valkey.dedupe", ("source", "urn:s"), ("id", "e1")), ct)).Entries.Single();
        var streams = (await inspector.InspectAsync(Query("valkey.stream", ("scope", "plant")), ct)).Entries;

        scopes.Fields["ids"].Text.ShouldBe("plant");
        (orders.Fields["total"].Text, orders.Fields["ids"].Text).ShouldBe(("1", "j1"));
        document.Fields["doc"].Sensitivity.ShouldBe(Sensitivity.Payload);
        document.Fields["v"].Text.ShouldBe("1");
        missing.Fields["exists"].Text.ShouldBe("false");
        dedupe.Fields["state"].Text.ShouldBe("done");
        streams.Single(e => e.Key == "changes").Fields["length"].Text.ShouldBe("1");
        streams.Single(e => e.Key == "sfc.work").Fields["exists"].Text.ShouldBe("false");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task RejectsUnknownKindsAndMissingArguments()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var inspector = new ValkeyInspector(env.Context);

        await Should.ThrowAsync<ArgumentException>(async () => await inspector.InspectAsync(Query("valkey.keys", ("kind", "bogus"), ("scope", "x")), ct));
        await Should.ThrowAsync<ArgumentException>(async () => await inspector.InspectAsync(Query("valkey.document", ("kind", "joborder")), ct));
    }
}

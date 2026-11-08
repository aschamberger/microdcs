using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace MicroDCS.Maintenance.Tests;

public sealed class SnapshotStoreTests
{
    private static List<KeyImage> Items(int count = 2) =>
        [.. Enumerable.Range(0, count).Select(i => new KeyImage { Key = "p:joborder:" + i, Type = "hash", Version = 3, Pairs = [new Pair("doc", "{\"a\":" + i + "}"), new Pair("v", "3")] })];

    private static SnapshotStore Store(string directory, FakeTimeProvider time, Action<MaintenanceOptions>? configure = null)
    {
        var options = new MaintenanceOptions { SnapshotDirectory = directory };
        configure?.Invoke(options);
        return new SnapshotStore(options, time, NullLogger.Instance);
    }

    [Fact]
    public async Task WrittenSnapshotLoadsBackWithTheExactStoredText()
    {
        var ct = TestContext.Current.CancellationToken;
        using var dir = new TempDirectory();
        var store = Store(dir.Path, new FakeTimeProvider());

        var id = await store.WriteAsync("maint_purge_scope", "scope=a", "abc", "p", Items(), ct);
        var loaded = await store.LoadAsync(id, ct);

        loaded.Tool.ShouldBe("maint_purge_scope");
        loaded.KeyPrefix.ShouldBe("p");
        loaded.Items.Select(static i => i.Canonical()).ShouldBe(Items().Select(static i => i.Canonical()));
        loaded.Items[1].Pairs!.Single(static p => p.Name == "doc").Value.ShouldBe("{\"a\":1}");
    }

    [Fact]
    public async Task ListShowsNewestFirstWithoutContent()
    {
        var ct = TestContext.Current.CancellationToken;
        using var dir = new TempDirectory();
        var time = new FakeTimeProvider();
        var store = Store(dir.Path, time);
        var first = await store.WriteAsync("maint_a", "x", "d", "p", Items(), ct);
        time.Advance(TimeSpan.FromSeconds(1));
        var second = await store.WriteAsync("maint_b", "y", "d", "p", Items(3), ct);

        var list = await store.ListAsync(ct);

        list.Select(static s => s.Id).ShouldBe([second, first]);
        list[0].Items.ShouldBe(3);
        list[0].Bytes.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task RetentionKeepsTheNewestSnapshots()
    {
        var ct = TestContext.Current.CancellationToken;
        using var dir = new TempDirectory();
        var time = new FakeTimeProvider();
        var store = Store(dir.Path, time, static o => o.SnapshotRetention = 3);
        var ids = new List<string>();
        for (var i = 0; i < 5; i++)
        {
            ids.Add(await store.WriteAsync("maint_x", "n=" + i, "d", "p", Items(1), ct));
            time.Advance(TimeSpan.FromSeconds(1));
        }

        (await store.ListAsync(ct)).Select(static s => s.Id).ShouldBe([ids[4], ids[3], ids[2]]);
    }

    [Fact]
    public async Task ASnapshotAboveTheSizeCapIsRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        using var dir = new TempDirectory();
        var store = Store(dir.Path, new FakeTimeProvider(), static o => o.SnapshotMaxBytes = 1024);

        var ex = await Should.ThrowAsync<MaintenanceRefusedException>(async () => await store.WriteAsync("maint_x", "a", "d", "p", Items(50), ct));

        ex.Message.ShouldContain("limit");
        (await store.ListAsync(ct)).ShouldBeEmpty();
    }

    [Fact]
    public async Task ADirectoryThatCannotBeWrittenRefusesTheSnapshot()
    {
        var ct = TestContext.Current.CancellationToken;
        using var dir = new TempDirectory();
        // A file where the directory should be: creating the directory fails.
        await File.WriteAllTextAsync(dir.Path, "not a directory", ct);
        var store = Store(Path.Combine(dir.Path, "snapshots"), new FakeTimeProvider());

        var ex = await Should.ThrowAsync<MaintenanceRefusedException>(async () => await store.WriteAsync("maint_x", "a", "d", "p", Items(), ct));

        ex.Message.ShouldContain("not writable");
        File.Delete(dir.Path);
    }

    [Theory]
    [InlineData("../etc/passwd")]
    [InlineData("a/b")]
    [InlineData("")]
    public async Task SnapshotIdsCannotLeaveTheDirectory(string id)
    {
        var ct = TestContext.Current.CancellationToken;
        using var dir = new TempDirectory();
        var store = Store(dir.Path, new FakeTimeProvider());

        await Should.ThrowAsync<MaintenanceRefusedException>(async () => await store.LoadAsync(id, ct));
    }

    [Fact]
    public async Task AnUnknownSnapshotIsRefusedWithAHint()
    {
        var ct = TestContext.Current.CancellationToken;
        using var dir = new TempDirectory();
        var store = Store(dir.Path, new FakeTimeProvider());

        var ex = await Should.ThrowAsync<MaintenanceRefusedException>(async () => await store.LoadAsync("20260101T000000000-maint_x-abcdef", ct));

        ex.Message.ShouldContain("pruned");
    }
}

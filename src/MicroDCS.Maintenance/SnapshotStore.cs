using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;

namespace MicroDCS.Maintenance;

/// <summary>The content of one snapshot file.</summary>
internal sealed class SnapshotDocument
{
    public int SchemaVersion { get; init; } = SnapshotStore.SchemaVersion;

    public required string Id { get; init; }

    public required DateTimeOffset CreatedAt { get; init; }

    public required string Tool { get; init; }

    public required string Arguments { get; init; }

    public required string PlanDigest { get; init; }

    public required string KeyPrefix { get; init; }

    public required List<KeyImage> Items { get; init; }
}

/// <summary>Source-generated JSON of the snapshot files (AOT).</summary>
[JsonSourceGenerationOptions(WriteIndented = true, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(SnapshotDocument))]
internal sealed partial class SnapshotJsonContext : JsonSerializerContext;

/// <summary>
/// Snapshot files in the dump directory (D-45): one JSON file per apply, written and flushed before the change, newest
/// <c>SnapshotRetention</c> kept. The payload data lands on the disk of the developer machine, so the directory is git-ignored and
/// outside the container image.
/// </summary>
internal sealed partial class SnapshotStore
{
    public const int SchemaVersion = 1;

    private static readonly Regex _idPattern = IdRegex();

    private readonly MaintenanceOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;

    public SnapshotStore(MaintenanceOptions options, TimeProvider time, ILogger logger)
    {
        _options = options;
        _time = time;
        _logger = logger;
    }

    private string Directory => Path.GetFullPath(_options.SnapshotDirectory);

    /// <summary>Writes a snapshot. Refuses when the file would exceed the size cap or cannot be written.</summary>
    public async Task<string> WriteAsync(string tool, string arguments, string digest, string keyPrefix, List<KeyImage> items, CancellationToken cancellationToken)
    {
        var now = _time.GetUtcNow();
        var id = string.Create(CultureInfo.InvariantCulture, $"{now:yyyyMMddTHHmmssfff}-{tool}-{Guid.NewGuid().ToString("N")[..6]}");
        var document = new SnapshotDocument { Id = id, CreatedAt = now, Tool = tool, Arguments = arguments, PlanDigest = digest, KeyPrefix = keyPrefix, Items = items };
        var bytes = JsonSerializer.SerializeToUtf8Bytes(document, SnapshotJsonContext.Default.SnapshotDocument);
        if (bytes.Length > _options.SnapshotMaxBytes)
        {
            throw new MaintenanceRefusedException($"The snapshot would be {bytes.Length} bytes; the limit is {_options.SnapshotMaxBytes} (MicroDcs:Maintenance:SnapshotMaxBytes). Nothing was changed.");
        }

        try
        {
            System.IO.Directory.CreateDirectory(Directory);
            var path = PathOf(id);
            var temporary = path + ".tmp";
            var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough);
            await using (stream.ConfigureAwait(false))
            {
                await stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                stream.Flush(flushToDisk: true);
            }

            File.Move(temporary, path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new MaintenanceRefusedException($"The snapshot directory '{Directory}' is not writable ({ex.GetType().Name}). Nothing was changed.", ex);
        }

        var pruned = Prune(id);
        Log.SnapshotWritten(_logger, id, items.Count, bytes.Length, pruned);
        return id;
    }

    /// <summary>Loads a snapshot with its content.</summary>
    public async Task<SnapshotDocument> LoadAsync(string id, CancellationToken cancellationToken)
    {
        if (!_idPattern.IsMatch(id))
        {
            throw new MaintenanceRefusedException("A snapshot id has letters, digits, '-' and '_' only.");
        }

        var path = PathOf(id);
        if (!File.Exists(path))
        {
            throw new MaintenanceRefusedException($"Snapshot '{id}' does not exist. It may have been pruned (retention {_options.SnapshotRetention}).");
        }

        var stream = File.OpenRead(path);
        await using var _ = stream.ConfigureAwait(false);
        var document = await JsonSerializer.DeserializeAsync(stream, SnapshotJsonContext.Default.SnapshotDocument, cancellationToken).ConfigureAwait(false)
            ?? throw new MaintenanceRefusedException($"Snapshot '{id}' is empty.");
        return document.SchemaVersion == SchemaVersion
            ? document
            : throw new MaintenanceRefusedException($"Snapshot '{id}' has schema version {document.SchemaVersion}; this build reads {SchemaVersion}.");
    }

    /// <summary>Lists the snapshot files, newest first, without payloads.</summary>
    public async Task<IReadOnlyList<SnapshotInfo>> ListAsync(CancellationToken cancellationToken)
    {
        var result = new List<SnapshotInfo>();
        foreach (var file in Files())
        {
            try
            {
                var stream = File.OpenRead(file.FullName);
                await using var _ = stream.ConfigureAwait(false);
                var document = await JsonSerializer.DeserializeAsync(stream, SnapshotJsonContext.Default.SnapshotDocument, cancellationToken).ConfigureAwait(false);
                if (document is not null)
                {
                    result.Add(new SnapshotInfo(document.Id, document.CreatedAt, document.Tool, document.Arguments, document.Items.Count, file.Length));
                }
            }
            catch (JsonException)
            {
                // A damaged file is skipped; it still counts for the retention.
            }
        }

        return result;
    }

    private int Prune(string keep)
    {
        var pruned = 0;
        foreach (var file in Files().Skip(_options.SnapshotRetention))
        {
            if (Path.GetFileNameWithoutExtension(file.Name) == keep)
            {
                continue;
            }

            try
            {
                file.Delete();
                pruned++;
            }
            catch (IOException)
            {
                // Pruning is best effort; the next apply tries again.
            }
        }

        return pruned;
    }

    private List<FileInfo> Files() =>
        System.IO.Directory.Exists(Directory)
            ? [.. new DirectoryInfo(Directory).EnumerateFiles("*.json").OrderByDescending(static f => f.Name, StringComparer.Ordinal)]
            : [];

    private string PathOf(string id) => Path.Combine(Directory, id + ".json");

    [GeneratedRegex("^[0-9A-Za-z_-]{1,120}$", RegexOptions.CultureInvariant)]
    private static partial Regex IdRegex();
}

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MicroDCS.Valkey;

/// <summary>Result of a version-guarded write.</summary>
/// <param name="Written">The document was written (now, or by an earlier attempt that the operation id proves).</param>
/// <param name="Version">The version after the write; on a conflict the version found in Valkey (zero when the document does not exist).</param>
public readonly record struct WriteResult(bool Written, long Version);

/// <summary>A document read from Valkey: the hash fields of D-41.</summary>
/// <param name="Id">The document id.</param>
/// <param name="Scope">The scope.</param>
/// <param name="Version">The version, incremented by every write.</param>
/// <param name="Operation">The operation id of the last write (R-4).</param>
/// <param name="Document">The serialized document.</param>
/// <param name="State">The normalized state (job responses), empty otherwise.</param>
/// <param name="OrderId">The job order id (job responses), empty otherwise.</param>
/// <param name="Score">The list score: priority (job orders) or start time (job responses).</param>
public sealed record StoredDocument(string Id, string Scope, long Version, string Operation, string Document, string State, string OrderId, double Score);

/// <summary>
/// Shared state of the DAOs: the connection, the key schema, the options and the write protocol for unknown outcomes (R-4, D-42).
/// </summary>
public sealed class ValkeyContext
{
    internal const int StatusWritten = 1;
    internal const int StatusConflict = 0;
    internal const int StatusRetry = 2;
    internal const int StatusScopeMismatch = 3;

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly ILogger _logger;

    /// <summary>Initializes a new instance of the <see cref="ValkeyContext"/> class.</summary>
    /// <param name="multiplexer">The connection.</param>
    /// <param name="options">The options.</param>
    /// <param name="time">The clock.</param>
    /// <param name="logger">The logger.</param>
    public ValkeyContext(IConnectionMultiplexer multiplexer, IOptions<ValkeyOptions> options, TimeProvider time, ILogger<ValkeyContext> logger)
    {
        ArgumentNullException.ThrowIfNull(multiplexer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(logger);
        _multiplexer = multiplexer;
        _logger = logger;
        Options = options.Value;
        Time = time;
        Keys = new KeySchema(Options.KeyPrefix);
    }

    /// <summary>The options.</summary>
    public ValkeyOptions Options { get; }

    /// <summary>The key schema.</summary>
    public KeySchema Keys { get; }

    /// <summary>The clock.</summary>
    public TimeProvider Time { get; }

    /// <summary>The connection.</summary>
    public IConnectionMultiplexer Multiplexer => _multiplexer;

    internal IDatabase Database => _multiplexer.GetDatabase();

    internal ILogger Logger => _logger;

    /// <summary>
    /// Runs a write script that returns <c>{status, version, operation}</c>. After a connection error or timeout the outcome is unknown:
    /// the document is read, and if its operation id is ours the write counts as done, otherwise the script runs again (R-4).
    /// A status of "retry" (an index precondition changed between the read and the script) runs it again as well.
    /// </summary>
    /// <param name="documentKey">The hash that carries the operation id.</param>
    /// <param name="operation">The operation id of this logical write.</param>
    /// <param name="run">Prepares and runs the script; called again for every attempt.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    internal async Task<WriteResult> WriteAsync(string documentKey, string operation, Func<CancellationToken, Task<RedisResult>> run, CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            attempt++;
            var last = attempt >= Options.WriteAttempts;
            try
            {
                var result = (RedisResult[]?)await run(cancellationToken).ConfigureAwait(false) ?? throw new InvalidOperationException("A write script returned no result.");
                var version = (long)result[1];
                switch ((int)result[0])
                {
                    case StatusWritten:
                        return new WriteResult(true, version);
                    case StatusConflict:
                        return new WriteResult(false, version);
                    case StatusScopeMismatch:
                        throw new InvalidOperationException("The document exists in another scope; a document id belongs to one scope (D-42).");
                    default:
                        // An indexed attribute changed between the read and the script: read again.
                        if (last)
                        {
                            throw new InvalidOperationException("The indexed attributes kept changing during the write.");
                        }

                        break;
                }
            }
            catch (Exception ex) when (ex is RedisConnectionException or RedisTimeoutException && !last)
            {
                Log.WriteOutcomeUnknown(_logger, ex, documentKey, attempt);
                ValkeyMetrics.WriteRetries.Add(1);
                await Pause(cancellationToken).ConfigureAwait(false);
                if (await FindAppliedAsync(documentKey, operation, cancellationToken).ConfigureAwait(false) is { } applied)
                {
                    Log.WriteAlreadyApplied(_logger, documentKey);
                    ValkeyMetrics.WritesAlreadyApplied.Add(1);
                    return new WriteResult(true, applied);
                }
            }
        }
    }

    private async Task<long?> FindAppliedAsync(string documentKey, string operation, CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            attempt++;
            try
            {
                var values = await Database.HashGetAsync(documentKey, ["op", "v"]).WaitAsync(cancellationToken).ConfigureAwait(false);
                return values[0] == operation && values[1].TryParse(out long version) ? version : null;
            }
            catch (Exception ex) when (ex is RedisConnectionException or RedisTimeoutException && attempt < Options.WriteAttempts)
            {
                await Pause(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private Task Pause(CancellationToken cancellationToken) =>
        Options.WriteRetryDelay > TimeSpan.Zero ? Task.Delay(Options.WriteRetryDelay, Time, cancellationToken) : Task.CompletedTask;
}

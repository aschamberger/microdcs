using System.Security.Cryptography;
using System.Text;

namespace MicroDCS.Valkey;

/// <summary>
/// Every Valkey key name in one place (D-42). All keys start with the prefix; a script receives every key it
/// touches in <c>KEYS</c>. The table of keys and their types is in the data layer research page.
/// </summary>
public sealed class KeySchema
{
    private readonly string _prefix;

    /// <summary>Initializes a new instance of the <see cref="KeySchema"/> class.</summary>
    /// <param name="prefix">The key prefix, one per island or test run; must not be empty.</param>
    public KeySchema(string prefix)
    {
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        _prefix = prefix;
    }

    /// <summary>Dedupe lease or <c>done</c> marker; the id is the first 16 bytes of SHA-256 over <c>source:id</c>.</summary>
    /// <param name="source">The CloudEvent source.</param>
    /// <param name="id">The CloudEvent id.</param>
    /// <returns>The key.</returns>
    public string CloudEventDedupe(string source, string id)
    {
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(string.Concat(source, ":", id)));
        return Key("ce:dedupe:", Convert.ToHexStringLower(hash.AsSpan(0, 16)));
    }

    /// <summary>Singleton lock of the post-start hook.</summary>
    /// <param name="configIdentifier">The processor's config identifier.</param>
    /// <returns>The key.</returns>
    public string PostStartLock(string configIdentifier) => Key("lock:poststart:", configIdentifier);

    /// <summary>Job order document hash.</summary>
    /// <param name="id">The job order id.</param>
    /// <returns>The key.</returns>
    public string JobOrder(string id) => Key("joborder:", id);

    /// <summary>Job orders of a scope, sorted set by priority.</summary>
    /// <param name="scope">The scope.</param>
    /// <returns>The key.</returns>
    public string JobOrderList(string scope) => Key("joborder:list:", scope);

    /// <summary>Job response document hash.</summary>
    /// <param name="id">The job response id.</param>
    /// <returns>The key.</returns>
    public string JobResponse(string id) => Key("jobresponse:", id);

    /// <summary>Job responses of a scope, sorted set by start time.</summary>
    /// <param name="scope">The scope.</param>
    /// <returns>The key.</returns>
    public string JobResponseList(string scope) => Key("jobresponse:list:", scope);

    /// <summary>Index: job order id to job response id.</summary>
    /// <param name="jobOrderId">The job order id.</param>
    /// <returns>The key.</returns>
    public string JobResponseByOrder(string jobOrderId) => Key("jobresponse:byorder:", jobOrderId);

    /// <summary>Index: job response ids of a scope in a normalized state.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="state">The normalized state, see <see cref="NormalizeState"/>.</param>
    /// <returns>The key.</returns>
    public string JobResponseByState(string scope, string state) => Key("jobresponse:bystate:", scope, ":", state);

    /// <summary>Work master document hash.</summary>
    /// <param name="id">The work master id.</param>
    /// <returns>The key.</returns>
    public string WorkMaster(string id) => Key("workmaster:", id);

    /// <summary>Work masters of a scope, set.</summary>
    /// <param name="scope">The scope.</param>
    /// <returns>The key.</returns>
    public string WorkMasterList(string scope) => Key("workmaster:list:", scope);

    /// <summary>Reference list (equipment, material class, personnel, physical asset, material definition) of a scope.</summary>
    /// <param name="kind">The list kind.</param>
    /// <param name="scope">The scope.</param>
    /// <returns>The key.</returns>
    public string Reference(string kind, string scope) => Key("ref:", kind, ":", scope);

    /// <summary>Job acceptance limit of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <returns>The key.</returns>
    public string JobAcceptance(string scope) => Key("jobacceptance:", scope);

    /// <summary>The persistent scope registry, a set (D-42).</summary>
    /// <returns>The key.</returns>
    public string Scopes() => Key("scopes");

    /// <summary>Change stream of a scope, read by the publisher by cursor.</summary>
    /// <param name="scope">The scope.</param>
    /// <returns>The key.</returns>
    public string Changes(string scope) => Key("changes:", scope);

    /// <summary>Publish sequence counter of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <returns>The key.</returns>
    public string PublishSequence(string scope) => Key("pubseq:", scope);

    /// <summary>Publisher cursors, a hash of last stream id per scope.</summary>
    /// <returns>The key.</returns>
    public string PublisherCursors() => Key("publisher:cursors");

    /// <summary>The lease of the active job order publisher, a string holding the instance id (D-51).</summary>
    /// <returns>The key.</returns>
    public string PublisherLease() => Key("publisher:lease");

    /// <summary>SFC state document hash of a job.</summary>
    /// <param name="jobId">The job id.</param>
    /// <returns>The key.</returns>
    public string SfcState(string jobId) => Key("sfc:state:", jobId);

    /// <summary>SFC work and outbox stream of a scope; never trimmed by length.</summary>
    /// <param name="scope">The scope.</param>
    /// <returns>The key.</returns>
    public string SfcWork(string scope) => Key("sfc:work:", scope);

    /// <summary>Dead-letter stream of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <returns>The key.</returns>
    public string SfcDead(string scope) => Key("sfc:dead:", scope);

    /// <summary>Running SFC jobs of a scope, set.</summary>
    /// <param name="scope">The scope.</param>
    /// <returns>The key.</returns>
    public string SfcActive(string scope) => Key("sfc:active:", scope);

    /// <summary>Due-index of a scope, sorted set of job ids by next deadline.</summary>
    /// <param name="scope">The scope.</param>
    /// <returns>The key.</returns>
    public string SfcDue(string scope) => Key("sfc:due:", scope);

    /// <summary>Jobs of a scope waiting for an event type, sorted set of job ids by wait start (R-1).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="typeId">The event type id.</param>
    /// <returns>The key.</returns>
    public string SfcWaits(string scope, string typeId) => Key("sfc:waits:", scope, ":", typeId);

    /// <summary>Marks an event as applied, so that a redelivery cannot complete a second job (R-1).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="eventKey">The identity of the event.</param>
    /// <returns>The key.</returns>
    public string SfcEventClaim(string scope, string eventKey) => Key("sfc:event:", scope, ":", eventKey);

    /// <summary>Last sweep time per scope, a hash, for the outage rule (D-18).</summary>
    /// <returns>The key.</returns>
    public string SfcHeartbeat() => Key("sfc:heartbeat");

    /// <summary>Recipe text by content hash.</summary>
    /// <param name="hash">The content hash.</param>
    /// <returns>The key.</returns>
    public string SfcRecipe(string hash) => Key("sfc:recipe:", hash);

    /// <summary>Marker of a dev or test instance: holds the environment name. Read by the maintenance operations as a second guard (D-45).</summary>
    /// <returns>The key.</returns>
    public string EnvironmentMarker() => Key("env");

    /// <summary>Pub/sub channel that nudges the poller; the message is the scope (D-43).</summary>
    /// <param name="kind">The stream kind, for example <c>sfc</c> or <c>changes</c>.</param>
    /// <returns>The channel name.</returns>
    public string Nudge(string kind) => Key("nudge:", kind);

    /// <summary>Joins state texts with an underscore, skipping empty ones; an empty result is not indexed.</summary>
    /// <param name="stateTexts">The state texts, main state first.</param>
    /// <returns>The normalized state such as <c>AllowedToStart_Ready</c>.</returns>
    public static string NormalizeState(IEnumerable<string?> stateTexts) =>
        string.Join('_', stateTexts.Where(static t => !string.IsNullOrEmpty(t)));

    private string Key(params string[] parts) => string.Concat(_prefix, ":", string.Concat(parts));
}

namespace MicroDCS.Valkey.Documents;

/// <summary>
/// The Lua scripts of the document types (D-41). Each put script checks the version, stores the hash, maintains the indexes and appends
/// the change entry in one atomic step. ARGV is the same for all of them: 1 expected version (-1 for any), 2 operation id, 3 id,
/// 4 scope, 5 score, 6 document, 7 change type, 8 timestamp, 9 job order id, 10 change stream length, 11 previous state, 12 state.
/// Scripts return <c>{status, version, operation}</c>: 1 written, 0 version conflict, 2 retry (an index precondition changed), 3 scope mismatch.
/// </summary>
internal static class DocumentScripts
{
    private const string Guard = """
        local current = redis.call('HMGET', KEYS[1], 'v', 'scope', 'op')
        local currentVersion = tonumber(current[1]) or 0
        if current[2] and current[2] ~= ARGV[4] then return {3, currentVersion, current[3] or ''} end
        local expected = tonumber(ARGV[1])
        if expected >= 0 and currentVersion ~= expected then return {0, currentVersion, current[3] or ''} end
        local nextVersion = currentVersion + 1
        """;

    /// <summary>KEYS: document, list, changes, scopes, nudge channel.</summary>
    public const string PutJobOrder = Guard + """

        redis.call('HSET', KEYS[1], 'v', nextVersion, 'scope', ARGV[4], 'op', ARGV[2], 'score', ARGV[5], 'doc', ARGV[6])
        redis.call('ZADD', KEYS[2], ARGV[5], ARGV[3])
        redis.call('SADD', KEYS[4], ARGV[4])
        redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[10], '*', 'change_type', ARGV[7], 'job_order_id', ARGV[9], 'scope', ARGV[4], 'ts', ARGV[8])
        redis.call('PUBLISH', KEYS[5], ARGV[4])
        return {1, nextVersion, ARGV[2]}
        """;

    /// <summary>KEYS: document, list, scope check not needed. Arguments: id, scope.</summary>
    public const string DeleteJobOrder = """
        if redis.call('HGET', KEYS[1], 'scope') ~= ARGV[2] and redis.call('EXISTS', KEYS[1]) == 1 then return 2 end
        redis.call('DEL', KEYS[1])
        redis.call('ZREM', KEYS[2], ARGV[1])
        return 1
        """;

    /// <summary>KEYS: document, list, changes, scopes, nudge channel, by-order key, previous state set, new state set.</summary>
    public const string PutJobResponse = Guard + """

        local oldState = redis.call('HGET', KEYS[1], 'state') or ''
        if oldState ~= ARGV[11] then return {2, currentVersion, ''} end
        redis.call('HSET', KEYS[1], 'v', nextVersion, 'scope', ARGV[4], 'op', ARGV[2], 'score', ARGV[5], 'doc', ARGV[6], 'state', ARGV[12], 'order', ARGV[9])
        redis.call('ZADD', KEYS[2], ARGV[5], ARGV[3])
        redis.call('SADD', KEYS[4], ARGV[4])
        if oldState ~= '' then redis.call('SREM', KEYS[7], ARGV[3]) end
        if ARGV[12] ~= '' then redis.call('SADD', KEYS[8], ARGV[3]) end
        if ARGV[9] ~= '' then redis.call('SET', KEYS[6], ARGV[3]) end
        redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[10], '*', 'change_type', ARGV[7], 'job_order_id', ARGV[9], 'scope', ARGV[4], 'ts', ARGV[8])
        redis.call('PUBLISH', KEYS[5], ARGV[4])
        return {1, nextVersion, ARGV[2]}
        """;

    /// <summary>KEYS: document, list, by-order key, state set. Arguments: id, scope, state, order id.</summary>
    public const string DeleteJobResponse = """
        local fields = redis.call('HMGET', KEYS[1], 'scope', 'state', 'order')
        if fields[1] and (fields[1] ~= ARGV[2] or fields[2] ~= ARGV[3] or fields[3] ~= ARGV[4]) then return 2 end
        redis.call('DEL', KEYS[1])
        redis.call('ZREM', KEYS[2], ARGV[1])
        if ARGV[3] ~= '' then redis.call('SREM', KEYS[4], ARGV[1]) end
        if ARGV[4] ~= '' and redis.call('GET', KEYS[3]) == ARGV[1] then redis.call('DEL', KEYS[3]) end
        return 1
        """;

    /// <summary>KEYS: document, list, scopes.</summary>
    public const string PutWorkMaster = Guard + """

        redis.call('HSET', KEYS[1], 'v', nextVersion, 'scope', ARGV[4], 'op', ARGV[2], 'doc', ARGV[6])
        redis.call('SADD', KEYS[2], ARGV[3])
        redis.call('SADD', KEYS[3], ARGV[4])
        return {1, nextVersion, ARGV[2]}
        """;

    /// <summary>KEYS: document, list. Arguments: id, scope.</summary>
    public const string DeleteWorkMaster = """
        if redis.call('EXISTS', KEYS[1]) == 1 and redis.call('HGET', KEYS[1], 'scope') ~= ARGV[2] then return 2 end
        redis.call('DEL', KEYS[1])
        redis.call('SREM', KEYS[2], ARGV[1])
        return 1
        """;
}

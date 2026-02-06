import hashlib

DEFAULT_KEY_PREFIX = "microdcs-test"


def prefixed_key(f):
    """
    A method decorator that prefixes return values.

    Prefixes any string that the decorated method `f` returns with the value of
    the `prefix` attribute on the owner object `self`.
    """

    def prefixed_method(self, *args, **kwargs):
        key = f(self, *args, **kwargs)
        return f"{self.prefix}:{key}"

    return prefixed_method


class RedisKeySchema:
    """
    Methods to generate key names for Redis data structures.

    These key names are used by the DAO classes. This class therefore contains
    a reference to all possible key names used by this application.
    """

    def __init__(self, prefix: str = DEFAULT_KEY_PREFIX):
        self.prefix = prefix

    @prefixed_key
    def cloudevent_dedupe_key(self, cloudevent_source: str, cloudevent_id: str) -> str:
        """
        cededupe:[hash]
        Redis type: string
        """
        raw = f"{cloudevent_source}:{cloudevent_id}"
        return f"cededupe:{hashlib.md5(raw.encode()).hexdigest()}"

    @prefixed_key
    def joborder_key(self, job_order_id: int) -> str:
        """
        joborder:[joborder_id]
        Redis type: json
        """
        return f"joborder:{job_order_id}"

    @prefixed_key
    def joborder_list_key(self) -> str:
        """
        joborder:list
        Redis type: sorted set
        """
        return "joborder:list"

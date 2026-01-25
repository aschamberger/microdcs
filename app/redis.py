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
    def joborder_json_key(self, joborder_id: int) -> str:
        """
        joborder:object:[joborder_id]
        Redis type: json
        """
        return f"joborder:object:{joborder_id}"

    @prefixed_key
    def joborder_list_key(self) -> str:
        """
        joborder:list
        Redis type: sorted set
        """
        return "joborder:list"

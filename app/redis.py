import functools
import hashlib

import redis.asyncio as redis

DEFAULT_KEY_PREFIX = "microdcs-test"


def prefixed_key(f):
    """
    A method decorator that prefixes return values.

    Prefixes any string that the decorated method `f` returns with the value of
    the `prefix` attribute on the owner object `self`.
    """

    @functools.wraps(f)
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
        # hash key to keep the Redis key size consistent and small
        raw = f"{cloudevent_source}:{cloudevent_id}"
        return f"cededupe:{hashlib.md5(raw.encode()).hexdigest()}"

    @prefixed_key
    def joborder_key(self, job_order_id: int) -> str:
        """
        joborder:[job_order_id]
        Redis type: json
        """
        return f"joborder:{job_order_id}"

    @prefixed_key
    def joborder_list_key(self) -> str:
        """
        joborder:list
        Redis type: sorted set (score: priority)
        """
        return "joborder:list"

    @prefixed_key
    def jobresponse_key(self, job_response_id: int) -> str:
        """
        jobresponse:[job_response_id]
        Redis type: json
        """
        return f"jobresponse:{job_response_id}"

    @prefixed_key
    def jobresponse_list_key(self) -> str:
        """
        jobresponse:list
        Redis type: sorted set (score: start_time)
        """
        return "jobresponse:list"

    @prefixed_key
    def workmaster_key(self, id: int) -> str:
        """
        workmaster:[id]
        Redis type: json
        """
        return f"workmaster:{id}"

    @prefixed_key
    def workmaster_list_key(self) -> str:
        """
        workmaster:list
        Redis type: set
        """
        return "workmaster:list"

    @prefixed_key
    def equipment_list_key(self) -> str:
        """
        equipment:list
        Redis type: set
        """
        return "equipment:list"

    @prefixed_key
    def materialclass_list_key(self) -> str:
        """
        materialclass:list
        Redis type: set
        """
        return "materialclass:list"

    @prefixed_key
    def personnel_list_key(self) -> str:
        """
        personnel:list
        Redis type: set
        """
        return "personnel:list"

    @prefixed_key
    def physicalasset_list_key(self) -> str:
        """
        physicalasset:list
        Redis type: set
        """
        return "physicalasset:list"

    @prefixed_key
    def materialdefinition_list_key(self) -> str:
        """
        materialdefinition:list
        Redis type: set
        """
        return "materialdefinition:list"

    @prefixed_key
    def event_receiver_key(self) -> str:
        """
        event:receiver:list
        Redis type: stream
        """
        return "event:receiver:list"

    @prefixed_key
    def event_responder_key(self) -> str:
        """
        event:responder:list
        Redis type: stream
        """
        return "event:responder:list"


class CloudEventDedupeDAO:
    """
    Data Access Object for CloudEvent deduplication.

    This class provides methods to interact with Redis for the purpose of
    deduplicating CloudEvents based on their source and ID.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
        ttl: int = 600,
    ):
        self.redis = redis_client
        self.key_schema = key_schema
        self.ttl = ttl

    async def is_duplicate(self, cloudevent_source: str, cloudevent_id: str) -> bool:
        """
        Check if a CloudEvent with the given source and ID has already been seen.

        Returns True if the event is a duplicate, False otherwise.
        """
        # create deduplication key based on CloudEvent source and ID
        key = self.key_schema.cloudevent_dedupe_key(cloudevent_source, cloudevent_id)
        # atomic SET NX (Set if Not eXists) with expiration
        return (
            False
            if await self.redis.set(
                key,
                "1",
                ex=self.ttl,
                nx=True,
            )
            else True
        )

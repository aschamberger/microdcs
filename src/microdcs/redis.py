import functools
import hashlib
import logging
import re
from datetime import UTC, datetime

import redis.asyncio as redis
from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from microdcs.models.machinery_jobs import (
    ISA95JobOrderAndStateDataType,
    ISA95JobResponseDataType,
    ISA95StateDataType,
)
from microdcs.models.machinery_jobs_ext import ISA95WorkMasterDataTypeExt

logger = logging.getLogger("redis")

_TAG_SPECIAL_RE = re.compile(r"([^\w])")


def _escape_tag(value: str) -> str:
    """Escape special characters for RediSearch TAG field queries."""
    return _TAG_SPECIAL_RE.sub(r"\\\1", value)


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
        return f"cededupe:{hashlib.blake2b(raw.encode(), digest_size=16).hexdigest()}"

    @prefixed_key
    def transaction_dedupe_key(self, scope: str, transaction_id: str) -> str:
        """
        transdedupe:[hash]
        Redis type: string
        """
        # hash key to keep the Redis key size consistent and small
        raw = f"{scope}:{transaction_id}"
        return (
            f"transdedupe:{hashlib.blake2b(raw.encode(), digest_size=16).hexdigest()}"
        )

    @prefixed_key
    def counter_key(self, name: str) -> str:
        """
        counter:[name]
        Redis type: string (integer value)
        """
        return f"counter:{name}"

    @prefixed_key
    def joborder_key(self, job_order_id: str) -> str:
        """
        joborder:[job_order_id]
        Redis type: json
        """
        return f"joborder:{job_order_id}"

    @prefixed_key
    def joborder_list_key(self, scope: str) -> str:
        """
        joborder:list:[scope]
        Redis type: sorted set (score: priority)
        """
        return f"joborder:list:{scope}"

    @prefixed_key
    def jobresponse_key(self, job_response_id: str) -> str:
        """
        jobresponse:[job_response_id]
        Redis type: json
        """
        return f"jobresponse:{job_response_id}"

    @prefixed_key
    def jobresponse_list_key(self, scope: str) -> str:
        """
        jobresponse:list:[scope]
        Redis type: sorted set (score: start_time)
        """
        return f"jobresponse:list:{scope}"

    @prefixed_key
    def jobresponse_index_name(self) -> str:
        """
        idx:jobresponse
        RediSearch index on jobresponse JSON documents.
        """
        return "idx:jobresponse"

    @prefixed_key
    def workmaster_key(self, work_master_id: str) -> str:
        """
        workmaster:[work_master_id]
        Redis type: json
        """
        return f"workmaster:{work_master_id}"

    @prefixed_key
    def workmaster_list_key(self, scope: str) -> str:
        """
        workmaster:list:[scope]
        Redis type: set
        """
        return f"workmaster:list:{scope}"

    @prefixed_key
    def equipment_list_key(self, scope: str) -> str:
        """
        equipment:list:[scope]
        Redis type: set
        """
        return f"equipment:list:{scope}"

    @prefixed_key
    def materialclass_list_key(self, scope: str) -> str:
        """
        materialclass:list:[scope]
        Redis type: set
        """
        return f"materialclass:list:{scope}"

    @prefixed_key
    def personnel_list_key(self, scope: str) -> str:
        """
        personnel:list:[scope]
        Redis type: set
        """
        return f"personnel:list:{scope}"

    @prefixed_key
    def physicalasset_list_key(self, scope: str) -> str:
        """
        physicalasset:list:[scope]
        Redis type: set
        """
        return f"physicalasset:list:{scope}"

    @prefixed_key
    def materialdefinition_list_key(self, scope: str) -> str:
        """
        materialdefinition:list:[scope]
        Redis type: set
        """
        return f"materialdefinition:list:{scope}"

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

    @prefixed_key
    def job_change_stream(self, scope: str) -> str:
        """
        joborder:changes:[scope]
        Redis type: stream
        Change log consumed by the Job Order Publisher.
        """
        return f"joborder:changes:{scope}"

    @prefixed_key
    def job_change_stream_global(self) -> str:
        """
        joborder:changes:_global
        Redis type: stream
        Sentinel stream — every DAO save appends here for new-scope discovery.
        """
        return "joborder:changes:_global"

    @prefixed_key
    def pub_seq(self, scope: str) -> str:
        """
        pubseq:[scope]
        Redis type: string (integer)
        Monotonic sequence counter per scope for state-index publishes.
        """
        return f"pubseq:{scope}"

    @prefixed_key
    def publisher_cursors(self) -> str:
        """
        publisher:stream-cursors
        Redis type: hash
        Last-processed stream ID per scope, for publisher restart recovery.
        """
        return "publisher:stream-cursors"

    @prefixed_key
    def active_scopes(self) -> str:
        """
        active-scopes
        Redis type: set
        All scopes that have had at least one job stored.
        """
        return "active-scopes"


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


class TransactionDedupeDAO:
    """
    Data Access Object for transaction deduplication.

    This class provides methods to interact with Redis for the purpose of
    deduplicating transactions based on their scope and transaction ID.
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

    async def is_duplicate(self, scope: str, transaction_id: str) -> bool:
        """
        Check if a transaction with the given scope and ID has already been seen.

        Returns True if the transaction is a duplicate, False otherwise.
        """
        # create deduplication key based on scope and transaction ID
        key = self.key_schema.transaction_dedupe_key(scope, transaction_id)
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


class CounterDAO:
    """
    Data Access Object for named counters.

    This class provides methods to interact with Redis for the purpose of
    incrementing and retrieving counters identified by name.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
    ):
        self.redis = redis_client
        self.key_schema = key_schema

    async def increment(self, name: str) -> int:
        """
        Increment a counter by 1 and return the new value.
        """
        key = self.key_schema.counter_key(name)
        return await self.redis.incr(key)

    async def get(self, name: str) -> int:
        """
        Get the current value of a counter.

        Returns 0 if the counter does not exist.
        """
        key = self.key_schema.counter_key(name)
        value = await self.redis.get(key)
        return int(value) if value is not None else 0


class JobOrderAndStateDAO:
    """
    Data Access Object for Job Orders.

    This class provides methods to interact with Redis for storing and retrieving
    Job Orders.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
    ):
        self.redis = redis_client
        self.key_schema = key_schema

    async def save(
        self,
        job_order_and_state: ISA95JobOrderAndStateDataType,
        scope: str,
        change_type: str,
    ) -> None:
        """
        Save a Job Order to Redis.

        The Job Order is serialized to JSON using the `to_json` method,
        with context to add the CloudEvent dataschema.
        The Job Order ID is also added to the sorted set with priority as score.
        A change record is appended to the per-scope and global sentinel streams
        atomically within the same pipeline transaction.
        """
        if (
            not job_order_and_state.job_order
            or not job_order_and_state.job_order.job_order_id
        ):
            raise ValueError("Job Order must have a job_order_id to be saved")
        job_order_id = job_order_and_state.job_order.job_order_id
        logger.debug(f"Saving Job Order with ID {job_order_id} to Redis")
        key = self.key_schema.joborder_key(job_order_id)
        priority = getattr(job_order_and_state.job_order, "priority", 0) or 0
        list_key = self.key_schema.joborder_list_key(scope)
        stream_key = self.key_schema.job_change_stream(scope)
        global_stream_key = self.key_schema.job_change_stream_global()
        active_scopes_key = self.key_schema.active_scopes()
        ts = datetime.now(UTC).isoformat()
        change_fields = {
            "change_type": change_type,
            "job_order_id": job_order_id,
            "scope": scope,
            "ts": ts,
        }
        # Execute state document + sorted-set index update atomically.
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.json().set(
                key,
                "$",
                job_order_and_state.to_dict(
                    context={"add_cloudevent_dataschema": True}
                ),
            )
            pipe.zadd(list_key, {job_order_id: priority})
            pipe.xadd(stream_key, change_fields, maxlen=5000, approximate=True)  # type: ignore[reportGeneralTypeIssues]
            pipe.xadd(global_stream_key, change_fields, maxlen=5000, approximate=True)  # type: ignore[reportGeneralTypeIssues]
            pipe.sadd(active_scopes_key, scope)
            await pipe.execute()

    async def retrieve(self, job_order_id: str) -> ISA95JobOrderAndStateDataType | None:
        """
        Retrieve a Job Order from Redis.

        The Job Order is deserialized from JSON using the `from_json` method,
        after checking the dataschema to determine the appropriate class.
        """
        logger.debug(f"Retrieving Job Order with ID {job_order_id} from Redis")
        key = self.key_schema.joborder_key(job_order_id)
        dataschema = await self.redis.json().get(key, "$._dataschema")  # type: ignore[reportGeneralTypeIssues]
        if dataschema:
            logger.debug(
                f"Found Job Order with ID {job_order_id} in Redis with dataschema {dataschema}"
            )
            # here we could check the dataschema to determine which class to deserialize into,
            # but since we only have one class for now, we will just deserialize into that class
            data = await self.redis.json().get(key, "$")  # type: ignore[reportGeneralTypeIssues]
            if isinstance(data, list):
                data = data[0]
            del data["_dataschema"]  # remove before deserialization
            return ISA95JobOrderAndStateDataType.from_dict(data)
        else:
            return None

    async def delete(self, job_order_id: str, scope: str) -> None:
        """
        Delete a Job Order from Redis and remove it from the sorted set.
        """
        logger.debug(f"Deleting Job Order with ID {job_order_id} from Redis")
        key = self.key_schema.joborder_key(job_order_id)
        list_key = self.key_schema.joborder_list_key(scope)
        async with self.redis.pipeline(transaction=True) as pipe:  # type: ignore[reportGeneralTypeIssues]
            pipe.delete(key)
            pipe.zrem(list_key, job_order_id)
            await pipe.execute()

    async def remove_from_list(self, job_order_id: str, scope: str) -> None:
        """
        Remove a Job Order ID from the sorted set.
        """
        list_key = self.key_schema.joborder_list_key(scope)
        await self.redis.zrem(list_key, job_order_id)  # type: ignore[reportGeneralTypeIssues]

    async def list(self, scope: str) -> list[str]:
        """
        List all Job Order IDs from the sorted set, ordered by priority.
        """
        key = self.key_schema.joborder_list_key(scope)
        return await self.redis.zrange(key, 0, -1)  # type: ignore[reportGeneralTypeIssues]


class JobResponseDAO:
    """
    Data Access Object for Job Responses.

    This class provides methods to interact with Redis for storing and retrieving
    Job Responses.  Queries by job_order_id and state use a RediSearch index
    on the JSON documents instead of manually maintained secondary keys.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
    ):
        self.redis = redis_client
        self.key_schema = key_schema

    # -- helpers --------------------------------------------------------------

    @staticmethod
    def normalize_state(state: list[ISA95StateDataType]) -> str:
        """Normalize a state list to a string key for indexing.

        Joins the state_text.text values with '_' to produce a key like
        'AllowedToStart_Ready' matching the state machine convention.
        """
        return "_".join(
            s.state_text.text for s in state if s.state_text and s.state_text.text
        )

    async def initialize(self) -> None:
        """Create the RediSearch index on job response documents (idempotent).

        Must be called once before any save/query operations.  The method is
        idempotent — calling it more than once is safe but unnecessary.
        """
        index_name = self.key_schema.jobresponse_index_name()
        try:
            await self.redis.ft(index_name).info()
        except redis.ResponseError:
            prefix = self.key_schema.jobresponse_key("")
            await self.redis.ft(index_name).create_index(
                [
                    TagField("$.JobOrderID", as_name="job_order_id"),
                    TagField("$._normalized_state", as_name="normalized_state"),
                    TagField("$._scope", as_name="scope"),
                ],
                definition=IndexDefinition(
                    prefix=[prefix],
                    index_type=IndexType.JSON,
                ),
            )

    # -- CRUD -----------------------------------------------------------------

    async def save(self, job_response: ISA95JobResponseDataType, scope: str) -> None:
        """
        Save a Job Response to Redis.

        The Job Response is serialized to JSON using the `to_json` method,
        with context to add the CloudEvent dataschema.
        The Job Response ID is also added to the sorted set with start_time as score.
        Metadata fields ``_scope`` and ``_normalized_state`` are stored alongside
        the document for the RediSearch index.
        """
        if not job_response.job_response_id:
            raise ValueError("Job Response must have a job_response_id to be saved")
        job_response_id = job_response.job_response_id
        logger.debug(f"Saving Job Response with ID {job_response_id} to Redis")
        key = self.key_schema.jobresponse_key(job_response_id)
        # Build serialization context with all metadata for the RediSearch index
        ctx: dict[str, object] = {
            "add_cloudevent_dataschema": True,
            "add_scope": scope,
        }
        if job_response.job_state:
            state_str = self.normalize_state(job_response.job_state)
            if state_str:
                ctx["add_normalized_state"] = state_str
        start_time = getattr(job_response, "start_time", 0) or 0
        list_key = self.key_schema.jobresponse_list_key(scope)
        stream_key = self.key_schema.job_change_stream(scope)
        global_stream_key = self.key_schema.job_change_stream_global()
        ts = datetime.now(UTC).isoformat()
        change_fields = {
            "change_type": "ResultUpdate",
            "job_order_id": job_response.job_order_id or "",
            "scope": scope,
            "ts": ts,
        }
        # Execute state document + sorted-set index update atomically.
        async with self.redis.pipeline(transaction=True) as pipe:  # type: ignore[reportGeneralTypeIssues]
            pipe.json().set(
                key,
                "$",
                job_response.to_dict(context=ctx),
            )
            pipe.zadd(list_key, {job_response_id: start_time})
            pipe.xadd(stream_key, change_fields, maxlen=5000, approximate=True)  # type: ignore[reportGeneralTypeIssues]
            pipe.xadd(global_stream_key, change_fields, maxlen=5000, approximate=True)  # type: ignore[reportGeneralTypeIssues]
            await pipe.execute()

    async def retrieve(self, job_response_id: str):
        """
        Retrieve a Job Response from Redis.

        The Job Response is deserialized from JSON using the `from_json` method,
        after checking the dataschema to determine the appropriate class.
        """
        logger.debug(f"Retrieving Job Response with ID {job_response_id} from Redis")
        key = self.key_schema.jobresponse_key(job_response_id)
        dataschema = await self.redis.json().get(key, "$._dataschema")  # type: ignore[reportGeneralTypeIssues]
        if dataschema:
            logger.debug(
                f"Found Job Response with ID {job_response_id} in Redis with dataschema {dataschema}"
            )
            data = await self.redis.json().get(key, "$")  # type: ignore[reportGeneralTypeIssues]
            if isinstance(data, list):
                data = data[0]
            # Remove internal metadata fields before deserialization
            del data["_dataschema"]
            del data["_scope"]
            data.pop("_normalized_state", None)  # conditionally stored
            return ISA95JobResponseDataType.from_dict(data)
        else:
            return None

    async def retrieve_by_job_order_id(
        self, job_order_id: str
    ) -> ISA95JobResponseDataType | None:
        """
        Retrieve the Job Response associated with a given Job Order ID
        via the RediSearch index.
        """
        logger.debug(
            f"Retrieving Job Response by Job Order ID {job_order_id} from Redis"
        )
        index_name = self.key_schema.jobresponse_index_name()
        escaped = _escape_tag(job_order_id)
        query = Query(f"@job_order_id:{{{escaped}}}").no_content().paging(0, 1)
        results = await self.redis.ft(index_name).search(query)
        total = (
            results.get(b"total_results", 0)
            if isinstance(results, dict)
            else results.total  # type: ignore[reportAttributeAccessIssue]
        )
        if total == 0:
            return None
        key_prefix = self.key_schema.jobresponse_key("")
        if isinstance(results, dict):
            doc_id = results[b"results"][0][b"id"]
            if isinstance(doc_id, bytes):
                doc_id = doc_id.decode()
        else:
            doc_id = results.docs[0].id  # type: ignore[reportAttributeAccessIssue]
        job_response_id = doc_id.removeprefix(key_prefix)
        return await self.retrieve(job_response_id)

    async def retrieve_by_state(
        self, scope: str, state: list[ISA95StateDataType]
    ) -> list[ISA95JobResponseDataType]:
        """
        Retrieve all Job Responses matching a given state within a scope
        via the RediSearch index.
        """
        state_str = self.normalize_state(state)
        if not state_str:
            return []
        logger.debug(
            f"Retrieving Job Responses by state {state_str} in scope {scope} from Redis"
        )
        index_name = self.key_schema.jobresponse_index_name()
        escaped_scope = _escape_tag(scope)
        escaped_state = _escape_tag(state_str)
        query = (
            Query(f"@scope:{{{escaped_scope}}} @normalized_state:{{{escaped_state}}}")
            .no_content()
            .paging(0, 1000)
        )
        results = await self.redis.ft(index_name).search(query)
        key_prefix = self.key_schema.jobresponse_key("")
        responses: list[ISA95JobResponseDataType] = []
        if isinstance(results, dict):
            docs = results.get(b"results", [])
            for doc in docs:
                doc_id = doc[b"id"]
                if isinstance(doc_id, bytes):
                    doc_id = doc_id.decode()
                job_response_id = doc_id.removeprefix(key_prefix)
                response = await self.retrieve(job_response_id)
                if response is not None:
                    responses.append(response)
        else:
            for doc in results.docs:  # type: ignore[reportAttributeAccessIssue]
                job_response_id = doc.id.removeprefix(key_prefix)
                response = await self.retrieve(job_response_id)
                if response is not None:
                    responses.append(response)
        return responses

    async def delete(self, job_response_id: str, scope: str) -> None:
        """
        Delete a Job Response from Redis and remove it from the sorted set.

        The RediSearch index is updated automatically when the key is deleted.
        """
        logger.debug(f"Deleting Job Response with ID {job_response_id} from Redis")
        key = self.key_schema.jobresponse_key(job_response_id)
        list_key = self.key_schema.jobresponse_list_key(scope)
        async with self.redis.pipeline(transaction=True) as pipe:  # type: ignore[reportGeneralTypeIssues]
            pipe.delete(key)
            pipe.zrem(list_key, job_response_id)
            await pipe.execute()

    async def remove_from_list(self, job_response_id: str, scope: str) -> None:
        """
        Remove a Job Response ID from the sorted set.
        """
        list_key = self.key_schema.jobresponse_list_key(scope)
        await self.redis.zrem(list_key, job_response_id)  # type: ignore[reportGeneralTypeIssues]

    async def list(self, scope: str) -> list[str]:
        """
        List all Job Response IDs from the sorted set, ordered by start_time.
        """
        key = self.key_schema.jobresponse_list_key(scope)
        return await self.redis.zrange(key, 0, -1)  # type: ignore[reportGeneralTypeIssues]


class WorkMasterDAO:
    """
    Data Access Object for Work Masters.

    This class provides methods to interact with Redis for storing and retrieving
    Work Masters.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
    ):
        self.redis = redis_client
        self.key_schema = key_schema

    async def save(self, work_master: ISA95WorkMasterDataTypeExt, scope: str) -> None:
        """
        Save a Work Master to Redis.

        The Work Master is serialized to JSON using the `to_json` method,
        with context to add the CloudEvent dataschema.
        The Work Master ID is also added to the set.
        """
        if not work_master.id:
            raise ValueError("Work Master must have an id to be saved")
        work_master_id = work_master.id
        logger.debug(f"Saving Work Master with ID {work_master_id} to Redis")
        key = self.key_schema.workmaster_key(work_master_id)
        await self.redis.json().set(
            key,
            "$",
            work_master.to_dict(context={"add_cloudevent_dataschema": True}),
        )  # type: ignore[reportGeneralTypeIssues]
        # Add to the set
        list_key = self.key_schema.workmaster_list_key(scope)
        await self.redis.sadd(list_key, work_master_id)  # type: ignore[reportGeneralTypeIssues]

    async def retrieve(self, work_master_id: str) -> ISA95WorkMasterDataTypeExt | None:
        """
        Retrieve a Work Master from Redis.

        The Work Master is deserialized from JSON using the `from_json` method,
        after checking the dataschema to determine the appropriate class.
        """
        logger.debug(f"Retrieving Work Master with ID {work_master_id} from Redis")
        key = self.key_schema.workmaster_key(work_master_id)
        dataschema = await self.redis.json().get(key, "$._dataschema")  # type: ignore[reportGeneralTypeIssues]
        if dataschema:
            logger.debug(
                f"Found Work Master with ID {work_master_id} in Redis with dataschema {dataschema}"
            )
            data = await self.redis.json().get(key, "$")  # type: ignore[reportGeneralTypeIssues]
            if isinstance(data, list):
                data = data[0]
            del data["_dataschema"]  # remove before deserialization
            return ISA95WorkMasterDataTypeExt.from_dict(data)
        else:
            return None

    async def delete(self, work_master_id: str, scope: str) -> None:
        """
        Delete a Work Master from Redis and remove it from the set.
        """
        logger.debug(f"Deleting Work Master with ID {work_master_id} from Redis")
        key = self.key_schema.workmaster_key(work_master_id)
        await self.redis.delete(key)
        await self.remove_from_list(work_master_id, scope)

    async def remove_from_list(self, work_master_id: str, scope: str) -> None:
        """
        Remove a Work Master ID from the set.
        """
        list_key = self.key_schema.workmaster_list_key(scope)
        await self.redis.srem(list_key, work_master_id)  # type: ignore[reportGeneralTypeIssues]

    async def list(self, scope: str) -> list[str]:
        """
        List all Work Master IDs from the set.
        """
        key = self.key_schema.workmaster_list_key(scope)
        return list(await self.redis.smembers(key))  # type: ignore[reportGeneralTypeIssues]

    async def is_member(self, work_master_id: str, scope: str) -> bool:
        key = self.key_schema.workmaster_list_key(scope)
        return bool(await self.redis.sismember(key, work_master_id))  # type: ignore[reportGeneralTypeIssues]


class EquipmentListDAO:
    """
    Data Access Object for the Equipment list.

    This class provides methods to add and remove equipment IDs
    from the Redis set.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
    ):
        self.redis = redis_client
        self.key_schema = key_schema

    async def add_to_list(self, equipment_id: str, scope: str) -> None:
        """
        Add an equipment ID to the set.
        """
        list_key = self.key_schema.equipment_list_key(scope)
        await self.redis.sadd(list_key, equipment_id)  # type: ignore[reportGeneralTypeIssues]

    async def remove_from_list(self, equipment_id: str, scope: str) -> None:
        """
        Remove an equipment ID from the set.
        """
        list_key = self.key_schema.equipment_list_key(scope)
        await self.redis.srem(list_key, equipment_id)  # type: ignore[reportGeneralTypeIssues]

    async def is_member(self, equipment_id: str, scope: str) -> bool:
        key = self.key_schema.equipment_list_key(scope)
        return bool(await self.redis.sismember(key, equipment_id))  # type: ignore[reportGeneralTypeIssues]


class MaterialClassListDAO:
    """
    Data Access Object for the Material Class list.

    This class provides methods to add and remove material class IDs
    from the Redis set.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
    ):
        self.redis = redis_client
        self.key_schema = key_schema

    async def add_to_list(self, material_class_id: str, scope: str) -> None:
        """
        Add a material class ID to the set.
        """
        list_key = self.key_schema.materialclass_list_key(scope)
        await self.redis.sadd(list_key, material_class_id)  # type: ignore[reportGeneralTypeIssues]

    async def remove_from_list(self, material_class_id: str, scope: str) -> None:
        """
        Remove a material class ID from the set.
        """
        list_key = self.key_schema.materialclass_list_key(scope)
        await self.redis.srem(list_key, material_class_id)  # type: ignore[reportGeneralTypeIssues]

    async def is_member(self, material_class_id: str, scope: str) -> bool:
        key = self.key_schema.materialclass_list_key(scope)
        return bool(await self.redis.sismember(key, material_class_id))  # type: ignore[reportGeneralTypeIssues]


class PersonnelListDAO:
    """
    Data Access Object for the Personnel list.

    This class provides methods to add and remove personnel IDs
    from the Redis set.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
    ):
        self.redis = redis_client
        self.key_schema = key_schema

    async def add_to_list(self, personnel_id: str, scope: str) -> None:
        """
        Add a personnel ID to the set.
        """
        list_key = self.key_schema.personnel_list_key(scope)
        await self.redis.sadd(list_key, personnel_id)  # type: ignore[reportGeneralTypeIssues]

    async def remove_from_list(self, personnel_id: str, scope: str) -> None:
        """
        Remove a personnel ID from the set.
        """
        list_key = self.key_schema.personnel_list_key(scope)
        await self.redis.srem(list_key, personnel_id)  # type: ignore[reportGeneralTypeIssues]

    async def is_member(self, personnel_id: str, scope: str) -> bool:
        key = self.key_schema.personnel_list_key(scope)
        return bool(await self.redis.sismember(key, personnel_id))  # type: ignore[reportGeneralTypeIssues]


class PhysicalAssetListDAO:
    """
    Data Access Object for the Physical Asset list.

    This class provides methods to add and remove physical asset IDs
    from the Redis set.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
    ):
        self.redis = redis_client
        self.key_schema = key_schema

    async def add_to_list(self, physical_asset_id: str, scope: str) -> None:
        """
        Add a physical asset ID to the set.
        """
        list_key = self.key_schema.physicalasset_list_key(scope)
        await self.redis.sadd(list_key, physical_asset_id)  # type: ignore[reportGeneralTypeIssues]

    async def remove_from_list(self, physical_asset_id: str, scope: str) -> None:
        """
        Remove a physical asset ID from the set.
        """
        list_key = self.key_schema.physicalasset_list_key(scope)
        await self.redis.srem(list_key, physical_asset_id)  # type: ignore[reportGeneralTypeIssues]

    async def is_member(self, physical_asset_id: str, scope: str) -> bool:
        key = self.key_schema.physicalasset_list_key(scope)
        return bool(await self.redis.sismember(key, physical_asset_id))  # type: ignore[reportGeneralTypeIssues]


class MaterialDefinitionListDAO:
    """
    Data Access Object for the Material Definition list.

    This class provides methods to add and remove material definition IDs
    from the Redis set.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
    ):
        self.redis = redis_client
        self.key_schema = key_schema

    async def add_to_list(self, material_definition_id: str, scope: str) -> None:
        """
        Add a material definition ID to the set.
        """
        list_key = self.key_schema.materialdefinition_list_key(scope)
        await self.redis.sadd(list_key, material_definition_id)  # type: ignore[reportGeneralTypeIssues]

    async def remove_from_list(self, material_definition_id: str, scope: str) -> None:
        """
        Remove a material definition ID from the set.
        """
        list_key = self.key_schema.materialdefinition_list_key(scope)
        await self.redis.srem(list_key, material_definition_id)  # type: ignore[reportGeneralTypeIssues]

    async def is_member(self, material_definition_id: str, scope: str) -> bool:
        key = self.key_schema.materialdefinition_list_key(scope)
        return bool(await self.redis.sismember(key, material_definition_id))  # type: ignore[reportGeneralTypeIssues]

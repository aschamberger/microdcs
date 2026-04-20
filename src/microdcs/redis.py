import functools
import hashlib
import logging
import re
from dataclasses import asdict
from datetime import UTC, datetime
from enum import StrEnum

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
from microdcs.models.sfc_recipe_ext import (
    SfcActionExecution,
    SfcActionState,
    SfcExecutionState,
    SfcWorkAction,
)

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
    def jobacceptance_key(self, scope: str) -> str:
        """
        jobacceptance:[scope]
        Redis type: string (integer value)
        """
        return f"jobacceptance:{scope}"

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

    @prefixed_key
    def sfc_work_stream(self, scope: str) -> str:
        """
        sfc:work:[scope]
        Redis type: stream
        Work items for the SFC engine consumer group.
        """
        return f"sfc:work:{scope}"

    @prefixed_key
    def sfc_execution_key(self, job_id: str) -> str:
        """
        sfc:execution:[job_id]
        Redis type: json
        SFC execution state for a running job.
        """
        return f"sfc:execution:{job_id}"

    @prefixed_key
    def sfc_active_jobs(self) -> str:
        """
        sfc:active-jobs
        Redis type: set
        All job IDs with active SFC execution state.
        """
        return "sfc:active-jobs"


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


class JobAcceptanceConfigDAO:
    """
    Data Access Object for persisting per-scope job acceptance configuration.

    Stores the ``max_downloadable_job_orders`` value per scope so it
    survives pod restarts.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
    ):
        self.redis = redis_client
        self.key_schema = key_schema

    async def save(self, max_downloadable_job_orders: int, scope: str) -> None:
        key = self.key_schema.jobacceptance_key(scope)
        await self.redis.set(key, max_downloadable_job_orders)

    async def retrieve(self, scope: str) -> int | None:
        key = self.key_schema.jobacceptance_key(scope)
        value = await self.redis.get(key)
        if value is not None:
            return int(value)
        return None

    async def delete(self, scope: str) -> None:
        key = self.key_schema.jobacceptance_key(scope)
        await self.redis.delete(key)


class SfcExecutionDAO:
    """Data Access Object for SFC engine execution state.

    Stores per-job execution state as Redis JSON documents and provides
    atomic compare-and-swap (CAS) operations via Lua scripts for
    multi-instance coordination.
    """

    # Lua script: atomic CAS on action state within sfc:execution:{job_id}.
    # KEYS[1] = sfc:execution:{job_id}
    # KEYS[2] = sfc:work:{scope} (work stream, may be empty string if no follow-up)
    # ARGV[1] = action name
    # ARGV[2] = expected current state
    # ARGV[3] = new state
    # ARGV[4] = correlation_id (or empty string)
    # ARGV[5] = attempt number (or empty string)
    # ARGV[6] = JSON-encoded follow-up stream fields (or empty string)
    # Returns: "OK", "ALREADY_HANDLED", or "NOT_FOUND"
    _CAS_ACTION_STATE_LUA = """
local key = KEYS[1]
local stream_key = KEYS[2]
local action_name = ARGV[1]
local expected_state = ARGV[2]
local new_state = ARGV[3]
local correlation_id = ARGV[4]
local attempt = ARGV[5]
local stream_fields = ARGV[6]

local path = '$.actions.' .. action_name .. '.state'
local current = redis.call('JSON.GET', key, path)
if current == nil or current == '' then
    return 'NOT_FOUND'
end

-- JSON.GET returns a JSON array like '["pending"]'
local current_state = string.match(current, '"([^"]+)"')
if current_state ~= expected_state then
    return 'ALREADY_HANDLED'
end

redis.call('JSON.SET', key, path, '"' .. new_state .. '"')
if correlation_id ~= '' then
    redis.call('JSON.SET', key, '$.actions.' .. action_name .. '.correlation_id', '"' .. correlation_id .. '"')
end
if attempt ~= '' then
    redis.call('JSON.SET', key, '$.actions.' .. action_name .. '.attempt', tonumber(attempt))
end

if stream_fields ~= '' and stream_key ~= '' then
    redis.call('XADD', stream_key, 'MAXLEN', '~', '5000', '*', unpack(cjson.decode(stream_fields)))
end

return 'OK'
"""

    # Lua script: atomic step advancement.
    # KEYS[1] = sfc:execution:{job_id}
    # KEYS[2] = sfc:work:{scope} (work stream, may be empty string if no follow-up)
    # ARGV[1] = expected current step
    # ARGV[2] = new current step
    # ARGV[3] = JSON-encoded new active_steps list
    # ARGV[4] = JSON-encoded follow-up stream fields (or empty string)
    # Returns: "OK", "ALREADY_HANDLED", or "NOT_FOUND"
    _CAS_ADVANCE_STEP_LUA = """
local key = KEYS[1]
local stream_key = KEYS[2]
local expected_step = ARGV[1]
local new_step = ARGV[2]
local new_active_steps = ARGV[3]
local stream_fields = ARGV[4]

local current = redis.call('JSON.GET', key, '$.current_step')
if current == nil or current == '' then
    return 'NOT_FOUND'
end

local current_step = string.match(current, '"([^"]+)"')
if current_step ~= expected_step then
    return 'ALREADY_HANDLED'
end

redis.call('JSON.SET', key, '$.current_step', '"' .. new_step .. '"')
redis.call('JSON.SET', key, '$.active_steps', new_active_steps)

if stream_fields ~= '' and stream_key ~= '' then
    redis.call('XADD', stream_key, 'MAXLEN', '~', '5000', '*', unpack(cjson.decode(stream_fields)))
end

return 'OK'
"""

    # Lua script: mark execution as completed/failed.
    # KEYS[1] = sfc:execution:{job_id}
    # KEYS[2] = sfc:active-jobs
    # ARGV[1] = "completed" or "failed"
    # ARGV[2] = error message (or empty string)
    # Returns: "OK" or "NOT_FOUND"
    _CAS_FINISH_LUA = """
local key = KEYS[1]
local active_jobs_key = KEYS[2]
local finish_type = ARGV[1]
local error_msg = ARGV[2]

local exists = redis.call('JSON.TYPE', key, '$')
if exists == nil or exists == '' then
    return 'NOT_FOUND'
end

if finish_type == 'completed' then
    redis.call('JSON.SET', key, '$.completed', 'true')
elseif finish_type == 'failed' then
    redis.call('JSON.SET', key, '$.failed', 'true')
    if error_msg ~= '' then
        redis.call('JSON.SET', key, '$.error', '"' .. error_msg .. '"')
    end
end

local job_id_raw = redis.call('JSON.GET', key, '$.job_id')
if job_id_raw and job_id_raw ~= '' then
    local job_id = string.match(job_id_raw, '"([^"]+)"')
    if job_id then
        redis.call('SREM', active_jobs_key, job_id)
    end
end

return 'OK'
"""

    # Lua script: atomic branch step advancement.
    # Replaces or removes a completed step from active_steps.
    # KEYS[1] = sfc:execution:{job_id}
    # KEYS[2] = sfc:work:{scope} (work stream, may be empty string)
    # ARGV[1] = completed step name (must be present in active_steps)
    # ARGV[2] = next step name (empty string = remove without replacement)
    # ARGV[3] = JSON-encoded follow-up stream fields (or empty string)
    # Returns: JSON-encoded new active_steps on success, "ALREADY_HANDLED"
    #          if the completed step is not in active_steps, or "NOT_FOUND".
    _CAS_BRANCH_ADVANCE_LUA = """
local key = KEYS[1]
local stream_key = KEYS[2]
local completed_step = ARGV[1]
local next_step = ARGV[2]
local stream_fields = ARGV[3]

local active_json = redis.call('JSON.GET', key, '$.active_steps')
if active_json == nil or active_json == '' then
    return 'NOT_FOUND'
end

local wrapper = cjson.decode(active_json)
local active = wrapper
if type(wrapper) == 'table' and #wrapper == 1 and type(wrapper[1]) == 'table' then
    active = wrapper[1]
end

local found = false
local new_active = {}
for _, step in ipairs(active) do
    if step == completed_step and not found then
        found = true
        if next_step ~= '' then
            new_active[#new_active + 1] = next_step
        end
    else
        new_active[#new_active + 1] = step
    end
end

if not found then
    return 'ALREADY_HANDLED'
end

redis.call('JSON.SET', key, '$.active_steps', cjson.encode(new_active))

if stream_fields ~= '' and stream_key ~= '' then
    redis.call('XADD', stream_key, 'MAXLEN', '~', '5000', '*', unpack(cjson.decode(stream_fields)))
end

return cjson.encode(new_active)
"""

    def __init__(
        self,
        redis_client: redis.Redis,
        key_schema: RedisKeySchema,
    ):
        self.redis = redis_client
        self.key_schema = key_schema
        self._cas_action_state_sha: str | None = None
        self._cas_advance_step_sha: str | None = None
        self._cas_finish_sha: str | None = None
        self._cas_branch_advance_sha: str | None = None

    async def _ensure_scripts(self) -> None:
        if self._cas_action_state_sha is None:
            self._cas_action_state_sha = await self.redis.script_load(
                self._CAS_ACTION_STATE_LUA
            )
        if self._cas_advance_step_sha is None:
            self._cas_advance_step_sha = await self.redis.script_load(
                self._CAS_ADVANCE_STEP_LUA
            )
        if self._cas_finish_sha is None:
            self._cas_finish_sha = await self.redis.script_load(self._CAS_FINISH_LUA)
        if self._cas_branch_advance_sha is None:
            self._cas_branch_advance_sha = await self.redis.script_load(
                self._CAS_BRANCH_ADVANCE_LUA
            )

    async def save(self, state: SfcExecutionState) -> None:
        """Create or overwrite the execution state for a job."""
        key = self.key_schema.sfc_execution_key(state.job_id)
        active_jobs_key = self.key_schema.sfc_active_jobs()
        data = asdict(state)
        # Convert enum values to strings for JSON storage
        for action_data in data.get("actions", {}).values():
            if isinstance(action_data.get("state"), StrEnum):
                action_data["state"] = action_data["state"].value
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.json().set(key, "$", data)
            if not state.completed and not state.failed:
                pipe.sadd(active_jobs_key, state.job_id)  # type: ignore[reportGeneralTypeIssues]
            await pipe.execute()

    async def retrieve(self, job_id: str) -> SfcExecutionState | None:
        """Load execution state for a job from Redis."""
        key = self.key_schema.sfc_execution_key(job_id)
        data = await self.redis.json().get(key, "$")  # type: ignore[reportGeneralTypeIssues]
        if not data:
            return None
        if isinstance(data, list):
            data = data[0]
        # Reconstruct dataclass from dict
        actions = {}
        for name, action_data in data.get("actions", {}).items():
            actions[name] = SfcActionExecution(
                name=action_data["name"],
                state=SfcActionState(action_data["state"]),
                correlation_id=action_data.get("correlation_id"),
                attempt=action_data.get("attempt", 0),
                result=action_data.get("result"),
                error=action_data.get("error"),
            )
        return SfcExecutionState(
            job_id=data["job_id"],
            scope=data["scope"],
            work_master_id=data["work_master_id"],
            current_step=data["current_step"],
            active_steps=data.get("active_steps", []),
            actions=actions,
            completed=data.get("completed", False),
            failed=data.get("failed", False),
            error=data.get("error"),
        )

    async def delete(self, job_id: str) -> None:
        """Remove execution state for a job."""
        key = self.key_schema.sfc_execution_key(job_id)
        active_jobs_key = self.key_schema.sfc_active_jobs()
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.delete(key)
            pipe.srem(active_jobs_key, job_id)  # type: ignore[reportGeneralTypeIssues]
            await pipe.execute()

    async def list_active_jobs(self) -> set[str]:
        """Return all job IDs with active SFC execution state."""
        key = self.key_schema.sfc_active_jobs()
        members = await self.redis.smembers(key)  # type: ignore[reportGeneralTypeIssues]
        return {m.decode() if isinstance(m, bytes) else m for m in members}

    async def cas_action_state(
        self,
        job_id: str,
        scope: str,
        action_name: str,
        expected_state: SfcActionState,
        new_state: SfcActionState,
        correlation_id: str = "",
        attempt: int | None = None,
        follow_up_stream_fields: list[str] | None = None,
    ) -> str:
        """Atomic compare-and-swap on an action's state.

        Returns ``"OK"``, ``"ALREADY_HANDLED"``, or ``"NOT_FOUND"``.
        """
        await self._ensure_scripts()
        key = self.key_schema.sfc_execution_key(job_id)
        stream_key = (
            self.key_schema.sfc_work_stream(scope) if follow_up_stream_fields else ""
        )
        import orjson

        stream_fields_json = (
            orjson.dumps(follow_up_stream_fields).decode()
            if follow_up_stream_fields
            else ""
        )
        result = await self.redis.evalsha(
            self._cas_action_state_sha,  # type: ignore[arg-type]
            2,
            key,
            stream_key,
            action_name,
            expected_state.value,
            new_state.value,
            correlation_id,
            str(attempt) if attempt is not None else "",
            stream_fields_json,
        )
        if isinstance(result, bytes):
            return result.decode()
        return str(result)

    async def cas_advance_step(
        self,
        job_id: str,
        scope: str,
        expected_step: str,
        new_step: str,
        new_active_steps: list[str],
        follow_up_stream_fields: list[str] | None = None,
    ) -> str:
        """Atomic compare-and-swap to advance the current step.

        Returns ``"OK"``, ``"ALREADY_HANDLED"``, or ``"NOT_FOUND"``.
        """
        await self._ensure_scripts()
        key = self.key_schema.sfc_execution_key(job_id)
        stream_key = (
            self.key_schema.sfc_work_stream(scope) if follow_up_stream_fields else ""
        )
        import orjson

        active_steps_json = orjson.dumps(new_active_steps).decode()
        stream_fields_json = (
            orjson.dumps(follow_up_stream_fields).decode()
            if follow_up_stream_fields
            else ""
        )
        result = await self.redis.evalsha(
            self._cas_advance_step_sha,  # type: ignore[arg-type]
            2,
            key,
            stream_key,
            expected_step,
            new_step,
            active_steps_json,
            stream_fields_json,
        )
        if isinstance(result, bytes):
            return result.decode()
        return str(result)

    async def cas_finish(
        self,
        job_id: str,
        finish_type: str,
        error: str = "",
    ) -> str:
        """Atomically mark a job as completed or failed.

        Returns ``"OK"`` or ``"NOT_FOUND"``.
        """
        await self._ensure_scripts()
        key = self.key_schema.sfc_execution_key(job_id)
        active_jobs_key = self.key_schema.sfc_active_jobs()
        result = await self.redis.evalsha(
            self._cas_finish_sha,  # type: ignore[arg-type]
            2,
            key,
            active_jobs_key,
            finish_type,
            error,
        )
        if isinstance(result, bytes):
            return result.decode()
        return str(result)

    async def cas_branch_advance(
        self,
        job_id: str,
        scope: str,
        completed_step: str,
        next_step: str = "",
        follow_up_stream_fields: list[str] | None = None,
    ) -> str | list[str]:
        """Atomic replace/remove of a step within ``active_steps``.

        Replaces *completed_step* with *next_step* (or removes it when
        *next_step* is empty).  Returns the JSON-decoded new
        ``active_steps`` list on success, ``"ALREADY_HANDLED"`` if
        *completed_step* was not found, or ``"NOT_FOUND"``.
        """
        await self._ensure_scripts()
        key = self.key_schema.sfc_execution_key(job_id)
        stream_key = (
            self.key_schema.sfc_work_stream(scope) if follow_up_stream_fields else ""
        )
        import orjson

        stream_fields_json = (
            orjson.dumps(follow_up_stream_fields).decode()
            if follow_up_stream_fields
            else ""
        )
        result = await self.redis.evalsha(
            self._cas_branch_advance_sha,  # type: ignore[arg-type]
            2,
            key,
            stream_key,
            completed_step,
            next_step,
            stream_fields_json,
        )
        if isinstance(result, bytes):
            decoded = result.decode()
            if decoded in ("ALREADY_HANDLED", "NOT_FOUND"):
                return decoded
            return orjson.loads(decoded)
        return str(result)

    async def enqueue_work(
        self,
        scope: str,
        job_id: str,
        action: SfcWorkAction | str,
    ) -> str:
        """Add a work item to the SFC work stream.

        Returns the stream entry ID.
        """
        stream_key = self.key_schema.sfc_work_stream(scope)
        action_value = action.value if isinstance(action, SfcWorkAction) else action
        entry_id = await self.redis.xadd(
            stream_key,
            {"job_id": job_id, "action": action_value, "scope": scope},
            maxlen=5000,
            approximate=True,
        )
        if isinstance(entry_id, bytes):
            return entry_id.decode()
        return str(entry_id)

    async def ensure_consumer_group(self, scope: str, group: str) -> None:
        """Create the consumer group on the SFC work stream if it doesn't exist."""
        stream_key = self.key_schema.sfc_work_stream(scope)
        try:
            await self.redis.xgroup_create(stream_key, group, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

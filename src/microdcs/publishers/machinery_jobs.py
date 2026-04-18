import logging
from datetime import UTC, datetime

import redis.asyncio as redis

from microdcs import ProcessingConfig, PublisherConfig
from microdcs.models.machinery_jobs import ISA95StateDataType
from microdcs.models.machinery_jobs_ext import StateIndex, StateIndexEntry
from microdcs.mqtt import MQTTPublisher
from microdcs.redis import JobOrderAndStateDAO, JobResponseDAO, RedisKeySchema

from .. import MQTTConfig

logger = logging.getLogger("publisher.job_order")

CLEARABLE_STATE_PREFIXES = ("Ended", "Aborted")


class JobOrderPublisher(MQTTPublisher):
    """Reads Redis change streams and maintains retained MQTT topics.

    Subclasses :class:`MQTTPublisher` — inherits connection lifecycle and
    reconnect/backoff.  Overrides :meth:`_run` with the XREAD loop that
    dispatches per-job retained publishes and state-index updates.
    """

    def __init__(
        self,
        config: MQTTConfig,
        publisher_config: PublisherConfig,
        processing_config: ProcessingConfig,
        config_identifier: str,
        redis_connection_pool: redis.ConnectionPool,
        redis_key_schema: RedisKeySchema,
    ) -> None:
        super().__init__(config)
        self._publisher_config = publisher_config
        self._redis_client: redis.Redis = redis.Redis(
            connection_pool=redis_connection_pool
        )
        self._key_schema = redis_key_schema
        self._joborder_dao = JobOrderAndStateDAO(self._redis_client, redis_key_schema)
        self._jobresponse_dao = JobResponseDAO(self._redis_client, redis_key_schema)
        topic_prefix = processing_config.get_topic_prefix_for_identifier(
            config_identifier
        )
        if topic_prefix is None:
            raise ValueError(f"No topic prefix configured for '{config_identifier}'")
        self._topic_prefix = topic_prefix
        self._cursors: dict[str, str] = {}
        self._active_scopes: set[str] = set()

    @staticmethod
    def _reconstruct_state_name(state: list[ISA95StateDataType]) -> str | None:
        if not state:
            return None
        parts = [s.state_text.text for s in state if s.state_text and s.state_text.text]
        return "_".join(parts) if parts else None

    async def _load_cursors(self) -> None:
        cursor_key = self._key_schema.publisher_cursors()
        raw = await self._redis_client.hgetall(cursor_key)  # type: ignore[reportGeneralTypeIssues]
        self._cursors = {
            k.decode() if isinstance(k, bytes) else k: v.decode()
            if isinstance(v, bytes)
            else v
            for k, v in raw.items()
        }

    async def _save_cursors(self) -> None:
        cursor_key = self._key_schema.publisher_cursors()
        if self._cursors:
            await self._redis_client.hset(cursor_key, mapping=self._cursors)  # type: ignore[reportGeneralTypeIssues]

    async def _publish_state_index(self, scope: str) -> None:
        """Build and publish the state-index retained topic for a scope."""
        job_ids = await self._joborder_dao.list(scope)
        entries: list[StateIndexEntry] = []
        for job_id_raw in job_ids:
            job_id = (
                job_id_raw.decode() if isinstance(job_id_raw, bytes) else job_id_raw
            )
            job = await self._joborder_dao.retrieve(job_id)
            if job is None:
                continue
            state_name = self._reconstruct_state_name(job.state or [])
            if state_name == "EndState":
                continue
            has_result = False
            if job.job_order and job.job_order.job_order_id:
                response = await self._jobresponse_dao.retrieve_by_job_order_id(
                    job.job_order.job_order_id
                )
                has_result = response is not None
            entries.append(
                StateIndexEntry(
                    job_order_id=job_id,
                    state=job.state or [],
                    has_result=has_result,
                )
            )
        seq_key = self._key_schema.pub_seq(scope)
        seq = await self._redis_client.incr(seq_key)  # type: ignore[reportGeneralTypeIssues]
        state_index = StateIndex(
            seq=seq,
            scope=scope,
            published_at=datetime.now(UTC).isoformat(),
            jobs=entries,
        )
        topic = f"{self._topic_prefix}/{scope}/state-index"
        await self.publish_retained(
            topic,
            state_index.to_jsonb(),
            self._publisher_config.retained_ttl_seconds,
        )
        logger.info(
            "Published state-index for scope=%s seq=%d jobs=%d",
            scope,
            seq,
            len(entries),
        )

    async def _publish_job_order(self, scope: str, job_order_id: str) -> None:
        """Publish retained order/{id} topic."""
        job = await self._joborder_dao.retrieve(job_order_id)
        if job is None:
            logger.warning(
                "Job order %s not found in scope %s, skipping publish",
                job_order_id,
                scope,
            )
            return
        topic = f"{self._topic_prefix}/{scope}/order/{job_order_id}"
        await self.publish_retained(
            topic,
            job.to_jsonb(),
            self._publisher_config.retained_ttl_seconds,
        )

    async def _publish_job_result(self, scope: str, job_order_id: str) -> None:
        """Publish retained result/{id} only when the job is in a clearable state."""
        job = await self._joborder_dao.retrieve(job_order_id)
        if job is None:
            return
        state_name = self._reconstruct_state_name(job.state or [])
        if state_name is None or not state_name.startswith(CLEARABLE_STATE_PREFIXES):
            logger.debug(
                "Job %s in state %s is not clearable, skipping result publish",
                job_order_id,
                state_name,
            )
            return
        response = await self._jobresponse_dao.retrieve_by_job_order_id(job_order_id)
        if response is None:
            logger.debug(
                "No response found for job %s, skipping result publish",
                job_order_id,
            )
            return
        topic = f"{self._topic_prefix}/{scope}/result/{job_order_id}"
        await self.publish_retained(
            topic,
            response.to_jsonb(),
            self._publisher_config.retained_ttl_seconds,
        )

    async def _delete_job_topics(self, scope: str, job_order_id: str) -> None:
        """Zero-byte retained publish for order/{id} and result/{id}."""
        order_topic = f"{self._topic_prefix}/{scope}/order/{job_order_id}"
        result_topic = f"{self._topic_prefix}/{scope}/result/{job_order_id}"
        await self.delete_retained(order_topic)
        await self.delete_retained(result_topic)

    async def _dispatch(self, change_type: str, scope: str, job_order_id: str) -> None:
        if change_type in ("Store", "StoreAndStart", "Update"):
            await self._publish_job_order(scope, job_order_id)
        elif change_type == "ResultUpdate":
            await self._publish_job_result(scope, job_order_id)
        elif change_type == "Clear":
            await self._delete_job_topics(scope, job_order_id)
        # All change types trigger a state-index update
        await self._publish_state_index(scope)

    async def _run(self) -> None:
        logger.info("Starting job order publisher")
        await self._load_cursors()

        # Load active scopes
        active_scopes_key = self._key_schema.active_scopes()
        raw_scopes = await self._redis_client.smembers(active_scopes_key)  # type: ignore[reportGeneralTypeIssues]
        self._active_scopes = {
            s.decode() if isinstance(s, bytes) else s for s in raw_scopes
        }

        # Publish initial state-index for all active scopes (covers restart gap)
        for scope in self._active_scopes:
            await self._publish_state_index(scope)
        await self._save_cursors()
        logger.info(
            "Initial state-index published for %d scopes", len(self._active_scopes)
        )

        # Enter XREAD loop
        global_stream_key = self._key_schema.job_change_stream_global()
        while not self._shutdown_event.is_set():
            streams: dict[str, str] = {}
            for scope in self._active_scopes:
                stream_key = self._key_schema.job_change_stream(scope)
                streams[stream_key] = self._cursors.get(scope, "0")
            streams[global_stream_key] = self._cursors.get("_global", "0")

            results = await self._redis_client.xread(
                streams=streams,  # type: ignore[reportGeneralTypeIssues]
                block=self._publisher_config.stream_block_ms,
                count=self._publisher_config.stream_read_count,
            )
            if not results:
                continue

            # RESP3 returns a dict {stream: [[(id, fields)]]},
            # RESP2 returns a list [(stream, [(id, fields)])]
            if isinstance(results, dict):
                stream_items = [
                    (k.decode() if isinstance(k, bytes) else k, msgs[0] if msgs else [])
                    for k, msgs in results.items()
                ]
            else:
                stream_items = [
                    (k.decode() if isinstance(k, bytes) else k, msgs)
                    for k, msgs in results
                ]

            for stream, messages in stream_items:
                for message_id_raw, fields_raw in messages:
                    message_id: str = (
                        message_id_raw.decode()
                        if isinstance(message_id_raw, bytes)
                        else message_id_raw
                    )
                    fields = {
                        (k.decode() if isinstance(k, bytes) else k): (
                            v.decode() if isinstance(v, bytes) else v
                        )
                        for k, v in fields_raw.items()
                    }
                    scope = fields["scope"]
                    if stream == global_stream_key:
                        # Discover new scope — add its stream to the XREAD set
                        if scope not in self._active_scopes:
                            self._active_scopes.add(scope)
                            logger.info("Discovered new scope: %s", scope)
                        self._cursors["_global"] = message_id
                    else:
                        await self._dispatch(
                            fields["change_type"], scope, fields["job_order_id"]
                        )
                        self._cursors[scope] = message_id

            await self._save_cursors()

        logger.info("Job order publisher shutdown complete")

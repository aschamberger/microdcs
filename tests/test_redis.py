import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis.asyncio as redis

from microdcs.models.machinery_jobs import (
    ISA95JobOrderAndStateDataType,
    ISA95JobOrderDataType,
    ISA95JobResponseDataType,
    ISA95StateDataType,
    ISA95WorkMasterDataType,
    LocalizedText,
)
from microdcs.redis import (
    CloudEventDedupeDAO,
    CounterDAO,
    EquipmentListDAO,
    JobOrderAndStateDAO,
    JobResponseDAO,
    MaterialClassListDAO,
    MaterialDefinitionListDAO,
    PersonnelListDAO,
    PhysicalAssetListDAO,
    RedisKeySchema,
    TransactionDedupeDAO,
    WorkMasterDAO,
    _escape_tag,
    prefixed_key,
)

DEFAULT_PREFIX = "microdcs-test"


# ---------------------------------------------------------------------------
# prefixed_key decorator
# ---------------------------------------------------------------------------


class TestPrefixedKey:
    def test_prefixes_return_value(self):
        class Dummy:
            prefix = "pfx"

            @prefixed_key
            def key(self) -> str:
                return "foo"

        assert Dummy().key() == "pfx:foo"

    def test_preserves_function_name(self):
        class Dummy:
            prefix = "pfx"

            @prefixed_key
            def my_func(self) -> str:
                return "bar"

        assert Dummy().my_func.__name__ == "my_func"

    def test_passes_args(self):
        class Dummy:
            prefix = "pfx"

            @prefixed_key
            def key(self, a: str, b: str) -> str:
                return f"{a}-{b}"

        assert Dummy().key("x", "y") == "pfx:x-y"


# ---------------------------------------------------------------------------
# _escape_tag helper
# ---------------------------------------------------------------------------


class TestEscapeTag:
    def test_plain_alphanumeric(self):
        assert _escape_tag("abc123") == "abc123"

    def test_underscores_unescaped(self):
        assert _escape_tag("AllowedToStart_Ready") == "AllowedToStart_Ready"

    def test_hyphens_escaped(self):
        assert _escape_tag("jo-1") == "jo\\-1"

    def test_dots_escaped(self):
        assert _escape_tag("app.jobs.scope") == "app\\.jobs\\.scope"

    def test_mixed_special_chars(self):
        result = _escape_tag("a-b.c@d")
        assert result == "a\\-b\\.c\\@d"


# ---------------------------------------------------------------------------
# RedisKeySchema
# ---------------------------------------------------------------------------


class TestRedisKeySchema:
    def setup_method(self) -> None:
        self.schema = RedisKeySchema(prefix="test")

    def test_default_prefix(self):
        schema = RedisKeySchema()
        assert schema.prefix == DEFAULT_PREFIX

    def test_custom_prefix(self):
        assert self.schema.prefix == "test"

    def test_cloudevent_dedupe_key(self):
        key = self.schema.cloudevent_dedupe_key("src", "id-1")
        raw = "src:id-1"
        expected_hash = hashlib.blake2b(raw.encode(), digest_size=16).hexdigest()
        assert key == f"test:cededupe:{expected_hash}"

    def test_cloudevent_dedupe_key_consistency(self):
        k1 = self.schema.cloudevent_dedupe_key("s", "i")
        k2 = self.schema.cloudevent_dedupe_key("s", "i")
        assert k1 == k2

    def test_cloudevent_dedupe_key_uniqueness(self):
        k1 = self.schema.cloudevent_dedupe_key("src", "id-1")
        k2 = self.schema.cloudevent_dedupe_key("src", "id-2")
        assert k1 != k2

    def test_transaction_dedupe_key(self):
        key = self.schema.transaction_dedupe_key("scope1", "tx-1")
        raw = "scope1:tx-1"
        expected_hash = hashlib.blake2b(raw.encode(), digest_size=16).hexdigest()
        assert key == f"test:transdedupe:{expected_hash}"

    def test_transaction_dedupe_key_consistency(self):
        k1 = self.schema.transaction_dedupe_key("s", "t")
        k2 = self.schema.transaction_dedupe_key("s", "t")
        assert k1 == k2

    def test_transaction_dedupe_key_uniqueness(self):
        k1 = self.schema.transaction_dedupe_key("scope", "tx-1")
        k2 = self.schema.transaction_dedupe_key("scope", "tx-2")
        assert k1 != k2

    def test_counter_key(self):
        assert self.schema.counter_key("my-counter") == "test:counter:my-counter"

    def test_counter_key_different_names(self):
        k1 = self.schema.counter_key("a")
        k2 = self.schema.counter_key("b")
        assert k1 != k2

    def test_joborder_key(self):
        assert self.schema.joborder_key("jo-1") == "test:joborder:jo-1"

    def test_joborder_list_key(self):
        assert self.schema.joborder_list_key("s1") == "test:joborder:list:s1"

    def test_jobresponse_key(self):
        assert self.schema.jobresponse_key("jr-1") == "test:jobresponse:jr-1"

    def test_jobresponse_list_key(self):
        assert self.schema.jobresponse_list_key("s1") == "test:jobresponse:list:s1"

    def test_workmaster_key(self):
        assert self.schema.workmaster_key("wm-1") == "test:workmaster:wm-1"

    def test_workmaster_list_key(self):
        assert self.schema.workmaster_list_key("s1") == "test:workmaster:list:s1"

    def test_equipment_list_key(self):
        assert self.schema.equipment_list_key("s1") == "test:equipment:list:s1"

    def test_materialclass_list_key(self):
        assert self.schema.materialclass_list_key("s1") == "test:materialclass:list:s1"

    def test_personnel_list_key(self):
        assert self.schema.personnel_list_key("s1") == "test:personnel:list:s1"

    def test_physicalasset_list_key(self):
        assert self.schema.physicalasset_list_key("s1") == "test:physicalasset:list:s1"

    def test_materialdefinition_list_key(self):
        assert (
            self.schema.materialdefinition_list_key("s1")
            == "test:materialdefinition:list:s1"
        )

    def test_jobresponse_index_name(self):
        assert self.schema.jobresponse_index_name() == "test:idx:jobresponse"

    def test_event_receiver_key(self):
        assert self.schema.event_receiver_key() == "test:event:receiver:list"

    def test_event_responder_key(self):
        assert self.schema.event_responder_key() == "test:event:responder:list"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_redis_mock() -> AsyncMock:
    """Create an AsyncMock that behaves like redis.asyncio.Redis."""
    mock = AsyncMock()
    # redis.json() is a sync call that returns a sub-client with async methods
    json_sub = AsyncMock()
    mock.json = MagicMock(return_value=json_sub)
    # redis.ft() is a sync call that returns a sub-client with async methods
    ft_sub = AsyncMock()
    mock.ft = MagicMock(return_value=ft_sub)
    # redis.pipeline(...) returns a sync object used as an async context manager
    pipe = AsyncMock()
    pipe.__aenter__.return_value = pipe
    pipe.__aexit__.return_value = False
    pipe_json_sub = MagicMock()
    pipe.json = MagicMock(return_value=pipe_json_sub)
    pipe.zadd = MagicMock()
    pipe.zrem = MagicMock()
    pipe.delete = MagicMock()
    mock.pipeline = MagicMock(return_value=pipe)
    return mock


def _make_schema(prefix: str = "test") -> RedisKeySchema:
    return RedisKeySchema(prefix=prefix)


# ---------------------------------------------------------------------------
# CloudEventDedupeDAO
# ---------------------------------------------------------------------------


class TestCloudEventDedupeDAO:
    def setup_method(self) -> None:
        self.redis = _make_redis_mock()
        self.schema = _make_schema()
        self.dao = CloudEventDedupeDAO(self.redis, self.schema, ttl=300)

    @pytest.mark.asyncio
    async def test_is_duplicate_returns_false_on_first_set(self):
        # SET NX returns True (key was set) → not a duplicate
        self.redis.set.return_value = True
        result = await self.dao.is_duplicate("src", "id-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_is_duplicate_returns_true_when_already_exists(self):
        # SET NX returns None/False (key already exists) → duplicate
        self.redis.set.return_value = None
        result = await self.dao.is_duplicate("src", "id-1")
        assert result is True

    @pytest.mark.asyncio
    async def test_set_called_with_correct_args(self):
        self.redis.set.return_value = True
        await self.dao.is_duplicate("src", "id-1")
        key = self.schema.cloudevent_dedupe_key("src", "id-1")
        self.redis.set.assert_awaited_once_with(key, "1", ex=300, nx=True)

    @pytest.mark.asyncio
    async def test_custom_ttl(self):
        dao = CloudEventDedupeDAO(self.redis, self.schema, ttl=60)
        self.redis.set.return_value = True
        await dao.is_duplicate("src", "id-1")
        call_kwargs = self.redis.set.call_args.kwargs
        assert call_kwargs["ex"] == 60


# ---------------------------------------------------------------------------
# TransactionDedupeDAO
# ---------------------------------------------------------------------------


class TestTransactionDedupeDAO:
    def setup_method(self) -> None:
        self.redis = _make_redis_mock()
        self.schema = _make_schema()
        self.dao = TransactionDedupeDAO(self.redis, self.schema, ttl=300)

    @pytest.mark.asyncio
    async def test_is_duplicate_returns_false_on_first_set(self):
        # SET NX returns True (key was set) → not a duplicate
        self.redis.set.return_value = True
        result = await self.dao.is_duplicate("scope1", "tx-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_is_duplicate_returns_true_when_already_exists(self):
        # SET NX returns None/False (key already exists) → duplicate
        self.redis.set.return_value = None
        result = await self.dao.is_duplicate("scope1", "tx-1")
        assert result is True

    @pytest.mark.asyncio
    async def test_set_called_with_correct_args(self):
        self.redis.set.return_value = True
        await self.dao.is_duplicate("scope1", "tx-1")
        key = self.schema.transaction_dedupe_key("scope1", "tx-1")
        self.redis.set.assert_awaited_once_with(key, "1", ex=300, nx=True)

    @pytest.mark.asyncio
    async def test_custom_ttl(self):
        dao = TransactionDedupeDAO(self.redis, self.schema, ttl=60)
        self.redis.set.return_value = True
        await dao.is_duplicate("scope1", "tx-1")
        call_kwargs = self.redis.set.call_args.kwargs
        assert call_kwargs["ex"] == 60


# ---------------------------------------------------------------------------
# CounterDAO
# ---------------------------------------------------------------------------


class TestCounterDAO:
    def setup_method(self) -> None:
        self.redis = _make_redis_mock()
        self.schema = _make_schema()
        self.dao = CounterDAO(self.redis, self.schema)

    @pytest.mark.asyncio
    async def test_increment_returns_new_value(self):
        self.redis.incr.return_value = 1
        result = await self.dao.increment("my-counter")
        assert result == 1

    @pytest.mark.asyncio
    async def test_increment_calls_incr_with_correct_key(self):
        self.redis.incr.return_value = 1
        await self.dao.increment("my-counter")
        key = self.schema.counter_key("my-counter")
        self.redis.incr.assert_awaited_once_with(key)

    @pytest.mark.asyncio
    async def test_increment_successive_calls(self):
        self.redis.incr.side_effect = [1, 2, 3]
        assert await self.dao.increment("c") == 1
        assert await self.dao.increment("c") == 2
        assert await self.dao.increment("c") == 3

    @pytest.mark.asyncio
    async def test_get_returns_value(self):
        self.redis.get.return_value = b"42"
        result = await self.dao.get("my-counter")
        assert result == 42

    @pytest.mark.asyncio
    async def test_get_calls_get_with_correct_key(self):
        self.redis.get.return_value = b"1"
        await self.dao.get("my-counter")
        key = self.schema.counter_key("my-counter")
        self.redis.get.assert_awaited_once_with(key)

    @pytest.mark.asyncio
    async def test_get_returns_zero_when_not_exists(self):
        self.redis.get.return_value = None
        result = await self.dao.get("missing")
        assert result == 0


# ---------------------------------------------------------------------------
# JobOrderAndStateDAO
# ---------------------------------------------------------------------------


class TestJobOrderAndStateDAO:
    def setup_method(self) -> None:
        self.redis = _make_redis_mock()
        self.schema = _make_schema()
        self.dao = JobOrderAndStateDAO(self.redis, self.schema)

    def _make_job_order_and_state(
        self,
        job_order_id: str = "jo-1",
        priority: int = 5,
    ) -> ISA95JobOrderAndStateDataType:
        return ISA95JobOrderAndStateDataType(
            job_order=ISA95JobOrderDataType(
                job_order_id=job_order_id,
                priority=priority,
            ),
        )

    @pytest.mark.asyncio
    async def test_save_stores_json_and_adds_to_sorted_set(self):
        jo = self._make_job_order_and_state()
        with patch.object(
            jo, "to_dict", return_value={"JobOrder": {"JobOrderID": "jo-1"}}
        ):
            await self.dao.save(jo, scope="s1")

        pipe = self.redis.pipeline.return_value
        self.redis.pipeline.assert_called_once_with(transaction=True)
        pipe.execute.assert_awaited_once()
        json_mock = pipe.json()
        json_mock.set.assert_called_once()
        call_args = json_mock.set.call_args
        assert call_args.args[0] == self.schema.joborder_key("jo-1")
        assert call_args.args[1] == "$"

        pipe.zadd.assert_called_once_with(
            self.schema.joborder_list_key("s1"), {"jo-1": 5}
        )

    @pytest.mark.asyncio
    async def test_save_raises_when_no_job_order(self):
        jo = ISA95JobOrderAndStateDataType()
        with pytest.raises(ValueError, match="job_order_id"):
            await self.dao.save(jo, scope="s1")

    @pytest.mark.asyncio
    async def test_save_raises_when_no_job_order_id(self):
        jo = ISA95JobOrderAndStateDataType(
            job_order=ISA95JobOrderDataType(job_order_id=None),
        )
        with pytest.raises(ValueError, match="job_order_id"):
            await self.dao.save(jo, scope="s1")

    @pytest.mark.asyncio
    async def test_save_with_zero_priority(self):
        jo = self._make_job_order_and_state(priority=None)  # type: ignore[arg-type]
        with patch.object(jo, "to_dict", return_value={}):
            await self.dao.save(jo, scope="s1")
        pipe = self.redis.pipeline.return_value
        pipe.zadd.assert_called_once_with(
            self.schema.joborder_list_key("s1"), {"jo-1": 0}
        )

    @pytest.mark.asyncio
    async def test_retrieve_returns_object_when_found(self):
        jo = self._make_job_order_and_state()
        fake_data = {"JobOrder": {"JobOrderID": "jo-1"}, "_dataschema": "s"}

        json_mock = self.redis.json()
        json_mock.get.side_effect = [
            ["some-schema"],  # first call: dataschema
            [fake_data],  # second call: full data (JSONPath list)
        ]

        with patch.object(
            ISA95JobOrderAndStateDataType,
            "from_dict",
            return_value=jo,
        ) as mock_from:
            result = await self.dao.retrieve("jo-1")
            mock_from.assert_called_once()
            assert result is jo

    @pytest.mark.asyncio
    async def test_retrieve_returns_none_when_not_found(self):
        json_mock = self.redis.json()
        json_mock.get.return_value = None  # no dataschema → not found
        result = await self.dao.retrieve("missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_removes_key_and_list_entry(self):
        await self.dao.delete("jo-1", scope="s1")
        pipe = self.redis.pipeline.return_value
        self.redis.pipeline.assert_called_once_with(transaction=True)
        pipe.delete.assert_called_once_with(self.schema.joborder_key("jo-1"))
        pipe.zrem.assert_called_once_with(self.schema.joborder_list_key("s1"), "jo-1")
        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_remove_from_list(self):
        await self.dao.remove_from_list("jo-1", scope="s1")
        self.redis.zrem.assert_awaited_once_with(
            self.schema.joborder_list_key("s1"), "jo-1"
        )

    @pytest.mark.asyncio
    async def test_list_returns_ids(self):
        self.redis.zrange.return_value = ["jo-1", "jo-2"]
        result = await self.dao.list(scope="s1")
        assert result == ["jo-1", "jo-2"]
        self.redis.zrange.assert_awaited_once_with(
            self.schema.joborder_list_key("s1"), 0, -1
        )


# ---------------------------------------------------------------------------
# JobResponseDAO
# ---------------------------------------------------------------------------


class TestJobResponseDAO:
    def setup_method(self) -> None:
        self.redis = _make_redis_mock()
        self.schema = _make_schema()
        self.dao = JobResponseDAO(self.redis, self.schema)

    def _make_job_response(
        self,
        job_response_id: str = "jr-1",
        start_time: str | None = None,
        job_order_id: str | None = None,
        job_state: list[ISA95StateDataType] | None = None,
    ) -> ISA95JobResponseDataType:
        return ISA95JobResponseDataType(
            job_response_id=job_response_id,
            start_time=start_time,
            job_order_id=job_order_id,
            job_state=job_state,
        )

    def _make_state(self, *state_names: str) -> list[ISA95StateDataType]:
        return [
            ISA95StateDataType(state_text=LocalizedText(text=name))
            for name in state_names
        ]

    @pytest.mark.asyncio
    async def test_save_stores_json_and_adds_to_sorted_set(self):
        jr = self._make_job_response()
        with patch.object(jr, "to_dict", return_value={"JobResponseID": "jr-1"}):
            await self.dao.save(jr, scope="s1")

        pipe = self.redis.pipeline.return_value
        self.redis.pipeline.assert_called_once_with(transaction=True)
        pipe.execute.assert_awaited_once()
        json_mock = pipe.json()
        json_mock.set.assert_called_once()
        call_args = json_mock.set.call_args
        assert call_args.args[0] == self.schema.jobresponse_key("jr-1")
        assert call_args.args[1] == "$"

        pipe.zadd.assert_called_once_with(
            self.schema.jobresponse_list_key("s1"), {"jr-1": 0}
        )

    @pytest.mark.asyncio
    async def test_save_raises_when_no_response_id(self):
        jr = ISA95JobResponseDataType()
        with pytest.raises(ValueError, match="job_response_id"):
            await self.dao.save(jr, scope="s1")

    @pytest.mark.asyncio
    async def test_retrieve_returns_object_when_found(self):
        jr = self._make_job_response()
        fake_data = {"JobResponseID": "jr-1", "_dataschema": "s", "_scope": "s1"}

        json_mock = self.redis.json()
        json_mock.get.side_effect = [
            ["some-schema"],
            [fake_data],
        ]

        with patch.object(
            ISA95JobResponseDataType,
            "from_dict",
            return_value=jr,
        ) as mock_from:
            result = await self.dao.retrieve("jr-1")
            mock_from.assert_called_once()
            assert result is jr

    @pytest.mark.asyncio
    async def test_retrieve_strips_metadata_fields(self):
        jr = self._make_job_response()
        fake_data = {
            "JobResponseID": "jr-1",
            "_dataschema": "s",
            "_scope": "s1",
            "_normalized_state": "Running",
        }

        json_mock = self.redis.json()
        json_mock.get.side_effect = [["some-schema"], [fake_data]]

        with patch.object(
            ISA95JobResponseDataType,
            "from_dict",
            return_value=jr,
        ) as mock_from:
            await self.dao.retrieve("jr-1")
            # Verify the three metadata fields were removed before from_dict
            passed_data = mock_from.call_args.args[0]
            assert "_dataschema" not in passed_data
            assert "_scope" not in passed_data
            assert "_normalized_state" not in passed_data
            assert "JobResponseID" in passed_data

    @pytest.mark.asyncio
    async def test_retrieve_returns_none_when_not_found(self):
        json_mock = self.redis.json()
        json_mock.get.return_value = None
        result = await self.dao.retrieve("missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_removes_key_and_list_entry(self):
        await self.dao.delete("jr-1", scope="s1")
        pipe = self.redis.pipeline.return_value
        self.redis.pipeline.assert_called_once_with(transaction=True)
        pipe.delete.assert_called_once_with(self.schema.jobresponse_key("jr-1"))
        pipe.zrem.assert_called_once_with(
            self.schema.jobresponse_list_key("s1"), "jr-1"
        )
        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_remove_from_list(self):
        await self.dao.remove_from_list("jr-1", scope="s1")
        self.redis.zrem.assert_awaited_once_with(
            self.schema.jobresponse_list_key("s1"), "jr-1"
        )

    @pytest.mark.asyncio
    async def test_list_returns_ids(self):
        self.redis.zrange.return_value = ["jr-2", "jr-1"]
        result = await self.dao.list(scope="s1")
        assert result == ["jr-2", "jr-1"]
        self.redis.zrange.assert_awaited_once_with(
            self.schema.jobresponse_list_key("s1"), 0, -1
        )

    # -- normalize_state -------------------------------------------------------

    def test_normalize_state_single(self):
        state = self._make_state("Running")
        assert JobResponseDAO.normalize_state(state) == "Running"

    def test_normalize_state_hierarchical(self):
        state = self._make_state("AllowedToStart", "Ready")
        assert JobResponseDAO.normalize_state(state) == "AllowedToStart_Ready"

    def test_normalize_state_empty_list(self):
        assert JobResponseDAO.normalize_state([]) == ""

    def test_normalize_state_skips_none_text(self):
        state = [ISA95StateDataType(state_text=None), *self._make_state("Running")]
        assert JobResponseDAO.normalize_state(state) == "Running"

    # -- save passes metadata via to_dict context ----------------------------

    @pytest.mark.asyncio
    async def test_save_passes_scope_in_context(self):
        jr = self._make_job_response()
        with patch.object(
            jr, "to_dict", return_value={"JobResponseID": "jr-1"}
        ) as mock_to_dict:
            await self.dao.save(jr, scope="s1")

        ctx = mock_to_dict.call_args.kwargs["context"]
        assert ctx["add_scope"] == "s1"

    @pytest.mark.asyncio
    async def test_save_passes_normalized_state_in_context(self):
        state = self._make_state("Running")
        jr = self._make_job_response(job_state=state)
        with patch.object(
            jr, "to_dict", return_value={"JobResponseID": "jr-1"}
        ) as mock_to_dict:
            await self.dao.save(jr, scope="s1")

        ctx = mock_to_dict.call_args.kwargs["context"]
        assert ctx["add_normalized_state"] == "Running"

    @pytest.mark.asyncio
    async def test_save_omits_normalized_state_when_no_state(self):
        jr = self._make_job_response()
        with patch.object(
            jr, "to_dict", return_value={"JobResponseID": "jr-1"}
        ) as mock_to_dict:
            await self.dao.save(jr, scope="s1")

        ctx = mock_to_dict.call_args.kwargs["context"]
        assert "add_normalized_state" not in ctx

    # -- initialize -----------------------------------------------------------

    @pytest.mark.asyncio
    async def test_initialize_creates_index_when_not_exists(self):
        ft_mock = self.redis.ft()
        ft_mock.info.side_effect = redis.ResponseError("Unknown index")
        await self.dao.initialize()
        ft_mock.create_index.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_initialize_skips_when_index_exists(self):
        ft_mock = self.redis.ft()
        ft_mock.info.return_value = {"index_name": "test"}  # index exists
        await self.dao.initialize()
        ft_mock.create_index.assert_not_awaited()

    # -- retrieve_by_job_order_id (FT.SEARCH) ---------------------------------

    @pytest.mark.asyncio
    async def test_retrieve_by_job_order_id_returns_response(self):
        jr = self._make_job_response()
        ft_mock = self.redis.ft()
        # FT.SEARCH result with one matching doc
        doc = MagicMock()
        doc.id = self.schema.jobresponse_key("jr-1")
        search_result = MagicMock(total=1, docs=[doc])
        ft_mock.search.return_value = search_result

        json_mock = self.redis.json()
        json_mock.get.side_effect = [
            ["some-schema"],
            [{"JobResponseID": "jr-1", "_dataschema": "s", "_scope": "s1"}],
        ]

        with patch.object(ISA95JobResponseDataType, "from_dict", return_value=jr):
            result = await self.dao.retrieve_by_job_order_id("jo-1")

        ft_mock.search.assert_awaited_once()
        assert result is jr

    @pytest.mark.asyncio
    async def test_retrieve_by_job_order_id_returns_none_when_not_found(self):
        ft_mock = self.redis.ft()
        search_result = MagicMock(total=0, docs=[])
        ft_mock.search.return_value = search_result
        result = await self.dao.retrieve_by_job_order_id("jo-missing")
        assert result is None

    # -- retrieve_by_state (FT.SEARCH) ----------------------------------------

    @pytest.mark.asyncio
    async def test_retrieve_by_state_returns_list(self):
        jr = self._make_job_response()
        state = self._make_state("Running")

        ft_mock = self.redis.ft()
        doc = MagicMock()
        doc.id = self.schema.jobresponse_key("jr-1")
        search_result = MagicMock(total=1, docs=[doc])
        ft_mock.search.return_value = search_result

        json_mock = self.redis.json()
        json_mock.get.side_effect = [
            ["some-schema"],
            [{"JobResponseID": "jr-1", "_dataschema": "s", "_scope": "s1"}],
        ]

        with patch.object(ISA95JobResponseDataType, "from_dict", return_value=jr):
            result = await self.dao.retrieve_by_state("s1", state)

        ft_mock.search.assert_awaited_once()
        assert result == [jr]

    @pytest.mark.asyncio
    async def test_retrieve_by_state_returns_empty_for_no_matches(self):
        state = self._make_state("Running")
        ft_mock = self.redis.ft()
        search_result = MagicMock(total=0, docs=[])
        ft_mock.search.return_value = search_result
        result = await self.dao.retrieve_by_state("s1", state)
        assert result == []

    @pytest.mark.asyncio
    async def test_retrieve_by_state_returns_empty_for_empty_state(self):
        result = await self.dao.retrieve_by_state("s1", [])
        assert result == []
        self.redis.ft().search.assert_not_awaited()


# ---------------------------------------------------------------------------
# WorkMasterDAO
# ---------------------------------------------------------------------------


class TestWorkMasterDAO:
    def setup_method(self) -> None:
        self.redis = _make_redis_mock()
        self.schema = _make_schema()
        self.dao = WorkMasterDAO(self.redis, self.schema)

    def _make_work_master(
        self, work_master_id: str = "wm-1"
    ) -> ISA95WorkMasterDataType:
        return ISA95WorkMasterDataType(id=work_master_id)

    @pytest.mark.asyncio
    async def test_save_stores_json_and_adds_to_set(self):
        wm = self._make_work_master()
        with patch.object(wm, "to_dict", return_value={"ID": "wm-1"}):
            await self.dao.save(wm, scope="s1")

        json_mock = self.redis.json()
        json_mock.set.assert_awaited_once()
        call_args = json_mock.set.call_args
        assert call_args.args[0] == self.schema.workmaster_key("wm-1")
        assert call_args.args[1] == "$"

        self.redis.sadd.assert_awaited_once_with(
            self.schema.workmaster_list_key("s1"), "wm-1"
        )

    @pytest.mark.asyncio
    async def test_save_raises_when_no_id(self):
        wm = ISA95WorkMasterDataType()
        with pytest.raises(ValueError, match="id"):
            await self.dao.save(wm, scope="s1")

    @pytest.mark.asyncio
    async def test_retrieve_returns_object_when_found(self):
        wm = self._make_work_master()
        fake_data = {"ID": "wm-1", "_dataschema": "s"}

        json_mock = self.redis.json()
        json_mock.get.side_effect = [
            ["some-schema"],
            [fake_data],
        ]

        with patch.object(
            ISA95WorkMasterDataType,
            "from_dict",
            return_value=wm,
        ) as mock_from:
            result = await self.dao.retrieve("wm-1")
            mock_from.assert_called_once()
            assert result is wm

    @pytest.mark.asyncio
    async def test_retrieve_returns_none_when_not_found(self):
        json_mock = self.redis.json()
        json_mock.get.return_value = None
        result = await self.dao.retrieve("missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_removes_key_and_set_entry(self):
        await self.dao.delete("wm-1", scope="s1")
        self.redis.delete.assert_awaited_once_with(self.schema.workmaster_key("wm-1"))
        self.redis.srem.assert_awaited_once_with(
            self.schema.workmaster_list_key("s1"), "wm-1"
        )

    @pytest.mark.asyncio
    async def test_remove_from_list(self):
        await self.dao.remove_from_list("wm-1", scope="s1")
        self.redis.srem.assert_awaited_once_with(
            self.schema.workmaster_list_key("s1"), "wm-1"
        )

    @pytest.mark.asyncio
    async def test_list_returns_ids(self):
        self.redis.smembers.return_value = {"wm-1", "wm-2"}
        result = await self.dao.list(scope="s1")
        assert set(result) == {"wm-1", "wm-2"}
        self.redis.smembers.assert_awaited_once_with(
            self.schema.workmaster_list_key("s1")
        )


# ---------------------------------------------------------------------------
# Simple set-based list DAOs (Equipment, MaterialClass, Personnel,
# PhysicalAsset, MaterialDefinition)
# ---------------------------------------------------------------------------


class TestEquipmentListDAO:
    def setup_method(self) -> None:
        self.redis = _make_redis_mock()
        self.schema = _make_schema()
        self.dao = EquipmentListDAO(self.redis, self.schema)

    @pytest.mark.asyncio
    async def test_add_to_list(self):
        await self.dao.add_to_list("eq-1", scope="s1")
        self.redis.sadd.assert_awaited_once_with(
            self.schema.equipment_list_key("s1"), "eq-1"
        )

    @pytest.mark.asyncio
    async def test_remove_from_list(self):
        await self.dao.remove_from_list("eq-1", scope="s1")
        self.redis.srem.assert_awaited_once_with(
            self.schema.equipment_list_key("s1"), "eq-1"
        )


class TestMaterialClassListDAO:
    def setup_method(self) -> None:
        self.redis = _make_redis_mock()
        self.schema = _make_schema()
        self.dao = MaterialClassListDAO(self.redis, self.schema)

    @pytest.mark.asyncio
    async def test_add_to_list(self):
        await self.dao.add_to_list("mc-1", scope="s1")
        self.redis.sadd.assert_awaited_once_with(
            self.schema.materialclass_list_key("s1"), "mc-1"
        )

    @pytest.mark.asyncio
    async def test_remove_from_list(self):
        await self.dao.remove_from_list("mc-1", scope="s1")
        self.redis.srem.assert_awaited_once_with(
            self.schema.materialclass_list_key("s1"), "mc-1"
        )


class TestPersonnelListDAO:
    def setup_method(self) -> None:
        self.redis = _make_redis_mock()
        self.schema = _make_schema()
        self.dao = PersonnelListDAO(self.redis, self.schema)

    @pytest.mark.asyncio
    async def test_add_to_list(self):
        await self.dao.add_to_list("p-1", scope="s1")
        self.redis.sadd.assert_awaited_once_with(
            self.schema.personnel_list_key("s1"), "p-1"
        )

    @pytest.mark.asyncio
    async def test_remove_from_list(self):
        await self.dao.remove_from_list("p-1", scope="s1")
        self.redis.srem.assert_awaited_once_with(
            self.schema.personnel_list_key("s1"), "p-1"
        )


class TestPhysicalAssetListDAO:
    def setup_method(self) -> None:
        self.redis = _make_redis_mock()
        self.schema = _make_schema()
        self.dao = PhysicalAssetListDAO(self.redis, self.schema)

    @pytest.mark.asyncio
    async def test_add_to_list(self):
        await self.dao.add_to_list("pa-1", scope="s1")
        self.redis.sadd.assert_awaited_once_with(
            self.schema.physicalasset_list_key("s1"), "pa-1"
        )

    @pytest.mark.asyncio
    async def test_remove_from_list(self):
        await self.dao.remove_from_list("pa-1", scope="s1")
        self.redis.srem.assert_awaited_once_with(
            self.schema.physicalasset_list_key("s1"), "pa-1"
        )


class TestMaterialDefinitionListDAO:
    def setup_method(self) -> None:
        self.redis = _make_redis_mock()
        self.schema = _make_schema()
        self.dao = MaterialDefinitionListDAO(self.redis, self.schema)

    @pytest.mark.asyncio
    async def test_add_to_list(self):
        await self.dao.add_to_list("md-1", scope="s1")
        self.redis.sadd.assert_awaited_once_with(
            self.schema.materialdefinition_list_key("s1"), "md-1"
        )

    @pytest.mark.asyncio
    async def test_remove_from_list(self):
        await self.dao.remove_from_list("md-1", scope="s1")
        self.redis.srem.assert_awaited_once_with(
            self.schema.materialdefinition_list_key("s1"), "md-1"
        )

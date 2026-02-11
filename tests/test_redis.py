import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.machinery_jobs import (
    ISA95JobOrderAndStateDataType,
    ISA95JobOrderDataType,
    ISA95JobResponseDataType,
    ISA95WorkMasterDataType,
)
from app.redis import (
    CloudEventDedupeDAO,
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
    prefixed_key,
    sanitize_scope,
)

DEFAULT_PREFIX = "microdcs-test"


# ---------------------------------------------------------------------------
# sanitize_scope helper
# ---------------------------------------------------------------------------


class TestSanitizeScope:
    def test_leading_slash_topic(self):
        assert sanitize_scope("/app/jobs/myscope") == "app.jobs.myscope"

    def test_no_leading_slash(self):
        assert sanitize_scope("app/jobs/myscope") == "app.jobs.myscope"

    def test_trailing_slash(self):
        assert sanitize_scope("/app/jobs/myscope/") == "app.jobs.myscope"

    def test_multiple_slashes(self):
        assert sanitize_scope("/app//jobs///myscope/") == "app.jobs.myscope"

    def test_single_segment(self):
        assert sanitize_scope("myscope") == "myscope"

    def test_preserves_dots(self):
        assert sanitize_scope("app.jobs/myscope") == "app.jobs.myscope"

    def test_empty_string(self):
        assert sanitize_scope("") == ""

    def test_only_slashes(self):
        assert sanitize_scope("///") == ""

    def test_special_chars_replaced(self):
        assert sanitize_scope("/app/jobs+my scope") == "app.jobs.my.scope"


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
        expected_hash = hashlib.md5(raw.encode()).hexdigest()
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
        expected_hash = hashlib.md5(raw.encode()).hexdigest()
        assert key == f"test:transdedupe:{expected_hash}"

    def test_transaction_dedupe_key_consistency(self):
        k1 = self.schema.transaction_dedupe_key("s", "t")
        k2 = self.schema.transaction_dedupe_key("s", "t")
        assert k1 == k2

    def test_transaction_dedupe_key_uniqueness(self):
        k1 = self.schema.transaction_dedupe_key("scope", "tx-1")
        k2 = self.schema.transaction_dedupe_key("scope", "tx-2")
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
            jo, "to_json", return_value='{"JobOrder":{"JobOrderID":"jo-1"}}'
        ):
            await self.dao.save(jo, scope="s1")

        json_mock = self.redis.json()
        json_mock.set.assert_awaited_once()
        call_args = json_mock.set.call_args
        assert call_args.args[0] == self.schema.joborder_key("jo-1")
        assert call_args.args[1] == "$"

        self.redis.zadd.assert_awaited_once_with(
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
        with patch.object(jo, "to_json", return_value="{}"):
            await self.dao.save(jo, scope="s1")
        self.redis.zadd.assert_awaited_once_with(
            self.schema.joborder_list_key("s1"), {"jo-1": 0}
        )

    @pytest.mark.asyncio
    async def test_retrieve_returns_object_when_found(self):
        jo = self._make_job_order_and_state()
        fake_data = {"JobOrder": {"JobOrderID": "jo-1"}, "_dataschema": "s"}

        json_mock = self.redis.json()
        json_mock.get.side_effect = [
            ["some-schema"],  # first call: dataschema
            fake_data,  # second call: full data (a dict)
        ]

        with patch.object(
            ISA95JobOrderAndStateDataType,
            "from_json",
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
        self.redis.delete.assert_awaited_once_with(self.schema.joborder_key("jo-1"))
        self.redis.zrem.assert_awaited_once_with(
            self.schema.joborder_list_key("s1"), "jo-1"
        )

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
    ) -> ISA95JobResponseDataType:
        return ISA95JobResponseDataType(
            job_response_id=job_response_id,
            start_time=start_time,
        )

    @pytest.mark.asyncio
    async def test_save_stores_json_and_adds_to_sorted_set(self):
        jr = self._make_job_response()
        with patch.object(jr, "to_json", return_value='{"JobResponseID":"jr-1"}'):
            await self.dao.save(jr, scope="s1")

        json_mock = self.redis.json()
        json_mock.set.assert_awaited_once()
        call_args = json_mock.set.call_args
        assert call_args.args[0] == self.schema.jobresponse_key("jr-1")
        assert call_args.args[1] == "$"

        self.redis.zadd.assert_awaited_once_with(
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
        fake_data = {"JobResponseID": "jr-1", "_dataschema": "s"}

        json_mock = self.redis.json()
        json_mock.get.side_effect = [
            ["some-schema"],
            fake_data,
        ]

        with patch.object(
            ISA95JobResponseDataType,
            "from_json",
            return_value=jr,
        ) as mock_from:
            result = await self.dao.retrieve("jr-1")
            mock_from.assert_called_once()
            assert result is jr

    @pytest.mark.asyncio
    async def test_retrieve_returns_none_when_not_found(self):
        json_mock = self.redis.json()
        json_mock.get.return_value = None
        result = await self.dao.retrieve("missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_removes_key_and_list_entry(self):
        await self.dao.delete("jr-1", scope="s1")
        self.redis.delete.assert_awaited_once_with(self.schema.jobresponse_key("jr-1"))
        self.redis.zrem.assert_awaited_once_with(
            self.schema.jobresponse_list_key("s1"), "jr-1"
        )

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
        with patch.object(wm, "to_json", return_value='{"ID":"wm-1"}'):
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
            fake_data,
        ]

        with patch.object(
            ISA95WorkMasterDataType,
            "from_json",
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

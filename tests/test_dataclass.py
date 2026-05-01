from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pytest
from mashumaro.config import BaseConfig

from microdcs.dataclass import (
    DataClassConfig,
    DataClassMixin,
    DataClassValidationMixin,
    get_type_id,
    get_type_schema,
    type_has_config_class,
)

# ---------------------------------------------------------------------------
# Test models
# ---------------------------------------------------------------------------


@dataclass
class ModelForTest(DataClassValidationMixin, DataClassMixin):
    name: str = field(metadata={"min_length": 3, "max_length": 10})
    _hidden: str = "secret"

    class Config(DataClassConfig):
        pass


@dataclass
class EventModel(DataClassMixin):
    class Config(DataClassConfig):
        type_id = "com.example.event.v1"
        type_schema = "https://example.com/schemas/event-v1"


@dataclass
class PlainModel(DataClassMixin):
    """Model without a DataClassConfig – used for negative-path tests."""

    value: str = ""

    class Config(BaseConfig):
        code_generation_options = ["ADD_SERIALIZATION_CONTEXT"]


# Models for get_field_types tests

type StrOrInt = str | int


@dataclass
class MyType1(DataClassMixin):
    """Simple test class for union type alias testing."""

    value: str = ""

    class Config(BaseConfig):
        code_generation_options = ["ADD_SERIALIZATION_CONTEXT"]


@dataclass
class MyType2(DataClassMixin):
    """Simple test class for union type alias testing."""

    value: int = 0

    class Config(BaseConfig):
        code_generation_options = ["ADD_SERIALIZATION_CONTEXT"]


type MyUnionType = MyType1 | MyType2


@dataclass(kw_only=True)
class FieldTypesModel(DataClassMixin):
    plain_str: str = ""
    plain_int: int = 0
    optional_str: Optional[str] = None
    union_field: str | int = ""
    list_field: list[str] = field(default_factory=list)
    dict_field: dict[str, int] = field(default_factory=dict)
    forward_ref: EventModel | None = None
    alias_field: StrOrInt = ""
    union_type_with_none: MyUnionType | None = None

    class Config(BaseConfig):
        code_generation_options = ["ADD_SERIALIZATION_CONTEXT"]


# ---------------------------------------------------------------------------
# type_has_config_class
# ---------------------------------------------------------------------------


class TestTypeHasConfigClass:
    def test_true_for_class_with_dataclass_config(self):
        assert type_has_config_class(EventModel) is True

    def test_false_for_class_without_config(self):
        assert type_has_config_class(PlainModel) is False

    def test_false_for_builtin_type(self):
        assert type_has_config_class(str) is False


# ---------------------------------------------------------------------------
# get_type_id / get_type_schema
# ---------------------------------------------------------------------------


class TestGetTypeId:
    def test_returns_type_when_present(self):
        assert get_type_id(EventModel) == "com.example.event.v1"

    def test_returns_none_when_no_config(self):
        assert get_type_id(PlainModel) is None

    def test_returns_none_for_builtin(self):
        assert get_type_id(str) is None


class TestGetTypeSchema:
    def test_returns_dataschema_when_present(self):
        assert get_type_schema(EventModel) == "https://example.com/schemas/event-v1"

    def test_returns_none_when_no_config(self):
        assert get_type_schema(PlainModel) is None

    def test_returns_none_for_builtin(self):
        assert get_type_schema(str) is None


# ---------------------------------------------------------------------------
# DataClassValidationMixin
# ---------------------------------------------------------------------------


class TestDataClassValidation:
    def test_validation_success(self):
        model = ModelForTest(name="valid")
        assert model.name == "valid"

    def test_validation_min_length(self):
        with pytest.raises(ValueError, match="less than minimum value"):
            ModelForTest(name="no")

    def test_validation_max_length(self):
        with pytest.raises(ValueError, match="greater than maximum value"):
            ModelForTest(name="verylongname")

    def test_get_field_metadata_returns_none_for_unknown_field(self):
        model = ModelForTest(name="valid")
        assert model.get_field_metadata("nonexistent") is None

    def test_setattr_no_metadata_field(self):
        """Setting a field that has no metadata should not raise."""
        model = ModelForTest(name="valid")
        model._hidden = "new_value"
        assert model._hidden == "new_value"


# ---------------------------------------------------------------------------
# DataClassMixin – serialization
# ---------------------------------------------------------------------------


class TestDataClassMixinSerialization:
    def test_hides_underscore_fields(self):
        model = ModelForTest(name="valid")
        data = model.to_dict()
        assert "name" in data
        assert "_hidden" not in data
        assert data["name"] == "valid"

    def test_add_type_schema_context(self):
        model = EventModel()
        data = model.to_dict(context={"add_type_schema": True})
        assert data["_dataschema"] == "https://example.com/schemas/event-v1"

    def test_add_type_schema_not_added_when_absent(self):
        """Class without type_schema in Config shouldn't get _dataschema."""
        model = PlainModel(value="x")
        data = model.to_dict(context={"add_type_schema": True})
        assert "_dataschema" not in data

    def test_add_type_schema_not_added_by_default(self):
        model = EventModel()
        data = model.to_dict()
        assert "_dataschema" not in data

    def test_add_scope_context(self):
        model = EventModel()
        data = model.to_dict(context={"add_scope": "app.jobs.s1"})
        assert data["_scope"] == "app.jobs.s1"

    def test_add_scope_not_added_when_absent(self):
        model = EventModel()
        data = model.to_dict()
        assert "_scope" not in data

    def test_add_normalized_state_context(self):
        model = EventModel()
        data = model.to_dict(context={"add_normalized_state": "Running_Active"})
        assert data["_normalized_state"] == "Running_Active"

    def test_add_normalized_state_not_added_when_absent(self):
        model = EventModel()
        data = model.to_dict()
        assert "_normalized_state" not in data

    def test_all_metadata_context_combined(self):
        model = EventModel()
        data = model.to_dict(
            context={
                "add_type_schema": True,
                "add_scope": "s1",
                "add_normalized_state": "Running",
            }
        )
        assert data["_dataschema"] == "https://example.com/schemas/event-v1"
        assert data["_scope"] == "s1"
        assert data["_normalized_state"] == "Running"

    def test_json_round_trip(self):
        model = EventModel()
        restored = EventModel.from_json(model.to_jsonb())
        assert isinstance(restored, EventModel)

    def test_msgpack_round_trip(self):
        model = EventModel()
        restored = EventModel.from_msgpack(model.to_msgpack())
        assert isinstance(restored, EventModel)


# ---------------------------------------------------------------------------
# DataClassConfig – pattern matching
# ---------------------------------------------------------------------------


class TestWildcardMatch:
    def test_exact_match(self):
        assert EventModel.Config.matches_type_id_pattern("com.example.event.v1")

    def test_prefix_wildcard(self):
        assert EventModel.Config.matches_type_id_pattern("com.example.event.*")

    def test_suffix_wildcard(self):
        assert EventModel.Config.matches_type_id_pattern("*.event.v1")

    def test_middle_wildcard(self):
        assert EventModel.Config.matches_type_id_pattern("com.*.v1")

    def test_no_match(self):
        assert not EventModel.Config.matches_type_id_pattern("com.other.event")


# ---------------------------------------------------------------------------
# DataClassMixin – get_field_types
# ---------------------------------------------------------------------------


class TestGetFieldTypes:
    def test_plain_type(self):
        model = FieldTypesModel()
        result = model.get_field_types("plain_str")
        assert result == [str]

    def test_plain_int(self):
        model = FieldTypesModel()
        result = model.get_field_types("plain_int")
        assert result == [int]

    def test_optional_field(self):
        model = FieldTypesModel()
        result = model.get_field_types("optional_str")
        assert result is not None
        assert set(result) == {str, type(None)}

    def test_union_field(self):
        model = FieldTypesModel()
        result = model.get_field_types("union_field")
        assert result is not None
        assert set(result) == {str, int}

    def test_list_field(self):
        model = FieldTypesModel()
        result = model.get_field_types("list_field")
        assert result == [str]

    def test_dict_field(self):
        model = FieldTypesModel()
        result = model.get_field_types("dict_field")
        assert result == [str, int]

    def test_forward_ref_resolved(self):
        model = FieldTypesModel()
        result = model.get_field_types("forward_ref")
        assert result is not None
        assert EventModel in result
        assert type(None) in result

    def test_nonexistent_field_returns_none(self):
        model = FieldTypesModel()
        assert model.get_field_types("nonexistent") is None

    def test_result_is_always_a_list(self):
        model = FieldTypesModel()
        for name in ("plain_str", "optional_str", "union_field", "list_field"):
            result = model.get_field_types(name)
            assert isinstance(result, list)

    def test_type_alias(self):
        model = FieldTypesModel()
        result = model.get_field_types("alias_field")
        assert result is not None
        assert set(result) == {str, int}

    def test_union_type_alias_with_none(self):
        model = FieldTypesModel()
        result = model.get_field_types("union_type_with_none")
        assert result is not None
        assert set(result) == {MyType1, MyType2, type(None)}

from dataclasses import dataclass, field

import pytest

from app.dataclass import DataClassConfig, DataClassMixin, DataClassValidationMixin


@dataclass
class ModelForTest(DataClassMixin, DataClassValidationMixin):
    name: str = field(metadata={"min_length": 3, "max_length": 10})
    _hidden: str = "secret"

    class Config(DataClassConfig):
        pass


def test_validation_success():
    model = ModelForTest(name="valid")
    assert model.name == "valid"


def test_validation_min_length():
    with pytest.raises(ValueError, match="less than minimum value"):
        ModelForTest(name="no")


def test_validation_max_length():
    with pytest.raises(ValueError, match="greater than maximum value"):
        ModelForTest(name="verylongname")


def test_serialization_hides_underscore_fields():
    model = ModelForTest(name="valid")
    data = model.to_dict()
    assert "name" in data
    assert "_hidden" not in data
    assert data["name"] == "valid"


@dataclass
class EventModel(DataClassMixin):
    class Config(DataClassConfig):
        cloudevent_type = "com.example.event.v1"


def test_wildcard_match():
    # Exact match (should work)
    assert EventModel.Config.matches_cloudevent_type_pattern("com.example.event.v1")

    # Prefix wildcard (should work after fix)
    assert EventModel.Config.matches_cloudevent_type_pattern("com.example.event.*")

    # Suffix wildcard (should work after fix)
    assert EventModel.Config.matches_cloudevent_type_pattern("*.event.v1")

    # Middle wildcard
    assert EventModel.Config.matches_cloudevent_type_pattern("com.*.v1")

    # No match
    assert not EventModel.Config.matches_cloudevent_type_pattern("com.other.event")

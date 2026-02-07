import dataclasses
import fnmatch
from typing import Any, Dict, Optional

from mashumaro.config import BaseConfig
from mashumaro.mixins.msgpack import DataClassMessagePackMixin
from mashumaro.mixins.orjson import DataClassORJSONMixin


def type_has_config_class(cls: type) -> bool:
    return hasattr(cls, "Config") and issubclass(cls.Config, DataClassConfig)


def get_cloudevent_type(cls: type) -> str | None:
    if type_has_config_class(cls):
        return cls.Config.cloudevent_type
    return None


def get_cloudevent_dataschema(cls: type) -> str | None:
    if type_has_config_class(cls):
        return cls.Config.cloudevent_dataschema
    return None


class DataClassValidationMixin:
    def get_field_metadata(self, name: str) -> Any:
        for field in dataclasses.fields(self.__class__.__mro__[0]):
            if field.name == name:
                return field.metadata
        return None

    def __setattr__(self, name: str, value: Any) -> None:
        metadata = self.get_field_metadata(name)
        if metadata is not None:
            for key, constraint in metadata.items():
                match key:
                    case "min_length":
                        if isinstance(value, str) and len(value) < constraint:
                            raise ValueError(
                                f"Value for field '{name}' is less than minimum value {constraint}"
                            )
                    case "max_length":
                        if isinstance(value, str) and len(value) > constraint:
                            raise ValueError(
                                f"Value for field '{name}' is greater than maximum value {constraint}"
                            )
        super().__setattr__(name, value)


class DataClassMixin(DataClassORJSONMixin, DataClassMessagePackMixin):
    # remove hidden fields starting with "_" from serialization
    # if context has "add_cloudevent_dataschema" set to True, add "dataschema" field with value from Config.cloudevent_dataschema
    # this is used for persisting in Redis with the correct dataschema for later retrieval and processing
    def __post_serialize__(
        self, d: dict[Any, Any], context: Optional[Dict] = None
    ) -> dict[Any, Any]:
        for key in list(d.keys()):
            if key.startswith("_"):
                d.pop(key)
        if context and context.get("add_cloudevent_dataschema", False):
            cloudevent_dataschema = get_cloudevent_dataschema(self.__class__)
            if cloudevent_dataschema:
                d["_dataschema"] = cloudevent_dataschema
        return d


class DataClassConfig(BaseConfig):
    code_generation_options = ["ADD_SERIALIZATION_CONTEXT"]
    allow_deserialization_not_by_alias = True
    serialize_by_alias = True
    omit_none = True
    cloudevent_type: str
    cloudevent_dataschema: str

    @classmethod
    def matches_cloudevent_type_pattern(cls, pattern: str) -> bool:
        """Checks if the type matches a shell-style pattern (e.g., 'com.app.*')."""
        return fnmatch.fnmatch(cls.cloudevent_type, pattern)

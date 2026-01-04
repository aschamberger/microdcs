import dataclasses
from typing import Any, Dict

from mashumaro.config import BaseConfig
from mashumaro.mixins.orjson import DataClassORJSONMixin


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


class DataClassMixin(DataClassORJSONMixin):
    # remove hidden fields starting with "_" from serialization
    def __post_serialize__(self, d: Dict[Any, Any]) -> Dict[Any, Any]:
        for key in list(d.keys()):
            if key.startswith("_"):
                d.pop(key)
        return d


class DataClassConfig(BaseConfig):
    allow_deserialization_not_by_alias = True
    serialize_by_alias = True
    omit_none = True
    cloudevent_type: str
    cloudevent_dataschema: str

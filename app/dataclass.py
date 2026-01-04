from typing import Any, Dict

from mashumaro.config import BaseConfig
from mashumaro.mixins.orjson import DataClassORJSONMixin


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

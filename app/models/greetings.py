# Auto-generated from "greetings.jsonschema.json". Do not modify!
from app.dataclass import (
    DataClassConfig,
    DataClassResponseMixin,
    DataClassValidationMixin,
)
from app.identity_processor import InitDataClassMixin
from dataclasses import InitVar, field
from typing import Any
from dataclasses import dataclass


type Greetings = Any
"""
JSON Schema for HiddenObject, Hello and Bye types.
"""


@dataclass(kw_only=True)
class HiddenObject(DataClassValidationMixin, InitDataClassMixin):
    field: str

    # mashumaro config class
    class Config(DataClassConfig):
        pass


@dataclass(kw_only=True)
class Hello(
    DataClassValidationMixin, DataClassResponseMixin["Hello"], InitDataClassMixin
):
    __request_object__: InitVar[Hello | None] = None
    _hidden_str: str | None = None
    _hidden_obj: HiddenObject | None = None
    name: str = field(metadata={"max_length": 20, "min_length": 3})

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "Hello"
        cloudevent_type: str = "com.github.aschamberger.microdcs.identity.hello.v1"
        cloudevent_dataschema: str = (
            "https://aschamberger.github.io/schemas/microdcs/identity/v1.0.0/hello"
        )
        aliases: dict[str, str] = {
            "name": "Name",
        }


@dataclass(kw_only=True)
class Bye(DataClassValidationMixin, InitDataClassMixin):
    _hidden_str: str | None = None
    _hidden_obj: HiddenObject | None = None
    name: str = field(metadata={"max_length": 20, "min_length": 3})

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "com.github.aschamberger.microdcs.identity.bye.v1"
        cloudevent_dataschema: str = (
            "https://aschamberger.github.io/schemas/microdcs/identity/v1.0.0/bye"
        )

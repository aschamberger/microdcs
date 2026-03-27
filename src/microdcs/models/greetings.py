# Auto-generated from "greetings.schema.json". Do not modify!
from microdcs.dataclass import (
    DataClassConfig,
    DataClassResponseMixin,
    DataClassValidationMixin,
)
from microdcs.models.greetings_mixin import GreetingsDataClassMixin
from dataclasses import InitVar, field
from typing import Any
from dataclasses import dataclass


@dataclass(kw_only=True)
class HiddenObject(DataClassValidationMixin, GreetingsDataClassMixin):
    field: str

    # mashumaro config class
    class Config(DataClassConfig):
        pass


@dataclass(kw_only=True)
class Hello(
    DataClassValidationMixin, DataClassResponseMixin["Hello"], GreetingsDataClassMixin
):
    __request_object__: InitVar[Hello | None] = None
    __custom_metadata__: InitVar[dict[str, Any] | None] = None
    _hidden_str: str | None = None
    _hidden_obj: HiddenObject | None = None
    name: str = field(metadata={"max_length": 20, "min_length": 3})

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "Hello"
        cloudevent_type: str = "com.github.aschamberger.microdcs.greetings.hello.v1"
        cloudevent_dataschema: str = (
            "https://aschamberger.github.io/schemas/microdcs/greetings/v1.0.0/hello"
        )
        aliases: dict[str, str] = {
            "name": "Name",
        }


@dataclass(kw_only=True)
class Bye(DataClassValidationMixin, GreetingsDataClassMixin):
    __custom_metadata__: InitVar[dict[str, Any] | None] = None
    _hidden_str: str | None = None
    _hidden_obj: HiddenObject | None = None
    name: str = field(metadata={"max_length": 20, "min_length": 3})

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "com.github.aschamberger.microdcs.greetings.bye.v1"
        cloudevent_dataschema: str = (
            "https://aschamberger.github.io/schemas/microdcs/greetings/v1.0.0/bye"
        )


type Greetings = Hello | Bye
"""
JSON Schema for HiddenObject, Hello and Bye types.
"""

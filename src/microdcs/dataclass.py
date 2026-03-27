import dataclasses
import fnmatch
import inspect
import sys
import typing
from typing import (
    Any,
    Dict,
    Generic,
    Optional,
    Type,
    TypeAliasType,
    TypeVar,
    get_args,
    get_origin,
)

from mashumaro.config import BaseConfig
from mashumaro.mixins.msgpack import DataClassMessagePackMixin
from mashumaro.mixins.orjson import DataClassORJSONMixin


def type_has_dataclass_mixin(cls: type) -> bool:
    return issubclass(cls.Config, DataClassMixin)


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
    # context keys for persisting metadata alongside the document in Redis:
    #   add_cloudevent_dataschema: bool – inject _dataschema from Config
    #   add_scope: str | None       – inject _scope with the given value
    #   add_normalized_state: str | None – inject _normalized_state with the given value
    def __post_serialize__(
        self, d: dict[Any, Any], context: Optional[Dict] = None
    ) -> dict[Any, Any]:
        for key in list(d.keys()):
            if key.startswith("_"):
                d.pop(key)
        if context:
            if context.get("add_cloudevent_dataschema", False):
                cloudevent_dataschema = get_cloudevent_dataschema(self.__class__)
                if cloudevent_dataschema:
                    d["_dataschema"] = cloudevent_dataschema
            scope = context.get("add_scope")
            if scope:
                d["_scope"] = scope
            normalized_state = context.get("add_normalized_state")
            if normalized_state:
                d["_normalized_state"] = normalized_state
        return d

    def get_field_types(self, fieldname: str) -> list[type] | None:
        cls = self.__class__.__mro__[0]
        # get_type_hints resolves string annotations and ForwardRef
        try:
            hints = typing.get_type_hints(cls)
        except Exception:
            hints = {f.name: f.type for f in dataclasses.fields(cls)}

        field_type = hints.get(fieldname)
        if field_type is None:
            return None

        # Unwrap TypeAliasType (e.g. `type MyAlias = str | int`)
        if isinstance(field_type, TypeAliasType):
            field_type = field_type.__value__

        # Fallback resolution if get_type_hints failed
        if isinstance(field_type, (str, typing.ForwardRef)):
            name = (
                field_type
                if isinstance(field_type, str)
                else field_type.__forward_arg__
            )
            module = sys.modules.get(cls.__module__)
            if module:
                resolved = getattr(module, name, None)
                if resolved is not None:
                    field_type = resolved

        # If still unresolved, we cannot return a meaningful type list
        if isinstance(field_type, (str, typing.ForwardRef)):
            return None

        # Unwrap generic/union types (list[str], Optional[int], str | int, etc.)
        origin = get_origin(field_type)
        if origin is not None:
            args = get_args(field_type)
            if args:
                return list(args)
            return [origin]

        return [field_type]


R = TypeVar("R")


class DataClassResponseMixin(Generic[R]):
    """
    Mixin that adds .response() and automatically injects the request instance
    if the response class asks for '__request_object__'.
    """

    @classmethod
    def _get_response_class(cls) -> Type[R]:
        """Introspects MRO to find the Generic[R] type."""
        for base in cls.__mro__:
            for orig_base in getattr(base, "__orig_bases__", []):
                if get_origin(orig_base) is DataClassResponseMixin:
                    args = get_args(orig_base)
                    if args:
                        result: Type[R] = args[0]
                        if isinstance(result, typing.ForwardRef):
                            module = sys.modules.get(cls.__module__, None)
                            if module:
                                result = getattr(module, result.__forward_arg__, result)
                        return result
        raise TypeError(f"Could not determine Response type for {cls.__name__}")

    def response(self, takeover: list[str] | None = None, **kwargs) -> R:
        """
        Creates the response object.
        Automatically injects 'self' if the response class has a '__request_object__' argument.
        """
        response_cls = self._get_response_class()

        # 1. Handle Void/None
        if response_cls is type(None):
            return None  # type: ignore[return-value]

        # 2. Inspect the Response Class Constructor
        # We check if '__request_object__' is in the __init__ arguments
        sig = inspect.signature(response_cls)

        if "__request_object__" in sig.parameters:
            # inject self (the request instance) into the arguments
            kwargs["__request_object__"] = self

        response_object = response_cls(**kwargs)
        if takeover:
            for field in takeover:
                if hasattr(self, field) and hasattr(response_object, field):
                    setattr(response_object, field, getattr(self, field))

        return response_object


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

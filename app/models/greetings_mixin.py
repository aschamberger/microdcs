from typing import Any

from app.dataclass import DataClassMixin


class GreetingsDataClassMixin(DataClassMixin):
    def __post_init__(
        self,
        __request_object__: Any = None,
        __custom_metadata__: dict[str, Any] | None = None,
    ) -> None:
        super_post_init = getattr(super(), "__post_init__", None)
        if super_post_init is not None:
            super_post_init()
        # is_request: Copy hidden fields from custom metadata only when one is provided
        if __custom_metadata__ is not None:
            if hasattr(self, "_hidden_str"):
                self._hidden_str = __custom_metadata__.get("_hidden_str")
            if hasattr(self, "_hidden_obj"):
                self._hidden_obj = __custom_metadata__.get("_hidden_obj")
        # create_response: Copy hidden fields from request object only when one is provided
        if __request_object__ is not None:
            if hasattr(self, "_hidden_str"):
                self._hidden_str = getattr(__request_object__, "_hidden_str", None)
            if hasattr(self, "_hidden_obj"):
                self._hidden_obj = getattr(__request_object__, "_hidden_obj", None)

    def __get_custom_metadata__(self) -> dict[str, str]:
        custom_metadata = {}
        str_data = getattr(self, "_hidden_str", None)
        if str_data is not None:
            custom_metadata["_hidden_str"] = str_data
        obj_data = getattr(self, "_hidden_obj", None)
        if obj_data is not None:
            custom_metadata["_hidden_obj"] = obj_data.to_json()
        return custom_metadata

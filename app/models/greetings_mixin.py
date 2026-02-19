from typing import TYPE_CHECKING, Any

from app.dataclass import DataClassMixin

if TYPE_CHECKING:
    from app.models.greetings import Greetings


class GreetingsDataClassMixin(DataClassMixin):
    def __post_init__(
        self,
        __request_object__: Greetings = None,
        __custom_metadata__: dict[str, Any] | None = None,
    ) -> None:
        super_post_init = getattr(super(), "__post_init__", None)
        if super_post_init is not None:
            super_post_init()
        # is_request: Copy hidden fields from custom metadata only when one is provided
        if __custom_metadata__ is not None:
            if hasattr(self, "_hidden_str"):
                self._hidden_str = __custom_metadata__.get("x-hidden-str")
            if hasattr(self, "_hidden_obj"):
                value = __custom_metadata__.get("x-hidden-obj")
                if value is not None:
                    from app.models.greetings import HiddenObject

                    self._hidden_obj = HiddenObject.from_json(value)
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
            custom_metadata["x-hidden-str"] = str_data
        obj_data = getattr(self, "_hidden_obj", None)
        if obj_data is not None:
            custom_metadata["x-hidden-obj"] = obj_data.to_json()
        return custom_metadata

from typing import Any

from app.dataclass import DataClassMixin


class GreetingsDataClassMixin(DataClassMixin):
    def __post_init__(self, __request_object__: Any = None) -> None:
        super_post_init = getattr(super(), "__post_init__", None)
        if super_post_init is not None:
            super_post_init()
        # Copy hidden fields from request object only when one is provided
        if __request_object__ is not None:
            self._hidden_str = getattr(__request_object__, "_hidden_str", None)
            self._hidden_obj = getattr(__request_object__, "_hidden_obj", None)

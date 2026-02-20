class JobStateMixin:
    _state: str = "InitialState"

    def trigger(self, trigger_name: str) -> bool:
        raise RuntimeError("Should be overridden!")

    def may_trigger(self, trigger_name: str) -> bool:
        raise RuntimeError("Should be overridden!")

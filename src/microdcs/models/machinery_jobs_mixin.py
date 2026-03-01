class JobStateMixin:
    # No type annotation — kept invisible to mashumaro so it is neither
    # serialised nor required during deserialisation.  The transitions
    # library sets _state via setattr on every model.
    _state = "InitialState"

    def trigger(self, trigger_name: str) -> bool:
        raise RuntimeError("Should be overridden!")

    def may_trigger(self, trigger_name: str) -> bool:
        raise RuntimeError("Should be overridden!")

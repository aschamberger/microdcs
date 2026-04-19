import asyncio
import fnmatch
import functools
import inspect
import logging
import sys
import typing
import uuid
from abc import ABC, abstractmethod
from annotationlib import ForwardRef
from asyncio import Queue
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import UnionType
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    TypeAliasType,
    TypeVar,
    get_args,
    get_origin,
)

import msgpack
import orjson
from mashumaro.config import BaseConfig

from microdcs import ProcessingConfig
from microdcs.dataclass import (
    DataClassConfig,
    DataClassMixin,
    get_cloudevent_type,
    type_has_config_class,
)

logger = logging.getLogger("app.common")


def get_deep_attr(obj, path) -> Any:
    """
    Navigates through attributes and dictionary keys.
    Example path: 'transportmetadata.mqtt_topic'
    """
    for part in path.split("."):
        if isinstance(obj, dict):
            obj = obj.get(part)
        elif obj is not None:
            obj = getattr(obj, part, None)
    return obj


class Direction(StrEnum):
    """Enumeration of message directions."""

    INCOMING = "in"
    """Message is incoming (received)."""

    OUTGOING = "out"
    """Message is outgoing (sent)."""


class MessageIntent(StrEnum):
    """Enumeration of message intents."""

    DATA = "data"
    """The message represents data being transmitted."""

    EVENT = "events"
    """The message represents an event that occurred."""

    COMMAND = "commands"
    """The message represents a command to be executed."""

    META = "metadata"
    """The message represents metadata about the system or other messages."""


class ProcessorBinding(StrEnum):
    """Defines the protocol binding direction of a processor."""

    NORTHBOUND = "northbound"
    """Northbound: subscribes to commands, publishes to data/events/meta."""

    SOUTHBOUND = "southbound"
    """Southbound: subscribes to data/events/meta, publishes to commands."""


# Default subscribe intents per binding direction
_BINDING_SUBSCRIBE_INTENTS: dict[ProcessorBinding, set[MessageIntent]] = {
    ProcessorBinding.NORTHBOUND: {MessageIntent.COMMAND},
    ProcessorBinding.SOUTHBOUND: {
        MessageIntent.DATA,
        MessageIntent.EVENT,
        MessageIntent.META,
    },
}

# Default publish intents per binding direction
_BINDING_PUBLISH_INTENTS: dict[ProcessorBinding, set[MessageIntent]] = {
    ProcessorBinding.NORTHBOUND: {
        MessageIntent.DATA,
        MessageIntent.EVENT,
        MessageIntent.META,
    },
    ProcessorBinding.SOUTHBOUND: {MessageIntent.COMMAND},
}


def processor_config(
    binding: ProcessorBinding,
    subscribe_intents: set[MessageIntent] | None = None,
    publish_intents: set[MessageIntent] | None = None,
) -> Callable:
    """Class decorator to configure a CloudEventProcessor subclass.

    The *binding* direction defines the default subscribe/publish intents.
    Override with explicit sets when the defaults don't fit.

    Usage::

        @processor_config(binding=ProcessorBinding.SOUTHBOUND)
        class MyProcessor(CloudEventProcessor):
            ...

        @processor_config(
            binding=ProcessorBinding.NORTHBOUND,
            publish_intents={MessageIntent.EVENT},  # override default
        )
        class MyProcessor(CloudEventProcessor):
            ...
    """

    def decorator(cls: type) -> type:
        cls._processor_binding = binding
        cls._subscribe_intents = (
            subscribe_intents
            if subscribe_intents is not None
            else _BINDING_SUBSCRIBE_INTENTS[binding]
        )
        cls._publish_intents = (
            publish_intents
            if publish_intents is not None
            else _BINDING_PUBLISH_INTENTS[binding]
        )
        return cls

    return decorator


class ErrorKind(StrEnum):
    """Enumeration of possible error kinds during message delivery."""

    CONFIGURATION_INVALID = "CONFIGURATION_INVALID"
    """The configuration provided is invalid."""

    NO_MATCHING_SUBSCRIBERS = "NO_MATCHING_SUBSCRIBERS"
    """There were no subscribers matching the message topic."""
    TIMEOUT = "TIMEOUT"
    """The message delivery timed out."""

    UNSUPPORTED_CONTENT_TYPE = "UNSUPPORTED_CONTENT_TYPE"
    """The content type of the message is unsupported."""
    UNKNOWN_PAYLOAD_TYPE = "UNKNOWN_PAYLOAD_TYPE"
    """The payload type of the message is unknown."""
    MISSING_PROPERTY = "MISSING_PROPERTY"
    """A required property is missing."""
    MISSING_USER_PROPERTY = "MISSING_USER_PROPERTY"
    """A required user property is missing."""
    PROPERTY_INVALID = "PROPERTY_INVALID"
    """A property value is invalid."""
    USER_PROPERTY_INVALID = "USER_PROPERTY_INVALID"
    """A user property value is invalid."""
    PAYLOAD_INVALID = "PAYLOAD_INVALID"

    STATE_INVALID = "STATE_INVALID"
    """The system is in an invalid state to process the message."""
    CANCELLATION = "CANCELLATION"
    """The operation was cancelled."""
    INTERNAL_LOGIC_ERROR = "INTERNAL_LOGIC_ERROR"
    """An internal logic error occurred."""
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
    """The requested feature is not implemented."""

    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    """The requested service is currently unavailable."""
    COMMUNICATION_ERROR = "COMMUNICATION_ERROR"
    """A communication error occurred while processing the message."""
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    """A required resource has been exhausted."""
    PERMISSION_DENIED = "PERMISSION_DENIED"
    """Permission to perform the operation was denied."""
    UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION"
    """The version of the protocol is unsupported."""

    UNKNOWN_ERROR = "UNKNOWN_ERROR"
    """An unknown error occurred."""


@dataclass(kw_only=True)
class CloudEvent(DataClassMixin):
    """
    Represents the attributes of a CloudEvent including the following extensions:
    - Distributed Tracing (traceparent, tracestate)
    - Expiry Time
    - custom: Expiry Interval (to map to MQTT message expiry interval)
    - Recorded Time
    - Correlation

    Also adds MicroDCS-specific extensions for error handling and custom metadata.

    It also includes transport metadata which is not serialized.
    """

    data: bytes | None = None
    """The event payload. It is encoded into a media format which is specified by
    the datacontenttype attribute (e.g. application/json), and adheres to the
    dataschema format when those respective attributes are present."""
    id: str | None = field(default_factory=lambda: str(uuid.uuid4()))
    """Identifies the event. Producers MUST ensure that source + id is unique for
    each distinct event. If a duplicate event is re-sent (e.g. due to a network
    error) it MAY have the same id. Consumers MAY assume that Events with identical
    source and id are duplicates. MUST be a non-empty string;
    MUST be unique within the scope of the producer"""
    source: str | None = None
    """Identifies the context in which an event happened.
    MUST be a non-empty URI-reference; An absolute URI is RECOMMENDED"""
    specversion: str = "1.0"
    """The version of the CloudEvents specification which the event uses.
    REQUIRED; MUST be a non-empty string"""
    type: str | None = None
    """This attribute contains a value describing the type of event related to the
    originating occurrence. MUST be a non-empty string; SHOULD be prefixed with a
    reverse-DNS name. The prefixed domain dictates the organization which defines
    the semantics of this event type.
    """
    datacontenttype: str | None = None
    """Content type of data value. This attribute enables data to carry any type of
    content, whereby format and encoding might differ from that of the chosen event
    format. If present, MUST adhere to the format specified in RFC 2046"""
    dataschema: str | None = None
    """Identifies the schema that data adheres to.If present, MUST be a non-empty URI"""
    subject: str | None = None
    """This describes the subject of the event in the context of the event producer
    (identified by source). In publish-subscribe scenarios, a subscriber will typically
    subscribe to events emitted by a source, but the source identifier alone might not
    be sufficient as a qualifier for any specific event if the source context has internal
    sub-structure. If present, MUST be a non-empty string"""
    time: datetime | None = None
    """Timestamp of when the occurrence happened.
    If present, MUST adhere to the format specified in RFC 3339"""
    traceparent: str | None = None
    """If present, contains a W3C Trace Context traceparent header value
    (version, trace ID, span ID, and trace options), MUST be a non-empty string"""
    tracestate: str | None = None
    """If present, contains a W3C Trace Context tracestate header value
    (comma-delimited list of key-value pairs), OPTIONAL"""
    expirytime: datetime | None = None
    """If present, contains a Timestamp indicating an event is no longer
    useful after the indicated time, SHOULD be equal to or later than the time
    attribute, if present"""
    expiryinterval: int | None = None
    """If present, indicates the time interval (in seconds) after which the event
    is no longer useful. The interval is relative to the time attribute, if present,
    or to the time the event was recorded (i.e. recordedtime) if the time attribute
    is not present."""
    recordedtime: datetime | None = field(default_factory=lambda: datetime.now())
    """If present, contains a Timestamp of when the occurrence was recorded
    in this CloudEvent, i.e. when the CloudEvent was created by a producer.
    MUST adhere to the format specified in RFC 3339, SHOULD be equal to or
    later than the occurrence time."""
    correlationid: str | None = field(default_factory=lambda: str(uuid.uuid4()))
    """If present, identifier that groups related events within the same logical
    flow or business transaction. All events sharing the same correlation ID are
    part of the same workflow. MUST be a non-empty string."""
    causationid: str | None = None
    """If present, unique identifier of the event that directly caused this event
    to be generated. This SHOULD be the id value of the causing event.
    MUST be a non-empty string."""

    mdcserrorkind: ErrorKind | None = None
    """Machine readable error kind."""
    mdcserrormessage: str | None = None
    """Humand readable explanation of the error."""
    mdcserrorcontext: dict[str, Any] | None = None
    """This holds timeout limits, retry counts, stack traces, etc.
    (serialized to comma-delimited list of key-value pairs)"""

    method: str | None = None
    """HTTP-style method hint for the event. ``PUT`` (default when absent)
    signals an upsert; ``DELETE`` signals removal. Used by station
    configuration delivery handlers."""

    custommetadata: dict[str, Any] | None = field(default_factory=dict)
    """Holds any custom metadata associated with the event.
    (serialized to individual fields with key names)"""

    transportmetadata: dict[str, Any] | None = field(default_factory=dict)
    """Holds any transport metadata (e.g. MQTT5 ResponseTopic property associated with the event.
    (not serialized)"""

    def __post_init__(self):
        if self.mdcserrorkind is not None and self.mdcserrormessage is None:
            self.mdcserrormessage = "Unknown error occurred."

    @classmethod
    def __pre_deserialize__(cls, dict: dict[Any, Any]) -> dict[Any, Any]:
        items = (
            dict["mdcserrorcontext"].split(",") if dict.get("mdcserrorcontext") else []
        )
        dict["mdcserrorcontext"] = {
            k: v for k, v in (item.split("=", 1) for item in items)
        }
        for k in list(dict.keys()):
            if k not in cls.__dataclass_fields__:
                if "custommetadata" not in dict or dict["custommetadata"] is None:
                    dict["custommetadata"] = {}
                dict["custommetadata"][k] = dict[k]
                del dict[k]
        # Remove any custom metadata keys that collide with CloudEvent fields
        # to prevent field injection on round-trip serialization
        if dict.get("custommetadata"):
            for field_name in cls.__dataclass_fields__:
                dict["custommetadata"].pop(field_name, None)
        return dict

    def __post_serialize__(
        self, dict: dict[Any, Any], context: Optional[Dict] = None
    ) -> dict[Any, Any]:
        pairs = (
            [f"{k}={str(v)}" for k, v in dict["mdcserrorcontext"].items()]
            if dict.get("mdcserrorcontext")
            else []
        )
        if len(pairs) > 0:
            dict["mdcserrorcontext"] = ",".join(pairs)
        else:
            dict.pop("mdcserrorcontext", None)
        custommetadata = dict.pop("custommetadata", None)
        if custommetadata is not None:
            for k, v in custommetadata.items():
                if k not in self.__dataclass_fields__:
                    dict[k] = v
        dict.pop("transportmetadata", None)
        if context and context.get("remove_data"):
            dict.pop("data", None)
        if context and context.get("make_str_values"):
            for k, v in dict.items():
                if v is not None and not isinstance(v, str):
                    dict[k] = str(v)
        return dict

    def unserialize_payload(self, payload_type: type) -> DataClassMixin | bytes:
        match self.datacontenttype:
            case "application/octet-stream":
                request = typing.cast(bytes, self.data)
            case (
                "application/json"
                | "application/json; charset=utf-8"
                | "application/msgpack"
                | "application/msgpack; charset=utf-8"
            ):
                if self.data:
                    # In case of the main payload we manually decode the payload before passing it
                    # to mashumaro. By default it is not possible to pass the custom metadata to the
                    # from_*() methods, so we add it to the raw payload dict before deserialization
                    # and let the dataclass handle it in the __post_init__ method.
                    # In other cases the normal from_*() method is sufficient.
                    if (
                        self.datacontenttype == "application/msgpack"
                        or self.datacontenttype == "application/msgpack; charset=utf-8"
                    ):
                        raw = msgpack.unpackb(self.data)
                    else:
                        raw = orjson.loads(self.data)
                    if self.custommetadata is not None and hasattr(
                        payload_type, "__custom_metadata__"
                    ):
                        raw["__custom_metadata__"] = self.custommetadata
                    request = payload_type.from_dict(raw)
                else:
                    if self.custommetadata is not None and hasattr(
                        payload_type, "__custom_metadata__"
                    ):
                        request = payload_type(__custom_metadata__=self.custommetadata)
                    else:
                        request = payload_type()
            case _:
                raise ValueError(f"Unsupported content type: {self.datacontenttype}")

        return request

    def serialize_payload(self, payload: DataClassMixin | str) -> None:
        match self.datacontenttype:
            case "application/octet-stream":
                self.data = typing.cast(bytes, payload)
            case "application/json" | "application/json; charset=utf-8":
                self.data = typing.cast(DataClassMixin, payload).to_jsonb()
            case "application/msgpack" | "application/msgpack; charset=utf-8":
                self.data = typing.cast(DataClassMixin, payload).to_msgpack()
            case _:
                raise ValueError(f"Unsupported content type: {self.datacontenttype}")
        # propagate cloudevent type and schema from payload
        if isinstance(payload, DataClassMixin):
            config_class = getattr(type(payload), "Config", None)
            if config_class is not None and issubclass(config_class, DataClassConfig):
                if hasattr(config_class, "cloudevent_type"):
                    self.type = getattr(config_class, "cloudevent_type")
                if hasattr(config_class, "cloudevent_dataschema"):
                    self.dataschema = getattr(config_class, "cloudevent_dataschema")
        # extract hidden fields from object
        hidden_fields = None
        if hasattr(payload, "__get_custom_metadata__") and callable(
            getattr(payload, "__get_custom_metadata__")
        ):
            hidden_fields = payload.__get_custom_metadata__()  # type: ignore
        if self.custommetadata is None:
            self.custommetadata = hidden_fields
        elif hidden_fields is not None:
            self.custommetadata.update(hidden_fields)

    class Config(BaseConfig):
        code_generation_options = ["ADD_SERIALIZATION_CONTEXT"]
        omit_none = True


CloudeventAttributeTuple = namedtuple("CloudeventAttributeTuple", ["attribute", "path"])


def incoming(cloudevent_dataclass: type | UnionType | TypeAliasType) -> Callable:
    """Decorator to register a method as an incoming cloud event callback.

    Usage::

        @incoming(Hello)
        async def handle_hello(self, hello: Hello) -> list[Hello] | Hello | None:
            ...
    """

    def decorator(func: Callable) -> Callable:
        if not hasattr(func, "_cloudevent_callbacks"):
            func._cloudevent_callbacks = []  # type: ignore[attr-defined]
        func._cloudevent_callbacks.append((cloudevent_dataclass, Direction.INCOMING))  # type: ignore[attr-defined]
        return func

    return decorator


def outgoing(cloudevent_dataclass: type | UnionType | TypeAliasType) -> Callable:
    """Decorator to register a method as an outgoing cloud event callback.

    Usage::

        @outgoing(Bye)
        async def handle_bye(self, **kwargs) -> list[Bye] | Bye | None:
            ...
    """

    def decorator(func: Callable) -> Callable:
        if not hasattr(func, "_cloudevent_callbacks"):
            func._cloudevent_callbacks = []  # type: ignore[attr-defined]
        func._cloudevent_callbacks.append((cloudevent_dataclass, Direction.OUTGOING))  # type: ignore[attr-defined]
        return func

    return decorator


def _filter_kwargs_for(func: Callable, kwargs: dict[str, Any]) -> dict[str, Any]:
    """Keep only kwargs that *func* accepts (unless it has ``**kwargs``)."""
    sig = inspect.signature(func, follow_wrapped=False)
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
        return kwargs
    accepted = set(sig.parameters.keys())
    return {k: v for k, v in kwargs.items() if k in accepted}


def scope_from_subject(func: Callable) -> Callable:
    """Decorator that derives the ``scope`` keyword argument from ``subject``.

    Extracts the part of ``subject`` before the first ``/`` and injects it
    as the ``scope`` keyword argument into the decorated function.  The
    original ``subject`` kwarg is consumed and not forwarded.

    Intended to be stacked above :func:`incoming`::

        @scope_from_subject
        @incoming(MyCall)
        async def process_my_call(self, method: MyCall, *, scope: str, ...) -> ...:
            ...
    """

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        subject: str | None = kwargs.pop("subject", None)
        if subject is not None:
            kwargs["scope"] = subject.split("/")[0]
        else:
            kwargs["scope"] = None
        return await func(*args, **_filter_kwargs_for(func, kwargs))

    return wrapper


def asset_id_from_subject(
    func: Callable | None = None,
    *,
    name: str = "asset_id",
    factory: Callable[[str], Any] | None = None,
) -> Callable:
    """Decorator that derives a keyword argument from ``subject``.

    Extracts the part of ``subject`` before the first ``/`` and injects it
    as the specified keyword argument (default: ``asset_id``) into the decorated
    function.  The original ``subject`` kwarg is consumed and not forwarded.

    If *factory* is provided it is called with the extracted string and its
    return value is passed instead of the raw string.  This is useful for
    parsing the segment into a typed object.

    Intended to be stacked above :func:`incoming`.  Supports three call forms::

        @asset_id_from_subject
        @incoming(MyCall)
        async def process_my_call(self, method: MyCall, *, asset_id: str, ...) -> ...:
            ...

        @asset_id_from_subject(name="custom_id")
        @incoming(MyCall)
        async def process_my_call(self, method: MyCall, *, custom_id: str, ...) -> ...:
            ...

        @asset_id_from_subject(factory=AssetId.from_string)
        @incoming(MyCall)
        async def process_my_call(self, method: MyCall, *, asset_id: AssetId, ...) -> ...:
            ...

        @asset_id_from_subject(name="asset", factory=AssetId.from_string)
        @incoming(MyCall)
        async def process_my_call(self, method: MyCall, *, asset: AssetId, ...) -> ...:
            ...
    """

    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            subject: str | None = kwargs.pop("subject", None)
            if subject is not None:
                raw = subject.split("/")[0]
                kwargs[name] = factory(raw) if factory is not None else raw
            else:
                kwargs[name] = None
            return await f(*args, **_filter_kwargs_for(f, kwargs))

        return wrapper

    if func is not None:
        # Used as @asset_id_from_subject (no parentheses)
        return decorator(func)
    # Used as @asset_id_from_subject(...) with parentheses
    return decorator


class CloudEventProcessor(ABC):
    _processor_binding: ProcessorBinding
    _subscribe_intents: set[MessageIntent] = set()
    _publish_intents: set[MessageIntent] = set()

    def __init__(
        self,
        instance_id: str,
        runtime_config: ProcessingConfig,
        config_identifier: str,
    ):
        self._instance_id: str = instance_id
        self._runtime_config: ProcessingConfig = runtime_config
        self._config_identifier: str = config_identifier
        self._type_classes: dict[str, type] = {}
        self._type_callbacks_in: dict[str, Callable[..., Any]] = {}
        self._type_callbacks_out: dict[str, Callable[..., Any]] = {}
        self._event_attributes: list[CloudeventAttributeTuple] = []
        self._publish_handlers: list[Callable[[CloudEvent, MessageIntent], None]] = []
        self._register_decorated_callbacks()

    @property
    def binding(self) -> ProcessorBinding:
        """Return the binding direction of this processor."""
        return type(self)._processor_binding

    def subscribe_intents(self) -> set[MessageIntent]:
        """Return the set of intents this processor subscribes to."""
        return type(self)._subscribe_intents

    def publish_intents(self) -> set[MessageIntent]:
        """Return the set of intents this processor publishes to."""
        return type(self)._publish_intents

    def _register_decorated_callbacks(self) -> None:
        """Scan for methods decorated with @incoming / @outgoing and register them."""
        for cls in type(self).__mro__:
            for name, func in vars(cls).items():
                if callable(func) and hasattr(func, "_cloudevent_callbacks"):
                    bound_method = getattr(self, name)
                    for dataclass_type, direction in func._cloudevent_callbacks:  # type: ignore
                        self.register_callback(
                            dataclass_type, bound_method, direction=direction
                        )

    def register_callback(
        self,
        cloudevent_dataclass: type | UnionType | TypeAliasType,
        callback: Callable[..., Any],
        direction: Direction = Direction.INCOMING,
    ) -> None:
        if not callable(callback):
            raise TypeError("callback must be callable")
        if isinstance(cloudevent_dataclass, TypeAliasType):
            cloudevent_dataclass = cloudevent_dataclass.__value__
        if isinstance(cloudevent_dataclass, UnionType):
            for subtype in cloudevent_dataclass.__args__:
                self.register_callback(subtype, callback)
            return
        if not isinstance(cloudevent_dataclass, type):
            raise TypeError("message_dataclass must be a class type")
        if not issubclass(cloudevent_dataclass, DataClassMixin):
            raise TypeError("message_dataclass must be a subclass of DataClassMixin")
        config_class = getattr(cloudevent_dataclass, "Config", None)
        if config_class is None or not issubclass(config_class, DataClassConfig):
            raise TypeError(
                "message_dataclass must have a Config subclass of DataClassConfig"
            )
        if not hasattr(config_class, "cloudevent_type"):
            raise TypeError(
                "message_dataclass must have a Config subclass with cloudevent_type attribute"
            )
        cloudevent_type = getattr(config_class, "cloudevent_type")
        self._type_classes[cloudevent_type] = cloudevent_dataclass
        getattr(
            self,
            f"_type_callbacks_{direction.value}",
        )[cloudevent_type] = callback

    def has_incoming_callback(self, cloudevent: CloudEvent) -> bool:
        return cloudevent.type in self._type_callbacks_in

    def has_outgoing_callback(self, cloudevent: CloudEvent) -> bool:
        return cloudevent.type in self._type_callbacks_out

    async def initialize(self) -> None:
        """Optional async initialisation hook.

        Called once by the main handler before the processor is registered
        in the handler. Override in subclasses that need async setup
        (e.g. creating database indices).
        """

    async def post_start(self) -> None:
        """Optional async post start hook.

        Called once by the main handler after the processor has been started
        in the handler. Override in subclasses that need to perform actions
        after start (e.g. publishing metadata).
        """

    async def shutdown(self) -> None:
        """Optional async shutdown hook.

        Called once during graceful shutdown after the task group has exited.
        Override in subclasses that need to release resources acquired in
        ``initialize()`` (e.g. closing database connections, flushing buffers).
        """

    def register_publish_handler(
        self, handler: Callable[[CloudEvent, MessageIntent], None]
    ) -> None:
        """Register a transport-specific publish handler.

        Each registered handler will be called when the processor publishes
        an outbound event, allowing multiple transports to receive the event.
        The handler receives the CloudEvent and an optional MessageIntent that
        indicates which publish-topic pattern should be used.
        """
        self._publish_handlers.append(handler)

    def publish_event(
        self, cloudevent: CloudEvent, intent: MessageIntent = MessageIntent.EVENT
    ) -> None:
        if not self._publish_handlers:
            logger.warning("No publish handlers registered; cannot publish event.")
            return
        for handler in self._publish_handlers:
            handler(cloudevent, intent)

    async def process_cloudevent(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        result = await self.callback_incoming(cloudevent)
        return result

    @abstractmethod
    async def process_response_cloudevent(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        """Handle a response event correlated to a previously sent request/event.

        This hook is called when a transport receives a message on a configured
        response topic/channel and maps it to a CloudEvent. Typical use cases are:
        - updating local process/job state based on response status
        - emitting follow-up events after successful/failed commands
        - triggering compensating actions for negative acknowledgements

        Return semantics:
        - ``None``: no follow-up event should be published
        - ``CloudEvent``: publish one follow-up event
        - ``list[CloudEvent]``: publish multiple follow-up events

        Implementations should treat this method as idempotent where practical,
        because response delivery can be at-least-once depending on transport QoS.
        """
        pass

    @abstractmethod
    async def handle_cloudevent_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        """Handle a timeout/expiry callback for a previously published event.

        This hook is invoked after the configured expiry interval elapses for an
        outbound event. Use it to implement timeout behavior such as:
        - retrying a command/event
        - raising timeout alarms/events
        - performing cleanup or state rollback

        Args:
            cloudevent: The original event that expired.
            timeout: The expiry interval in seconds that elapsed.

        Return semantics:
        - ``None``: no timeout follow-up should be published
        - ``CloudEvent``: publish one timeout follow-up event
        - ``list[CloudEvent]``: publish multiple timeout follow-up events
        """
        pass

    @abstractmethod
    async def trigger_outgoing_event(
        self, **kwargs
    ) -> list[CloudEvent] | CloudEvent | None:
        """Entry point for application-driven outbound event generation.

        Implement this method when outbound events are initiated by local logic
        (timers, API calls, state changes) rather than by an incoming CloudEvent.
        A common pattern is to translate ``kwargs`` into a dataclass payload,
        call ``callback_outgoing(...)`` for the matching ``@outgoing`` callback,
        and return the resulting CloudEvent(s).

        Return semantics:
        - ``None``: nothing should be published
        - ``CloudEvent``: publish one generated event
        - ``list[CloudEvent]``: publish multiple generated events

        Keep ``kwargs`` names stable and documented in concrete processors so
        callers can use this method as a clear processor API.
        """
        pass

    def create_event(
        self,
        datacontenttype: str | None = "application/json; charset=utf-8",
    ) -> CloudEvent:
        cloudevent = CloudEvent(
            datacontenttype=datacontenttype,
        )
        if self._runtime_config.cloudevent_source is not None:
            cloudevent.source = self._runtime_config.cloudevent_source
        if (
            self._runtime_config.message_expiry_interval is not None
            and int(self._runtime_config.message_expiry_interval) > 0
        ):
            cloudevent.expiryinterval = self._runtime_config.message_expiry_interval
        return cloudevent

    def _cloudevent_attributes_for_callback(
        self, cloudevent: CloudEvent, callback: Callable[..., Any]
    ) -> dict[str, Any]:
        kwargs = {}
        for arg in self._event_attributes:
            kwargs[arg.attribute] = get_deep_attr(cloudevent, arg.path)
        # Only pass kwargs that the callback accepts, unless it has **kwargs.
        # Use follow_wrapped=False so we see the *wrapper's* signature when
        # decorators like @scope_from_subject wrap the real handler with
        # functools.wraps.  The wrapper needs to receive all kwargs (it has
        # **kwargs) so it can consume e.g. ``subject`` before forwarding.
        sig = inspect.signature(callback, follow_wrapped=False)
        has_var_keyword = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )
        if not has_var_keyword:
            accepted = set(sig.parameters.keys())
            kwargs = {k: v for k, v in kwargs.items() if k in accepted}
        return kwargs

    async def callback_incoming(
        self, request_cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        if request_cloudevent.type not in self._type_callbacks_in:
            logger.error(
                "No callback registered for cloud event type: %s",
                request_cloudevent.type,
            )
            return None

        payload_type = self._type_classes[request_cloudevent.type]
        callback: Callable[..., Any] = self._type_callbacks_in[request_cloudevent.type]
        try:
            request: DataClassMixin | bytes = request_cloudevent.unserialize_payload(
                payload_type
            )
        except ValueError as e:
            logger.error(e)
            return None
        logger.debug("Request before callback: %s", request)

        kwargs = self._cloudevent_attributes_for_callback(request_cloudevent, callback)
        call_callback = True
        responses: list[DataClassMixin] | DataClassMixin | None = None
        # a pre-callback can be defined to inspect the incoming cloudevent
        # and decide whether the main callback should be called.
        if hasattr(self, "__pre_outgoing_callback__") and callable(
            getattr(self, "__pre_outgoing_callback__")
        ):
            call_callback, responses = await self.__pre_outgoing_callback__(  # type: ignore
                request_cloudevent, **kwargs
            )
        # The pre-callback can decide if the main callback should be called.
        # So it can intercept or act additionally before the main callback is executed.
        if call_callback is True:
            responses: list[DataClassMixin] | DataClassMixin | None = await callback(
                request, **kwargs
            )
        # A post-callback can be defined to inspect or modify the responses from
        # the main callback before they are returned.
        if hasattr(self, "__post_outgoing_callback__") and callable(
            getattr(self, "__post_outgoing_callback__")
        ):
            responses = await self.__post_outgoing_callback__(  # type: ignore
                responses, request_cloudevent, **kwargs
            )

        if responses is None:
            return None

        if not isinstance(responses, list):
            responses = [responses]

        response_cloudevents: list[CloudEvent] = []
        for response in responses:
            logger.debug("Response from callback: %s", response)
            if not type_has_config_class(type(response)):
                logger.warning("Response has no Config class")
                continue

            response_cloudevent = self.create_event()
            response_cloudevent.correlationid = request_cloudevent.correlationid
            response_cloudevent.causationid = request_cloudevent.id
            if request_cloudevent.subject is not None:
                response_cloudevent.subject = request_cloudevent.subject
            try:
                response_cloudevent.serialize_payload(response)
            except ValueError as e:
                logger.exception(
                    "Error serializing payload for message type %s: %s",
                    response_cloudevent.type,
                    e,
                )
                return None

            response_cloudevents.append(response_cloudevent)
        return response_cloudevents

    async def callback_outgoing(
        self,
        payload_type: type,
        intent: MessageIntent,
        topic: str | None = None,
        **kwargs,
    ) -> list[CloudEvent] | CloudEvent | None:
        cloudevent_type = get_cloudevent_type(payload_type)
        if cloudevent_type is None or cloudevent_type not in self._type_callbacks_out:
            logger.error(
                "No callback registered for cloud event type: %s", cloudevent_type
            )
            return None

        payload_type = self._type_classes[cloudevent_type]
        callback: Callable[..., Any] = self._type_callbacks_out[cloudevent_type]
        responses: list[DataClassMixin] | DataClassMixin | None = await callback(
            **kwargs
        )

        if responses is None:
            return None

        if not isinstance(responses, list):
            responses = [responses]

        response_cloudevents: list[CloudEvent] = []
        for response in responses:
            logger.debug("Response from callback: %s", response)
            if not type_has_config_class(type(response)):
                logger.warning("Response has no Config class")
                continue

            response_cloudevent = self.create_event()
            if topic is not None:
                if response_cloudevent.transportmetadata is None:
                    response_cloudevent.transportmetadata = {}
                response_cloudevent.transportmetadata["mqtt_topic"] = topic
            try:
                response_cloudevent.serialize_payload(response)
            except ValueError as e:
                logger.exception(
                    "Error serializing payload for message type %s: %s",
                    response_cloudevent.type,
                    e,
                )
                return None

            self.publish_event(response_cloudevent, intent=intent)
            response_cloudevents.append(response_cloudevent)
        return response_cloudevents


class AdditionalTask(ABC):
    """Abstract base class for additional tasks that run alongside protocol handlers."""

    def __init__(self):
        self._shutdown_event: asyncio.Event = asyncio.Event()

    def register_shutdown_event(self, event: asyncio.Event) -> None:
        self._shutdown_event = event

    @abstractmethod
    async def task(self) -> None:
        """Long-running task entry point, called by the main task group.

        Implementations should monitor ``self._shutdown_event`` to exit
        gracefully when the application is shutting down::

            async def task(self) -> None:
                while not self._shutdown_event.is_set():
                    # do work ...
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(), timeout=interval
                        )
                    except asyncio.TimeoutError:
                        pass  # timeout expired, loop continues
        """
        pass


PB = TypeVar("PB", bound="ProtocolBinding")


class ProtocolHandler(Generic[PB], ABC):
    def __init__(self):
        self._bindings: list[PB] = []
        self._cloudevent_processors: list[CloudEventProcessor] = []
        self._shutdown_event: asyncio.Event = asyncio.Event()

    def register_shutdown_event(self, event: asyncio.Event) -> None:
        self._shutdown_event = event

    def register_binding(self, binding: PB) -> None:
        self._bindings.append(binding)
        self._cloudevent_processors.append(binding.processor)

    @abstractmethod
    async def task(self) -> None:
        pass


PH = TypeVar("PH", bound="ProtocolHandler")


class ProtocolBinding(Generic[PH], ABC):
    _resolved_handler: type[PH]

    @classmethod
    def get_protocol_handler(cls) -> type[ProtocolHandler]:
        # 1. Return cached if available
        if hasattr(cls, "_cached_handler"):
            return cls._cached_handler

        # 2. Iterate through the generic hierarchy
        # We use __orig_bases__ to find the ProtocolBinding[ActualClass] entry
        for base in getattr(cls, "__orig_bases__", []):
            if get_origin(base) is ProtocolBinding:
                args = get_args(base)
                if not args:
                    continue

                candidate = args[0]

                # 3. Handle String or ForwardRef (Lazy Resolution)
                if isinstance(candidate, (str, ForwardRef)):
                    name = (
                        candidate
                        if isinstance(candidate, str)
                        else candidate.__forward_arg__
                    )

                    # Fetch the module where the subclass (cls) was defined
                    module = sys.modules.get(cls.__module__)
                    if module is None:
                        raise ImportError(f"Could not find module {cls.__module__}")

                    # Resolve the name from the module's namespace
                    resolved = getattr(module, name, None)

                    if resolved is None:
                        raise NameError(
                            f"Name '{name}' not found in module '{cls.__module__}'. "
                            "Ensure the handler class is defined in the same module."
                        )
                    candidate = resolved

                # 4. Type Safety Check & Caching
                if isinstance(candidate, type) and issubclass(
                    candidate, ProtocolHandler
                ):
                    cls._cached_handler = candidate
                    return candidate

        raise TypeError(f"Could not resolve a valid ProtocolHandler for {cls.__name__}")

    def __init__(
        self,
        processor: CloudEventProcessor,
        processing_config: ProcessingConfig,
        queue_size: int = 5,
        outgoing_ce_type_filter: set[str] = set(),
    ):
        if not hasattr(type(processor), "_processor_binding"):
            raise ValueError(
                f"{type(processor).__name__} must be decorated with @processor_config"
            )

        self.processor = processor
        self.processing_config = processing_config
        max_queue_size = self.processing_config.binding_outgoing_queue_max_size
        effective_queue_size = queue_size if queue_size > 0 else max_queue_size
        if effective_queue_size > max_queue_size:
            logger.warning(
                "Configured binding queue size %d exceeds processing max %d. Capping to max.",
                effective_queue_size,
                max_queue_size,
            )
            effective_queue_size = max_queue_size
        if effective_queue_size <= 0:
            raise ValueError("Outgoing binding queue size must be > 0")

        self.outgoing_queue: Queue[tuple[CloudEvent, MessageIntent]] = Queue(
            effective_queue_size
        )
        self.outgoing_ce_type_filter = outgoing_ce_type_filter

    def _enqueue_outgoing_event(
        self,
        cloudevent: CloudEvent,
        intent: MessageIntent,
    ) -> None:
        try:
            self.outgoing_queue.put_nowait((cloudevent, intent))
        except asyncio.QueueFull as exc:
            raise RuntimeError(
                "Outgoing queue is full. Apply backpressure or increase "
                "processing.binding_outgoing_queue_max_size / protocol "
                "binding_outgoing_queue_size."
            ) from exc

    def publish_handler(self, cloudevent: CloudEvent, intent: MessageIntent) -> None:
        """Default publish handler that puts the event into the outgoing queue.
        Applies cloudevent type filtering based on the outgoing_ce_type_filter.
        Method needs to be overwritten by protocol bindings that require more
        advanced routing behavior."""
        if (
            self.outgoing_ce_type_filter is None
            or len(self.outgoing_ce_type_filter) == 0
        ):
            self._enqueue_outgoing_event(cloudevent, intent)
            return
        if cloudevent.type is None:
            logger.warning(
                "CloudEvent has no type; cannot apply outgoing filter. Event: %s",
                cloudevent,
            )
            return
        for filter in self.outgoing_ce_type_filter:
            if filter == cloudevent.type or fnmatch.fnmatch(cloudevent.type, filter):
                self._enqueue_outgoing_event(cloudevent, intent)
                return

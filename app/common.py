import logging
import typing
import uuid
from abc import ABC, abstractmethod
from asyncio import Queue
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import UnionType
from typing import Any, Callable, Dict, Optional

from mashumaro.config import BaseConfig

from app import ProcessingConfig
from app.dataclass import DataClassConfig, DataClassMixin

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


class ErrorKind(StrEnum):
    """Enumeration of possible error kinds during message delivery."""

    CONFIGURATION_INVALID = "CONFIGURATION_INVALID"
    """The configuration provided is invalid."""

    NO_MATCHING_SUBSCRIBERS = "NO_MATCHING_SUBSCRIBERS"
    """IThere were no subscribers matching the message topic."""
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
                dict[k] = v
        dict.pop("transportmetadata", None)
        if context and context.get("remove_data"):
            dict.pop("data", None)
        if context and context.get("make_str_values"):
            for k, v in dict.items():
                if v is not None and not isinstance(v, str):
                    dict[k] = str(v)
        return dict

    def unserialize_payload(
        self,
        payload_type: type,
        hidden_field_processors: dict[
            str,
            tuple[
                Callable[[DataClassMixin, dict[str, str]], None] | None,
                Callable[[DataClassMixin, dict[str, str]], None] | None,
            ],
        ]
        | None = None,
    ) -> DataClassMixin | bytes:
        match self.datacontenttype:
            case "application/octet-stream":
                request = typing.cast(bytes, self.data)
            case "application/json" | "application/json; charset=utf-8":
                if self.data:
                    request = payload_type.from_json(self.data)
                else:
                    request = payload_type()
            case "application/msgpack" | "application/msgpack; charset=utf-8":
                if self.data:
                    request = payload_type.from_msgpack(self.data)
                else:
                    request = payload_type()
            case _:
                raise ValueError(f"Unsupported content type: {self.datacontenttype}")
        # extract hidden fields from user properties
        if (
            hidden_field_processors is not None
            and isinstance(request, DataClassMixin)
            and self.custommetadata is not None
        ):
            for cloudevent_type, (extractor, _) in hidden_field_processors.items():
                if extractor is not None:
                    if payload_type.Config.matches_cloudevent_type_pattern(
                        cloudevent_type
                    ):
                        logger.debug("Extracting hidden field %s", cloudevent_type)
                        extractor(request, self.custommetadata)

        return request

    def serialize_payload(
        self,
        payload: DataClassMixin | str,
        hidden_field_processors: dict[
            str,
            tuple[
                Callable[[DataClassMixin, dict[str, str]], None] | None,
                Callable[[DataClassMixin, dict[str, str]], None] | None,
            ],
        ]
        | None = None,
    ) -> None:
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
        # insert hidden fields from user properties
        payload_type = type(payload)
        if hidden_field_processors is not None and isinstance(payload, DataClassMixin):
            if self.custommetadata is None:
                self.custommetadata = {}
            for cloudevent_type, (_, inserter) in hidden_field_processors.items():
                if inserter is not None:
                    if hasattr(payload_type, "Config"):
                        config = getattr(payload_type, "Config")
                        if config.matches_cloudevent_type_pattern(cloudevent_type):
                            inserter(payload, self.custommetadata)

    class Config(BaseConfig):
        code_generation_options = ["ADD_SERIALIZATION_CONTEXT"]
        omit_none = True


CloudeventAttributeTuple = namedtuple("CloudeventAttributeTuple", ["attribute", "path"])


class CloudEventProcessor(ABC):
    _instance_id: str
    _runtime_config: ProcessingConfig
    _type_classes: dict[str, type[DataClassMixin]] = {}
    _type_callbacks_in: dict[str, Callable[..., Any]] = {}
    _type_callbacks_out: dict[str, Callable[..., Any]] = {}
    _hidden_field_processors: dict[
        str,
        tuple[
            Callable[[DataClassMixin, dict[str, str]], None] | None,
            Callable[[DataClassMixin, dict[str, str]], None] | None,
        ],
    ] = {}
    _event_attributes: list[CloudeventAttributeTuple] = []
    outgoing_queue: Queue[CloudEvent]

    @abstractmethod
    def __init__(
        self,
        instance_id: str,
        runtime_config: Any,
        topic_identifier: str | None = None,
        queue_size: int = 1,
    ):
        pass

    def register_callback(
        self,
        cloudevent_dataclass: type | UnionType,
        callback: Callable[..., Any],
        direction: Direction = Direction.INCOMING,
    ) -> None:
        if not callable(callback):
            raise TypeError("callback must be callable")
        if isinstance(cloudevent_dataclass, UnionType):
            for subtype in cloudevent_dataclass.__args__:
                self.register_callback(subtype, callback)
            return
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

    def event_has_callback(self, cloudevent: CloudEvent) -> bool:
        return cloudevent.type in self._type_callbacks_in

    def register_hidden_field_processor(
        self,
        cloudevent_type: str,
        extractor: Callable[..., Any] | None = None,
        inserter: Callable[..., Any] | None = None,
    ) -> None:
        if not callable(extractor):
            raise TypeError("extractor must be callable")
        if not callable(inserter):
            raise TypeError("inserter must be callable")
        self._hidden_field_processors[cloudevent_type] = (extractor, inserter)

    def publish_event(self, cloudevent: CloudEvent) -> None:
        self.outgoing_queue.put_nowait(cloudevent)

    def create_event(
        self,
        datacontenttype: str | None = "application/json; charset=utf-8",
    ) -> CloudEvent:
        cloudevent = CloudEvent(
            datacontenttype=datacontenttype,
        )
        if self._runtime_config.cloudevent_source is not None and isinstance(
            cloudevent, CloudEvent
        ):
            cloudevent.source = self._runtime_config.cloudevent_source
        if (
            self._runtime_config.message_expiry_interval is not None
            and int(self._runtime_config.message_expiry_interval) > 0
            and isinstance(cloudevent, CloudEvent)
        ):
            cloudevent.expiryinterval = self._runtime_config.message_expiry_interval
        return cloudevent

    def get_event_args(self, cloudevent: CloudEvent) -> dict[str, Any]:
        kwargs = {}
        for arg in self._event_attributes:
            kwargs[arg.attribute] = get_deep_attr(cloudevent, arg.path)
        return kwargs

    @abstractmethod
    async def process_event(self, cloudevent: CloudEvent) -> Any:
        pass

    @abstractmethod
    async def process_response_event(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        pass

    @abstractmethod
    async def handle_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        pass


class ProtocolHandler(ABC):
    _cloudevent_processors: list[CloudEventProcessor] = []

    def register_processor(self, processor: CloudEventProcessor) -> None:
        self._cloudevent_processors.append(processor)

    @abstractmethod
    async def task(self) -> None:
        pass

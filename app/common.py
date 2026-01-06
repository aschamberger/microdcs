import logging
import typing
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable, Dict

from app.dataclass import DataClassConfig, DataClassMixin, DataClassValidationMixin

logger = logging.getLogger("app.common")


class ErrorCode(StrEnum):
    DELIVERY_TIMEOUT = "DELIVERY_TIMEOUT"
    UNSUPPORTED_CONTENT_TYPE = "UNSUPPORTED_CONTENT_TYPE"
    UNKNOWN_PAYLOAD_TYPE = "UNKNOWN_PAYLOAD_TYPE"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    CONNECTION_LOST = "CONNECTION_LOST"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


@dataclass
class DeliveryError(DataClassMixin, DataClassValidationMixin):
    """Represents an error that occurred during message delivery.
    error_context can hold additional information such as timeout limits, retry counts, stack traces, etc.
    The original message details are included to help with debugging and potential retries."""

    error_code: ErrorCode
    """Machine readable error code."""
    error_message: str
    """Humand readable explanation of the error."""
    error_context: dict[str, Any] | None = None
    """This holds timeout limits, retry counts, stack traces, etc."""
    original_topic: str | None = None
    """The topic of the original message that caused the error."""
    original_payload: bytes | None = None
    """The payload of the original message that caused the error."""
    original_properties: dict[str, Any] | None = None
    """The properties of the original message that caused the error."""
    original_user_properties: dict[str, str] | None = None
    """The user properties of the original message that caused the error."""

    @classmethod
    def __pre_deserialize__(cls, d: Dict[Any, Any]) -> Dict[Any, Any]:
        items = d["error_context"].split(",") if d.get("error_context") else []
        d["error_context"] = {k: v for k, v in (item.split("=", 1) for item in items)}  # type: ignore
        return d

    def __post_serialize__(self, d: Dict[Any, Any]) -> Dict[Any, Any]:
        pairs = (
            [f"{k}={str(v)}" for k, v in d["error_context"].items()]
            if d["error_context"]
            else []
        )
        d["error_context"] = ",".join(pairs)
        return d

    class Config(DataClassConfig):
        cloudevent_type: str = "com.github.aschamberger.micro-dcs.deliveryerror.v1"
        cloudevent_dataschema: str = (
            "https://aschamberger.github.io/schemas/micro-dcs/deliveryerror/v1"
        )
        aliases = {
            "name": "NameField",
        }


@dataclass
class CloudEventAttributes(DataClassMixin):
    specversion: str = "1.0"
    """The version of the CloudEvents specification used by this event."""
    id: str | None = None
    """Populated from MQTT message correlation_data; MUST be a non-empty string;
    MUST be unique within the scope of the producer"""
    source: str | None = None
    """MUST be a non-empty URI-reference; An absolute URI is RECOMMENDED"""
    type: str | None = None
    """MUST be a non-empty string; SHOULD be prefixed with a reverse-DNS name.
    The prefixed domain dictates the organization which defines the semantics of this event type.
    """
    datacontenttype: str | None = None
    """Populated from MQTT message content_type;
    If present, MUST adhere to the format specified in RFC 2046"""
    dataschema: str | None = None
    """If present, MUST be a non-empty URI"""
    subject: str | None = None
    """If present, MUST be a non-empty string"""
    time: str | None = None
    """If present, MUST adhere to the format specified in RFC 3339"""
    traceparent: str | None = None
    """If present, contains a W3C Trace Context traceparent header value
    (version, trace ID, span ID, and trace options)"""
    tracestate: str | None = None
    """If present, contains a W3C Trace Context tracestate header value
    (comma-delimited list of key-value pairs)"""
    abort_message_delivery_timeout: float | None = None
    """If present, indicates the maximum time in seconds the message is valid for delivery
    (especially relevant if the processing party is not the final destination and control
    should be given back to the originator to decide on next steps)."""
    message_expiry_interval: int | None = None
    """If present, allows the publisher to set an expiry interval for time-sensitive messages.
    If the message remains on the server beyond this specified interval,
    the server will no longer distribute it to the subscribers."""


def unserialize_payload(
    payload: bytes,
    payload_type: type,
    content_type: str,
    user_properties: dict[str, str],
    hidden_field_processors: dict[
        str,
        tuple[
            Callable[[DataClassMixin, dict[str, str]], None] | None,
            Callable[[DataClassMixin, dict[str, str]], None] | None,
        ],
    ],
) -> DataClassMixin | str:
    match content_type:
        case "text/plain" | "text/plain; charset=utf-8":
            request = payload.decode("utf-8")
        case "application/json" | "application/json; charset=utf-8":
            request = payload_type.from_json(payload)
        case "application/msgpack" | "application/msgpack; charset=utf-8":
            request = payload_type.from_msgpack(payload)
        case _:
            raise ValueError(f"Unsupported content type: {content_type}")
    # extract hidden fields from user properties
    if isinstance(request, DataClassMixin):
        for cloudevent_type, (extractor, _) in hidden_field_processors.items():
            if extractor is not None:
                if payload_type.Config.matches_cloudevent_type_pattern(cloudevent_type):
                    logger.debug("Extracting hidden field %s", cloudevent_type)
                    extractor(request, user_properties)

    return request


def serialize_payload(
    payload: DataClassMixin | str,
    content_type: str,
    hidden_field_processors: dict[
        str,
        tuple[
            Callable[[DataClassMixin, dict[str, str]], None] | None,
            Callable[[DataClassMixin, dict[str, str]], None] | None,
        ],
    ],
) -> tuple[bytes, dict[str, str]]:
    response: bytes
    if isinstance(payload, str):
        if content_type not in [
            "text/plain",
            "text/plain; charset=utf-8",
        ]:
            raise ValueError(
                f"Cannot serialize str payload to content type: {content_type}"
            )
        response = payload.encode("utf-8")
        return response, {}
    else:
        match content_type:
            case "application/json" | "application/json; charset=utf-8":
                response = typing.cast(DataClassMixin, payload).to_jsonb()
            case "application/msgpack" | "application/msgpack; charset=utf-8":
                response = typing.cast(DataClassMixin, payload).to_msgpack()
            case _:
                raise ValueError(f"Unsupported content type: {content_type}")
        # insert hidden fields from user properties
        user_properties: dict[str, str] = {}
        payload_type = type(payload)
        if isinstance(payload, DataClassMixin):
            for cloudevent_type, (_, inserter) in hidden_field_processors.items():
                if inserter is not None:
                    if hasattr(payload_type, "Config"):
                        config = getattr(payload_type, "Config")
                        if config.matches_cloudevent_type_pattern(cloudevent_type):
                            inserter(payload, user_properties)
    return response, user_properties

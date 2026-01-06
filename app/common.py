from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from app.dataclass import DataClassConfig, DataClassMixin, DataClassValidationMixin
from app.mqtt import MQTTProcessorMessage


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

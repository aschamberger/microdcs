# Your First Processor

This tutorial walks through building a CloudEvent processor from scratch, covering the processor lifecycle, incoming/outgoing event handlers, the response chain, and wiring into the MicroDCS application.

## Prerequisites

- A MicroDCS project initialized with `microdcs init` (see the [README](https://github.com/aschamberger/microdcs))
- A JSON Schema describing your message types
- Generated dataclasses from that schema via `uv run microdcs dataclassgen dataclasses <schema>.schema.json`

## Overview

A **processor** is a class that handles CloudEvents for a specific domain. It receives incoming events, deserializes them into typed dataclasses, runs your business logic, and returns response dataclasses that the framework wraps back into outgoing CloudEvents. Processors are protocol-agnostic — the same processor works over MQTT and MessagePack-RPC.

Processors are **stateless protocol adapters** — they handle serialization, routing, and single request-response exchanges. For multi-step recipe orchestration (e.g., executing an SFC recipe across multiple equipment actions), see the [SFC Engine](sfc_engine.md), which coordinates processors via direct method calls without adding orchestration logic to the processors themselves.

## Step 1: Define a JSON Schema

Create a schema file in `schemas/`. Here is a minimal example with a `Ping` request and a `Pong` response:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.com/schemas/ping-pong/",
  "title": "PingPong",
  "description": "Ping/Pong message types.",
  "oneOf": [
    { "$ref": "#/$defs/Ping" },
    { "$ref": "#/$defs/Pong" }
  ],
  "$defs": {
    "Ping": {
      "type": "object",
      "properties": {
        "message": { "type": "string" }
      },
      "required": ["message"],
      "x-request-type": { "$ref": "#/$defs/Ping" },
      "x-response-type": { "$ref": "#/$defs/Pong" },
      "x-type-id": "com.example.ping.v1",
      "x-type-schema": "https://example.com/schemas/ping-pong/v1.0.0/ping"
    },
    "Pong": {
      "type": "object",
      "properties": {
        "reply": { "type": "string" }
      },
      "required": ["reply"],
      "x-type-id": "com.example.pong.v1",
      "x-type-schema": "https://example.com/schemas/ping-pong/v1.0.0/pong"
    }
  }
}
```

Key schema extensions:

| Extension | Purpose |
|---|---|
| `x-type-id` | Sets the `type` field on the CloudEvent envelope |
| `x-type-schema` | Sets the `dataschema` field on the CloudEvent envelope |
| `x-request-type` | Declares the request dataclass (self-referencing for echo patterns) |
| `x-response-type` | Declares the response dataclass — this drives the response chain |

Generate the dataclasses:

```bash
uv run microdcs dataclassgen dataclasses schemas/ping_pong.schema.json
```

This produces a file like `models/ping_pong.py` with `Ping` and `Pong` dataclasses. If `x-response-type` is set, `Ping` will inherit from `DataClassResponseMixin[Pong]`.

## Step 2: Understand the Generated Dataclasses

The generator produces dataclasses that look like this (simplified):

```python
from dataclasses import dataclass
from microdcs.dataclass import (
    DataClassConfig,
    DataClassResponseMixin,
    DataClassValidationMixin,
)
from microdcs.models.ping_pong_mixin import PingPongDataClassMixin


@dataclass(kw_only=True)
class Ping(
    DataClassValidationMixin, DataClassResponseMixin["Pong"], PingPongDataClassMixin
):
    __request_object__: InitVar[Ping | None] = None
    message: str

    class Config(DataClassConfig):
        response_type: str = "Pong"
        type_id: str = "com.example.ping.v1"
        type_schema: str = "https://example.com/schemas/ping-pong/v1.0.0/ping"


@dataclass(kw_only=True)
class Pong(DataClassValidationMixin, PingPongDataClassMixin):
    reply: str

    class Config(DataClassConfig):
        type_id: str = "com.example.pong.v1"
        type_schema: str = "https://example.com/schemas/ping-pong/v1.0.0/pong"
```

## Step 3: Write the Processor

Create your processor in `app/processors/ping_pong.py`:

```python
import logging

from microdcs import ProcessingConfig
from microdcs.common import (
    CloudEvent,
    CloudEventProcessor,
    MessageIntent,
    ProcessorBinding,
    incoming,
    outgoing,
    processor_config,
)
from app.models.ping_pong import Ping, Pong

logger = logging.getLogger("processor.ping_pong")


@processor_config(binding=ProcessorBinding.NORTHBOUND)
class PingPongProcessor(CloudEventProcessor):
    def __init__(
        self,
        instance_id: str,
        runtime_config: ProcessingConfig,
        config_identifier: str,
    ):
        super().__init__(instance_id, runtime_config, config_identifier)

    @incoming(Ping)
    async def handle_ping(self, ping: Ping) -> Pong | None:
        logger.info("Received ping: %s", ping.message)
        # Use the response chain to create a Pong from the Ping
        return ping.response(reply=f"pong: {ping.message}")

    @outgoing(Pong)
    async def produce_pong(self, **kwargs) -> Pong | None:
        return Pong(reply=kwargs.get("reply", "unsolicited pong"))

    async def initialize(self) -> None:
        logger.info("PingPongProcessor initialized")

    async def post_start(self) -> None:
        logger.info("PingPongProcessor started")

    async def shutdown(self) -> None:
        logger.info("PingPongProcessor shutdown")

    async def process_response_cloudevent(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        return None

    async def handle_cloudevent_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        logger.warning("Event %s expired after %ds", cloudevent.id, timeout)
        return None

    async def trigger_outgoing_event(self, **kwargs) -> None:
        return None
```

### Anatomy of this processor

**`@processor_config(binding=...)`** — Required class decorator. The binding direction controls which MQTT topic intents the processor subscribes and publishes to:

| Binding | Subscribes to | Publishes to |
|---|---|---|
| `NORTHBOUND` | commands | data, events, metadata |
| `SOUTHBOUND` | data, events, metadata | commands |

The names come from the OT/ISA-95 automation pyramid. **Northbound** means the processor faces *up* toward higher-level systems (MES, ERP, cloud) — it receives commands from above and publishes data/events/metadata back up. **Southbound** means the processor faces *down* toward field-level systems (PLCs, sensors, equipment) — it receives data/events/metadata from below and sends commands down. In practice: if your processor *executes* work when told to, it is northbound; if it *orchestrates* other systems by issuing commands, it is southbound.

You can override the defaults with explicit `subscribe_intents` and `publish_intents` sets.

**`@incoming(Ping)`** — Registers `handle_ping` as the callback for incoming CloudEvents whose `type` matches `Ping.Config.type_id`. The framework deserializes the CloudEvent payload into a `Ping` instance and passes it as the first argument.

**`@outgoing(Pong)`** — Registers `produce_pong` as a callback for programmatically generating outgoing events. Invoke it via `self.callback_outgoing(Pong, intent=MessageIntent.EVENT)`.

**Three abstract methods** must be implemented:

- `process_response_cloudevent` — handles transport-level response messages (e.g. MQTT response topic replies)
- `handle_cloudevent_expiration` — called when a published event's expiry interval elapses
- `trigger_outgoing_event` — entry point for application-driven outbound events (timers, API calls, etc.)

**Three optional lifecycle hooks** can be overridden (all default to no-ops):

- `initialize()` — async setup before handlers start (Redis index creation, dependency checks)
- `post_start()` — actions after handlers are live (send startup messages, publish metadata)
- `shutdown()` — async cleanup after the task group exits (release resources, flush buffers)

## Step 4: Wire It Up

In `app/__main__.py`, register the processor with protocol handlers:

```python
from microdcs.core import MicroDCS
from microdcs.mqtt import MQTTHandler, MQTTProtocolBinding, OTELInstrumentedMQTTHandler
from app.processors.ping_pong import PingPongProcessor

microdcs = MicroDCS()
microdcs.register_protocol_handler(
    MQTTHandler(
        microdcs.runtime_config.mqtt,
        microdcs.redis_connection_pool,
        microdcs.redis_key_schema,
    ),
    OTELInstrumentedMQTTHandler(
        microdcs.runtime_config.mqtt,
        microdcs.redis_connection_pool,
        microdcs.redis_key_schema,
    ),
)

ping_pong_processor = PingPongProcessor(
    microdcs.runtime_config.instance_id,
    microdcs.runtime_config.processing,
    "ping-pong",
)

microdcs.register_protocol_binding(
    MQTTProtocolBinding(
        ping_pong_processor,
        microdcs.runtime_config.processing,
        microdcs.runtime_config.mqtt,
    )
)

asyncio.run(microdcs.main())
```

The `config_identifier` string (`"ping-pong"`) determines the MQTT topic namespace for this processor's bindings.

### Adding a MessagePack-RPC Binding

To also expose the processor over MessagePack-RPC (e.g. for a sidecar container), register a `MessagePackHandler` and a `MessagePackProtocolBinding` for the same processor:

```python
from microdcs.msgpack import (
    MessagePackHandler,
    MessagePackProtocolBinding,
    OTELInstrumentedMessagePackHandler,
)

microdcs.register_protocol_handler(
    MessagePackHandler(
        microdcs.runtime_config.msgpack,
        microdcs.redis_connection_pool,
        microdcs.redis_key_schema,
    ),
    OTELInstrumentedMessagePackHandler(
        microdcs.runtime_config.msgpack,
        microdcs.redis_connection_pool,
        microdcs.redis_key_schema,
    ),
)

microdcs.register_protocol_binding(
    MessagePackProtocolBinding(
        ping_pong_processor,
        microdcs.runtime_config.processing,
        microdcs.runtime_config.msgpack,
    )
)
```

The processor instance is the same — one processor, multiple transports. A sidecar container can now call the `publish` RPC method to submit CloudEvents for processing, and receives outgoing events as notification frames. See [MessagePack-RPC Transport](concepts.md#messagepack-rpc-transport) for the sidecar architecture and client usage.

## The Response Chain

The response chain is the mechanism by which a request dataclass creates properly typed response objects. It is built from three components:

### `DataClassResponseMixin[R]`

When a JSON Schema type has `x-response-type`, the code generator makes the request class inherit from `DataClassResponseMixin[R]` where `R` is the response type. This adds a `.response(**kwargs)` method to every instance of the request class.

```python
# Ping inherits DataClassResponseMixin["Pong"]
pong = ping.response(reply="hello back")
# pong is a Pong instance
```

### The `takeover` parameter

`.response()` accepts an optional `takeover` parameter — a list of field names to copy from the request to the response. This is useful when request and response share fields (common in OPC UA patterns):

```python
@dataclass(kw_only=True)
class StoreCall(DataClassResponseMixin["StoreResponse"], ...):
    job_order_id: str
    job_order: ISA95JobOrderDataType


@dataclass(kw_only=True)
class StoreResponse(...):
    job_order_id: str
    return_status: MethodReturnStatus


# In the processor:
response = store_call.response(
    takeover=["job_order_id"],
    return_status=MethodReturnStatus(status_code=0),
)
# response.job_order_id is copied from store_call.job_order_id
```

### `__request_object__` injection

When the response dataclass declares an `__request_object__` `InitVar` field, `.response()` automatically injects the calling request instance into it. The mixin `__post_init__` method can then copy hidden fields or perform computed initialization from the request object.

This is the pattern used when request and response are the *same* type (echo/mirror patterns). The `Hello` dataclass in the greetings example demonstrates this:

```python
@dataclass(kw_only=True)
class Hello(DataClassResponseMixin["Hello"], GreetingsDataClassMixin):
    __request_object__: InitVar[Hello | None] = None
    _hidden_str: str | None = None
    name: str
```

When you call `hello.response(name="world")`:

1. `DataClassResponseMixin.response()` sees `__request_object__` in the response class constructor
2. It injects `self` (the request `Hello`) as `__request_object__`
3. The mixin `__post_init__` receives the original `Hello` and copies `_hidden_str` and other hidden fields from it

This means hidden fields (prefixed with `_`) survive the request-to-response round-trip without appearing in the serialized payload.

### Full response chain flow

```
Incoming CloudEvent
  → framework deserializes payload into request dataclass
    → __post_init__ runs, populating hidden fields from custom metadata
      → your @incoming handler receives the typed request
        → you call request.response(**kwargs)
          → DataClassResponseMixin resolves the response type from Generic[R]
          → if response class has __request_object__, injects the request
          → if takeover is set, copies listed fields
          → response __post_init__ runs, copying hidden fields from __request_object__
            → you return the response
              → framework serializes it into an outgoing CloudEvent
                → hidden fields are extracted via __get_custom_metadata__()
                  and placed into CloudEvent custommetadata
```

### How `callback_incoming` orchestrates the response

When you return a dataclass (or list of dataclasses) from an `@incoming` handler, the framework's `callback_incoming` method in `CloudEventProcessor` handles the rest:

1. **Wraps each response** in a new `CloudEvent` via `create_event()` (sets `source`, `datacontenttype`, `expiryinterval` from `ProcessingConfig`)
2. **Propagates envelope attributes** from the request CloudEvent to the response:
    - `correlationid` — copied so the response stays correlated to the request
    - `causationid` — set to the request CloudEvent's `id` (causal chain)
    - `subject` — copied if present on the request
3. **Serializes the payload** via `serialize_payload()`, which:
    - Calls `.to_jsonb()` or `.to_msgpack()` on the dataclass depending on `datacontenttype`
    - Sets `type` and `dataschema` on the CloudEvent from the dataclass `Config`
    - Calls `__get_custom_metadata__()` to extract hidden fields into `custommetadata`

You do not need to create `CloudEvent` objects yourself in `@incoming` handlers — just return your typed dataclass instances.

### Writing a mixin for hidden fields

Hidden fields (prefixed with `_`) are excluded from the serialized payload by `DataClassMixin.__post_serialize__`. To make them survive the request → response round-trip, you need a mixin that:

1. **On deserialization**: reads hidden field values from CloudEvent `custommetadata` (passed via `__custom_metadata__` or `_consume_custom_metadata()`)
2. **On response creation**: copies hidden fields from the request object (passed via `__request_object__`)
3. **On serialization**: exports hidden fields back into `custommetadata` (via `__get_custom_metadata__()`)

Here is the pattern, using the generated greetings mixin as a reference:

```python
from typing import TYPE_CHECKING, Any
from microdcs.dataclass import DataClassMixin

if TYPE_CHECKING:
    from app.models.ping_pong import PingPong


class PingPongDataClassMixin(DataClassMixin):
    def __post_init__(
        self,
        __request_object__: PingPong | None = None,
        __custom_metadata__: dict[str, Any] | None = None,
    ) -> None:
        # Call parent __post_init__ if it exists (important for MRO chain)
        super_post_init = getattr(super(), "__post_init__", None)
        if super_post_init is not None:
            super_post_init()

        # --- Deserialization path ---
        # When the framework deserializes a CloudEvent payload, custom metadata
        # from the CloudEvent envelope is injected via __custom_metadata__ InitVar
        # or stored in a context var consumed by _consume_custom_metadata().
        if __custom_metadata__ is None:
            __custom_metadata__ = self._consume_custom_metadata()
        if __custom_metadata__ is not None:
            if hasattr(self, "_my_hidden_field"):
                self._my_hidden_field = __custom_metadata__.get("x-my-hidden-field")

        # --- Response creation path ---
        # When .response() is called and the response class has __request_object__,
        # the request instance is passed here so hidden fields can be copied over.
        if __request_object__ is not None:
            if hasattr(self, "_my_hidden_field"):
                self._my_hidden_field = getattr(
                    __request_object__, "_my_hidden_field", None
                )

    def __get_custom_metadata__(self) -> dict[str, str]:
        # --- Serialization path ---
        # Called by CloudEvent.serialize_payload() to extract hidden fields
        # back into the CloudEvent custommetadata dict.
        custom_metadata = {}
        value = getattr(self, "_my_hidden_field", None)
        if value is not None:
            custom_metadata["x-my-hidden-field"] = value
        return custom_metadata
```

The three paths correspond to the three lifecycle stages:

| Stage | Mechanism | What happens |
|---|---|---|
| Incoming deserialization | `__custom_metadata__` / `_consume_custom_metadata()` | Hidden fields populated from CloudEvent `custommetadata` |
| Response creation | `__request_object__` | Hidden fields copied from request to response instance |
| Outgoing serialization | `__get_custom_metadata__()` | Hidden fields exported back into CloudEvent `custommetadata` |

If your dataclass has no hidden fields, you still need to create the mixin file (the code generator expects it), but it can simply inherit from `DataClassMixin` with no overrides.

## CloudEvent Attributes in Handlers

Processors can declare which CloudEvent envelope attributes should be extracted and passed as keyword arguments to `@incoming` handlers:

```python
class MyProcessor(CloudEventProcessor):
    def __init__(self, ...):
        super().__init__(...)
        self._event_attributes.extend([
            CloudeventAttributeTuple("subject", "subject"),
            CloudeventAttributeTuple("correlationid", "correlationid"),
        ])

    @incoming(Ping)
    async def handle_ping(self, ping: Ping, *, subject: str, correlationid: str) -> ...:
        ...
```

Helper decorators `@scope_from_subject` and `@asset_id_from_subject` extract structured information from the `subject` attribute automatically:

```python
@scope_from_subject
@incoming(Ping)
async def handle_ping(self, ping: Ping, *, scope: str) -> ...:
    # scope is the part of subject before the first "/"
    ...
```

## Pre/Post Callback Hooks

Two optional hooks let you intercept the incoming callback flow:

**`__pre_outgoing_callback__`** — Called before the `@incoming` handler. Can inspect the raw CloudEvent and decide whether to proceed:

```python
async def __pre_outgoing_callback__(self, cloudevent, **kwargs):
    if some_condition:
        return False, None  # skip the main callback
    return True, None  # proceed normally
```

**`__post_outgoing_callback__`** — Called after the `@incoming` handler. Can inspect or modify the response list:

```python
async def __post_outgoing_callback__(self, responses, cloudevent, **kwargs):
    # responses is the list of dataclass objects returned by @incoming handler
    # modify or filter them before the framework wraps them into CloudEvents
    return responses
```

## Processor Lifecycle Hooks

`CloudEventProcessor` provides three optional async hooks called by `MicroDCS.main()` at well-defined points. All three have default no-op implementations so you only override what you need.

| Hook | When called | Typical use |
|---|---|---|
| `initialize()` | Before the task group starts (before protocol handlers are running) | Create Redis indices, validate external dependencies |
| `post_start()` | After all protocol handlers are running and accepting messages | Publish initial metadata/state, send startup messages |
| `shutdown()` | After the task group exits (graceful shutdown) | Release resources acquired in `initialize()`, flush buffers |

```python
@processor_config(binding=ProcessorBinding.NORTHBOUND)
class PingPongProcessor(CloudEventProcessor):
    async def initialize(self) -> None:
        # Runs before message handlers are started.
        # Safe to call async setup code here (e.g. database index creation).
        logger.info("Initializing PingPongProcessor")

    async def post_start(self) -> None:
        # Runs after all handlers are registered and the transport is live.
        # Use this to publish initial state or metadata events.
        self.publish_event(self.create_event(), intent=MessageIntent.META)

    async def shutdown(self) -> None:
        # Runs during graceful shutdown after the task group has exited.
        logger.info("Shutting down PingPongProcessor")
```

### Execution order

```
initialize()            ← before task group / before handlers accept messages
  task_group start
    handler tasks start
  post_start()          ← handlers are live; messages can be sent and received
  [application runs]
  task group exits
shutdown()              ← after task group; no more messages
Redis pool closed
```

### `post_start_singleton` — run once across replicas

When running multiple replicas, every instance calls `post_start()` on startup. If `post_start()` sends a message (e.g. an init command), this results in N identical messages — one per replica.

Set `post_start_singleton = True` on the processor class to ensure only one replica executes `post_start()`:

```python
@processor_config(binding=ProcessorBinding.NORTHBOUND)
class PingPongProcessor(CloudEventProcessor):
    post_start_singleton = True  # only one replica across the set runs post_start()

    async def post_start(self) -> None:
        # This runs on exactly one replica. Other replicas skip it silently.
        self.publish_event(self.create_event(), intent=MessageIntent.META)
```

Under the hood `MicroDCS` uses a Redis `SET NX EX` lock keyed to `poststartlock:{config_identifier}`. The lock expires after `ProcessingConfig.post_start_lock_ttl` seconds (default 30 s) so that a crashed replica does not permanently block `post_start()` from running after a restart. See [Persistence — Distributed post_start Lock](persistence.md#distributed-post_start-lock) for details.

## Testing Your Processor

Use `pytest` and `unittest.mock` to test processors without external dependencies:

```python
import pytest
from unittest.mock import AsyncMock, MagicMock

from microdcs import ProcessingConfig
from app.processors.ping_pong import PingPongProcessor
from app.models.ping_pong import Ping


@pytest.fixture
def processor():
    config = MagicMock(spec=ProcessingConfig)
    config.cloudevent_source = "test-source"
    config.message_expiry_interval = 60
    return PingPongProcessor("test-instance", config, "ping-pong")


class TestPingPongProcessor:
    @pytest.mark.asyncio
    async def test_handle_ping(self, processor):
        ping = Ping(message="hello")
        result = await processor.handle_ping(ping)
        assert result.reply == "pong: hello"
```

## Next Steps

- Study the built-in `GreetingsCloudEventProcessor` in `src/microdcs/processors/greetings.py` for a complete working example
- Study `MachineryJobsCloudEventProcessor` in `src/microdcs/processors/machinery_jobs.py` for a production-grade processor with Redis persistence, state machines, and `post_start()` metadata publishing
- See [Persistence](persistence.md) for Redis-backed state management patterns and the distributed `post_start` lock

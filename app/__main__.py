import asyncio
import logging
import os

from app.core import MicroDCS
from app.mqtt import MQTTHandler, MQTTProtocolBinding, OTELInstrumentedMQTTHandler
from app.msgpack import (
    MessagePackHandler,
    MessagePackProtocolBinding,
    OTELInstrumentedMessagePackHandler,
)
from app.processors.greetings import GreetingsCloudEventProcessor
from app.processors.machinery_jobs import MachineryJobsCloudEventProcessor

logger = logging.getLogger("app.main")

# Create MicroDCS application object
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

# Create a single protocol-agnostic greetings processor
greetings_processor = GreetingsCloudEventProcessor(
    microdcs.runtime_config.instance_id, microdcs.runtime_config.processing, "greetings"
)

# Register with MQTT handler
#   For advanced filtering the `publish_handler` of the protocol binding can be overridden
#   to inspect the CloudEvent and decide whether to enqueue it for sending or not. This allows
#   for filtering based on any attribute of the CloudEvent, including extensions.
#   Basic filtering can be done via the `outgoing_ce_type_filter` argument, which filters based
#   on the CloudEvent type attribute. It supports wildcard matching (e.g. "com.example.greeting.*").`
#   filters: set[str] = {"com.example.greeting.created", "com.example.greeting.updated"}
mqtt_type_filters: set[str] = set()
microdcs.register_protocol_binding(
    MQTTProtocolBinding(
        greetings_processor,
        microdcs.runtime_config.processing,
        microdcs.runtime_config.mqtt,
        True,
        mqtt_type_filters,
    )
)

# Register with MessagePack handler
# FIXME: MessagePackProtocolBinding does not currently support outgoing events.
microdcs.register_protocol_binding(
    MessagePackProtocolBinding(
        greetings_processor,
        microdcs.runtime_config.processing,
        microdcs.runtime_config.msgpack,
    )
)

# Create a single protocol-agnostic machinery-jobs processor
machinery_jobs_processor = MachineryJobsCloudEventProcessor(
    microdcs.runtime_config.instance_id,
    microdcs.runtime_config.processing,
    "machinery-jobs",
    microdcs.redis_connection_pool,
    microdcs.redis_key_schema,
)

# Register with MQTT handler
microdcs.register_protocol_binding(
    MQTTProtocolBinding(
        machinery_jobs_processor,
        microdcs.runtime_config.processing,
        microdcs.runtime_config.mqtt,
    )
)

# Register with MessagePack handler
microdcs.register_protocol_binding(
    MessagePackProtocolBinding(
        machinery_jobs_processor,
        microdcs.runtime_config.processing,
        microdcs.runtime_config.msgpack,
    )
)

# Add additional task to MicroDCS main task group
# microdcs.add_additional_task(additional_task())

# Run MicroDCS main application logic
loop_factory = asyncio.SelectorEventLoop if os.name == "nt" else None
asyncio.run(microdcs.main(), loop_factory=loop_factory)

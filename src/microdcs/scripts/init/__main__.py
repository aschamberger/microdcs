import asyncio
import logging
import os

from microdcs.core import MicroDCS
from microdcs.mqtt import MQTTHandler, MQTTProtocolBinding, OTELInstrumentedMQTTHandler
from microdcs.msgpack import (
    MessagePackHandler,
    MessagePackProtocolBinding,
    OTELInstrumentedMessagePackHandler,
)
from microdcs.processors.greetings import GreetingsCloudEventProcessor
from microdcs.processors.machinery_jobs import (
    JobAcceptanceConfig,
    MachineryJobsCloudEventProcessor,
)
from microdcs.publishers import JobOrderPublisher

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
msgpack_type_filters: set[str] = set()
microdcs.register_protocol_binding(
    MessagePackProtocolBinding(
        greetings_processor,
        microdcs.runtime_config.processing,
        microdcs.runtime_config.msgpack,
        msgpack_type_filters,
    )
)

# Create a single protocol-agnostic machinery-jobs processor
job_acceptance_config = JobAcceptanceConfig()
machinery_jobs_processor = MachineryJobsCloudEventProcessor(
    microdcs.runtime_config.instance_id,
    microdcs.runtime_config.processing,
    "machinery-jobs",
    microdcs.redis_connection_pool,
    microdcs.redis_key_schema,
    job_acceptance_config,
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

# Add job order publisher as additional task
job_order_publisher = JobOrderPublisher(
    microdcs.runtime_config.mqtt,
    microdcs.runtime_config.publisher,
    microdcs.runtime_config.processing,
    "machinery-jobs",
    microdcs.redis_connection_pool,
    microdcs.redis_key_schema,
)
microdcs.add_additional_task(job_order_publisher)

# Run MicroDCS main application logic
loop_factory = asyncio.SelectorEventLoop if os.name == "nt" else None
asyncio.run(microdcs.main(), loop_factory=loop_factory)

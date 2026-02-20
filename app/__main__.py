import asyncio
import logging
import os

from app.microdcs import MicroDCS
from app.processors.greetings import GreetingsCloudEventProcessor
from app.processors.machinery_jobs import MachineryJobsCloudEventProcessor

logger = logging.getLogger("app.main")

# Create MicroDCS application object
microdcs = MicroDCS()

# Create a single protocol-agnostic greetings processor
greetings_processor = GreetingsCloudEventProcessor(
    microdcs.runtime_config.instance_id, microdcs.runtime_config.processing
)

# Register with MQTT handler (topic identifier selects config section)
microdcs.register_mqtt_processor(greetings_processor, "greetings")

# Register with MessagePack handler
microdcs.register_msgpack_processor(greetings_processor)

# Create a single protocol-agnostic machinery-jobs processor
machinery_jobs_processor = MachineryJobsCloudEventProcessor(
    microdcs.runtime_config.instance_id,
    microdcs.runtime_config.processing,
    microdcs.redis_connection_pool,
    microdcs.redis_key_schema,
)

# Register with MQTT handler (topic identifier selects config section)
microdcs.register_mqtt_processor(machinery_jobs_processor, "machinery_jobs")

# Register with MessagePack handler
microdcs.register_msgpack_processor(machinery_jobs_processor)

# Run MicroDCS main application logic
loop_factory = asyncio.SelectorEventLoop if os.name == "nt" else None
asyncio.run(microdcs.main(), loop_factory=loop_factory)

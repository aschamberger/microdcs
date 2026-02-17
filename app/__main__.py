import asyncio
import logging
import os

from app.identity_processor import (
    IdentityMessagePackCloudEventProcessor,
    IdentityMQTTCloudEventProcessor,
)
from app.microdcs import MicroDCS

logger = logging.getLogger("app.main")

# Create MicroDCS application object
microdcs = MicroDCS()

# Register MQTT processors as needed
microdcs.processor(
    IdentityMQTTCloudEventProcessor(
        microdcs.runtime_config.instance_id, microdcs.runtime_config.processing
    )
)

# Register MessagePack processors as needed
microdcs.processor(
    IdentityMessagePackCloudEventProcessor(
        microdcs.runtime_config.instance_id, microdcs.runtime_config.processing
    )
)

# Run MicroDCS main application logic
loop_factory = asyncio.SelectorEventLoop if os.name == "nt" else None
asyncio.run(microdcs.main(), loop_factory=loop_factory)

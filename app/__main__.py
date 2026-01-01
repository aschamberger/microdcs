import asyncio
import logging
import os

from app import RuntimeConfig, SystemEventTaskGroup
from app.mqtt import MQTTHandler, OTELInstrumentedMQTTHandler

logger = logging.getLogger("app.main")


async def task(name: str, duration: int):
    logger.info("Task %s: Starting", name)
    await asyncio.sleep(duration)
    logger.info("Task %s: Completed", name)


async def main():
    logger.info("Setting up runtime configuration")
    runtime_config = RuntimeConfig()
    logger.debug("Runtime config: %s", runtime_config)

    logger.info("Starting main application logic")
    async with SystemEventTaskGroup() as task_group:
        # MQTTHandler setup based on OTEL instrumentation flag
        if runtime_config.processing.otel_instrumentation_enabled:
            logger.info("Starting MQTTHandler with OTEL instrumentation enabled")
            mqtt_handler = MQTTHandler(runtime_config.mqtt)
        else:
            logger.info("Starting MQTTHandler with OTEL instrumentation disabled")
            mqtt_handler = OTELInstrumentedMQTTHandler(runtime_config.mqtt)
        # Register MQTT processors as needed
        # e.g., mqtt_handler.register_message_processor(your_processor_instance)
        task_group.create_task(mqtt_handler.task())
        # Example additional tasks to demonstrate task group usage
        task_group.create_task(task("A", 2))
        task_group.create_task(task("B", 3))

    logger.info("Application shutdown complete")


loop_factory = asyncio.SelectorEventLoop if os.name == "nt" else None
asyncio.run(main(), loop_factory=loop_factory)

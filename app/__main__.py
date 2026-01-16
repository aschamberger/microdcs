import asyncio
import logging
import os

import redis.asyncio as redis

from app import RuntimeConfig, SystemEventTaskGroup
from app.identity_processor import IdentityMQTTCloudEventProcessor
from app.mqtt import MQTTHandler, OTELInstrumentedMQTTHandler
from app.msgpack import MessagePackHandler, OTELInstrumentedMessagePackHandler

logger = logging.getLogger("app.main")


async def main():
    logger.info("Setting up runtime configuration")
    runtime_config = RuntimeConfig()
    logger.debug("Runtime config: %s", runtime_config)

    logger.info("Initializing Redis connection pool")
    redis_connection_pool = redis.ConnectionPool(
        host=runtime_config.redis.hostname, port=runtime_config.redis.port, protocol=3
    )

    logger.info("Starting main application logic")
    async with SystemEventTaskGroup() as task_group:
        # MQTTHandler setup based on OTEL instrumentation flag
        if runtime_config.processing.otel_instrumentation_enabled:
            logger.info("Starting MQTTHandler with OTEL instrumentation enabled")
            mqtt_handler = MQTTHandler(runtime_config.mqtt, redis_connection_pool)
        else:
            logger.info("Starting MQTTHandler with OTEL instrumentation disabled")
            mqtt_handler = OTELInstrumentedMQTTHandler(
                runtime_config.mqtt, redis_connection_pool
            )
        # Register MQTT processors as needed
        # e.g., mqtt_handler.register_processor(your_processor_instance)
        ip = IdentityMQTTCloudEventProcessor(
            runtime_config.instance_id, runtime_config.processing
        )
        mqtt_handler.register_processor(ip)
        task_group.create_task(mqtt_handler.task())
        # MessagePackHandler setup based on OTEL instrumentation flag
        if runtime_config.processing.otel_instrumentation_enabled:
            logger.info("Starting MessagePackHandler with OTEL instrumentation enabled")
            msgpack_handler = MessagePackHandler(
                runtime_config.msgpack, redis_connection_pool
            )
        else:
            logger.info(
                "Starting MessagePackHandler with OTEL instrumentation disabled"
            )
            msgpack_handler = OTELInstrumentedMessagePackHandler(
                runtime_config.msgpack, redis_connection_pool
            )
        # Register MessagePack processors as needed
        # e.g., msgpack_handler.register_message_processor(your_processor_instance)
        # msgpack_handler.register_message_processor(
        #     IdentityMessagePackMessageProcessor(runtime_config.processing)
        # )
        task_group.create_task(msgpack_handler.task())
        task_group.create_task(ip.send_event())
    logger.info("Main application logic has completed")

    logger.info("Closing Redis connection pool")
    await redis_connection_pool.aclose()

    logger.info("Application shutdown complete")


loop_factory = asyncio.SelectorEventLoop if os.name == "nt" else None
asyncio.run(main(), loop_factory=loop_factory)

import logging

import redis.asyncio as redis

from app import RuntimeConfig, SystemEventTaskGroup
from app.common import CloudEventProcessor
from app.mqtt import MQTTHandler, OTELInstrumentedMQTTHandler
from app.msgpack import (
    MessagePackHandler,
    OTELInstrumentedMessagePackHandler,
)
from app.redis import RedisKeySchema

logger = logging.getLogger("app.main")


class MicroDCS:
    def __init__(self) -> None:
        logger.info("Setting up runtime configuration")
        self.runtime_config: RuntimeConfig = RuntimeConfig()
        logger.debug("Runtime config: %s", self.runtime_config)

        logger.info("Initializing Redis connection pool")
        self.redis_connection_pool: redis.ConnectionPool = redis.ConnectionPool(
            host=self.runtime_config.redis.hostname,
            port=self.runtime_config.redis.port,
            protocol=3,
        )
        self.redis_key_schema: RedisKeySchema = RedisKeySchema(
            self.runtime_config.redis.key_prefix
        )
        self._mqtt_processors: list[CloudEventProcessor] = []
        self._msgpack_processors: list[CloudEventProcessor] = []

    def register_mqtt_processor(self, processor: CloudEventProcessor):
        self._mqtt_processors.append(processor)

    def register_msgpack_processor(self, processor: CloudEventProcessor):
        self._msgpack_processors.append(processor)

    async def main(self):
        logger.info("Starting main application logic")
        # Initialise every registered processor exactly once, even when a
        # processor is shared across multiple protocol handlers.
        seen: set[int] = set()
        for processor in self._mqtt_processors:
            if id(processor) not in seen:
                await processor.initialize()
                seen.add(id(processor))
        for processor in self._msgpack_processors:
            if id(processor) not in seen:
                await processor.initialize()
                seen.add(id(processor))
        async with SystemEventTaskGroup() as task_group:
            # MQTTHandler setup based on OTEL instrumentation flag
            if self.runtime_config.processing.otel_instrumentation_enabled:
                logger.info("Starting MQTTHandler with OTEL instrumentation enabled")
                mqtt_handler = OTELInstrumentedMQTTHandler(
                    self.runtime_config.mqtt,
                    self.redis_connection_pool,
                    self.redis_key_schema,
                )
            else:
                logger.info("Starting MQTTHandler with OTEL instrumentation disabled")
                mqtt_handler = MQTTHandler(
                    self.runtime_config.mqtt,
                    self.redis_connection_pool,
                    self.redis_key_schema,
                )
            for processor in self._mqtt_processors:
                mqtt_handler.register_mqtt_processor(processor)
            task_group.create_task(mqtt_handler.task())
            # MessagePackHandler setup based on OTEL instrumentation flag
            if self.runtime_config.processing.otel_instrumentation_enabled:
                logger.info(
                    "Starting MessagePackHandler with OTEL instrumentation enabled"
                )
                msgpack_handler = OTELInstrumentedMessagePackHandler(
                    self.runtime_config.msgpack,
                    self.redis_connection_pool,
                    self.redis_key_schema,
                )
            else:
                logger.info(
                    "Starting MessagePackHandler with OTEL instrumentation disabled"
                )
                msgpack_handler = MessagePackHandler(
                    self.runtime_config.msgpack,
                    self.redis_connection_pool,
                    self.redis_key_schema,
                )
            for processor in self._msgpack_processors:
                msgpack_handler.register_processor(processor)
            task_group.create_task(msgpack_handler.task())
        logger.info("Main application logic has completed")

        logger.info("Closing Redis connection pool")
        await self.redis_connection_pool.aclose()

        logger.info("Application shutdown complete")

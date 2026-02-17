from collections import defaultdict
import logging

import redis.asyncio as redis

from app import RuntimeConfig, SystemEventTaskGroup
from app.common import CloudEventProcessor
from app.mqtt import MQTTCloudEventProcessor, MQTTHandler, OTELInstrumentedMQTTHandler
from app.msgpack import (
    MessagePackCloudEventProcessor,
    MessagePackHandler,
    OTELInstrumentedMessagePackHandler,
)
from app.redis import RedisKeySchema

logger = logging.getLogger("app.main")


class MicroDCS:
    runtime_config: RuntimeConfig
    redis_connection_pool: redis.ConnectionPool
    redis_key_schema: RedisKeySchema
    _processors: dict[type, list[CloudEventProcessor]] = defaultdict(list)

    def __init__(self) -> None:
        logger.info("Setting up runtime configuration")
        self.runtime_config = RuntimeConfig()
        logger.debug("Runtime config: %s", self.runtime_config)

        logger.info("Initializing Redis connection pool")
        self.redis_connection_pool = redis.ConnectionPool(
            host=self.runtime_config.redis.hostname,
            port=self.runtime_config.redis.port,
            protocol=3,
        )
        self.redis_key_schema = RedisKeySchema(self.runtime_config.redis.key_prefix)

    def processor(self, processor: CloudEventProcessor):
        def wrapper(cls):
            processor_type = type(processor)
            self._processors[processor_type].append(processor)
            return cls

        return wrapper

    async def main(self):
        logger.info("Starting main application logic")
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
            for mqtt_processor in self._processors[MQTTCloudEventProcessor]:
                mqtt_handler.register_processor(mqtt_processor)
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
            for msgpack_processor in self._processors[MessagePackCloudEventProcessor]:
                msgpack_handler.register_processor(msgpack_processor)
            task_group.create_task(msgpack_handler.task())
        logger.info("Main application logic has completed")

        logger.info("Closing Redis connection pool")
        await self.redis_connection_pool.aclose()

        logger.info("Application shutdown complete")

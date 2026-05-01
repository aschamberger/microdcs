import logging

import redis.asyncio as redis

from microdcs import RuntimeConfig, SystemEventTaskGroup
from microdcs.common import (
    AdditionalTask,
    CloudEventProcessor,
    ProtocolBinding,
    ProtocolHandler,
)
from microdcs.mqtt import MQTTPublisher
from microdcs.redis import PostStartLockDAO, RedisKeySchema

logger = logging.getLogger("app.main")


class MicroDCS:
    def __init__(self) -> None:
        logger.info("Setting up runtime configuration")
        self.runtime_config: RuntimeConfig = RuntimeConfig()
        logger.debug("Runtime config: %s", self.runtime_config)

        logger.info("Initializing Redis connection pool")
        redis_kwargs: dict = {
            "host": self.runtime_config.redis.hostname,
            "port": self.runtime_config.redis.port,
            "protocol": 3,
        }
        if self.runtime_config.redis.username is not None:
            redis_kwargs["username"] = self.runtime_config.redis.username
        if self.runtime_config.redis.password is not None:
            redis_kwargs["password"] = self.runtime_config.redis.password
        if self.runtime_config.redis.ssl:
            redis_kwargs["connection_class"] = redis.SSLConnection
            if self.runtime_config.redis.ssl_ca_certs is not None:
                redis_kwargs["ssl_ca_certs"] = str(
                    self.runtime_config.redis.ssl_ca_certs
                )
        self.redis_connection_pool: redis.ConnectionPool = redis.ConnectionPool(
            **redis_kwargs
        )
        self.redis_key_schema: RedisKeySchema = RedisKeySchema(
            self.runtime_config.redis.key_prefix
        )
        self._protocol_handlers: dict[
            type[ProtocolHandler], tuple[ProtocolHandler, ProtocolHandler]
        ] = {}
        self._handler_bindings: dict[type[ProtocolHandler], set[ProtocolBinding]] = {}
        self._processors: set[CloudEventProcessor] = set()
        self._additional_tasks: set[AdditionalTask] = set()

    def register_protocol_handler(
        self, handler: ProtocolHandler, instrumented_handler: ProtocolHandler
    ):
        self._protocol_handlers[handler.__class__] = (
            handler,
            instrumented_handler,
        )

    def register_protocol_binding(self, binding: ProtocolBinding):
        protocol_handler_cls = binding.get_protocol_handler()
        if protocol_handler_cls not in self._protocol_handlers:
            raise ValueError(
                f"Protocol handler {protocol_handler_cls.__name__} not registered in MicroDCS"
            )
        if protocol_handler_cls not in self._handler_bindings:
            self._handler_bindings[protocol_handler_cls] = set()
        self._handler_bindings[protocol_handler_cls].add(binding)
        self._processors.add(binding.processor)

    def add_additional_task(self, task: AdditionalTask):
        self._additional_tasks.add(task)

    async def main(self):
        logger.info("Starting main application logic")
        await self.runtime_config.validate()

        redis_client = redis.Redis(connection_pool=self.redis_connection_pool)
        post_start_lock_dao = PostStartLockDAO(
            redis_client,
            self.redis_key_schema,
            ttl=self.runtime_config.processing.post_start_lock_ttl,
        )

        # Initialise every registered processor (only when acting as processor)
        if self.runtime_config.is_processor_instance:
            for processor in self._processors:
                await processor.initialize()
        async with SystemEventTaskGroup(
            grace_period=self.runtime_config.processing.shutdown_grace_period,
        ) as task_group:
            if self.runtime_config.is_processor_instance:
                for handler_cls, (
                    handler,
                    instrumented_handler,
                ) in self._protocol_handlers.items():
                    if self.runtime_config.processing.otel_instrumentation_enabled:
                        logger.info(
                            f"Registering OTEL-instrumented handler: {handler_cls.__name__}"
                        )
                        handler_to_use = instrumented_handler
                    else:
                        logger.info(
                            f"Registering non-OTEL-instrumented handler: {handler_cls.__name__}"
                        )
                        handler_to_use = handler
                    handler_to_use.register_shutdown_event(task_group.shutdown_event)
                    bindings = self._handler_bindings.get(handler_cls)
                    if bindings is None:
                        raise ValueError(
                            f"No ProtocolBinding found for protocol handler {handler_cls.__name__}"
                        )
                    for binding in bindings:
                        handler_to_use.register_binding(binding)
                    task_group.create_task(handler_to_use.task())

            for task in self._additional_tasks:
                if (
                    isinstance(task, MQTTPublisher)
                    and not self.runtime_config.is_publisher_instance
                ):
                    continue
                task.register_shutdown_event(task_group.shutdown_event)
                task_group.create_task(task.task())

            # Post start every registered processor
            if self.runtime_config.is_processor_instance:
                for processor in self._processors:
                    if processor.post_start_singleton:
                        acquired = await post_start_lock_dao.acquire(
                            processor._config_identifier
                        )
                        if acquired:
                            logger.info(
                                "Acquired post_start lock for %s; executing post_start",
                                processor._config_identifier,
                            )
                            await processor.post_start()
                        else:
                            logger.info(
                                "Another instance holds post_start lock for %s; skipping",
                                processor._config_identifier,
                            )
                    else:
                        await processor.post_start()

        logger.info("Main application logic has completed")

        # Shutdown every registered processor
        if self.runtime_config.is_processor_instance:
            for processor in self._processors:
                await processor.shutdown()

        logger.info("Closing Redis connection pool")
        await self.redis_connection_pool.aclose()

        logger.info("Application shutdown complete")

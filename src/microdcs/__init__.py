import asyncio
import dataclasses
import importlib
import importlib.metadata
import logging.config
import os
import signal
import uuid
from dataclasses import MISSING, dataclass, field, fields
from pathlib import Path
from types import FrameType
from typing import Any

__version__ = importlib.metadata.version("microdcs")

logger = logging.getLogger("app.main")


@dataclass
class RedisConfig:
    hostname: str = "localhost"
    port: int = 6379
    key_prefix: str = "microdcs"
    username: str | None = None
    password: str | None = None
    ssl: bool = False
    ssl_ca_certs: Path | None = None


@dataclass
class MQTTConfig:
    hostname: str = "localhost"
    port: int = 1883
    identifier: str = "app_client"
    connect_timeout: int = 10
    publish_timeout: int = 5
    sat_token_path: Path = Path("/var/run/secrets/tokens/broker-sat")
    tls_cert_path: Path = Path("/var/run/certs/ca.crt")
    incoming_queue_size: int = 0
    outgoing_queue_size: int = 0
    message_workers: int = 5
    dedupe_ttl_seconds: int = 60 * 10  # 10 minutes
    binding_outgoing_queue_size: int = 5


@dataclass
class MessagePackConfig:
    hostname: str = "localhost"
    port: int = 8888
    tls_cert_path: Path = Path("/var/run/certs/ca.crt")
    tls_client_auth: bool = False
    keep_alive: bool = True
    max_queued_connections: int = 100
    max_concurrent_requests: int = 10
    max_buffer_size: int = 8 * 1024 * 1024
    binding_outgoing_queue_size: int = 5


@dataclass
class LoggingConfig:
    disable_if_otel_enabled: bool = True
    level: str = "INFO"
    filename: str = "app.log"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    def set_logging_config(self):
        config: dict[str, Any] = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"simple": {"format": self.format}},
            "handlers": {
                "stdout": {
                    "class": "logging.StreamHandler",
                    "level": "INFO",
                    "formatter": "simple",
                    "stream": "ext://sys.stdout",
                },
                "stderr": {
                    "class": "logging.StreamHandler",
                    "level": "ERROR",
                    "formatter": "simple",
                    "stream": "ext://sys.stderr",
                },
                "file": {
                    "class": "logging.FileHandler",
                    "formatter": "simple",
                    "filename": self.filename,
                    "mode": "w",
                },
            },
            "root": {"level": self.level, "handlers": ["stderr", "stdout", "file"]},
        }
        logging.config.dictConfig(config)

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if not getattr(self, "_initialized", False):
            return
        if (
            not self.disable_if_otel_enabled
            or os.getenv("OTEL_LOGS_EXPORTER", "none").lower() == "none"
        ):
            self.set_logging_config()

    def __post_init__(self) -> None:
        object.__setattr__(self, "_initialized", True)
        if (
            not self.disable_if_otel_enabled
            or os.getenv("OTEL_LOGS_EXPORTER", "none").lower() == "none"
        ):
            self.set_logging_config()


@dataclass
class ProcessingConfig:
    otel_instrumentation_enabled: bool = False
    cloudevent_source: str | None = None
    message_expiry_interval: int | None = None
    shared_subscription_name: str | None = None
    topic_prefixes: set[str] = field(default_factory=set)
    topic_wildcard_levels: set[str] = field(default_factory=set)
    response_topics: set[str] = field(default_factory=set)
    shutdown_grace_period: int = 30
    binding_outgoing_queue_max_size: int = 1000

    def get_topic_prefix_for_identifier(self, topic_identifier: str) -> str | None:
        for entry in self.topic_prefixes:
            name, _, prefix = entry.partition(":")
            if name.strip() == topic_identifier:
                return prefix.strip()
        return None

    def get_wildcard_levels_for_identifier(self, topic_identifier: str) -> int:
        for entry in self.topic_wildcard_levels:
            name, _, levels_str = entry.partition(":")
            if name.strip() == topic_identifier:
                return int(levels_str.strip())
        return 0

    def get_response_topic_for_identifier(self, topic_identifier: str) -> str | None:
        for entry in self.response_topics:
            name, _, topic = entry.partition(":")
            if name.strip() == topic_identifier:
                return topic.strip()
        return None


@dataclass
class RuntimeConfig:
    instance_id: str
    redis: RedisConfig
    mqtt: MQTTConfig
    msgpack: MessagePackConfig
    logging: LoggingConfig
    processing: ProcessingConfig

    def __init__(self, prefix: str = "APP_"):
        # Get the ID from Env, fallback to UUID if running locally
        self.instance_id = os.getenv("POD_ID", str(uuid.uuid4()))
        # Initialize nested dataclasses
        for field_main in fields(self):
            if dataclasses.is_dataclass(field_main.type) and callable(field_main.type):
                setattr(self, field_main.name, field_main.type())
                for field_child in fields(field_main.type):
                    env_var = (
                        f"{prefix}{field_main.name.upper()}_{field_child.name.upper()}"
                    )
                    default = None
                    if field_child.default != MISSING:
                        default = field_child.default
                    elif callable(field_child.default_factory):
                        default = field_child.default_factory()
                    value: (
                        str | int | float | bool | Path | set[str] | list[str] | None
                    ) = os.getenv(env_var, default)
                    if value is not None:
                        if (
                            field_child.type == set[str]
                            and not isinstance(value, set)
                            and isinstance(value, str)
                        ):
                            value = {v.strip() for v in value.split(",")}
                        elif (
                            field_child.type == list[str]
                            and not isinstance(value, list)
                            and isinstance(value, str)
                        ):
                            value = [v.strip() for v in value.split(",")]
                        elif field_child.type is bool and not isinstance(value, bool):
                            value = value in [1, "1", "true", "True", "TRUE"]
                        elif (
                            field_child.type == Path
                            and not isinstance(value, Path)
                            and isinstance(value, str)
                        ):
                            value = Path(value)
                        elif (
                            field_child.type is str
                            or str(field_child.type) == "str | None"
                        ):
                            value = str(value)
                        elif (
                            field_child.type is int
                            or str(field_child.type) == "int | None"
                        ) and isinstance(value, str):
                            value = int(value)
                        elif (
                            field_child.type is float
                            or str(field_child.type) == "float | None"
                        ) and (isinstance(value, str) or isinstance(value, int)):
                            value = float(value)

                        setattr(getattr(self, field_main.name), field_child.name, value)

    async def _check_tcp_connection(
        self, service_name: str, hostname: str, port: int, timeout_seconds: int = 3
    ) -> None:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(hostname, port),
                timeout=timeout_seconds,
            )
            writer.close()
            await writer.wait_closed()
        except Exception as exc:
            raise ValueError(
                f"Unable to connect to {service_name} at {hostname}:{port}: {exc}"
            ) from exc

    async def _check_bindable(
        self, service_name: str, hostname: str, port: int, timeout_seconds: int = 3
    ) -> None:
        server: asyncio.base_events.Server | None = None
        try:
            server = await asyncio.wait_for(
                asyncio.start_server(lambda _reader, _writer: None, hostname, port),
                timeout=timeout_seconds,
            )
        except Exception as exc:
            raise ValueError(
                f"Unable to bind {service_name} to {hostname}:{port}: {exc}"
            ) from exc
        finally:
            if server is not None:
                server.close()
                await server.wait_closed()

    async def validate(self) -> None:
        errors: list[str] = []

        def require_non_empty(value: str, field_name: str) -> None:
            if not value or not value.strip():
                errors.append(f"{field_name} must not be empty")

        def require_port(port: int, field_name: str) -> None:
            if port < 1 or port > 65535:
                errors.append(f"{field_name} must be in range 1..65535")

        def require_non_negative(value: int, field_name: str) -> None:
            if value < 0:
                errors.append(f"{field_name} must be >= 0")

        def require_positive(value: int, field_name: str) -> None:
            if value <= 0:
                errors.append(f"{field_name} must be > 0")

        require_non_empty(self.instance_id, "instance_id")
        require_non_empty(self.redis.hostname, "redis.hostname")
        require_non_empty(self.redis.key_prefix, "redis.key_prefix")
        require_non_empty(self.mqtt.hostname, "mqtt.hostname")
        require_non_empty(self.mqtt.identifier, "mqtt.identifier")
        require_non_empty(self.msgpack.hostname, "msgpack.hostname")

        require_port(self.redis.port, "redis.port")
        require_port(self.mqtt.port, "mqtt.port")
        require_port(self.msgpack.port, "msgpack.port")

        require_positive(self.mqtt.connect_timeout, "mqtt.connect_timeout")
        require_positive(self.mqtt.publish_timeout, "mqtt.publish_timeout")
        require_non_negative(self.mqtt.incoming_queue_size, "mqtt.incoming_queue_size")
        require_non_negative(self.mqtt.outgoing_queue_size, "mqtt.outgoing_queue_size")
        require_positive(self.mqtt.message_workers, "mqtt.message_workers")
        require_non_negative(self.mqtt.dedupe_ttl_seconds, "mqtt.dedupe_ttl_seconds")
        require_non_negative(
            self.mqtt.binding_outgoing_queue_size,
            "mqtt.binding_outgoing_queue_size",
        )

        require_positive(
            self.msgpack.max_queued_connections,
            "msgpack.max_queued_connections",
        )
        require_positive(
            self.msgpack.max_concurrent_requests,
            "msgpack.max_concurrent_requests",
        )
        require_positive(self.msgpack.max_buffer_size, "msgpack.max_buffer_size")
        require_non_negative(
            self.msgpack.binding_outgoing_queue_size,
            "msgpack.binding_outgoing_queue_size",
        )

        require_non_negative(
            self.processing.shutdown_grace_period,
            "processing.shutdown_grace_period",
        )
        require_positive(
            self.processing.binding_outgoing_queue_max_size,
            "processing.binding_outgoing_queue_max_size",
        )
        if self.processing.message_expiry_interval is not None:
            require_non_negative(
                self.processing.message_expiry_interval,
                "processing.message_expiry_interval",
            )

        if errors:
            raise ValueError("RuntimeConfig validation failed: " + "; ".join(errors))

        # Connectivity and bindability pre-checks so startup fails fast with clear errors.
        await self._check_tcp_connection("Redis", self.redis.hostname, self.redis.port)
        await self._check_tcp_connection("MQTT", self.mqtt.hostname, self.mqtt.port)
        await self._check_bindable(
            "MessagePack server", self.msgpack.hostname, self.msgpack.port
        )


class SystemEventTaskGroup(asyncio.TaskGroup):
    """Custom TaskGroup with two-phase shutdown: graceful event then force-cancel."""

    def _force_shutdown(self):
        """Force-cancel all tasks after grace period expires."""
        logger.info("Grace period expired: force-cancelling remaining tasks")
        for task in self._tasks:
            task.cancel()

    def shutdown(self, signal: signal.Signals):
        logger.info("Received signal %s: initiating graceful shutdown", signal.name)
        self._shutdown_event.set()
        loop = asyncio.get_running_loop()
        self._force_shutdown_handle = loop.call_later(
            self._grace_period, self._force_shutdown
        )

    @property
    def shutdown_event(self) -> asyncio.Event:
        return self._shutdown_event

    def __init__(
        self,
        grace_period: int = 30,
        signals: set[signal.Signals] = {signal.SIGINT, signal.SIGTERM},
        *args: Any,
        **kwargs: Any,
    ):
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._grace_period = grace_period
        self._force_shutdown_handle: asyncio.TimerHandle | None = None
        self._signals = signals
        super().__init__(*args, **kwargs)
        logger.info(
            "Setting up signal handlers for: %s",
            ", ".join(s.name for s in self._signals),
        )
        try:
            loop = asyncio.get_running_loop()
            for sig in self._signals:
                loop.add_signal_handler(sig, self.shutdown, sig)
        except NotImplementedError:  # Windows compatibility
            self._win_signal = None

            def set_win_signal(sig: int, frame: FrameType | None):
                self._win_signal = sig

            for sig in self._signals:
                signal.signal(sig, set_win_signal)

            async def win_signal_watcher():
                while self._win_signal is None:
                    await asyncio.sleep(1)
                self.shutdown(signal.Signals(self._win_signal))

            self.create_task(win_signal_watcher())

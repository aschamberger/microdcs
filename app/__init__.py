import asyncio
import dataclasses
import logging.config
import os
import signal
import uuid
from dataclasses import MISSING, dataclass, field, fields
from pathlib import Path
from types import FrameType
from typing import Any

logger = logging.getLogger("app.main")


@dataclass
class RedisConfig:
    hostname: str = "localhost"
    port: int = 6379
    key_prefix: str = "microdcs"


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


@dataclass
class MessagePackConfig:
    hostname: str = "localhost"
    port: int = 8888
    tls_cert_path: Path = Path("/var/run/certs/ca.crt")
    keep_alive: bool = True
    max_queued_connections: int = 100
    max_concurrent_requests: int = 10


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
    topics: set[str] = field(default_factory=lambda: {"app/events/#", "app/invoke/#"})
    response_topics: set[str] = field(default_factory=lambda: {"app/errors/delivery"})


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


class SystemEventTaskGroup(asyncio.TaskGroup):
    """Custom TaskGroup gracefully handling system events."""

    _signals: set[signal.Signals]
    _win_signal: int | None

    def shutdown(self, signal: signal.Signals):
        logger.info("Received signal %s: shutting down tasks", signal.name)
        for task in self._tasks:
            task.cancel()

    def __init__(
        self,
        signals: set[signal.Signals] = {signal.SIGINT, signal.SIGTERM},
        *args: Any,
        **kwargs: Any,
    ):
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

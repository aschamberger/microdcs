import asyncio
from dataclasses import dataclass, field, fields, MISSING
import dataclasses
import logging.config
import os
from pathlib import Path
import signal
from types import FrameType
from typing import Any


logger = logging.getLogger('app.main')


@dataclass
class MQTTConfig:
    hostname: str = "localhost"
    port: int = 1883
    identifier: str = "app_client"
    timeout: int = 10
    sat_token_path: Path = Path("/var/run/secrets/tokens/broker-sat")
    tls_cert_path: Path = Path("/var/run/certs/ca.crt")
    message_publication_timeout: float = 1.0
    message_expiry_interval: int = 10
    incoming_queue_size: int = 5
    outgoing_queue_size: int = 1
    message_workers: int = 5


@dataclass
class LoggingConfig:
    disable_if_otel_enabled: bool = True
    level: str = "INFO"
    filename: str = "app.log"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    def set_logging_config(self):
        config: dict[str, Any] = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'simple': {
                    'format': self.format
                }
            },
            'handlers': {
                'stdout': {
                    'class': 'logging.StreamHandler',
                    'level': 'INFO',
                    'formatter': 'simple',
                    'stream': 'ext://sys.stdout'
                },
                'stderr': {
                    'class': 'logging.StreamHandler',
                    'level': 'ERROR',
                    'formatter': 'simple',
                    'stream': 'ext://sys.stderr'
                },
                'file': {
                    'class': 'logging.FileHandler',
                    'formatter': 'simple',
                    'filename': self.filename,
                    'mode': 'w'
                }
            },
            'root': {
                'level': self.level,
                'handlers': [
                    'stderr',
                    'stdout',
                    'file'
                ]
            }
        }
        logging.config.dictConfig(config)

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if not self.disable_if_otel_enabled or os.getenv("OTEL_LOGS_EXPORTER", "none").lower() == "none":
            self.set_logging_config()


@dataclass
class ProcessingConfig:
    otel_instrumentation_enabled: bool = False
    interval: int = 300
    timeout: int = 30
    topics: set[str] = field(default_factory=lambda: {"app/events/#", "app/invoke/#"})


@dataclass
class RuntimeConfig:
    mqtt: MQTTConfig
    logging: LoggingConfig
    processing: ProcessingConfig

    def __init__(self, prefix: str = "APP_"):
        for field in fields(self):
            if dataclasses.is_dataclass(field.type) and callable(field.type):
                setattr(self, field.name, field.type())
                for field2 in fields(field.type):
                    env_var = f"{prefix}{field.name.upper()}_{field2.name.upper()}"
                    default = None
                    if field2.default != MISSING:
                        default = field2.default
                    elif callable(field2.default_factory):
                        default = field2.default_factory()
                    value: str | int | float | bool | Path | set[str] | list[str] | None = os.getenv(env_var, default)
                    if value is not None:
                        if field2.type == set[str] and not isinstance(value, set) and isinstance(value, str):
                            value = {v.strip() for v in value.split(",")}
                        elif field2.type == list[str] and not isinstance(value, list) and isinstance(value, str):
                            value = [v.strip() for v in value.split(",")]
                        elif field2.type == bool and not isinstance(value, bool):
                            value = value in [1, "1", "true", "True", "TRUE"]
                        elif field2.type == Path and not isinstance(value, Path) and isinstance(value, str):
                            value = Path(value)
                        elif field2.type == str:
                            value = str(value)
                        elif field2.type == int and isinstance(value, str):
                            value = int(value)
                        elif field2.type == float and (isinstance(value, str) or isinstance(value, int)):
                            value = float(value)

                        setattr(getattr(self, field.name), field2.name, value)


class SystemEventTaskGroup(asyncio.TaskGroup):
    """Custom TaskGroup gracefully handling system events."""

    _signals: set[signal.Signals]
    _win_signal: int | None

    def shutdown(self, signal: signal.Signals):
        logger.info("Received signal %s: shutting down tasks", signal.name)
        for task in self._tasks:
            task.cancel()

    def __init__(self, signals: set[signal.Signals] = {signal.SIGINT, signal.SIGTERM}, *args: Any, **kwargs: Any):
        self._signals = signals
        super().__init__(*args, **kwargs)
        logger.info("Setting up signal handlers for: %s", ', '.join(s.name for s in self._signals))
        try:
            loop = asyncio.get_running_loop()
            for sig in self._signals:
                loop.add_signal_handler(sig, self.shutdown, sig)
        except NotImplementedError: # Windows compatibility
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
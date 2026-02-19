import os
from pathlib import Path
from unittest.mock import patch

import pytest

from app import (
    LoggingConfig,
    MessagePackConfig,
    MQTTConfig,
    ProcessingConfig,
    RedisConfig,
    RuntimeConfig,
    SystemEventTaskGroup,
)

# ---------------------------------------------------------------------------
# Config dataclasses – defaults
# ---------------------------------------------------------------------------


class TestRedisConfig:
    def test_defaults(self):
        cfg = RedisConfig()
        assert cfg.hostname == "localhost"
        assert cfg.port == 6379
        assert cfg.key_prefix == "microdcs"


class TestMQTTConfig:
    def test_defaults(self):
        cfg = MQTTConfig()
        assert cfg.hostname == "localhost"
        assert cfg.port == 1883
        assert cfg.identifier == "app_client"
        assert cfg.connect_timeout == 10
        assert cfg.publish_timeout == 5
        assert cfg.sat_token_path == Path("/var/run/secrets/tokens/broker-sat")
        assert cfg.tls_cert_path == Path("/var/run/certs/ca.crt")
        assert cfg.incoming_queue_size == 0
        assert cfg.outgoing_queue_size == 0
        assert cfg.message_workers == 5
        assert cfg.dedupe_ttl_seconds == 600


class TestMessagePackConfig:
    def test_defaults(self):
        cfg = MessagePackConfig()
        assert cfg.hostname == "localhost"
        assert cfg.port == 8888
        assert cfg.tls_cert_path == Path("/var/run/certs/ca.crt")
        assert cfg.keep_alive is True
        assert cfg.max_queued_connections == 100
        assert cfg.max_concurrent_requests == 10


class TestProcessingConfig:
    def test_defaults(self):
        cfg = ProcessingConfig()
        assert cfg.otel_instrumentation_enabled is False
        assert cfg.cloudevent_source is None
        assert cfg.message_expiry_interval is None
        assert cfg.shared_subscription_name is None
        assert cfg.topics == set()
        assert cfg.response_topics == set()


# ---------------------------------------------------------------------------
# LoggingConfig
# ---------------------------------------------------------------------------


class TestLoggingConfig:
    def test_defaults(self):
        with patch.object(LoggingConfig, "set_logging_config"):
            cfg = LoggingConfig()
        assert cfg.disable_if_otel_enabled is True
        assert cfg.level == "INFO"
        assert cfg.filename == "app.log"

    def test_set_logging_config_called_when_otel_disabled(self):
        """When OTEL_LOGS_EXPORTER is 'none' (default), logging should be configured."""
        with (
            patch.dict(os.environ, {"OTEL_LOGS_EXPORTER": "none"}),
            patch.object(LoggingConfig, "set_logging_config") as mock_set,
        ):
            LoggingConfig()
        assert mock_set.called

    def test_set_logging_config_not_called_when_otel_enabled(self):
        """When OTEL_LOGS_EXPORTER is set and disable_if_otel_enabled is True,
        set_logging_config should NOT be called after __init__."""
        with (
            patch.dict(os.environ, {"OTEL_LOGS_EXPORTER": "otlp"}),
            patch.object(LoggingConfig, "set_logging_config") as mock_set,
        ):
            LoggingConfig()
        # set_logging_config is still invoked during __init__ for each field set,
        # but the branch guard should prevent it when OTEL is active
        # We just verify it was NOT called (the guard blocks it)
        assert not mock_set.called

    def test_set_logging_config_produces_valid_dict_config(self):
        """Ensure set_logging_config can be called without raising."""
        with (
            patch.dict(os.environ, {"OTEL_LOGS_EXPORTER": "none"}),
            patch("logging.config.dictConfig") as mock_dictcfg,
        ):
            cfg = LoggingConfig()
            cfg.set_logging_config()
        mock_dictcfg.assert_called()
        config_dict = mock_dictcfg.call_args.args[0]
        assert config_dict["version"] == 1
        assert "root" in config_dict
        assert "handlers" in config_dict

    def test_setattr_triggers_config_when_allowed(self):
        """Changing a field triggers set_logging_config when OTEL is off."""
        with (
            patch.dict(os.environ, {"OTEL_LOGS_EXPORTER": "none"}),
            patch.object(LoggingConfig, "set_logging_config") as mock_set,
        ):
            cfg = LoggingConfig()
            mock_set.reset_mock()
            cfg.level = "DEBUG"
        assert mock_set.called

    def test_disable_if_otel_enabled_false_always_applies(self):
        """When disable_if_otel_enabled is False, logging config always applies
        regardless of OTEL env var."""
        with (
            patch.dict(os.environ, {"OTEL_LOGS_EXPORTER": "otlp"}),
            patch.object(LoggingConfig, "set_logging_config") as mock_set,
        ):
            cfg = LoggingConfig(disable_if_otel_enabled=False)
            mock_set.reset_mock()
            cfg.level = "WARNING"
        assert mock_set.called


# ---------------------------------------------------------------------------
# RuntimeConfig
# ---------------------------------------------------------------------------


class TestRuntimeConfig:
    def test_defaults_loaded(self):
        """RuntimeConfig should populate nested dataclasses with defaults."""
        with patch.dict(os.environ, {}, clear=False):
            cfg = RuntimeConfig()
        assert isinstance(cfg.redis, RedisConfig)
        assert isinstance(cfg.mqtt, MQTTConfig)
        assert isinstance(cfg.msgpack, MessagePackConfig)
        assert isinstance(cfg.logging, LoggingConfig)
        assert isinstance(cfg.processing, ProcessingConfig)
        assert cfg.redis.hostname == "localhost"

    def test_instance_id_from_env(self):
        with patch.dict(os.environ, {"POD_ID": "my-pod-123"}, clear=False):
            cfg = RuntimeConfig()
        assert cfg.instance_id == "my-pod-123"

    def test_instance_id_uuid_fallback(self):
        with patch.dict(os.environ, {}, clear=False):
            env = os.environ.copy()
            env.pop("POD_ID", None)
            with patch.dict(os.environ, env, clear=True):
                cfg = RuntimeConfig()
        # Should be a valid UUID string
        assert len(cfg.instance_id) == 36  # UUID format

    def test_env_override_str(self):
        with patch.dict(os.environ, {"APP_REDIS_HOSTNAME": "redis.prod"}, clear=False):
            cfg = RuntimeConfig()
        assert cfg.redis.hostname == "redis.prod"

    def test_env_override_int(self):
        with patch.dict(os.environ, {"APP_REDIS_PORT": "6380"}, clear=False):
            cfg = RuntimeConfig()
        assert cfg.redis.port == 6380

    def test_env_override_bool_true(self):
        with patch.dict(
            os.environ,
            {"APP_PROCESSING_OTEL_INSTRUMENTATION_ENABLED": "true"},
            clear=False,
        ):
            cfg = RuntimeConfig()
        assert cfg.processing.otel_instrumentation_enabled is True

    def test_env_override_bool_false(self):
        with patch.dict(
            os.environ,
            {"APP_PROCESSING_OTEL_INSTRUMENTATION_ENABLED": "false"},
            clear=False,
        ):
            cfg = RuntimeConfig()
        assert cfg.processing.otel_instrumentation_enabled is False

    def test_env_override_path(self):
        with patch.dict(
            os.environ, {"APP_MQTT_TLS_CERT_PATH": "/custom/cert.pem"}, clear=False
        ):
            cfg = RuntimeConfig()
        assert cfg.mqtt.tls_cert_path == Path("/custom/cert.pem")

    def test_env_override_set(self):
        with patch.dict(
            os.environ,
            {"APP_PROCESSING_TOPICS": "greetings:t/a/#,greetings:t/b/#"},
            clear=False,
        ):
            cfg = RuntimeConfig()
        assert cfg.processing.topics == {"greetings:t/a/#", "greetings:t/b/#"}

    def test_env_override_str_or_none(self):
        with patch.dict(
            os.environ,
            {"APP_PROCESSING_CLOUDEVENT_SOURCE": "my-source"},
            clear=False,
        ):
            cfg = RuntimeConfig()
        assert cfg.processing.cloudevent_source == "my-source"

    def test_env_override_int_or_none(self):
        with patch.dict(
            os.environ,
            {"APP_PROCESSING_MESSAGE_EXPIRY_INTERVAL": "120"},
            clear=False,
        ):
            cfg = RuntimeConfig()
        assert cfg.processing.message_expiry_interval == 120

    def test_custom_prefix(self):
        with patch.dict(
            os.environ, {"CUSTOM_REDIS_HOSTNAME": "custom-host"}, clear=False
        ):
            cfg = RuntimeConfig(prefix="CUSTOM_")
        assert cfg.redis.hostname == "custom-host"


# ---------------------------------------------------------------------------
# SystemEventTaskGroup
# ---------------------------------------------------------------------------


class TestSystemEventTaskGroup:
    @pytest.mark.asyncio
    async def test_basic_task_completion(self):
        """Tasks complete normally under the group."""
        results: list[int] = []

        async with SystemEventTaskGroup() as tg:
            tg.create_task(self._append(results, 1))
            tg.create_task(self._append(results, 2))

        assert sorted(results) == [1, 2]

    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(self):
        """Calling shutdown should cancel running tasks."""
        import asyncio
        import signal as sig

        cancelled = False

        async def long_running():
            nonlocal cancelled
            try:
                await asyncio.sleep(999)
            except asyncio.CancelledError:
                cancelled = True
                # Swallow the error so TaskGroup exits cleanly

        async with SystemEventTaskGroup() as tg:
            tg.create_task(long_running())
            # Give the task time to start
            await asyncio.sleep(0.05)
            tg.shutdown(sig.SIGTERM)

        assert cancelled

    @staticmethod
    async def _append(lst: list[int], val: int) -> None:
        lst.append(val)

import socket

import pytest

from app import MessagePackConfig, MQTTConfig, RedisConfig

MQTT_CONFIG = MQTTConfig()
REDIS_CONFIG = RedisConfig()
MSGPACK_CONFIG = MessagePackConfig()


def _is_service_available(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a TCP service is reachable."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


mqtt_available = pytest.mark.skipif(
    not _is_service_available(MQTT_CONFIG.hostname, MQTT_CONFIG.port),
    reason=f"MQTT broker not reachable at {MQTT_CONFIG.hostname}:{MQTT_CONFIG.port}",
)

redis_available = pytest.mark.skipif(
    not _is_service_available(REDIS_CONFIG.hostname, REDIS_CONFIG.port),
    reason=f"Redis server not reachable at {REDIS_CONFIG.hostname}:{REDIS_CONFIG.port}",
)

msgpack_server_available = pytest.mark.skipif(
    not _is_service_available(MSGPACK_CONFIG.hostname, MSGPACK_CONFIG.port),
    reason=f"MessagePack RPC server not reachable at {MSGPACK_CONFIG.hostname}:{MSGPACK_CONFIG.port}",
)

app_available = pytest.mark.skipif(
    not _is_service_available(MSGPACK_CONFIG.hostname, MSGPACK_CONFIG.port),
    reason=f"App not running (MessagePack RPC server not reachable at {MSGPACK_CONFIG.hostname}:{MSGPACK_CONFIG.port})",
)

integration = pytest.mark.integration

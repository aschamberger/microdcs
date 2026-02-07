import asyncio
from unittest.mock import MagicMock, patch

import pytest

from app import MessagePackConfig
from app.msgpack import MessagePackHandler, RpcDispatcher
from app.redis import RedisKeySchema


class MockMessagePackConfig(MessagePackConfig):
    pass


@pytest.mark.asyncio
async def test_dispatcher_sync():
    dispatcher = RpcDispatcher()
    dispatcher.register("add", lambda a, b: a + b)

    result = await dispatcher.dispatch("add", [1, 2])
    assert result == 3


@pytest.mark.asyncio
async def test_dispatcher_async():
    dispatcher = RpcDispatcher()

    async def async_add(a, b):
        return a + b

    dispatcher.register("add", async_add)

    result = await dispatcher.dispatch("add", [2, 3])
    assert result == 5


@pytest.mark.asyncio
async def test_dispatcher_not_found():
    dispatcher = RpcDispatcher()
    with pytest.raises(ValueError, match="Method 'unknown' not found"):
        await dispatcher.dispatch("unknown", [])


@pytest.mark.asyncio
async def test_handler_log_heartbeat():
    config = MockMessagePackConfig()
    mock_pool = MagicMock()
    key_schema = RedisKeySchema()

    with patch("app.msgpack.redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value = MagicMock()
        handler = MessagePackHandler(config, mock_pool, key_schema)

    # Just ensure it doesn't raise
    await handler.heartbeat("2023-01-01")

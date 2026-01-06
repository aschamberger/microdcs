import asyncio

import pytest

from app import MessagePackConfig
from app.msgpack import MessagePackHandler, RpcDispatcher


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
async def test_handler_process_hello():
    config = MockMessagePackConfig()
    handler = MessagePackHandler(config)

    user_dict = {"name": "Tester"}
    # We mock sleep to speed up test
    original_sleep = asyncio.sleep

    async def mock_sleep(x):
        pass

    asyncio.sleep = mock_sleep

    try:
        result = await handler.process_hello(user_dict)
        assert result["status"] == "ok"
        assert result["name"] == "Tester"
    finally:
        asyncio.sleep = original_sleep


@pytest.mark.asyncio
async def test_handler_log_heartbeat():
    config = MockMessagePackConfig()
    handler = MessagePackHandler(config)

    # Just ensure it doesn't raise
    await handler.heartbeat("2023-01-01")

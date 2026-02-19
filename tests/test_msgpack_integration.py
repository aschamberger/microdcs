import asyncio
import time
from datetime import datetime

import orjson
import pytest

from app import MessagePackConfig
from app.common import CloudEvent
from app.models.greetings import Hello
from app.msgpack import MessagePackRpcClient
from tests.conftest import integration, msgpack_server_available

MSGPACK_CONFIG = MessagePackConfig()

CE_SOURCE = "https://example.com/sender"
CE_TYPE = "com.github.aschamberger.microdcs.greetings.hello.v1"
CE_DATASCHEMA = "https://aschamberger.github.io/schemas/microdcs/greetings/v1.0.0/hello"


@pytest.mark.asyncio
@integration
@msgpack_server_available
async def test_notify_heartbeat():
    """Send a fire-and-forget heartbeat notification."""
    async with MessagePackRpcClient(
        host=MSGPACK_CONFIG.hostname, port=MSGPACK_CONFIG.port
    ) as client:
        await client.notify("heartbeat", time.time())


@pytest.mark.asyncio
@integration
@msgpack_server_available
async def test_sequential_call_publish():
    """Send a single publish RPC call and verify a response is returned."""
    async with MessagePackRpcClient(
        host=MSGPACK_CONFIG.hostname, port=MSGPACK_CONFIG.port
    ) as client:
        hello = Hello(name="Alice")
        cloudevent = CloudEvent(
            data=hello.to_msgpack(),
            source=CE_SOURCE,
            type=CE_TYPE,
            datacontenttype="application/msgpack",
            dataschema=CE_DATASCHEMA,
            subject="test",
            time=datetime.now(),
            custommetadata={
                "x-hidden-str": "This is a hidden string",
                "x-hidden-obj": str(
                    orjson.dumps({"field": "This is a hidden object field"}),
                    "utf-8",
                ),
            },
            transportmetadata={
                "example-transport-key": "example-transport-value",
            },
        )

        response = await client.call(
            "publish", cloudevent.to_dict(), cloudevent.transportmetadata
        )
        assert response is not None


@pytest.mark.asyncio
@integration
@msgpack_server_available
async def test_parallel_calls_publish():
    """Send 5 parallel publish RPC calls and verify all responses are returned."""
    async with MessagePackRpcClient(
        host=MSGPACK_CONFIG.hostname, port=MSGPACK_CONFIG.port
    ) as client:
        tasks = []
        for i in range(5):
            hello = Hello(name=f"Bot-{i}")
            cloudevent = CloudEvent(
                data=hello.to_msgpack(),
                source=CE_SOURCE,
                type=CE_TYPE,
                datacontenttype="application/msgpack",
                dataschema=CE_DATASCHEMA,
                subject="test",
                time=datetime.now(),
                custommetadata={
                    "example-metadata-key": "example-metadata-value",
                },
                transportmetadata={
                    "example-transport-key": "example-transport-value",
                },
            )
            tasks.append(
                client.call(
                    "publish",
                    cloudevent.to_dict(),
                    cloudevent.transportmetadata,
                )
            )

        results = await asyncio.gather(*tasks)

        assert len(results) == 5
        assert all(r is not None for r in results)

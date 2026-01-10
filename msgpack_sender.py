import asyncio
import datetime
import itertools
import time
import uuid

import msgpack

from app import MessagePackConfig
from app.common import CloudEventAttributes
from app.identity_processor import Hello
from app.msgpack import RpcMessageType


class MessagePackRpcClient:
    def __init__(self, host="localhost", port=8888):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self._id_counter = itertools.count(1)
        self._pending_requests = {}
        self._listen_task = None
        self._lock = asyncio.Lock()

    async def connect(self):
        if self.writer:
            return  # Already connected
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self._listen_task = asyncio.create_task(self._reader_loop())
        print(f"Connected to {self.host}:{self.port}")
        return self

    async def close(self):
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.writer = None  # Reset to None so we know it's closed

        print("Disconnected.")

    # --- MAGIC METHODS FOR 'async with' ---
    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def _send(self, payload):
        if self.writer is None:
            raise RuntimeError(
                "Client is not connected. Did you forget 'await client.connect()'?"
            )

        data = msgpack.packb(payload)
        if data is not None:
            # print(f"Sending: {data}")
            async with self._lock:
                self.writer.write(data)
                await self.writer.drain()

    async def call(self, method: str, *args):
        msg_id = next(self._id_counter)
        future = asyncio.get_running_loop().create_future()
        self._pending_requests[msg_id] = future

        # [0, msgid, method, params]
        payload = [RpcMessageType.REQUEST, msg_id, method, list(args)]

        try:
            await self._send(payload)
            return await future
        except Exception:
            # If send fails, cleanup the future so we don't leak memory
            self._pending_requests.pop(msg_id, None)
            raise
        finally:
            self._pending_requests.pop(msg_id, None)

    async def notify(self, method: str, *args):
        # [2, method, params]
        payload = [RpcMessageType.NOTIFICATION, method, list(args)]
        await self._send(payload)

    async def _reader_loop(self):
        if self.reader is None:
            raise RuntimeError(
                "Client is not connected. Did you forget 'await client.connect()'?"
            )

        unpacker = msgpack.Unpacker(raw=False)
        try:
            while True:
                data = await self.reader.read(4096)
                if not data:
                    break
                unpacker.feed(data)

                for msg in unpacker:
                    if (
                        isinstance(msg, list)
                        and len(msg) == 4
                        and msg[0] == RpcMessageType.RESPONSE
                    ):
                        _, msg_id, error, result = msg
                        future = self._pending_requests.get(msg_id)

                        if future and not future.done():
                            if error:
                                future.set_exception(
                                    RuntimeError(f"RPC Error: {error}")
                                )
                            else:
                                future.set_result(result)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Reader Error: {e}")


async def main():
    msgpack_config = MessagePackConfig()

    async with MessagePackRpcClient(
        host=msgpack_config.hostname, port=msgpack_config.port
    ) as client:
        # --- 1. Notification (Fire & Forget) ---
        print("Sending heartbeat...")
        await client.notify("heartbeat", time.time())

        # --- 2. Sequential Call (Standard) ---
        print("\n--- Sequential Call ---")
        hello = Hello(name="Alice")
        cloud_event: CloudEventAttributes = CloudEventAttributes(
            id=uuid.uuid4().hex,
            source="https://example.com/sender",
            type="com.github.aschamberger.micro-dcs.identity.hello.v1",
            datacontenttype="application/msgpack",
            dataschema="https://aschamberger.github.io/schemas/micro-dcs/identity/hello-v1",
            subject="test",
            time=datetime.datetime.now().isoformat() + "Z",
        )
        user_properties: dict[str, str] = {}

        # We pass the DICT version of the user because MsgPack expects basic types
        response = await client.call(
            "publish", hello.to_msgpack(), cloud_event.to_dict(), user_properties
        )
        print(f"Result: {response}")

        # --- 3. Parallel Calls (High Performance) ---
        print("\n--- Parallel Calls (Pipelining) ---")
        # We launch 5 requests at once. The client manages 5 futures internally.
        start = time.time()

        tasks = []
        for i in range(5):
            h = Hello(name=f"Bot-{i}")
            cloud_event: CloudEventAttributes = CloudEventAttributes(
                id=uuid.uuid4().hex,
                source="https://example.com/sender",
                type="com.github.aschamberger.micro-dcs.identity.hello.v1",
                datacontenttype="application/msgpack",
                dataschema="https://aschamberger.github.io/schemas/micro-dcs/identity/hello-v1",
                subject="test",
                time=datetime.datetime.now().isoformat() + "Z",
            )
            user_properties: dict[str, str] = {}
            tasks.append(
                client.call(
                    "publish", h.to_msgpack(), cloud_event.to_dict(), user_properties
                )
            )

        # Wait for all of them to finish
        results = await asyncio.gather(*tasks)

        duration = time.time() - start
        print(f"Got {len(results)} results in {duration:.2f}s")
        print(f"First result: {results[0]}")

        await client.close()


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import time
import typing

import msgpack

from app import MessagePackConfig
from app.identity_processor import Hello
from app.msgpack import RpcMessageType


async def main():
    msgpack_config = MessagePackConfig()

    reader, writer = await asyncio.open_connection(
        msgpack_config.hostname, msgpack_config.port
    )

    # --- 1. Send Notification ---
    print("Sending Notification...")
    notify_msg = [RpcMessageType.NOTIFICATION, "log_heartbeat", [time.time()]]
    writer.write(typing.cast(bytes, msgpack.packb(notify_msg)))
    await writer.drain()

    # --- 2. Send Request ---
    print("Sending Request...")
    hello = Hello(name="Bob")
    req_id = 1
    req_msg = [RpcMessageType.REQUEST, req_id, "process_hello", [hello.to_dict()]]
    writer.write(typing.cast(bytes, msgpack.packb(req_msg)))
    await writer.drain()

    # --- 3. Read Response ---
    data = await reader.read(4096)
    unpacker = msgpack.Unpacker(raw=False)
    unpacker.feed(data)

    for msg in unpacker:
        # msg: [Type, ID, Error, Result]
        if msg[0] == RpcMessageType.RESPONSE:
            print(f"Received Result: {msg[3]}")

    writer.close()
    await writer.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())

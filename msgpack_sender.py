import asyncio
import struct

from app import MessagePackConfig
from app.identity_processor import Hello


async def main():
    msgpack_config = MessagePackConfig()

    reader, writer = await asyncio.open_connection(
        msgpack_config.hostname, msgpack_config.port
    )

    # Send 5 requests over the single persistent connection
    for i in range(5):
        hello = Hello(name=f"Worker-{i}")

        # 1. Serialize (Mashumaro)
        payload = hello.to_msgpack()

        # 2. Add Length Header (4 bytes)
        # >I means "Big Endian Unsigned Int"
        header = struct.pack(">I", len(payload))

        # 3. Send as one flush
        writer.write(header + payload)
        await writer.drain()

        # 4. Wait for simple Ack
        _ = await reader.readexactly(1)

        print(f"Sent Hello from: {hello.name}")

    print("Finished.")
    writer.close()
    await writer.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())

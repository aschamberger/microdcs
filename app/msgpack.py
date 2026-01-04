import asyncio
import logging
import ssl
import struct

from app import MessagePackConfig
from app.identity_processor import Hello

logger = logging.getLogger("handler.msgpack")


# @dataclass
# class MessagePackProcessorMessage:
#     version: int
#     payload: bytes
#     content_type: str | None
#     correlation_data: bytes | None
#     user_properties: dict | None = None
#     cloudevent: CloudEventAttributes


class MessagePackHandler:
    _runtime_config: MessagePackConfig
    # _message_processors: list[MessagePackMessageProcessor] = []

    def __init__(self, runtime_config: MessagePackConfig):
        self._runtime_config = runtime_config

    async def _process_message(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        print(f"Connected: {addr}")

        try:
            while True:
                try:
                    # Try to read the header
                    header_data = await reader.readexactly(4)
                except asyncio.IncompleteReadError as e:
                    # If we read 0 bytes, it's a clean close.
                    if len(e.partial) == 0:
                        print(f"Client {addr} disconnected cleanly.")
                        break
                    else:
                        # If we read 1-3 bytes, it's an actual error.
                        print(f"Client disconnected mid-header! Data: {e.partial}")
                        break

                # Unpack the integer length
                (msg_length,) = struct.unpack(">I", header_data)

                # Read the Body
                body_data = await reader.readexactly(msg_length)

                # Deserialize
                hello = Hello.from_msgpack(body_data)

                # App Logic
                print(f"Processing: {hello.name}")

                # Send Ack
                writer.write(b"\x01")
                await writer.drain()

        except Exception as e:
            print(f"Error: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def task(self) -> None:
        logger.info("Starting MessagePack handler task")
        ssl_context = None
        if self._runtime_config.tls_cert_path.exists():
            ssl_context = ssl.create_default_context(
                cafile=str(self._runtime_config.tls_cert_path)
            )
        server = await asyncio.start_server(
            self._process_message,
            self._runtime_config.hostname,
            self._runtime_config.port,
            ssl=ssl_context,
            backlog=self._runtime_config.max_queued_connections,
        )
        logger.info(
            "MessagePack TCP Server running on %s:%d",
            self._runtime_config.hostname,
            self._runtime_config.port,
        )
        async with server:
            await server.serve_forever()


class OTELInstrumentedMessagePackHandler(MessagePackHandler):
    def __init__(self, runtime_config: MessagePackConfig):
        super().__init__(runtime_config)

    async def _process_message(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        return await super()._process_message(reader, writer)

import asyncio
import inspect
import logging
import ssl
from enum import IntEnum
from typing import Callable

import msgpack

from app import MessagePackConfig
from app.identity_processor import Hello

logger = logging.getLogger("handler.msgpack")


class RpcMessageType(IntEnum):
    REQUEST = 0  # [0, msgid, method, params]
    RESPONSE = 1  # [1, msgid, error, result]
    NOTIFICATION = 2  # [2, method, params]


class RpcDispatcher:
    def __init__(self):
        self._methods: dict[str, Callable] = {}

    def register(self, name: str, func: Callable):
        """Register a function to handle RPC calls."""
        self._methods[name] = func

    async def dispatch(self, method_name: str, params: list):
        """Finds the method and executes it."""
        if method_name not in self._methods:
            raise ValueError(f"Method '{method_name}' not found")

        func = self._methods[method_name]

        # Check if the function is a coroutine (async)
        if inspect.iscoroutinefunction(func):
            return await func(*params)
        else:
            return func(*params)


class MessagePackHandler:
    _runtime_config: MessagePackConfig
    dispatcher: RpcDispatcher

    async def process_hello(self, user_dict):
        hello = Hello.from_dict(user_dict)
        logger.debug("Processing user: %s", hello.name)
        await asyncio.sleep(1)  # Simulate work
        return {"status": "ok", "name": hello.name}

    async def log_heartbeat(self, timestamp):
        logger.debug("Heartbeat: %s", timestamp)
        # No return value needed for notification

    def __init__(self, runtime_config: MessagePackConfig):
        self._runtime_config = runtime_config
        self.dispatcher = RpcDispatcher()
        self.register_method("process_hello", self.process_hello)
        self.register_method("log_heartbeat", self.log_heartbeat)

    def register_method(self, name, func):
        self.dispatcher.register(name, func)

    async def _send_response(self, writer, lock, msg_id, error, result):
        """Sends the response safely using the Write Lock."""
        response = [RpcMessageType.RESPONSE, msg_id, error, result]
        try:
            serialized_data = msgpack.packb(response)
            async with lock:
                writer.write(serialized_data)
                await writer.drain()
        except Exception as e:
            # If writing fails (e.g., pipe broken), we just log it.
            # The main loop will handle the disconnect.
            logger.error("Failed to write response: %s", e)

    async def _handle_rpc_task(
        self, writer, lock, semaphore, msg_type, msg_id, method, params
    ):
        """
        Executes business logic.
        CRITICAL: Wraps everything in try/finally to ensure the semaphore is ALWAYS released.
        """
        try:
            error = None
            result = None

            try:
                # Execute business logic
                result = await self.dispatcher.dispatch(method, params)
            except asyncio.CancelledError:
                # If the server cancels us (client disconnect), we stop immediately.
                logger.info("Task cancelled for %s", method)
                raise
            except Exception as e:
                error = str(e)
                logger.error("RPC Error executing '%s': %s", method, e)
            # Send Response (only for Requests, not Notifications)
            if msg_type == RpcMessageType.REQUEST:
                await self._send_response(writer, lock, msg_id, error, result)

        finally:
            # --- RELEASE BACKPRESSURE ---
            # Whether we succeeded, failed, or were cancelled, we MUST give back the slot.
            semaphore.release()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logger.info("Connected: %s", addr)

        # 1. Thread-safe Write Lock
        socket_lock = asyncio.Lock()

        # 2. Backpressure Control
        semaphore = asyncio.Semaphore(self._runtime_config.max_concurrent_requests)

        # 3. Active Task Tracking (for cleanup)
        active_tasks = set()

        unpacker = msgpack.Unpacker(raw=False)

        try:
            while True:
                # A. Pre-check: Do we have room for at least ONE request?
                # If we are full, this blocks, stopping us from reading the socket.
                await semaphore.acquire()

                try:
                    data = await reader.read(4096)
                except Exception:
                    # Read failed? Release the token we just grabbed.
                    semaphore.release()
                    raise

                if not data:
                    # Clean disconnect? Release the token.
                    semaphore.release()
                    break

                unpacker.feed(data)

                # B. Process Batch
                # We currently hold 1 semaphore token (from step A).
                first_msg_processed = False

                for msg in unpacker:
                    if not isinstance(msg, list):
                        continue

                    # For the first message, we use the token we acquired in step A.
                    # For subsequent messages in this batch, we must acquire NEW tokens.
                    if first_msg_processed:
                        await semaphore.acquire()

                    first_msg_processed = True

                    # Parse Header
                    msg_type = msg[0]
                    msg_id, method, params = None, None, []

                    if msg_type == RpcMessageType.REQUEST and len(msg) == 4:
                        msg_id, method, params = msg[1], msg[2], msg[3]
                    elif msg_type == RpcMessageType.NOTIFICATION and len(msg) == 3:
                        method, params = msg[1], msg[2]
                    else:
                        # Malformed? Release the token immediately and skip
                        semaphore.release()
                        continue

                    # Spawn the Task
                    task = asyncio.create_task(
                        self._handle_rpc_task(
                            writer,
                            socket_lock,
                            semaphore,
                            msg_type,
                            msg_id,
                            method,
                            params,
                        )
                    )

                    # Track it
                    active_tasks.add(task)
                    # Automatically remove from set when done
                    task.add_done_callback(active_tasks.discard)

                # Corner Case: We read data, but it didn't complete a full message yet.
                # We are holding a token that wasn't used. Give it back.
                if not first_msg_processed:
                    semaphore.release()

        except Exception as e:
            logger.error("Connection error: %s", e)

        finally:
            logger.info(
                "Disconnecting %s. Cleaning up %d pending tasks...",
                addr,
                len(active_tasks),
            )

            # --- CLEANUP PHASE ---
            # 1. Cancel all running tasks
            for task in active_tasks:
                task.cancel()

            # 2. Wait for them to finish cancelling (they will release semaphores)
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)

            logger.info("Cleanup complete for %s", addr)
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
            self._handle_client,
            self._runtime_config.hostname,
            self._runtime_config.port,
            ssl=ssl_context,
            backlog=self._runtime_config.max_queued_connections,
        )
        logger.info(
            "MessagePack-RPC Server running on %s:%d",
            self._runtime_config.hostname,
            self._runtime_config.port,
        )
        async with server:
            await server.serve_forever()


class OTELInstrumentedMessagePackHandler(MessagePackHandler):
    def __init__(self, runtime_config: MessagePackConfig):
        super().__init__(runtime_config)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        return await super()._handle_client(reader, writer)

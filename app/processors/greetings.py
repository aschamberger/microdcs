import asyncio
import logging

from app import ProcessingConfig
from app.common import (
    CloudEvent,
    CloudEventProcessor,
    MessageIntent,
    ProcessorBinding,
    incoming,
    outgoing,
    processor_config,
)
from app.models.greetings import Bye, Hello

logger = logging.getLogger("processor.greetings")


@processor_config(binding=ProcessorBinding.SOUTHBOUND)
class GreetingsCloudEventProcessor(CloudEventProcessor):
    def __init__(self, instance_id: str, runtime_config: ProcessingConfig):
        super().__init__(instance_id, runtime_config)

    @incoming(Hello)
    async def handle_hello(self, hello: Hello) -> list[Hello] | Hello | None:
        logger.info("Received hello from: %s", hello.name)

        logger.debug("Processing %s %s %s", hello, hello._hidden_str, hello._hidden_obj)

        h1 = hello.response(name=hello.name)
        h2 = Hello(name="Alice")
        return [
            h1,
            h2,
        ]

    @outgoing(Bye)
    async def handle_bye(self, **kwargs) -> list[Bye] | Bye | None:
        h1 = Bye(name="Bob")
        h2 = Bye(name="Alice")
        return [
            h1,
            h2,
        ]

    async def process_event(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        if not self.event_has_callback(cloudevent):
            logger.info("No handler registered for message type: %s", cloudevent.type)
            # Special case: raw greetings messages are echoed back
            # Normally, we would not do this here, this is just to demo the functionality
            response = CloudEvent(
                data=cloudevent.data,
                type="com.github.aschamberger.microdcs.greetings.raw.v1",
                dataschema="https://aschamberger.github.io/schemas/microdcs/greetings/v1.0.0/raw",
                datacontenttype=cloudevent.datacontenttype,
                correlationid=cloudevent.correlationid,
                causationid=cloudevent.id,
            )
            return response

        result = await self.callback_incoming(cloudevent)
        return result

    async def process_response_event(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        logger.debug("Response message: %s", cloudevent)

        # For error messages, we do not send any response here
        # however we could retry in some cases or log to an external system
        return None

    async def send_event(self) -> None:
        await asyncio.sleep(5)  # wait for system to be ready
        logger.info("Sending bye event")
        await self.callback_outgoing(Bye, intent=MessageIntent.COMMAND)

    async def handle_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        await asyncio.sleep(timeout)  # yield control
        logger.info("Message expired: %s", cloudevent.id)
        return None

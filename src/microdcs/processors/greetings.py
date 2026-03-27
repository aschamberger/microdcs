import logging

from microdcs import ProcessingConfig
from microdcs.common import (
    CloudEvent,
    CloudEventProcessor,
    MessageIntent,
    ProcessorBinding,
    incoming,
    outgoing,
    processor_config,
)
from microdcs.models.greetings import Bye, Greetings, Hello

logger = logging.getLogger("processor.greetings")


@processor_config(binding=ProcessorBinding.SOUTHBOUND)
class GreetingsCloudEventProcessor(CloudEventProcessor):
    def __init__(
        self,
        instance_id: str,
        runtime_config: ProcessingConfig,
        config_identifier: str,
    ):
        super().__init__(instance_id, runtime_config, config_identifier)

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

    async def send_event(self) -> None:
        logger.info("Sending bye event")
        await self.callback_outgoing(Bye, intent=MessageIntent.COMMAND)

    async def process_cloudevent(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        if not self.has_incoming_callback(cloudevent):
            logger.info(
                "No callback registered, directly handling cloudevent type: %s",
                cloudevent.type,
            )
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

        return await super().process_cloudevent(cloudevent)

    async def process_response_cloudevent(
        self, cloudevent: CloudEvent
    ) -> list[CloudEvent] | CloudEvent | None:
        logger.debug("Response message: %s", cloudevent)

        # For error messages, we do not send any response here
        # however we could retry in some cases or log to an external system
        return None

    async def handle_cloudevent_expiration(
        self, cloudevent: CloudEvent, timeout: int
    ) -> list[CloudEvent] | CloudEvent | None:
        logger.info("Message expired: %s", cloudevent.id)
        return None

    async def trigger_outgoing_event(
        self, *, event_type: type[Greetings], **kwargs
    ) -> None:
        logger.info("Triggering outgoing event: %s", event_type)
        logger.debug("Triggering with kwargs: %s", kwargs)
        logger.warning(
            "Triggering outgoing events is currently not implemented in this processor!"
        )
        return None

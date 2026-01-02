import logging

from app import ProcessingConfig
from app.mqtt import MQTTMessageProcessor, MQTTProcessorMessage

logger = logging.getLogger("processor.identity")


class IdentityMQTTMessageProcessor(MQTTMessageProcessor):
    def __init__(self, runtime_config: ProcessingConfig):
        super().__init__(runtime_config, "identity")

    async def process_message(
        self, message: MQTTProcessorMessage
    ) -> MQTTProcessorMessage | None:
        logger.info("Processing identity message on topic: %s", message.topic)

        if message.response_topic is None:
            logger.warning(
                "No response topic specified; cannot send identity response."
            )
            return None

        # Echo the message back as is
        response = self.create_message(
            topic=message.response_topic,
            payload=message.payload,
            qos=message.qos,
            retain=message.retain,
            cloudevent_type="com.github.aschamberger.micro-dcs.identity.response",
            cloudevent_dataschema="https://aschamberger.github.io/schemas/micro-dcs/identity/response",
        )

        return response

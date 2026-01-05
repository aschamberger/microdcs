import pytest

from app import ProcessingConfig
from app.identity_processor import Hello, HiddenObject, IdentityMQTTMessageProcessor
from app.mqtt import MQTTProcessorMessage


# Mock ProcessingConfig since it's just a type hint in __init__ usually
class MockProcessingConfig(ProcessingConfig):
    pass


@pytest.fixture
def processor():
    config = MockProcessingConfig()
    return IdentityMQTTMessageProcessor(config)


@pytest.mark.asyncio
async def test_handle_hello():
    hello_input = Hello(name="World")
    results = await IdentityMQTTMessageProcessor.handle_hello(hello_input)

    assert isinstance(results, list)
    assert len(results) == 2
    assert results[0].name == "World"
    assert results[1].name == "Alice"


def test_hello_validation():
    with pytest.raises(ValueError):
        Hello(name="A")  # Too short


def test_hidden_str_extraction_insertion():
    msg = MQTTProcessorMessage(topic="test", payload=b"")

    # Test insertion
    IdentityMQTTMessageProcessor.insert_hidden_str(msg, "secret_value")
    assert msg.user_properties is not None
    assert msg.user_properties["x-hidden-str"] == "secret_value"

    # Test extraction
    extracted = IdentityMQTTMessageProcessor.extract_hidden_str(msg)
    assert extracted == "secret_value"


def test_hidden_obj_extraction_insertion():
    msg = MQTTProcessorMessage(topic="test", payload=b"")
    hidden_obj = HiddenObject(field="value")

    # Test insertion
    IdentityMQTTMessageProcessor.insert_hidden_obj(msg, hidden_obj)
    assert msg.user_properties is not None
    assert "x-hidden-obj" in msg.user_properties

    # Test extraction
    extracted = IdentityMQTTMessageProcessor.extract_hidden_obj(msg)
    assert isinstance(extracted, HiddenObject)
    assert extracted.field == "value"


def test_extract_hidden_none():
    msg = MQTTProcessorMessage(topic="test", payload=b"")
    assert IdentityMQTTMessageProcessor.extract_hidden_str(msg) is None
    assert IdentityMQTTMessageProcessor.extract_hidden_obj(msg) is None

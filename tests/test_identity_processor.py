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
    # Setup Hello object with hidden str
    hello = Hello(name="Test", _hidden_str="secret_value")
    user_properties = {}

    # Test insertion
    IdentityMQTTMessageProcessor.insert_hidden_fields(hello, user_properties)
    assert user_properties is not None
    assert user_properties["x-hidden-str"] == "secret_value"

    # Test extraction
    hello_new = Hello(name="Test")
    # Simulate receiving the user property
    IdentityMQTTMessageProcessor.extract_hidden_fields(hello_new, user_properties)
    assert hello_new._hidden_str == "secret_value"


def test_hidden_obj_extraction_insertion():
    hidden_obj = HiddenObject(field="value")
    hello = Hello(name="Test", _hidden_obj=hidden_obj)
    user_properties = {}

    # Test insertion
    IdentityMQTTMessageProcessor.insert_hidden_fields(hello, user_properties)
    assert user_properties is not None
    assert "x-hidden-obj" in user_properties

    # Test extraction
    hello_new = Hello(name="Test")
    IdentityMQTTMessageProcessor.extract_hidden_fields(hello_new, user_properties)
    assert isinstance(hello_new._hidden_obj, HiddenObject)
    assert hello_new._hidden_obj.field == "value"


def test_extract_hidden_none():
    hello = Hello(name="Test")
    user_properties = {}

    # Just ensure it doesn't raise and nothing is set
    IdentityMQTTMessageProcessor.extract_hidden_fields(hello, user_properties)
    assert hello._hidden_str is None
    assert hello._hidden_obj is None

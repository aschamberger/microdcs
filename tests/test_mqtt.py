from app.mqtt import MQTTProcessorMessage, QoS


def test_mqtt_message_defaults():
    msg = MQTTProcessorMessage(topic="test/topic", payload=b"payload")

    assert msg.topic == "test/topic"
    assert msg.payload == b"payload"
    assert msg.qos == QoS.AT_LEAST_ONCE
    assert msg.cloudevent.id is not None
    assert msg.cloudevent.time is not None


def test_cloudevent_to_user_properties():
    msg = MQTTProcessorMessage(topic="test/topic", payload=b"")
    msg.cloudevent.type = "com.example.test"
    msg.cloudevent.source = "test-source"

    msg.cloudevent_to_user_properties()

    assert msg.user_properties is not None
    assert msg.user_properties["type"] == "com.example.test"
    assert msg.user_properties["source"] == "test-source"
    assert "id" in msg.user_properties
    assert "time" in msg.user_properties
    assert "specversion" in msg.user_properties


def test_custom_user_properties():
    msg = MQTTProcessorMessage(
        topic="test", payload=b"", user_properties={"custom": "value"}
    )
    msg.cloudevent.type = "type"

    msg.cloudevent_to_user_properties()

    assert msg.user_properties is not None
    assert msg.user_properties["custom"] == "value"
    assert msg.user_properties["type"] == "type"

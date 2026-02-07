from app.common import CloudEvent
from app.mqtt import QoS


def test_cloudevent_defaults():
    ce = CloudEvent()

    assert ce.id is not None
    assert ce.recordedtime is not None
    assert ce.specversion == "1.0"
    assert ce.correlationid is not None


def test_cloudevent_to_user_properties():
    ce = CloudEvent(
        type="com.example.test",
        source="test-source",
    )

    props = ce.to_dict(context={"remove_data": True, "make_str_values": True})

    assert props["type"] == "com.example.test"
    assert props["source"] == "test-source"
    assert "id" in props
    assert "specversion" in props


def test_custom_user_properties():
    ce = CloudEvent(
        type="type",
        custommetadata={"custom": "value"},
    )

    props = ce.to_dict(context={"remove_data": True, "make_str_values": True})

    assert props["custom"] == "value"
    assert props["type"] == "type"

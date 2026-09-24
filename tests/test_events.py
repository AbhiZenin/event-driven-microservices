from services.common.events import EventEnvelope

def test_event_envelope():
    e = EventEnvelope(event_type="OrderCreated", aggregate_id="1", payload={"x":1})
    assert e.event_id
    assert e.event_type == "OrderCreated"

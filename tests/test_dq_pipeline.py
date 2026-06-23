from eightball.dq.pipeline import process_event, DQResult


def test_valid_event_is_transformed_and_ok():
    raw = {"event-type": "init", "time": 1719100000000,
           "user-id": "u1", "country": "1", "platform": "ios"}
    result = process_event(raw)
    assert result.status == "ok"
    assert result.event["platform"] == "IOS"
    assert result.event["country_name"] == "Portugal"
    assert result.reason is None


def test_unknown_event_type_is_dead_lettered():
    result = process_event({"event-type": "logout"})
    assert result.status == "dead_letter"
    assert "schema" in result.reason


def test_schema_invalid_event_is_dead_lettered():
    # init missing required 'country'
    raw = {"event-type": "init", "time": 1, "user-id": "u1", "platform": "ios"}
    result = process_event(raw)
    assert result.status == "dead_letter"
    assert "schema" in result.reason


def test_transform_error_is_dead_lettered():
    # A schema-valid event whose rule raises must land in the transform branch,
    # not crash. We inject a raising rule by patching rules_for as imported into
    # the pipeline module's namespace.
    import eightball.dq.pipeline as p
    from eightball.dq.rules import Rule

    def boom(value, **_):
        raise ValueError("kaboom")

    raw = {"event-type": "in-app-purchase", "time": 1, "purchase_value": 1.0,
           "user-id": "u1", "product-id": "p1"}
    original_rules_for = p.rules_for
    try:
        p.rules_for = lambda et: [Rule(boom, "user-id")]
        result = process_event(raw)
    finally:
        p.rules_for = original_rules_for
    assert result.status == "dead_letter"
    assert "transform" in result.reason


def test_dead_letter_keeps_original_payload():
    bad = {"event-type": "logout", "x": 1}
    result = process_event(bad)
    assert result.event == bad   # original preserved for replay

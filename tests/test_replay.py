from eightball.dq.replay import extract_original


def test_extract_original_returns_payload():
    dlq = {"reason": "schema: ...", "original": {"event-type": "init", "x": 1},
           "failed_at": 1}
    assert extract_original(dlq) == {"event-type": "init", "x": 1}


def test_extract_original_handles_raw_string_payload():
    # malformed-JSON dead letters store the original as a raw string
    dlq = {"reason": "decode: ...", "original": "{not json", "failed_at": 1}
    assert extract_original(dlq) == "{not json"


def test_extract_original_missing_field_returns_none():
    assert extract_original({"reason": "x"}) is None

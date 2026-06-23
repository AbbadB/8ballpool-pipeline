import pytest
from eightball.schemas import SchemaError, validate_event

VALID_INIT = {"event-type": "init", "time": 1719100000000,
              "user-id": "u1", "country": "1", "platform": "ios"}


def test_valid_init_passes():
    validate_event(VALID_INIT)  # should not raise


def test_missing_required_field_fails():
    bad = dict(VALID_INIT)
    del bad["country"]
    with pytest.raises(SchemaError):
        validate_event(bad)


def test_unknown_event_type_fails():
    bad = dict(VALID_INIT)
    bad["event-type"] = "logout"
    with pytest.raises(SchemaError):
        validate_event(bad)

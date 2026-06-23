import json

from eightball.events import make_init, make_match, make_purchase
from eightball.schemas import validate_event


def test_init_is_valid_and_lowercase_platform():
    e = make_init(user_id="u1", country_id="1", platform="ios", time_ms=1719100000000)
    validate_event(e)
    assert e["event-type"] == "init"
    assert e["platform"] == "ios"          # lowercase, DQ will uppercase
    assert e["country"] == "1"             # id, DQ will map to name


def test_match_is_valid_with_two_players():
    e = make_match(user_a="u1", user_b="u2", winner="u1", time_ms=1719100001000)
    validate_event(e)
    assert e["user-a"] == "u1" and e["user-b"] == "u2"


def test_purchase_is_valid():
    e = make_purchase(user_id="u1", value=4.99, product_id="coins_100",
                      time_ms=1719100002000)
    validate_event(e)
    assert e["purchase_value"] == 4.99


def test_event_json_roundtrip():
    e = make_init(user_id="u1", country_id="1", platform="ios", time_ms=1)
    assert json.loads(json.dumps(e)) == e

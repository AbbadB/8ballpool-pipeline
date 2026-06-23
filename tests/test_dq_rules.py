from eightball.dq.rules import Rule, apply_rules, map_id_to_name, uppercase

LOOKUP = {"1": "Portugal", "2": "Brazil"}


def test_uppercase_top_level():
    out = apply_rules({"platform": "ios"}, [Rule(uppercase, "platform")])
    assert out["platform"] == "IOS"


def test_map_id_to_name_adds_resolved_field():
    rule = Rule(map_id_to_name, "country",
                params={"lookup": LOOKUP, "target": "country_name"})
    out = apply_rules({"country": "1"}, [rule])
    assert out["country_name"] == "Portugal"


def test_nested_field_path():
    e = {"user-a-postmatch-info": {"platform": "android"}}
    out = apply_rules(e, [Rule(uppercase, "user-a-postmatch-info.platform")])
    assert out["user-a-postmatch-info"]["platform"] == "ANDROID"


def test_unknown_id_maps_to_unknown():
    rule = Rule(map_id_to_name, "country",
                params={"lookup": LOOKUP, "target": "country_name"})
    out = apply_rules({"country": "99"}, [rule])
    assert out["country_name"] == "UNKNOWN"


def test_missing_field_is_skipped_not_errored():
    out = apply_rules({"event-type": "init"}, [Rule(uppercase, "platform")])
    assert out == {"event-type": "init"}


def test_extensibility_new_rule_no_engine_change():
    # adding a rule is data, not code:
    def reverse(v, **_):
        return v[::-1]
    out = apply_rules({"x": "abc"}, [Rule(reverse, "x")])
    assert out["x"] == "cba"


def test_init_config_uppercases_and_maps_country():
    from eightball.dq.config import rules_for
    e = {"event-type": "init", "platform": "ios", "country": "2"}
    out = apply_rules(e, rules_for("init"))
    assert out["platform"] == "IOS"
    assert out["country_name"] == "Brazil"

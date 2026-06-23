"""Declarative DQ configuration: the country lookup and the rule set applied to
each event-type. This is the "extensible" surface -- add a Rule here, no engine
change."""
from eightball.dq.rules import Rule, map_id_to_name, uppercase

COUNTRY_LOOKUP = {"1": "Portugal", "2": "Brazil", "3": "United Kingdom",
                  "4": "Germany", "5": "United States"}


def _country_rule() -> Rule:
    return Rule(map_id_to_name, "country",
                params={"lookup": COUNTRY_LOOKUP, "target": "country_name"})


RULES_BY_TYPE = {
    "init": [Rule(uppercase, "platform"), _country_rule()],
    "match": [Rule(uppercase, "user-a-postmatch-info.platform"),
              Rule(uppercase, "user-b-postmatch-info.platform")],
    "in-app-purchase": [],
}


def rules_for(event_type: str) -> list[Rule]:
    return RULES_BY_TYPE.get(event_type, [])

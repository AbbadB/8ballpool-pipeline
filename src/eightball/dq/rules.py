"""Generic, extensible data-quality rule engine.

A Rule binds a pure transform function to a dotted field path. `apply_rules`
walks the rules in order over a copy of the event. Adding a field or a new
transform is a config change (a new Rule), never an engine change -- which is the
spec's "the component should be generic" requirement.
"""
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class Rule:
    transform: Callable          # (value, **params) -> new_value
    path: str                    # dotted path, e.g. "user-a-postmatch-info.platform"
    params: dict = field(default_factory=dict)


def uppercase(value, **_):
    return value.upper()


def map_id_to_name(value, *, lookup: dict, target: str = None, **_):
    # `target` (in params) tells apply_rules to write the resolved value to a new
    # field rather than overwriting the id in place.
    return lookup.get(value, "UNKNOWN")


def _resolve_parent(obj, parts):
    """Walk to the dict holding the final key. Returns (parent, key) or (None, None)
    if any segment is missing -- a missing field is skipped, not an error."""
    for p in parts[:-1]:
        if not isinstance(obj, dict) or p not in obj:
            return None, None
        obj = obj[p]
    if not isinstance(obj, dict) or parts[-1] not in obj:
        return None, None
    return obj, parts[-1]


def apply_rules(event: dict, rules: list[Rule]) -> dict:
    out = deepcopy(event)
    for rule in rules:
        parent, key = _resolve_parent(out, rule.path.split("."))
        if parent is None:
            continue  # field absent -- skip, do not error
        result = rule.transform(parent[key], **rule.params)
        target = rule.params.get("target")
        if target:                       # write resolved value to a new field
            parent[target] = result
        else:                            # in-place transform
            parent[key] = result
    return out

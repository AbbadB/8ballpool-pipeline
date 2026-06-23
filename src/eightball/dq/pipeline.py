"""Pure DQ decision function: validate, transform, and decide each event's fate.

process_event never raises -- it returns a DQResult of either 'ok' (with the
clean event) or 'dead_letter' (with a reason and the original payload preserved
for replay). All Kafka I/O, logging, and counting live in apps/dq_app.py.
"""
from dataclasses import dataclass
from typing import Optional

from eightball.dq.config import rules_for
from eightball.dq.rules import apply_rules
from eightball.schemas import validate_event, SchemaError


@dataclass
class DQResult:
    status: str                 # "ok" | "dead_letter"
    event: dict                 # clean event (ok) or original payload (dead_letter)
    reason: Optional[str] = None


def process_event(raw: dict) -> DQResult:
    # 1. Validate at the boundary (don't trust upstream).
    try:
        validate_event(raw)
    except SchemaError as e:
        return DQResult("dead_letter", raw, f"schema: {e}")
    # 2. Apply the generic DQ rules; isolate any transform failure.
    try:
        clean = apply_rules(raw, rules_for(raw.get("event-type")))
    except Exception as e:                       # noqa: BLE001 - DQ must not crash
        return DQResult("dead_letter", raw, f"transform: {e}")
    return DQResult("ok", clean)

"""Validate events against the provided draft-03 JSON schemas.

The schemas live in `schemas/` and use draft-03 semantics (property-level
`"required": true`). We pick the schema by the event's `event-type` and validate
with jsonschema's Draft3Validator.
"""
import json
from pathlib import Path

from jsonschema import Draft3Validator

_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"
_FILES = {
    "init": "init.json",
    "match": "match.json",
    "in-app-purchase": "in-app-purchase.json",
}


class SchemaError(ValueError):
    """Raised when an event does not conform to its schema."""


def _load(event_type: str) -> dict:
    fname = _FILES.get(event_type)
    if fname is None:
        raise SchemaError(f"unknown event-type: {event_type!r}")
    schema = json.loads((_SCHEMA_DIR / fname).read_text())
    # Draft-03 quirk: the provided schemas carry a top-level `"required": true`,
    # which is not a valid root keyword. Strip it so the validator treats the
    # object structurally; property-level `required` is still honoured.
    schema.pop("required", None)
    return schema


def validate_event(event: dict) -> None:
    """Raise SchemaError if `event` is invalid for its declared event-type."""
    schema = _load(event.get("event-type"))
    errors = sorted(Draft3Validator(schema).iter_errors(event), key=str)
    if errors:
        raise SchemaError("; ".join(e.message for e in errors))

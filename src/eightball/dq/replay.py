"""Pure helper for DLQ replay: pull the original payload out of a dead-letter
record so it can be re-produced to events.raw after the fix."""


def extract_original(dlq_record: dict):
    """Return the original payload from a dead-letter record, or None if absent."""
    return dlq_record.get("original")

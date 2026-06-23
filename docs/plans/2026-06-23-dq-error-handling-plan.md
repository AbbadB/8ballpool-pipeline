# DQ Error Handling & Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the DQ component resilient: never crash on a bad record, validate every event at the boundary, route failures to a dead-letter topic with a reason, and emit structured logs + counters — turning silent/fatal failures into an observable, reprocessable flow.

**Architecture:** A pure, testable `process_event(raw) -> DQResult` decides each event's fate (validate → apply rules → `ok` | `dead_letter(reason)`), with all exceptions caught. `apps/dq_app.py` becomes a thin loop that decodes JSON (catching decode errors), routes `ok` to `events.clean` and `dead_letter` to a new `events.dlq` topic (carrying `{reason, original, failed_at}`), and logs each outcome with running counters. The DLQ topic **is** the reprocessing path: inspect → fix rule/producer → replay.

**Tech Stack:** Python stdlib `logging` (structured JSON-ish lines), `kafka-python`, existing `jsonschema` validation and DQ rule engine. No new dependencies.

---

## Design decisions (locked)

- **Boundary validation:** DQ re-validates every event against its schema (don't trust upstream). Schema-invalid → DLQ.
- **DLQ:** a Kafka topic `events.dlq`; messages are `{"reason": str, "original": <raw>, "failed_at": <epoch_ms>}`. Reprocessing = consume DLQ, fix, re-produce to `events.raw`.
- **Observability:** stdlib `logging` emitting structured lines per outcome + a periodic `{processed, clean, dead_letter}` counter line.
- Recorded as **ADR-0009**.

## Failure taxonomy (what the DLQ must catch)

| Failure | Detection | Result |
|---|---|---|
| Malformed JSON bytes | `json.loads` raises | DLQ, reason `"decode: <msg>"` (original = raw string) |
| Unknown / missing `event-type` | no schema for type | DLQ, reason `"schema: unknown event-type ..."` |
| Schema-invalid (missing field, wrong type) | `validate_event` raises `SchemaError` | DLQ, reason `"schema: <msg>"` |
| Transform raises (e.g. uppercase on non-str) | `apply_rules` raises | DLQ, reason `"transform: <msg>"` |
| Valid | — | `events.clean` |

---

## File Structure

```
src/eightball/dq/
  pipeline.py        # NEW: pure process_event(raw_dict) -> DQResult; DQResult dataclass
  rules.py           # unchanged
  config.py          # unchanged
apps/
  dq_app.py          # MODIFY: thin resilient loop (decode-catch, route ok/dlq, log, count)
tests/
  test_dq_pipeline.py  # NEW: unit tests for process_event outcomes
docs/decisions/
  0009-dq-error-handling.md   # NEW ADR
README.md            # MODIFY: DQ failure-handling + DLQ reprocessing note
docker-compose.yml   # unchanged (events.dlq auto-created)
```

`process_event` is pure (dict → result) so it is unit-tested with no Kafka. The
Kafka I/O, logging, and counters stay in the thin `apps/dq_app.py` wrapper.

---

## Task 1: Pure DQ pipeline — `process_event`

**Files:**
- Create: `src/eightball/dq/pipeline.py`
- Test: `tests/test_dq_pipeline.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dq_pipeline.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dq_pipeline.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'eightball.dq.pipeline'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/eightball/dq/pipeline.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dq_pipeline.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add src/eightball/dq/pipeline.py tests/test_dq_pipeline.py
git commit -m "feat: pure DQ pipeline — validate + transform, never raises (DLQ decision)"
```

---

## Task 2: Resilient DQ app loop with DLQ + structured logging

**Files:**
- Modify: `apps/dq_app.py` (replace the loop body)

- [ ] **Step 1: Replace `apps/dq_app.py` with the resilient version**

```python
"""Consume raw events, validate + apply the generic DQ rule set, produce clean
events. Failures never crash the loop: they are routed to a dead-letter topic
(events.dlq) with a reason, logged, and counted.

This stays a *pure column transformer* (uppercase, id->name). It does NOT enrich
match/purchase with country -- that is Spark's job (ADR-0001). Error-handling and
reprocessing design: ADR-0009.
"""
import json
import logging
import os
import time

from kafka import KafkaConsumer, KafkaProducer

from eightball.dq.pipeline import process_event

# In Docker, services set KAFKA_BOOTSTRAP=kafka:9092 (internal listener).
# On the host, the default uses the external listener on localhost:29092.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
IN, OUT, DLQ = "events.raw", "events.clean", "events.dlq"
LOG_EVERY = 500                                   # counter cadence

logging.basicConfig(level=logging.INFO,
                    format='{"lvl":"%(levelname)s","evt":"dq","msg":%(message)s}')
log = logging.getLogger("dq")


def _dead_letter(producer, reason, original):
    record = {"reason": reason, "original": original,
              "failed_at": int(time.time() * 1000)}
    producer.send(DLQ, record)
    log.warning(json.dumps({"action": "dead_letter", "reason": reason}))


def run():
    consumer = KafkaConsumer(
        IN, bootstrap_servers=BOOTSTRAP, group_id="dq-app",
        auto_offset_reset="earliest",
        # Raw bytes: we decode inside the loop so a bad payload becomes a DLQ
        # record instead of crashing the consumer's deserializer.
        value_deserializer=lambda b: b,
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    counts = {"processed": 0, "clean": 0, "dead_letter": 0}
    for msg in consumer:                          # at-least-once: see ADR-0002
        counts["processed"] += 1
        # 1. Decode -> bad JSON is a dead letter, not a crash.
        try:
            raw = json.loads(msg.value.decode("utf-8"))
        except Exception as e:                    # noqa: BLE001
            _dead_letter(producer, f"decode: {e}",
                         msg.value.decode("utf-8", errors="replace"))
            counts["dead_letter"] += 1
            continue
        # 2. Validate + transform via the pure pipeline.
        result = process_event(raw)
        if result.status == "ok":
            producer.send(OUT, result.event)
            counts["clean"] += 1
        else:
            _dead_letter(producer, result.reason, result.event)
            counts["dead_letter"] += 1
        # 3. Periodic counter line for observability.
        if counts["processed"] % LOG_EVERY == 0:
            log.info(json.dumps({"action": "progress", **counts}))


if __name__ == "__main__":
    run()
```

- [ ] **Step 2: Verify the unit suite still passes (logic unchanged, wrapper only)**

Run: `python -m pytest -q`
Expected: PASS (existing tests green; no Kafka needed)

- [ ] **Step 3: Commit**

```bash
git add apps/dq_app.py
git commit -m "feat: resilient DQ loop — DLQ routing, structured logs, counters"
```

---

## Task 3: Live verification — DLQ receives bad data, clean is unaffected

**Files:** none (manual verification against the running stack)

- [ ] **Step 1: Bring up Kafka + DQ**

Run: `docker compose up -d kafka dq`
Wait for kafka healthy (`docker inspect -f '{{.State.Health.Status}}' kafka`).

- [ ] **Step 2: Produce a mix of good and bad events to events.raw**

Run (from host, external listener):
```bash
python - <<'PY'
import json
from kafka import KafkaProducer
p = KafkaProducer(bootstrap_servers="localhost:29092",
                  value_serializer=lambda v: v.encode() if isinstance(v, str) else json.dumps(v).encode())
# good init
p.send("events.raw", {"event-type":"init","time":1,"user-id":"u1","country":"1","platform":"ios"})
# schema-invalid: missing country
p.send("events.raw", {"event-type":"init","time":1,"user-id":"u1","platform":"ios"})
# unknown event-type
p.send("events.raw", {"event-type":"logout"})
# malformed JSON (raw string)
p.send("events.raw", "{not valid json")
p.flush(); p.close(); print("sent 4 (1 good, 3 bad)")
PY
```

- [ ] **Step 3: Assert events.clean has the 1 good event**

Run:
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic events.clean --from-beginning --timeout-ms 5000
```
Expected: exactly one record, `platform":"IOS"`, `country_name":"Portugal"`.

- [ ] **Step 4: Assert events.dlq has the 3 bad events with reasons**

Run:
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic events.dlq --from-beginning --timeout-ms 5000
```
Expected: three records with `"reason"` starting `schema:` (missing country),
`schema:` (unknown event-type), and `decode:` (malformed JSON); each keeps its
`original` payload.

- [ ] **Step 5: Confirm the DQ service is still alive (did not crash)**

Run: `docker compose ps dq`
Expected: `running`. Then `docker compose logs dq | tail` shows the dead_letter
warnings. Tear down: `docker compose down -v`.

---

## Task 4: ADR-0009 + README + walkthrough

**Files:**
- Create: `docs/decisions/0009-dq-error-handling.md`
- Modify: `README.md` (add a "Data-quality failure handling" subsection)
- Modify: `docs/specs/2026-06-23-8ballpool-pipeline-design.md` (ADR table row + component note)

- [ ] **Step 1: Write `docs/decisions/0009-dq-error-handling.md`**

Content: Context (DQ had no error handling — bad data crashed the loop or passed
silently); Decision (boundary validation; failures → `events.dlq` with
`{reason, original, failed_at}`; never crash; structured logs + counters; DLQ is
the reprocessing path); Consequences (resilient, observable, replayable; at-least-once
means a DLQ record may be re-emitted on restart — acceptable, dedupe on replay);
Alternatives rejected (crash-and-alert; log-only without a topic; skip-silently).

- [ ] **Step 2: Add a README subsection "Data-quality failure handling"**

Cover: the failure taxonomy table, the `events.dlq` record shape, and the
reprocessing flow (consume DLQ → fix rule/producer → replay to `events.raw`).
Link ADR-0009.

- [ ] **Step 3: Add ADR-0009 row to the spec's §5 table and a note in §4.2**

Row: `0009 | DQ error handling: validate at boundary, dead-letter failures, never crash | ...`
§4.2 note: "Failures are validated/caught and routed to `events.dlq`; see ADR-0009."

- [ ] **Step 4: Commit**

```bash
git add docs/decisions/0009-dq-error-handling.md README.md docs/specs/2026-06-23-8ballpool-pipeline-design.md
git commit -m "docs: ADR-0009 DQ error handling + DLQ reprocessing; README + spec sync"
```

---

## Task 5: (Optional) Automated DLQ test in the unit suite

**Files:**
- Modify: `tests/test_dq_pipeline.py` (add a batch-level assertion)

Only if it adds value beyond Task 1's cases — a small test that a list of mixed
events partitions correctly into ok/dead_letter, documenting the contract at the
batch level.

- [ ] **Step 1: Add the test**

```python
def test_batch_partitions_into_ok_and_dead_letter():
    from eightball.dq.pipeline import process_event
    events = [
        {"event-type": "init", "time": 1, "user-id": "u1", "country": "1", "platform": "ios"},  # ok
        {"event-type": "logout"},                                                                # dlq
        {"event-type": "init", "time": 1, "user-id": "u2", "platform": "ios"},                   # dlq (no country)
    ]
    results = [process_event(e) for e in events]
    assert [r.status for r in results] == ["ok", "dead_letter", "dead_letter"]
```

- [ ] **Step 2: Run + commit**

Run: `python -m pytest tests/test_dq_pipeline.py -q` → PASS
```bash
git add tests/test_dq_pipeline.py
git commit -m "test: DQ batch partitioning into ok/dead-letter"
```

---

## Self-Review

**Spec coverage:**
- Boundary validation → Task 1 (`process_event` validates first). ✅
- Never crash → Task 1 (catches all) + Task 2 (decode try/except). ✅
- DLQ topic with reason + original → Task 2 (`_dead_letter`), verified Task 3. ✅
- Structured logs + counters → Task 2. ✅
- Reprocessing path → documented Task 4 (DLQ = replay source). ✅
- ADR-0009 + README + spec sync → Task 4. ✅

**Placeholder scan:** none — every step has concrete code/commands.

**Type consistency:** `DQResult(status, event, reason)` defined in Task 1 and used
identically in Task 2 (`result.status`, `result.event`, `result.reason`).
`process_event` signature consistent across Tasks 1, 2, 5. Topic name `events.dlq`
consistent across Tasks 2–4.

**Watch during execution:**
- The transform-error test (Task 1) monkeypatches `pipeline.rules_for`; ensure the
  patch targets the name as imported into `pipeline` (it imports `rules_for` into
  its own namespace, so patch `eightball.dq.pipeline.rules_for`). The test does this.
- `events.dlq` relies on Kafka auto-create (already enabled) — no compose change.

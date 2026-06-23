# Final Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Three high-ROI improvements without over-engineering: (1) document the streaming restart / no-event-id correctness limitation honestly, (2) add a lint gate to CI, (3) add a runnable DLQ replay tool so "reprocessing" is demonstrable.

**Architecture:** Doc-only for (1). For (2), a `ruff check` CI job + `make lint`. For (3), a pure `extract_original(dlq_record)` (TDD) + a thin `apps/replay_dlq.py` Kafka loop that reads `events.dlq` and re-produces originals to `events.raw`.

**Tech Stack:** ruff (lint), existing kafka-python, pytest.

---

## Task 1: Document the restart / exactly-once limitation (ADR-0008 + README + walkthrough)

**Files:**
- Modify: `docs/decisions/0008-streaming-mechanics.md`
- Modify: `README.md` (the "at 100x scale" section)

- [ ] **Step 1: Append a "Known limitation" subsection to ADR-0008**

Add, before "Alternatives rejected":
```markdown
## Known limitation — restart & exactly-once

The enriched events are appended (`mode("append")`) to `_enriched_*` outside the
Kafka checkpoint transaction. If the job dies between the append and the offset
commit, a restart replays those offsets and **re-appends** the same enriched
events, inflating the recomputed aggregates. True exactly-once is not achievable
here because **the events carry no unique id** — there is nothing to deduplicate
on. Closing this would require either (a) a producer-assigned event id + a dedupe
on the enriched store, or (b) native stateful streaming with Spark's
checkpointed state and idempotent sinks (the documented upgrade path). For this
self-contained demo it is at-least-once, and the limitation is documented rather
than hidden.
```

- [ ] **Step 2: Add a bullet to README "What I'd do at 100× scale"**

```markdown
- **Exactly-once / restart safety.** Enriched events are appended outside the
  checkpoint transaction, so a mid-batch restart can double-count; events have no
  unique id to dedupe on. Production fix: producer-assigned event id + idempotent
  sink, or native checkpointed stateful aggregation (ADR-0008).
```

- [ ] **Step 3: Commit**
```bash
git add docs/decisions/0008-streaming-mechanics.md README.md
git commit -m "docs: document streaming restart/exactly-once limitation (no event id) in ADR-0008"
```

---

## Task 2: Lint gate in CI (ruff)

**Files:**
- Modify: `requirements-dev.txt`, `.github/workflows/ci.yml`, `Makefile`
- Create: `ruff.toml`

- [ ] **Step 1: Add ruff to dev deps**

Append to `requirements-dev.txt`:
```
ruff==0.5.0
```

- [ ] **Step 2: Write `ruff.toml` (lenient, sensible)**
```toml
line-length = 100
target-version = "py311"

[lint]
select = ["E", "F", "I"]   # pycodestyle errors, pyflakes, import sorting
```

- [ ] **Step 3: Add `make lint` target** (append to Makefile)
```makefile
lint:        ; ruff check src apps tests
```

- [ ] **Step 4: Run ruff locally and FIX every finding**

Run: `./.venv/bin/ruff check src apps tests`
Expected: fix all reported issues (unused imports, import order, line length) until
it reports "All checks passed!". Re-run the unit suite after fixes:
`./.venv/bin/python -m pytest -q` → still green.

- [ ] **Step 5: Add a `lint` job to `.github/workflows/ci.yml`**

Insert as the first job (before unit-tests):
```yaml
  lint:
    name: Lint (ruff)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install ruff==0.5.0
      - run: ruff check src apps tests
```

- [ ] **Step 6: Commit**
```bash
git add requirements-dev.txt ruff.toml Makefile .github/workflows/ci.yml
git commit -m "ci: add ruff lint gate (+ make lint); fix lint findings"
```

---

## Task 3: DLQ replay tool

**Files:**
- Create: `src/eightball/dq/replay.py` (pure `extract_original`)
- Create: `apps/replay_dlq.py` (thin Kafka loop)
- Create: `tests/test_replay.py`
- Modify: `Makefile` (add `replay-dlq` target), `README.md` (replay how-to)

- [ ] **Step 1: Write the failing test** (`tests/test_replay.py`)
```python
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
```

- [ ] **Step 2: Run test, verify it fails**
Run: `./.venv/bin/python -m pytest tests/test_replay.py -v`
Expected: FAIL (ModuleNotFoundError).

- [ ] **Step 3: Implement `src/eightball/dq/replay.py`**
```python
"""Pure helper for DLQ replay: pull the original payload out of a dead-letter
record so it can be re-produced to events.raw after the fix."""


def extract_original(dlq_record: dict):
    """Return the original payload from a dead-letter record, or None if absent."""
    return dlq_record.get("original")
```

- [ ] **Step 4: Run test, verify pass**
Run: `./.venv/bin/python -m pytest tests/test_replay.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Implement `apps/replay_dlq.py`**
```python
"""Replay dead-lettered events back onto events.raw.

The DLQ is the reprocessing path (ADR-0009): after fixing the offending rule or
upstream producer, run this to re-feed the original payloads through the pipeline.
Reads events.dlq once (until idle), re-produces each original to events.raw.
Skips records whose original is a non-JSON string (unfixable without manual edit).
"""
import json
import logging
import os

from kafka import KafkaConsumer, KafkaProducer

from eightball.dq.replay import extract_original

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
DLQ, RAW = "events.dlq", "events.raw"

logging.basicConfig(level=logging.INFO, format='{"evt":"replay","msg":%(message)s}')
log = logging.getLogger("replay")


def run(idle_ms: int = 5000):
    consumer = KafkaConsumer(
        DLQ, bootstrap_servers=BOOTSTRAP, group_id="dlq-replay",
        auto_offset_reset="earliest", consumer_timeout_ms=idle_ms,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    replayed = skipped = 0
    for msg in consumer:
        original = extract_original(msg.value)
        if isinstance(original, dict):           # only structured payloads are replayable
            producer.send(RAW, original)
            replayed += 1
        else:
            skipped += 1                          # e.g. malformed-JSON string originals
    producer.flush()
    log.info(json.dumps({"replayed": replayed, "skipped": skipped}))


if __name__ == "__main__":
    run()
```

- [ ] **Step 6: Add `make replay-dlq` target** (append to Makefile)
```makefile
replay-dlq:  ; KAFKA_BOOTSTRAP=localhost:29092 python apps/replay_dlq.py
```

- [ ] **Step 7: Add a "Replaying the DLQ" note to README** (under the DQ failure section)
```markdown
**Replay in practice:** after fixing the rule/producer, run `make replay-dlq`
(or `python apps/replay_dlq.py`) to re-feed the dead-lettered `original` payloads
onto `events.raw`. Structured payloads are replayed; malformed-JSON originals are
skipped (they need a manual fix). The pure extraction is in
`src/eightball/dq/replay.py`.
```

- [ ] **Step 8: Run full suite + lint, then commit**
Run: `./.venv/bin/python -m pytest -q` → green; `./.venv/bin/ruff check src apps tests` → clean
```bash
git add src/eightball/dq/replay.py apps/replay_dlq.py tests/test_replay.py Makefile README.md
git commit -m "feat: DLQ replay tool (extract_original + replay_dlq app) — reprocessing made runnable"
```

---

## Task 4: Final verification

- [ ] **Step 1: Full suite + lint green**
Run: `./.venv/bin/python -m pytest -q` and `./.venv/bin/ruff check src apps tests`

- [ ] **Step 2: Push and confirm CI (lint + unit + e2e all green)**
```bash
git push origin main
```
Watch the run; expect three green jobs.

---

## Self-Review

- Restart/exactly-once honesty → Task 1 (ADR-0008 + README). ✅
- Lint gate → Task 2 (ruff job + make lint + findings fixed). ✅
- DLQ replay runnable → Task 3 (pure `extract_original` TDD + `replay_dlq.py` + make + README). ✅
- No placeholders; `extract_original` signature consistent across replay.py, app, tests.
- Proportionality: deliberately NO prometheus/mypy/format-rewrite/transactional-kafka.

**Watch during execution:** ruff may flag the existing `# noqa: BLE001` as unused
(RUF100 is not in the selected rules, so it won't) and import ordering in existing
files — fix in Task 2 Step 4 and re-run pytest to confirm no behavioural change.

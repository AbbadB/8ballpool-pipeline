# 8 Ball Pool Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained, real-time 8 Ball Pool telemetry pipeline (Kafka + Spark) that ingests `init`/`match`/`in-app-purchase` events, applies generic data-quality transforms, and emits daily and per-minute aggregates — all three brief tiers.

**Architecture:** Python producer → Kafka `events.raw` → generic DQ rule engine → Kafka `events.clean` → Spark batch (daily distinct users) + Spark Structured Streaming (per-minute aggregates with country enrichment via a maintained user dimension). One `docker compose up`.

**Tech Stack:** Python 3.11, `kafka-python`, `jsonschema` (Draft 3), PySpark 3.5 (Structured Streaming + `spark-sql-kafka`), Docker Compose (Kafka in KRaft mode, Spark), pytest.

**Working method (ADR-0007 harness):** spec-first, TDD control loop, human owns decisions, small reviewable steps, verify-before-done, grounded in real schemas, provenance honesty. A `CLAUDE.md` + a `.claude/settings.json` test-gate hook enforce it.

**Learning sequence (Kafka is new to the author):** Phase 2 introduces topics/partitions/offsets/consumer-groups/delivery-semantics as they're first used; each is annotated in code comments + the relevant ADR.

---

## File Structure

```
8ballpool-pipeline/
  CLAUDE.md                       # agentic-dev harness (7 rules, conventions, DoD)
  .claude/settings.json           # test-gate hook
  docker-compose.yml              # kafka (KRaft) + producer + dq + spark services
  Makefile                        # up / demo / test / down
  requirements.txt                # runtime deps
  requirements-dev.txt            # pyspark, pytest, jsonschema (for local tests)
  README.md                       # run guide, trade-offs, at-100x, schema notes, AI disclosure
  schemas/                        # provided draft-03 JSON schemas (already present)
  docs/
    specs/2026-06-23-8ballpool-pipeline-design.md
    plans/2026-06-23-8ballpool-pipeline-plan.md   # this file
    decisions/                    # ADR-0001 … 0008
  src/eightball/
    __init__.py
    schemas.py                    # load + validate events against draft-03 schemas
    events.py                     # event factories (valid dict generators)
    dq/
      __init__.py
      rules.py                    # generic rule engine (pure): uppercase, map_id_to_name
      config.py                   # declarative rule set + country lookup
    aggregations/
      __init__.py
      daily.py                    # pure: daily distinct users by country+platform
      minute.py                   # pure: enrichment + per-minute aggregates
  apps/
    producer.py                   # event factories -> validate -> Kafka events.raw
    dq_app.py                     # consume events.raw -> rules -> events.clean
    spark_batch.py                # daily aggregation job
    spark_streaming.py            # foreachBatch streaming aggregator
  tests/
    test_schemas.py
    test_events.py
    test_dq_rules.py
    test_daily.py
    test_minute.py
    test_smoke_e2e.py             # docker-compose integration smoke
```

---

## Phase 0 — Harness & scaffolding

### Task 0: Project skeleton, harness, and tooling

**Files:**
- Create: `requirements.txt`, `requirements-dev.txt`, `CLAUDE.md`, `.claude/settings.json`, `Makefile`, `src/eightball/__init__.py`, `tests/__init__.py`, `pytest.ini`

- [ ] **Step 1: Write `requirements.txt`**
```
kafka-python==2.0.2
jsonschema==4.22.0
```

- [ ] **Step 2: Write `requirements-dev.txt`**
```
-r requirements.txt
pyspark==3.5.1
pytest==8.2.0
```

- [ ] **Step 3: Write `pytest.ini`**
```ini
[pytest]
pythonpath = src
testpaths = tests
addopts = -q
```

- [ ] **Step 4: Write `CLAUDE.md`** (the harness — 7 rules)
```markdown
# 8 Ball Pool Pipeline — Engineering Conventions (Agentic Harness)

This project is built with an AI agent as a pair-programmer under these rules.
See `docs/decisions/0007-agentic-development.md`.

## The 7 rules
1. **Spec-first.** No code without an approved spec/plan. Architecture decisions
   are the human's; record them as ADRs in `docs/decisions/`.
2. **TDD control loop.** For every unit: write a failing test → run it (see it
   fail) → minimal implementation → run it (see it pass) → commit. No
   implementation before a red test.
3. **Human owns decisions; AI proposes options.** Surface 2-3 approaches with
   trade-offs; the human chooses; record rejected alternatives in the ADR.
4. **Small, reviewable increments.** One unit per commit. No large unreviewed diffs.
5. **Verify before "done."** Never claim passing without showing test output.
6. **Grounded in real artifacts.** Work from `schemas/` and the spec, not assumptions.
7. **Provenance honesty.** Comments/ADRs reflect real reasoning; AI use is disclosed.

## Conventions
- Python 3.11, `src/` layout, tests in `tests/`, `pytest` from repo root.
- Pure logic (DQ rules, aggregations) lives in `src/eightball/` and is unit-tested
  with no Kafka/Spark running. Kafka/Spark wrappers live in `apps/` and stay thin.
- Field names follow the schemas exactly (hyphenated keys: `event-type`, `user-id`).

## Definition of done (per task)
- New behavior has a test; `pytest` is green; the change is committed.
- If a decision was made, its ADR exists.
```

- [ ] **Step 5: Write `.claude/settings.json`** (test-gate hook — mechanizes Rule 5)
```json
{
  "hooks": {
    "Stop": [
      {
        "matcher": "",
        "hooks": [
          { "type": "command", "command": "cd \"$CLAUDE_PROJECT_DIR\" && python -m pytest -q 2>&1 | tail -n 20" }
        ]
      }
    ]
  }
}
```

- [ ] **Step 6: Write `Makefile`**
```makefile
.PHONY: test up down demo logs
test:        ; python -m pytest -q
up:          ; docker compose up -d --build
down:        ; docker compose down -v
demo:        ; docker compose up --build
logs:        ; docker compose logs -f
```

- [ ] **Step 7: Create empty `src/eightball/__init__.py` and `tests/__init__.py`**

- [ ] **Step 8: Verify pytest runs (no tests yet, exit 0 or "no tests ran")**

Run: `python -m pytest -q`
Expected: "no tests ran" (exit 5) — acceptable; confirms discovery works.

- [ ] **Step 9: Write `docs/decisions/_template.md`**
```markdown
# ADR-NNNN: <title>
**Status:** Accepted · **Date:** 2026-06-2x
## Context
## Decision
## Consequences
## Alternatives rejected
```

- [ ] **Step 10: Write `docs/decisions/0007-agentic-development.md`** (full text from the design spec §1 harness + the "minimal tooling" proportionality decision).

- [ ] **Step 11: Commit**
```bash
git add -A
git commit -m "chore: project skeleton, agentic harness (CLAUDE.md, hook), tooling"
```

---

## Phase 1 — Domain core (pure, no infra)

### Task 1: Schema validation

**Files:**
- Create: `src/eightball/schemas.py`, `tests/test_schemas.py`

- [ ] **Step 1: Write the failing test** (`tests/test_schemas.py`)
```python
import pytest
from eightball.schemas import validate_event, SchemaError

VALID_INIT = {"event-type": "init", "time": 1719100000000,
              "user-id": "u1", "country": "1", "platform": "ios"}

def test_valid_init_passes():
    validate_event(VALID_INIT)  # should not raise

def test_missing_required_field_fails():
    bad = dict(VALID_INIT); del bad["country"]
    with pytest.raises(SchemaError):
        validate_event(bad)

def test_unknown_event_type_fails():
    bad = dict(VALID_INIT); bad["event-type"] = "logout"
    with pytest.raises(SchemaError):
        validate_event(bad)
```

- [ ] **Step 2: Run test, verify it fails**
Run: `python -m pytest tests/test_schemas.py -v`
Expected: FAIL (ModuleNotFoundError / ImportError: SchemaError).

- [ ] **Step 3: Implement `src/eightball/schemas.py`**
```python
"""Validate events against the provided draft-03 JSON schemas.

The schemas live in `schemas/` and use draft-03 semantics (property-level
`"required": true`). We pick the schema by the event's `event-type` and
validate with jsonschema's Draft3Validator.
"""
import json
from pathlib import Path
from jsonschema import Draft3Validator

_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"
_FILES = {"init": "init.json", "match": "match.json",
          "in-app-purchase": "in-app-purchase.json"}

class SchemaError(ValueError):
    """Raised when an event does not conform to its schema."""

def _load(event_type: str) -> dict:
    fname = _FILES.get(event_type)
    if fname is None:
        raise SchemaError(f"unknown event-type: {event_type!r}")
    return json.loads((_SCHEMA_DIR / fname).read_text())

def validate_event(event: dict) -> None:
    """Raise SchemaError if `event` is invalid for its declared event-type."""
    schema = _load(event.get("event-type"))
    errors = sorted(Draft3Validator(schema).iter_errors(event), key=str)
    if errors:
        raise SchemaError("; ".join(e.message for e in errors))
```

- [ ] **Step 4: Run tests, verify pass**
Run: `python -m pytest tests/test_schemas.py -v`
Expected: PASS (3 passed). If Draft3 rejects the root `"required": true`, add a
test-confirmed shim: strip the top-level `"required"` key in `_load` before
validating (document the draft-03 quirk in a code comment).

- [ ] **Step 5: Commit**
```bash
git add src/eightball/schemas.py tests/test_schemas.py
git commit -m "feat: draft-03 schema validation for game events"
```

---

### Task 2: Event factories

**Files:**
- Create: `src/eightball/events.py`, `tests/test_events.py`

Factories produce *valid* event dicts. `country` is emitted as an **id string**
and `platform` **lowercase** so the DQ layer has real work (ADR-0003).

- [ ] **Step 1: Write the failing test** (`tests/test_events.py`)
```python
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
```

- [ ] **Step 2: Run test, verify it fails**
Run: `python -m pytest tests/test_events.py -v`
Expected: FAIL (ImportError).

- [ ] **Step 3: Implement `src/eightball/events.py`**
```python
"""Factories that build schema-valid 8 Ball Pool events.

`country` is emitted as an id string and `platform` lowercase on purpose, so the
downstream DQ rules (uppercase, id->name) have real work to do. See ADR-0003.
"""

def make_init(*, user_id: str, country_id: str, platform: str, time_ms: int) -> dict:
    return {"event-type": "init", "time": time_ms, "user-id": user_id,
            "country": country_id, "platform": platform}

def _postmatch(*, coins: int, level: int, device: str, platform: str) -> dict:
    return {"coin-balance-after-match": coins, "level-after-match": level,
            "device": device, "platform": platform}

def make_match(*, user_a: str, user_b: str, winner: str, time_ms: int,
               game_tier: int = 5, duration: int = 120,
               platform: str = "ios") -> dict:
    return {
        "event-type": "match", "time": time_ms,
        "user-a": user_a, "user-b": user_b, "winner": winner,
        "user-a-postmatch-info": _postmatch(coins=100, level=3,
                                            device="iphone", platform=platform),
        "user-b-postmatch-info": _postmatch(coins=80, level=2,
                                            device="android", platform=platform),
        "game-tier": game_tier, "duration": duration,
    }

def make_purchase(*, user_id: str, value: float, product_id: str,
                  time_ms: int) -> dict:
    return {"event-type": "in-app-purchase", "time": time_ms,
            "purchase_value": value, "user-id": user_id, "product-id": product_id}
```

- [ ] **Step 4: Run tests, verify pass**
Run: `python -m pytest tests/test_events.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**
```bash
git add src/eightball/events.py tests/test_events.py
git commit -m "feat: schema-valid event factories (country as id, lowercase platform)"
```

---

### Task 3: Generic DQ rule engine

**Files:**
- Create: `src/eightball/dq/__init__.py`, `src/eightball/dq/rules.py`, `src/eightball/dq/config.py`, `tests/test_dq_rules.py`
- Create: `docs/decisions/0003-country-as-id.md`

The engine is the spec's "generic, extensible" requirement. Rules are declarative
`(transform, field_path)` entries; `field_path` is dotted to reach nested fields
(e.g. `user-a-postmatch-info.platform`).

- [ ] **Step 1: Write the failing test** (`tests/test_dq_rules.py`)
```python
from eightball.dq.rules import apply_rules, Rule, uppercase, map_id_to_name

LOOKUP = {"1": "Portugal", "2": "Brazil"}

def test_uppercase_top_level():
    out = apply_rules({"platform": "ios"}, [Rule(uppercase, "platform")])
    assert out["platform"] == "IOS"

def test_map_id_to_name_adds_resolved_field():
    rule = Rule(map_id_to_name, "country", params={"lookup": LOOKUP,
                                                    "target": "country_name"})
    out = apply_rules({"country": "1"}, [rule])
    assert out["country_name"] == "Portugal"

def test_nested_field_path():
    e = {"user-a-postmatch-info": {"platform": "android"}}
    out = apply_rules(e, [Rule(uppercase, "user-a-postmatch-info.platform")])
    assert out["user-a-postmatch-info"]["platform"] == "ANDROID"

def test_unknown_id_maps_to_unknown():
    rule = Rule(map_id_to_name, "country", params={"lookup": LOOKUP,
                                                    "target": "country_name"})
    out = apply_rules({"country": "99"}, [rule])
    assert out["country_name"] == "UNKNOWN"

def test_missing_field_is_skipped_not_errored():
    out = apply_rules({"event-type": "init"}, [Rule(uppercase, "platform")])
    assert out == {"event-type": "init"}

def test_extensibility_new_rule_no_engine_change():
    # adding a rule is data, not code:
    def reverse(v, **_): return v[::-1]
    out = apply_rules({"x": "abc"}, [Rule(reverse, "x")])
    assert out["x"] == "cba"
```

- [ ] **Step 2: Run test, verify it fails**
Run: `python -m pytest tests/test_dq_rules.py -v`
Expected: FAIL (ImportError).

- [ ] **Step 3: Implement `src/eightball/dq/rules.py`**
```python
"""Generic, extensible data-quality rule engine.

A Rule binds a pure transform function to a dotted field path. `apply_rules`
walks the rules in order over a copy of the event. Adding a field or a new
transform is a config change (a new Rule), never an engine change — which is the
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
    # returns a (write_path_override, resolved) signal via params['target']
    return lookup.get(value, "UNKNOWN")

def _get(obj, parts):
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
        parts = rule.path.split(".")
        parent, key = _get(out, parts)
        if parent is None:
            continue  # field absent — skip, do not error
        value = parent[key]
        result = rule.transform(value, **rule.params)
        target = rule.params.get("target")
        if target:                       # write resolved value to a new field
            parent[target] = result
        else:                            # in-place transform
            parent[key] = result
    return out
```

- [ ] **Step 4: Run tests, verify pass**
Run: `python -m pytest tests/test_dq_rules.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Implement `src/eightball/dq/config.py`** (the declarative rule set)
```python
"""Declarative DQ configuration: the country lookup and the rule set applied to
each event-type. This is the "extensible" surface — add a Rule here, no engine
change."""
from eightball.dq.rules import Rule, uppercase, map_id_to_name

COUNTRY_LOOKUP = {"1": "Portugal", "2": "Brazil", "3": "United Kingdom",
                  "4": "Germany", "5": "United States"}

def _country_rule():
    return Rule(map_id_to_name, "country",
                params={"lookup": COUNTRY_LOOKUP, "target": "country_name"})

RULES_BY_TYPE = {
    "init": [uppercase_platform := Rule(uppercase, "platform"), _country_rule()],
    "match": [Rule(uppercase, "user-a-postmatch-info.platform"),
              Rule(uppercase, "user-b-postmatch-info.platform")],
    "in-app-purchase": [],
}

def rules_for(event_type: str) -> list[Rule]:
    return RULES_BY_TYPE.get(event_type, [])
```

- [ ] **Step 6: Add test for config wiring** (append to `tests/test_dq_rules.py`)
```python
from eightball.dq.config import rules_for
from eightball.dq.rules import apply_rules as _apply

def test_init_config_uppercases_and_maps_country():
    e = {"event-type": "init", "platform": "ios", "country": "2"}
    out = _apply(e, rules_for("init"))
    assert out["platform"] == "IOS"
    assert out["country_name"] == "Brazil"
```

- [ ] **Step 7: Run tests, verify pass**
Run: `python -m pytest tests/test_dq_rules.py -v`
Expected: PASS (7 passed).

- [ ] **Step 8: Write `docs/decisions/0003-country-as-id.md`** (context: schema field is `country` string but DQ brief says `country_id`→name; decision: producer emits id, DQ resolves; consequence: README assumption note; alternatives rejected: emit names + no-op rule).

- [ ] **Step 9: Commit**
```bash
git add src/eightball/dq tests/test_dq_rules.py docs/decisions/0003-country-as-id.md
git commit -m "feat: generic extensible DQ rule engine + country id->name config (ADR-0003)"
```

---

## Phase 2 — Kafka integration (Beginner tier)

> **Learning notes (annotate in code comments as you go):** a *topic* is a named
> log; *partitions* are its parallel shards; an *offset* is a message's position;
> a *consumer group* splits partitions across members and tracks committed
> offsets. We rely on Kafka auto-creating topics (brief says defaults are fine)
> and at-least-once delivery (see ADR-0002 consequences).

### Task 4: Docker Compose — Kafka (KRaft) + smoke

**Files:**
- Create: `docker-compose.yml` (kafka service only for now)
- Create: `docs/decisions/0002-runtime-docker-compose.md`

- [ ] **Step 1: Write `docker-compose.yml` (kafka service)**
```yaml
services:
  kafka:
    image: apache/kafka:3.7.1
    container_name: kafka
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 10
```

- [ ] **Step 2: Bring Kafka up and verify**
Run: `docker compose up -d kafka && sleep 20 && docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`
Expected: command succeeds (empty list is fine).

- [ ] **Step 3: Manual produce/consume smoke (learning step)**
Run (produce): `echo '{"hello":"kafka"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic smoke`
Run (consume): `docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic smoke --from-beginning --timeout-ms 5000`
Expected: the JSON line is read back. Then `docker compose down`.

- [ ] **Step 4: Write `docs/decisions/0002-runtime-docker-compose.md`** (real Kafka+Spark over embedded; KRaft over ZooKeeper; consequences: at-least-once, single broker for demo; alternatives rejected: embedded runtime).

- [ ] **Step 5: Commit**
```bash
git add docker-compose.yml docs/decisions/0002-runtime-docker-compose.md
git commit -m "feat: Kafka (KRaft) in docker-compose + ADR-0002"
```

### Task 5: Producer app

**Files:**
- Create: `apps/producer.py`
- Test: covered by Task 1/2 (factories validated); add a serialization test `tests/test_events.py::test_event_json_roundtrip`

- [ ] **Step 1: Write the failing test** (append to `tests/test_events.py`)
```python
import json
from eightball.events import make_init
def test_event_json_roundtrip():
    e = make_init(user_id="u1", country_id="1", platform="ios", time_ms=1)
    assert json.loads(json.dumps(e)) == e
```

- [ ] **Step 2: Run, verify pass** (factories already serialize cleanly)
Run: `python -m pytest tests/test_events.py::test_event_json_roundtrip -v`
Expected: PASS.

- [ ] **Step 3: Implement `apps/producer.py`**
```python
"""Synthetic 8 Ball Pool event producer.

Guarantees `init` is sent before any match/purchase for a user (brief: init is
the first event of all). Validates each event against its schema before sending.
Publishes JSON to topic `events.raw`. Bootstrap server from $KAFKA_BOOTSTRAP.
"""
import json, os, random, time
from kafka import KafkaProducer
from eightball.events import make_init, make_match, make_purchase
from eightball.schemas import validate_event

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "events.raw"
COUNTRIES = ["1", "2", "3", "4", "5"]
PLATFORMS = ["ios", "android", "web"]
PRODUCTS = ["coins_100", "coins_500", "cue_gold", "spin_pack"]

def now_ms() -> int:
    return int(time.time() * 1000)

def send(producer, event):
    validate_event(event)                      # fail fast on bad data
    producer.send(TOPIC, event)

def run(n_users=20, rate_per_sec=10):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    seen = set()
    while True:
        uid = f"u{random.randint(1, n_users)}"
        if uid not in seen:                    # init must come first
            send(producer, make_init(user_id=uid, country_id=random.choice(COUNTRIES),
                                     platform=random.choice(PLATFORMS), time_ms=now_ms()))
            seen.add(uid)
            continue
        roll = random.random()
        if roll < 0.5:
            other = f"u{random.randint(1, n_users)}"
            send(producer, make_match(user_a=uid, user_b=other,
                                      winner=random.choice([uid, other]),
                                      time_ms=now_ms(),
                                      platform=random.choice(PLATFORMS)))
        else:
            send(producer, make_purchase(user_id=uid,
                                         value=round(random.uniform(0.99, 49.99), 2),
                                         product_id=random.choice(PRODUCTS),
                                         time_ms=now_ms()))
        time.sleep(1.0 / rate_per_sec)

if __name__ == "__main__":
    run()
```

- [ ] **Step 4: Commit**
```bash
git add apps/producer.py tests/test_events.py
git commit -m "feat: synthetic event producer (init-first, schema-validated) -> events.raw"
```

### Task 6: DQ streaming app

**Files:**
- Create: `apps/dq_app.py`
- Create: `docs/decisions/0001-enrichment-in-spark.md`

- [ ] **Step 1: Implement `apps/dq_app.py`** (logic already tested in Task 3; this is a thin Kafka loop)
```python
"""Consume raw events, apply the generic DQ rule set, produce clean events.

This is intentionally a *pure column transformer* (uppercase, id->name). It does
NOT do the country enrichment join for match/purchase — that is relational work
left to Spark (ADR-0001). Consumes `events.raw`, produces `events.clean`.
"""
import json, os
from kafka import KafkaConsumer, KafkaProducer
from eightball.dq.rules import apply_rules
from eightball.dq.config import rules_for

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
IN, OUT = "events.raw", "events.clean"

def run():
    consumer = KafkaConsumer(
        IN, bootstrap_servers=BOOTSTRAP, group_id="dq-app",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for msg in consumer:                       # at-least-once: see ADR-0002
        event = msg.value
        clean = apply_rules(event, rules_for(event.get("event-type")))
        producer.send(OUT, clean)

if __name__ == "__main__":
    run()
```

- [ ] **Step 2: Write `docs/decisions/0001-enrichment-in-spark.md`** (the full trade-off analysis we wrote: DQ = generic column transforms; enrichment join = Spark; Python has no native Kafka Streams; alternatives rejected = enrich in DQ via hand-rolled state).

- [ ] **Step 3: Commit**
```bash
git add apps/dq_app.py docs/decisions/0001-enrichment-in-spark.md
git commit -m "feat: DQ streams app (raw->clean, generic transforms) + ADR-0001"
```

---

## Phase 3 — Spark batch (Beginner tier)

### Task 7: Daily distinct users by country + platform

**Files:**
- Create: `src/eightball/aggregations/__init__.py`, `src/eightball/aggregations/daily.py`, `apps/spark_batch.py`, `tests/test_daily.py`

The aggregation is a **pure DataFrame transform** so it's testable with a local
SparkSession and no Kafka. Source = clean `init` events (they carry both
`country_name` and `platform`).

- [ ] **Step 1: Write the failing test** (`tests/test_daily.py`)
```python
import pytest
from pyspark.sql import SparkSession
from eightball.aggregations.daily import daily_distinct_users

@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield s
    s.stop()

def test_daily_distinct_users(spark):
    # two events same user same day -> distinct count 1
    rows = [
        ("init", 1719100000000, "u1", "Portugal", "IOS"),
        ("init", 1719100050000, "u1", "Portugal", "IOS"),
        ("init", 1719100060000, "u2", "Portugal", "IOS"),
        ("init", 1719100070000, "u3", "Brazil",   "ANDROID"),
    ]
    df = spark.createDataFrame(rows,
        ["event-type", "time", "user-id", "country_name", "platform"])
    out = {(r["country_name"], r["platform"], r["distinct_users"])
           for r in daily_distinct_users(df).collect()}
    assert ("Portugal", "IOS", 2) in out
    assert ("Brazil", "ANDROID", 1) in out
```

- [ ] **Step 2: Run test, verify it fails**
Run: `python -m pytest tests/test_daily.py -v`
Expected: FAIL (ImportError).

- [ ] **Step 3: Implement `src/eightball/aggregations/daily.py`**
```python
"""Daily distinct users by country and platform (Beginner tier).

Pure transform: DataFrame of clean init events -> daily aggregate. Event time is
epoch millis (ADR-0006)."""
from pyspark.sql import DataFrame, functions as F

def daily_distinct_users(events: DataFrame) -> DataFrame:
    init = events.filter(F.col("event-type") == "init")
    return (init
            .withColumn("event_date",
                        F.to_date(F.timestamp_millis(F.col("time"))))
            .groupBy("event_date", "country_name", "platform")
            .agg(F.countDistinct("user-id").alias("distinct_users")))
```

- [ ] **Step 4: Run tests, verify pass**
Run: `python -m pytest tests/test_daily.py -v`
Expected: PASS.

- [ ] **Step 5: Implement `apps/spark_batch.py`** (reads clean topic from earliest, runs transform, writes parquet + console)
```python
"""Batch job: read all clean events from Kafka, write daily distinct-user agg."""
import os
from pyspark.sql import SparkSession, functions as F
from eightball.aggregations.daily import daily_distinct_users

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

def main():
    spark = SparkSession.builder.appName("daily-batch").getOrCreate()
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", "events.clean")
           .option("startingOffsets", "earliest").load())
    events = raw.select(F.from_json(F.col("value").cast("string"),
        "`event-type` STRING, `time` LONG, `user-id` STRING, "
        "country_name STRING, platform STRING").alias("e")).select("e.*")
    out = daily_distinct_users(events)
    out.show(truncate=False)
    out.write.mode("overwrite").parquet("/output/daily_distinct_users")

if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Commit**
```bash
git add src/eightball/aggregations tests/test_daily.py apps/spark_batch.py
git commit -m "feat: Spark batch daily distinct users by country+platform (TDD)"
```

---

## Phase 4 — Spark Structured Streaming (Pro tier)

### Task 8: Minute aggregation logic (pure) + enrichment

**Files:**
- Create: `src/eightball/aggregations/minute.py`, `tests/test_minute.py`
- Create: `docs/decisions/0004-match-attribution.md`, `0005-structured-streaming.md`, `0006-event-time.md`, `0008-streaming-mechanics.md`

All metrics are **pure transforms** over enriched DataFrames so they test without
streaming. `enrich(events, user_dim)` left-joins match/purchase rows to the user
dimension (country_name, platform) built from init (ADR-0001); missing dim →
"UNKNOWN" (honest counts).

- [ ] **Step 1: Write the failing test** (`tests/test_minute.py`)
```python
import pytest
from pyspark.sql import SparkSession
from eightball.aggregations.minute import (
    build_user_dim, enrich, minute_purchase_metrics,
    minute_revenue_by_country, minute_matches_by_country)

@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("t").getOrCreate()
    yield s; s.stop()

def _events(spark):
    rows = [
        ("init", 1719100000000, "u1", None, None, "Portugal", "IOS", None, None),
        ("init", 1719100000000, "u2", None, None, "Brazil",   "IOS", None, None),
        ("in-app-purchase", 1719100001000, "u1", 4.99, "p1", None, None, None, None),
        ("in-app-purchase", 1719100002000, "u2", 1.00, "p2", None, None, None, None),
        ("match", 1719100003000, None, None, None, None, None, "u1", "u2"),
    ]
    cols = ["event-type","time","user-id","purchase_value","product-id",
            "country_name","platform","user-a","user-b"]
    return spark.createDataFrame(rows, cols)

def test_build_user_dim(spark):
    dim = {r["user-id"]: r["country_name"] for r in build_user_dim(_events(spark)).collect()}
    assert dim == {"u1": "Portugal", "u2": "Brazil"}

def test_purchase_metrics_per_minute(spark):
    df = _events(spark)
    dim = build_user_dim(df)
    m = minute_purchase_metrics(enrich(df, dim)).collect()
    assert len(m) == 1                          # all in one minute
    row = m[0]
    assert row["purchase_count"] == 2
    assert abs(row["revenue"] - 5.99) < 1e-6
    assert row["distinct_users"] == 2

def test_revenue_by_country(spark):
    df = _events(spark); dim = build_user_dim(df)
    rev = {r["country_name"]: r["revenue"] for r in
           minute_revenue_by_country(enrich(df, dim)).collect()}
    assert abs(rev["Portugal"] - 4.99) < 1e-6
    assert abs(rev["Brazil"] - 1.00) < 1e-6

def test_matches_by_country_counts_both_players(spark):
    df = _events(spark); dim = build_user_dim(df)
    mc = {r["country_name"]: r["matches"] for r in
          minute_matches_by_country(enrich(df, dim)).collect()}
    assert mc["Portugal"] == 1 and mc["Brazil"] == 1   # both players (ADR-0004)
```

- [ ] **Step 2: Run test, verify it fails**
Run: `python -m pytest tests/test_minute.py -v`
Expected: FAIL (ImportError).

- [ ] **Step 3: Implement `src/eightball/aggregations/minute.py`**
```python
"""Per-minute aggregates (Pro tier). All functions are pure DataFrame transforms.

Country enrichment (ADR-0001): match/purchase carry no country, so we join them
to a user dimension built from init events. Matches are attributed to BOTH
players' countries (ADR-0004). Windows are 1-minute tumbling on event-time
(epoch millis -> timestamp, ADR-0006)."""
from pyspark.sql import DataFrame, functions as F

_MIN = F.window(F.col("ts"), "1 minute")

def _with_ts(df: DataFrame) -> DataFrame:
    return df.withColumn("ts", F.timestamp_millis(F.col("time")))

def build_user_dim(events: DataFrame) -> DataFrame:
    return (events.filter(F.col("event-type") == "init")
            .select(F.col("user-id").alias("uid"), "country_name", "platform")
            .dropDuplicates(["uid"]))

def enrich(events: DataFrame, user_dim: DataFrame) -> DataFrame:
    """Attach country_name to purchase (user-id) and match (user-a/user-b)."""
    e = _with_ts(events)
    dim = user_dim
    purchases = (e.filter(F.col("event-type") == "in-app-purchase")
                 .join(dim, e["user-id"] == dim["uid"], "left")
                 .select("ts", "event-type", "purchase_value", "user-id",
                         F.coalesce("country_name", F.lit("UNKNOWN")).alias("country_name")))
    # match -> one row per participating player, each carrying that player's country
    m = e.filter(F.col("event-type") == "match")
    players = (m.select("ts", F.col("user-a").alias("uid")).unionByName(
               m.select("ts", F.col("user-b").alias("uid")))
               .join(dim, "uid", "left")
               .select("ts", F.lit("match").alias("event-type"),
                       F.coalesce("country_name", F.lit("UNKNOWN")).alias("country_name")))
    return purchases, players

def minute_purchase_metrics(enriched) -> DataFrame:
    purchases, _ = enriched
    return (purchases.groupBy(_MIN)
            .agg(F.count("*").alias("purchase_count"),
                 F.sum("purchase_value").alias("revenue"),
                 F.countDistinct("user-id").alias("distinct_users")))

def minute_revenue_by_country(enriched) -> DataFrame:
    purchases, _ = enriched
    return (purchases.groupBy(_MIN, "country_name")
            .agg(F.sum("purchase_value").alias("revenue")))

def minute_matches_by_country(enriched) -> DataFrame:
    _, players = enriched
    return (players.groupBy(_MIN, "country_name")
            .agg(F.count("*").alias("matches")))
```
*Note:* `enrich` returns a `(purchases, players)` tuple; the test calls each
metric with that tuple. Keep the test and signature in sync.

- [ ] **Step 4: Run tests, verify pass**
Run: `python -m pytest tests/test_minute.py -v`
Expected: PASS (4 passed). Fix the window-grouping assertions if the `window`
column changes row counts (the tests assert on metric values, not the window
struct).

- [ ] **Step 5: Write ADRs 0004, 0005, 0006, 0008**
- `0004-match-attribution.md` — both players; consequence: per-country sum ≥ total.
- `0005-structured-streaming.md` — Structured Streaming over DStreams; event-time + watermark.
- `0006-event-time.md` — epoch millis as event-time; watermark for late data.
- `0008-streaming-mechanics.md` — `foreachBatch` + maintained user-dim table (Task 9); why not stream-stream join (watermark/time-bound complexity); at-scale: Delta merge / native windowed agg.

- [ ] **Step 6: Commit**
```bash
git add src/eightball/aggregations/minute.py tests/test_minute.py docs/decisions/000{4,5,6,8}-*.md
git commit -m "feat: pure per-minute aggregates + enrichment (ADR-0004/0005/0006/0008)"
```

### Task 9: Streaming app (foreachBatch)

**Files:**
- Create: `apps/spark_streaming.py`

- [ ] **Step 1: Implement `apps/spark_streaming.py`**
```python
"""Structured Streaming minute aggregator (Pro tier).

Reads `events.clean`, and per micro-batch (foreachBatch, ADR-0008):
  1. updates the user dimension (parquet) from any init events in the batch,
  2. reads the full dimension, enriches match/purchase,
  3. computes minute aggregates for the batch and merges into output parquet,
  4. prints current minute aggregates to the console.
At demo scale read-modify-write on parquet is fine; at scale -> Delta merge.
"""
import os
from pyspark.sql import SparkSession, functions as F
from eightball.aggregations.minute import (
    build_user_dim, enrich, minute_purchase_metrics,
    minute_revenue_by_country, minute_matches_by_country)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DIM = "/output/user_dim"
SCHEMA = ("`event-type` STRING, `time` LONG, `user-id` STRING, "
          "purchase_value DOUBLE, `product-id` STRING, country_name STRING, "
          "platform STRING, `user-a` STRING, `user-b` STRING")

def _merge(df, path, keys):
    try:
        existing = df.sparkSession.read.parquet(path)
        df = existing.unionByName(df, allowMissingColumns=True)
    except Exception:
        pass
    df.write.mode("overwrite").parquet(path)

def process_batch(batch_df, _epoch):
    spark = batch_df.sparkSession
    events = batch_df.select(F.from_json(F.col("value").cast("string"), SCHEMA)
                             .alias("e")).select("e.*")
    # 1. update user dim from init events in this batch
    new_dim = build_user_dim(events)
    if new_dim.head(1):
        _merge(new_dim.withColumnRenamed("uid", "user-id"), DIM, ["user-id"])
    # 2. read full dim
    try:
        dim = spark.read.parquet(DIM).withColumnRenamed("user-id", "uid") \
                  .dropDuplicates(["uid"])
    except Exception:
        return
    # 3. enrich + aggregate this batch, merge outputs
    enriched = enrich(events, dim)
    _merge(minute_purchase_metrics(enriched), "/output/minute_purchase_metrics", None)
    _merge(minute_revenue_by_country(enriched), "/output/minute_revenue_by_country", None)
    _merge(minute_matches_by_country(enriched), "/output/minute_matches_by_country", None)
    # 4. console visibility
    minute_purchase_metrics(enriched).show(truncate=False)

def main():
    spark = SparkSession.builder.appName("minute-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", "events.clean")
              .option("startingOffsets", "earliest").load())
    (stream.writeStream.foreachBatch(process_batch)
     .option("checkpointLocation", "/output/_chk").start().awaitTermination())

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**
```bash
git add apps/spark_streaming.py
git commit -m "feat: Structured Streaming minute aggregator via foreachBatch (ADR-0008)"
```

---

## Phase 5 — Integration & polish

### Task 10: Wire all services into docker-compose

**Files:**
- Modify: `docker-compose.yml` (add producer, dq, spark-batch, spark-streaming)
- Create: `Dockerfile`

- [ ] **Step 1: Write `Dockerfile`** (python app image)
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
COPY apps/ ./apps/
COPY schemas/ ./schemas/
ENV PYTHONPATH=/app/src
```

- [ ] **Step 2: Add services to `docker-compose.yml`**
```yaml
  producer:
    build: .
    command: python apps/producer.py
    environment: { KAFKA_BOOTSTRAP: kafka:9092 }
    depends_on: { kafka: { condition: service_healthy } }

  dq:
    build: .
    command: python apps/dq_app.py
    environment: { KAFKA_BOOTSTRAP: kafka:9092 }
    depends_on: { kafka: { condition: service_healthy } }

  spark-streaming:
    image: apache/spark:3.5.1
    command: >
      /opt/spark/bin/spark-submit
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
      --conf spark.jars.ivy=/tmp/.ivy
      /app/apps/spark_streaming.py
    environment: { KAFKA_BOOTSTRAP: kafka:9092, PYTHONPATH: /app/src }
    volumes: [".:/app", "./output:/output"]
    depends_on: { kafka: { condition: service_healthy } }

  spark-batch:
    image: apache/spark:3.5.1
    profiles: ["batch"]   # run on demand: docker compose run spark-batch
    command: >
      /opt/spark/bin/spark-submit
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
      --conf spark.jars.ivy=/tmp/.ivy
      /app/apps/spark_batch.py
    environment: { KAFKA_BOOTSTRAP: kafka:9092, PYTHONPATH: /app/src }
    volumes: [".:/app", "./output:/output"]
    depends_on: { kafka: { condition: service_healthy } }
```

- [ ] **Step 3: Bring the whole stack up, verify data flows**
Run: `docker compose up --build -d && sleep 60 && ls output/`
Expected: `output/minute_purchase_metrics`, `output/user_dim` appear; streaming
container logs show minute tables. Then run batch: `docker compose run --rm spark-batch`.

- [ ] **Step 4: Commit**
```bash
git add Dockerfile docker-compose.yml
git commit -m "feat: full docker-compose stack (producer, dq, spark batch+streaming)"
```

### Task 11: End-to-end smoke test

**Files:**
- Create: `tests/test_smoke_e2e.py`

- [ ] **Step 1: Write the smoke test** (skipped unless `RUN_E2E=1`)
```python
import os, subprocess, time, glob
import pytest

pytestmark = pytest.mark.skipif(os.getenv("RUN_E2E") != "1",
                                reason="set RUN_E2E=1 to run docker e2e")

def test_pipeline_produces_output():
    subprocess.run(["docker", "compose", "up", "--build", "-d"], check=True)
    try:
        deadline = time.time() + 120
        while time.time() < deadline:
            if glob.glob("output/minute_purchase_metrics/*.parquet"):
                break
            time.sleep(5)
        assert glob.glob("output/minute_purchase_metrics/*.parquet"), "no minute output"
    finally:
        subprocess.run(["docker", "compose", "down", "-v"], check=True)
```

- [ ] **Step 2: Run it**
Run: `RUN_E2E=1 python -m pytest tests/test_smoke_e2e.py -v`
Expected: PASS (or actionable failure pointing at a specific service log).

- [ ] **Step 3: Commit**
```bash
git add tests/test_smoke_e2e.py
git commit -m "test: end-to-end docker smoke (gated on RUN_E2E)"
```

### Task 12: README + final ADR sweep

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write `README.md`** with sections:
  - **Quick start:** `make up` / `make demo` / `docker compose run --rm spark-batch` / `make test`.
  - **Architecture:** the diagram from the spec + topic/flow description.
  - **What each tier delivers** (Beginner/Semi-Pro/Pro) mapped to files.
  - **Schema notes:** the duplicate `minimum` on `game-tier`; `country` vs
    `country_id` assumption (ADR-0003); nested `platform`; optional user-b block.
  - **Trade-offs / decisions:** link each ADR (0001–0008) with a one-line summary.
  - **What I'd do at 100x scale:** the non-goals list (exactly-once, Schema
    Registry, partition tuning, serving DB).
  - **Development approach (AI disclosure):** 3–4 confident sentences — built with
    an AI agent under a defined harness (link `CLAUDE.md` + ADR-0007); architecture
    and decisions are the author's; TDD + ADRs document the reasoning.

- [ ] **Step 2: Verify all 8 ADRs exist and are non-empty**
Run: `ls docs/decisions/000{1,2,3,4,5,6,7,8}-*.md && wc -l docs/decisions/*.md`
Expected: 8 files, each non-trivial.

- [ ] **Step 3: Final test run**
Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 4: Commit**
```bash
git add README.md docs/decisions
git commit -m "docs: README (run guide, trade-offs, at-100x, AI disclosure) + ADR sweep"
```

---

## Self-Review (spec coverage)

- **Beginner/Kafka** → Tasks 4, 5 (topics, produce). ✅
- **Beginner/Spark batch (daily distinct by country+platform)** → Task 7. ✅
- **Semi-Pro/Kafka Streams DQ (uppercase, id→name, generic)** → Tasks 3, 6. ✅
- **Pro/Spark Streaming (minute: purchases, revenue, distinct users, revenue by
  country, matches by country, transformed)** → Tasks 8, 9. ✅
- **Self-contained / docker compose** → Tasks 4, 10. ✅
- **Country enrichment (schema trap)** → Tasks 8/9 + ADR-0001. ✅
- **TDD + ADRs + agentic harness** → Task 0 + ADRs written per decision. ✅
- **Schema edges documented** → Task 12 README + ADR-0003. ✅

**Known sync points to watch during execution:**
- `enrich()` returns a `(purchases, players)` tuple — Task 8 tests and Task 9 app
  both depend on that exact shape. Keep them aligned.
- Draft-03 root `"required": true` may need the documented shim in Task 1 Step 4.
- The Spark minute tests assert on metric values, not the `window` struct; if you
  add window columns to assertions, update both files.
```

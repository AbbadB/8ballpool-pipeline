# 8 Ball Pool Real-Time Data Pipeline — Design

> Miniclip Data Engineer take-home challenge.
> Status: **design approved**, pre-implementation.
> Deadline: Wed 1 Jul 2026 (submitting before travel on Tue 30 Jun).

---

## 1. Goal

Build a self-contained system that ingests real-time `8ballpool` game events
(`init`, `match`, `in-app-purchase`), applies generic data-quality
transformations, and produces clean, aggregated outputs — covering all three
tiers of the brief (Beginner → Semi-Pro → Pro).

### Success criteria
- One `docker compose up` brings the whole pipeline live (real Kafka + Spark).
- Every component is developed **test-first (TDD)** and has passing tests.
- Every non-trivial decision is captured as an ADR in `docs/decisions/`.
- The author can explain **every line and every trade-off** in the Stage-3 review.

### Explicit secondary goal — learning + defensibility
The author has not worked with Kafka before. The implementation plan therefore
doubles as a Kafka learning path: each step introduces exactly the concept it
needs (topic, partition, offset, consumer group, delivery semantics, event-time
vs processing-time), and ADRs/comments are written in "explain it back to the
interviewer" language so the architecture can be defended cold.

### Agentic-development harness (first-class goal)
This project is built with an AI agent as a pair-programmer under an explicit,
**disclosed** harness (ADR-0007). Seven rules govern it: (1) spec-first, never
freestyle; (2) TDD as the control loop; (3) human owns decisions, AI proposes
options; (4) small reviewable increments; (5) verification before "done"; (6)
grounded in real artifacts (schemas/spec/files); (7) provenance honesty. The
rules are enforced operationally by a `CLAUDE.md` and a single test-gate hook in
`.claude/settings.json`. Tooling is deliberately minimal — no custom skills or
subagent fleet — as a proportionality decision.

### Companion prep artifact (outside the repo)
A Stage-3 presentation walkthrough lives in the author's interview-prep folder
(not in the deliverable repo). It is kept in sync with the ADRs: every decision
recorded in `docs/decisions/` has a matching talking point + probe in the
walkthrough. The repo carries the trade-offs; the walkthrough coaches the live
defense.

---

## 2. The problem the schemas hide

Read carefully, the three schemas expose constraints the prose glosses over —
these drive the design:

1. **Only `init` carries `country` and `platform`** (per user). `match` and
   `in-app-purchase` have **no country**. Every "by country" aggregation
   therefore requires **enriching** the event with the player's country, looked
   up from their `init` event on user id. → See ADR-0001.
2. The DQ brief says *"map a field ID to a name, e.g. `country_id → country_name`"*,
   but the schema field is literally `country` (a string). Resolution: the
   **producer emits `country` as an id** (e.g. `"1"`, `"2"`); the DQ layer maps
   it to a name. → ADR-0003.
3. `platform` is **top-level on `init`** but **nested inside `*-postmatch-info`
   on `match`**, and absent on purchase. The DQ rule engine must address fields
   by path, not assume flat top-level columns.
4. `match.game-tier` declares `minimum` **twice** (`1` then `5`) — a bug in the
   provided schema (JSON duplicate key, last wins = 5). Flagged in README.
5. `time` is an integer epoch — interpreted as **epoch milliseconds**, used as
   event-time. → ADR (time semantics).
6. `match.user-b-postmatch-info` is **optional** while `user-a`'s is required —
   the match parser tolerates a missing B block.

---

## 3. Architecture

```
                    ┌─────────────┐
                    │  producer   │  synthetic events; init always first per user;
                    │  (python)   │  country emitted as id, platform lowercase;
                    └──────┬──────┘  validated vs JSON schemas before send
                           │
                           ▼  topic: events.raw
                    ┌──────────────┐
                    │    Kafka     │
                    └──────┬───────┘
                           ▼
                ┌──────────────────────┐
                │  dq-streams (python) │  generic rule engine (consume→transform→produce):
                │                      │    • uppercase(platform)
                │                      │    • map_id_to_name(country → country_name)
                │                      │    • rules are declarative config, extensible
                └──────┬───────────────┘
                       │  topic: events.clean
                       ▼
        ┌──────────────┴───────────────┐
        ▼                               ▼
┌─────────────────┐          ┌──────────────────────────┐
│ spark-batch     │          │ spark-streaming          │
│ daily distinct  │          │ user dim (from init)      │
│ users by        │          │ + join match/purchase ────┼─ enrichment (ADR-0001)
│ country+platform│          │ minute tumbling windows:  │
└────────┬────────┘          │  purchase count, revenue, │
         ▼                   │  distinct users,          │
   parquet + console         │  revenue/country,         │
                             │  matches/country (both)   │
                             └────────────┬──────────────┘
                                          ▼
                                   parquet + console
```

**Data flow:** producer → `events.raw` → DQ rule engine → `events.clean` →
(Spark batch: daily) + (Spark streaming: per-minute).

---

## 4. Components

Each unit has one purpose, a defined interface, and is independently testable.

### 4.1 `apps/producer.py`
- **Does:** generates a realistic stream of `init`/`match`/`in-app-purchase`
  events; guarantees `init` precedes any other event for a given user; emits
  `country` as an id and `platform` lowercase so DQ rules have real work;
  validates each event against the provided JSON schema before publishing.
- **Interface:** config (event rate, # users, country/platform pools) → Kafka
  topic `events.raw`. Pure logic in `src/eightball/events.py` + `schemas.py`.
- **Depends on:** Kafka, schemas.

### 4.2 `apps/dq_app.py` — generic DQ rule engine
- **Does:** consume `events.raw`, apply an ordered, **declarative** rule set,
  produce to `events.clean`. Rules: `uppercase(field)`,
  `map_id_to_name(field, lookup)`. Adding a field/transform is one config entry,
  no code change — satisfies the spec's "must be generic/extensible."
- **Interface:** `apply_rules(event: dict, rules: list[Rule]) -> dict` (pure,
  in `src/eightball/dq/rules.py`; config in `dq/config.py`) + a thin Kafka
  consume/produce loop in `apps/dq_app.py`.
- **Depends on:** Kafka. (No native Kafka Streams in Python — see ADR-0001.)
- **Failure handling:** validates each event at the boundary; decode/schema/transform
  failures are routed to `events.dlq` (reason + original payload), logged and
  counted, never crashing the loop. The pure decision lives in
  `src/eightball/dq/pipeline.py::process_event`. See ADR-0009.

### 4.3 `apps/spark_batch.py` — daily aggregator (Beginner tier)
- **Does:** read `events.clean`, compute **distinct users per day by country and
  platform** (from `init` events, which carry both). Output Parquet + console.
- **Interface:** `daily_distinct_users(df) -> df` (pure DataFrame transform in
  `src/eightball/aggregations/daily.py`, tested with fixtures); `apps/spark_batch.py`
  is the thin Kafka-read + write wrapper.

### 4.4 `apps/spark_streaming.py` — minute aggregator (Pro tier)
- **Does:** Structured Streaming via `foreachBatch`. Per micro-batch: update a
  user dimension from `init`, enrich match/purchase against it, **append** the
  enriched events to accumulating stores, and **recompute** the per-minute
  aggregates from the full accumulated set — purchase count, revenue sum, distinct
  users, revenue by country, matches by country (under **both** players, ADR-0004).
  Recomputing from accumulated enriched events (not per-batch partials) keeps the
  totals correct across batches, including distinct users (ADR-0008). Output
  Parquet + console.
- **Interface:** pure aggregation functions in `src/eightball/aggregations/minute.py`
  (`build_user_dim`, `enrich`, `minute_*`); `apps/spark_streaming.py` is the
  `foreachBatch` wrapper.

---

## 5. Key decisions (ADRs)

| ADR | Decision | Summary |
|---|---|---|
| 0001 | Enrich in Spark, not DQ | DQ stays a generic column transformer (spec's literal ask); relational enrichment is Spark's job. Python has no Kafka Streams to do a clean stateful join anyway. |
| 0002 | Real Kafka + Spark via Docker Compose | Matches the stack hint and "high standards" culture; one `docker compose up`. |
| 0003 | `country` emitted as id, mapped in DQ | Reconciles schema (`country` string) with DQ's `country_id→country_name` requirement. |
| 0004 | Matches counted under both players' countries | "Matches involving players from country X." Documented caveat: per-country sum ≥ total matches. |
| 0005 | Structured Streaming, not legacy DStreams | Event-time windows + watermarking; the modern, defensible choice. |
| 0006 | Event-time = epoch milliseconds | Faithful to real telemetry; demonstrates event-time vs processing-time understanding. |
| 0007 | AI-assisted agentic development under a defined harness | Disclosed, not hidden. 7 rules (spec-first, TDD control loop, human-owns-decisions, small reviewable steps, verify-before-done, grounded-in-real-files, provenance honesty) enforced by `CLAUDE.md` + a test-gate hook. Deliberately minimal tooling — no custom skills/subagent fleet (proportionality). |
| 0008 | Streaming via `foreachBatch` + recompute from accumulated enriched events | Per micro-batch: update user dim from init, enrich match/purchase, append enriched events, recompute minute aggregates from the full accumulated set. Correct across batches incl. distinct users (can't sum partials). O(n)/batch; at scale → native stateful aggregation with watermark + `approx_count_distinct`. |
| 0009 | DQ error handling: validate at boundary, dead-letter failures, never crash | Each event validated on the way in; decode/schema/transform failures routed to `events.dlq` with `{reason, original, failed_at}`, logged + counted. DLQ is the reprocessing path. |

Each ADR is a short file in `docs/decisions/` (context → decision → consequences
→ alternatives rejected).

---

## 6. Testing strategy — TDD throughout

**Rule: no implementation code is written before a failing test for it.**

- **Pure-function core first:** DQ rules and Spark aggregation logic are written
  as pure functions (`dict→dict`, `df→df`) so they are unit-tested without a
  live broker or cluster.
- **DQ engine:** test each rule, rule ordering, nested-field paths, extensibility
  (adding a rule via config), and unknown-id handling.
- **Spark jobs:** small fixture DataFrames assert daily distinct counts, the
  enrichment join, minute-window aggregates, and the both-players match rule.
- **Schema validation:** tests that valid events pass and malformed events are
  rejected at the producer.
- **Integration (lightweight):** one end-to-end smoke test through Docker Compose
  (produce N events → assert clean topic + aggregate output), kept minimal.

~20 focused unit tests + one gated end-to-end smoke. Quality and intent over count.

---

## 7. Non-goals (documented in README as "what I'd do at 100x scale")

- Exactly-once delivery / transactional Kafka (at-least-once + idempotent
  aggregates is enough here; note the upgrade path).
- Schema Registry + Avro/Protobuf (would replace hand-rolled JSON validation).
- Persistent late-data reprocessing beyond the watermark.
- Horizontal scaling / partition-count tuning (note the reasoning, don't build).
- A real serving DB / BI layer (Parquet + console is sufficient for review).

---

## 8. Repo structure

```
8ballpool-pipeline/
  docker-compose.yml        # kafka (KRaft) + producer + dq + spark services
  Dockerfile                # python image for producer/dq
  README.md                 # run instructions, trade-offs, "at 100x", schema notes,
                            #   "Development approach" (AI disclosure → ADR-0007)
  Makefile                  # make up / make demo / make test / make down
  CLAUDE.md                 # the agentic-dev harness (7 rules, conventions, DoD)
  pytest.ini
  requirements.txt          # runtime deps        requirements-dev.txt  # + pyspark, pytest
  .claude/settings.json     # test-gate hook (verify-before-done, mechanized)
  .github/workflows/ci.yml  # CI: unit suite + end-to-end pipeline
  docs/
    specs/                  # this document
    plans/                  # TDD implementation plan
    decisions/              # ADR-0001 … 0008
  schemas/                  # provided JSON schemas (draft-03)
  src/eightball/            # pure, infra-free logic (unit-tested)
    schemas.py  events.py
    dq/         rules.py, config.py, pipeline.py   # pipeline.py = validate+transform decision
    aggregations/  daily.py, minute.py
  apps/                     # thin Kafka/Spark wrappers
    producer.py  dq_app.py  spark_batch.py  spark_streaming.py
  tests/                    # schemas/events/dq_rules/dq_pipeline/daily/minute + smoke_e2e
```

# 8 Ball Pool — Real-Time Data Pipeline

[![CI](https://github.com/AbbadB/8ballpool-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/AbbadB/8ballpool-pipeline/actions/workflows/ci.yml)

A self-contained streaming pipeline for 8 Ball Pool game telemetry. It ingests
`init`, `match`, and `in-app-purchase` events, applies generic data-quality
transformations, and produces clean daily and per-minute aggregates — covering
all three tiers of the brief (Beginner → Semi-Pro → Pro).

**Stack:** Python · Apache Kafka (KRaft) · Apache Spark (Structured Streaming) ·
Docker Compose.

---

## Quick start

```bash
# 1. Run the unit test suite (no infra needed — pure logic)
make test

# 2. Bring the whole pipeline up (Kafka + producer + DQ + Spark streaming)
make demo                      # foreground, streams minute aggregates to console
#   or: make up                # detached

# 3. Run the daily batch aggregation on demand
docker compose run --rm spark-batch

# 4. Inspect outputs (written under ./output/)
#    minute_purchase_metrics, minute_revenue_by_country,
#    minute_matches_by_country, daily_distinct_users, user_dim

# 5. Tear down
make down

# End-to-end smoke test (builds + runs the stack, ~1 min):
RUN_E2E=1 python -m pytest tests/test_smoke_e2e.py -v
```

Requirements: Docker + Docker Compose. For running the unit tests directly,
Python 3.11+ and `pip install -r requirements-dev.txt` (Java 8/11/17 for PySpark).

---

## Architecture

```
 producer ──▶ Kafka: events.raw ──▶ dq_app ──▶ Kafka: events.clean ──┬──▶ spark_batch     (daily)
 (init-first,                       (generic                          └──▶ spark_streaming (per-minute)
  schema-valid)                      column DQ)                            enrich vs user dim, aggregate
```

- **producer** emits schema-valid events; `init` always precedes a user's other
  events; `country` is emitted as an **id** and `platform` lowercase so the DQ
  layer has real work (see ADR-0003).
- **dq_app** is a *generic, extensible* rule engine: uppercase fields, map id→name.
  Rules are declarative config — adding one needs no engine change. It is a pure
  column transformer; it does **not** enrich (see ADR-0001).
- **spark_batch** computes daily distinct users by country and platform.
- **spark_streaming** builds a user dimension from `init`, enriches match/purchase
  with the player's country, and computes per-minute aggregates correctly across
  micro-batches (see ADR-0008).

Two Kafka listeners are configured: `kafka:9092` (internal, for the containers)
and `localhost:29092` (external, for clients on the host).

---

## What each tier delivers

| Tier | Requirement | Where |
|---|---|---|
| **Beginner** | Kafka: produce init/match/purchase to a topic | `apps/producer.py` |
| **Beginner** | Spark batch: daily distinct users by country + platform | `apps/spark_batch.py`, `src/eightball/aggregations/daily.py` |
| **Semi-Pro** | Kafka Streams DQ: uppercase, id→name, generic/extensible | `apps/dq_app.py`, `src/eightball/dq/` |
| **Pro** | Spark Streaming: per-minute purchases, revenue, distinct users, revenue by country, matches by country (enriched) | `apps/spark_streaming.py`, `src/eightball/aggregations/minute.py` |

---

## Schema notes (things found reading the provided schemas closely)

- **Only `init` carries `country` and `platform`.** match/purchase have no
  country, so "by country" aggregations require enriching against the user's init
  event (ADR-0001).
- **`country` vs `country_id`.** The schema field is `country` (a string), but the
  DQ brief asks to map `country_id → country_name`. The producer emits `country`
  as an id and the DQ layer resolves it; unknown ids → `UNKNOWN` (ADR-0003).
- **`platform` is nested on `match`** (`user-a-postmatch-info.platform`) but
  top-level on `init`. The DQ engine addresses fields by dotted path.
- **`match.game-tier` declares `minimum` twice** (`1` then `5`) — a duplicate-key
  bug in the provided schema (last value wins). Flagged, not depended on.
- **`time`** is treated as epoch milliseconds, used as event-time (ADR-0006).
- **`match.user-b-postmatch-info` is optional** while `user-a`'s is required.

---

## Decisions (ADRs)

Full reasoning, with rejected alternatives, in `docs/decisions/`:

| ADR | Decision |
|---|---|
| [0001](docs/decisions/0001-enrichment-in-spark.md) | Country enrichment happens in Spark, not the DQ layer |
| [0002](docs/decisions/0002-runtime-docker-compose.md) | Real Kafka + Spark via Docker Compose (KRaft) |
| [0003](docs/decisions/0003-country-as-id.md) | `country` emitted as id, resolved to name in DQ |
| [0004](docs/decisions/0004-match-attribution.md) | Matches counted under both players' countries |
| [0005](docs/decisions/0005-structured-streaming.md) | Structured Streaming, not legacy DStreams |
| [0006](docs/decisions/0006-event-time.md) | `time` is event-time in epoch milliseconds |
| [0007](docs/decisions/0007-agentic-development.md) | AI-assisted agentic development under a defined harness |
| [0008](docs/decisions/0008-streaming-mechanics.md) | Streaming via `foreachBatch` + recompute from accumulated enriched events |

The full design spec is in [`docs/specs/`](docs/specs/) and the implementation
plan (TDD, step-by-step) in [`docs/plans/`](docs/plans/).

---

## What I'd do at 100× scale (deliberate non-goals)

- **Exactly-once.** Currently at-least-once with idempotent aggregates. For real
  money, transactional Kafka + checkpointed exactly-once.
- **Native stateful streaming aggregation.** The per-minute recompute is O(n) per
  batch (correct, not incremental). At scale: `withWatermark().groupBy(window()).agg()`
  with a state store and `approx_count_distinct` for mergeable distinct counts
  (ADR-0008).
- **Schema Registry + Avro/Protobuf** on the producer side, replacing hand-rolled
  JSON validation.
- **Partition + parallelism tuning.** At 150M+ events/day, partition topics by
  user/country and scale consumers/Spark to match peak, not average.
- **A serving layer.** Outputs land as Parquet for review; in production they'd
  feed a warehouse / serving DB for the reports.

---

## Development approach

This project was built with an AI agent (Claude Code) as a pair-programmer, under
an explicit, disclosed harness: spec-first, strict TDD as the control loop,
every architectural decision recorded as an ADR, small reviewable commits, and
verification before "done." The architecture and trade-offs are mine — the agent
accelerated typing and boilerplate while I owned the decisions, each documented
with its rejected alternatives. See [`CLAUDE.md`](CLAUDE.md) and
[ADR-0007](docs/decisions/0007-agentic-development.md).

# ADR-0006: Treat `time` as event-time in epoch milliseconds

**Status:** Accepted · **Date:** 2026-06-23

## Context

Every event schema carries an integer `time` field (`minimum: 0`) with no unit
stated. Aggregations are "per day" (batch) and "per minute" (streaming), so the
unit and the time semantics matter.

## Decision

Interpret `time` as **epoch milliseconds** and use it as **event-time**.
`timestamp_millis(time)` converts it to a Spark timestamp; daily aggregation
buckets by `to_date(...)` and minute aggregation by a 1-minute tumbling
`window(...)`.

## Consequences

- Aggregates reflect when events *happened*, not when they were processed —
  correct under consumer lag, replay, or backfill.
- In streaming, a watermark bounds how long late events are accepted before the
  window state is finalized (production tuning; demo uses a permissive default).

## Alternatives rejected

- **Epoch seconds.** Plausible, but milliseconds is the more common telemetry
  convention and the producer emits millis; documented so it is unambiguous.
- **Processing-time windows.** Simpler, but buckets by arrival time, which
  diverges from reality under lag/replay. Rejected for a telemetry pipeline.

# ADR-0008: Streaming via `foreachBatch` + recompute from accumulated enriched events

**Status:** Accepted · **Date:** 2026-06-23

## Context

The minute aggregator must enrich match/purchase events with the player's country
(ADR-0001), but country lives only on `init` events, which may have arrived in any
earlier micro-batch. It must also produce **correct per-minute totals across
batches** — a single minute's events are spread over many micro-batches.

A first design computed each batch's minute aggregate and *appended* it to the
output. That is wrong: it yields multiple partial rows per minute, and — critically
— **distinct-user counts cannot be reconstructed by summing per-batch partials**
(batch 1 sees {u1,u2}=2, batch 2 sees {u2,u3}=2; the true distinct is 3, not 4).

## Decision

Run the query with `foreachBatch`. Per micro-batch:
1. update a persisted user dimension from this batch's `init` events;
2. enrich this batch's match/purchase against the full dimension and **append the
   enriched events** to accumulating append-only stores;
3. **recompute** the minute aggregates from the *full accumulated enriched stores*
   (not from per-batch partials);
4. print the current minute aggregates to the console.

The pure transforms in `aggregations/minute.py` (unit-tested) are reused unchanged
for both enrichment and the recompute.

## Consequences

- Per-minute totals are **correct across batches, including distinct users**,
  because every recompute aggregates raw enriched events, never partials.
  (`tests/test_minute.py::test_distinct_users_correct_across_accumulated_batches`
  pins this guarantee.)
- Recompute is **O(n) per batch** — fine at demo scale, not at production scale.
- The append-only `_enriched_*` stores are the stable source of truth; the
  `minute_*` output dirs are written with `overwrite`, so an external reader can
  catch one mid-write (retryable; a demo-scale characteristic, not a data bug).

## Known limitation — restart & exactly-once

The enriched events are appended (`mode("append")`) to `_enriched_*` outside the
Kafka checkpoint transaction. If the job dies between the append and the offset
commit, a restart replays those offsets and **re-appends** the same enriched
events, inflating the recomputed aggregates. True exactly-once is not achievable
here because **the events carry no unique id** — there is nothing to deduplicate
on. Closing this would require either (a) a producer-assigned event id + a dedupe
on the enriched store, or (b) native stateful streaming with Spark's checkpointed
state and idempotent sinks (the documented upgrade path). For this self-contained
demo it is at-least-once, and the limitation is documented rather than hidden.

## Alternatives rejected

- **Append per-batch partials (the first design).** Incorrect for distinct counts
  and produces duplicate rows per minute. Rejected — this ADR exists because of it.
- **Native stateful streaming aggregation** (`withWatermark().groupBy(window()).agg()`
  with `update` output mode). This is the correct *production* design: Spark keeps
  per-window state across batches and uses a HyperLogLog sketch
  (`approx_count_distinct`) for mergeable distinct counts. It was **not** chosen
  for this submission because correct enrichment under it requires either a
  stream-stream join (init ⋈ events) with watermarks on both sides and a bounded
  time constraint — but init can precede an event by an unbounded interval, making
  that window huge or lossy — or a live-updating stream-static dimension that does
  not compose cleanly inside one native query. Recompute-from-accumulated gives
  identical correctness with a far smaller, fully-owned surface. Native stateful
  aggregation is documented here as the deliberate at-scale upgrade path.

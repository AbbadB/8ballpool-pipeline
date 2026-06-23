# ADR-0008: Streaming via `foreachBatch` + a maintained user dimension

**Status:** Accepted · **Date:** 2026-06-23

## Context

The minute aggregator must enrich match/purchase events with the player's country
(ADR-0001), but country lives only on `init` events. In streaming, the init for a
user may have arrived in any earlier micro-batch. We need fresh enrichment without
the complexity of a stream-stream join.

## Decision

Run the query with `foreachBatch`. Per micro-batch: (1) update a persisted user
dimension from any `init` events in the batch, (2) read the full dimension,
(3) enrich match/purchase against it and compute the batch's minute aggregates,
(4) merge results into the output and print to console. The pure transforms from
`aggregations/minute.py` (already unit-tested) are reused unchanged.

## Consequences

- Enrichment uses all users seen so far, not just the current batch.
- The per-minute aggregates accumulate across batches via a read-modify-write
  merge on the output (fine at demo scale).
- Logic stays testable as pure functions; `foreachBatch` is the only thin,
  Spark-specific wrapper.

## Alternatives rejected

- **Stream-stream join (init ⋈ match/purchase).** Requires watermarks on both
  sides and a bounded time constraint, but init can precede a match by an
  arbitrary interval — so the constraint window would be huge or lossy. Rejected.
- **Native windowed streaming aggregation with state store.** The cleanest
  production answer; deferred as the documented at-scale upgrade (Delta merge /
  stateful aggregation) to keep the demo self-contained. Noted, not built.

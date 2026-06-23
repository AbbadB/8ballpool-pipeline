# ADR-0005: Use Spark Structured Streaming, not legacy DStreams

**Status:** Accepted · **Date:** 2026-06-23

## Context

The Pro tier asks for a "Spark Streaming" real-time aggregator. Spark offers two
streaming APIs: the legacy RDD-based DStreams, and the DataFrame-based Structured
Streaming. The per-minute aggregations are defined over the event's own timestamp.

## Decision

Use **Structured Streaming**. It is the modern, supported API and provides
event-time windowing and watermarking out of the box — exactly what per-minute
aggregation over an event timestamp needs.

## Consequences

- Windows are defined on event-time (the `time` field), so a late-arriving event
  lands in the minute it actually happened, not the minute it was processed.
- The same pure DataFrame transforms used in batch tests are reused in streaming
  via `foreachBatch` (ADR-0008) — one code path, tested once.

## Alternatives rejected

- **Legacy DStreams.** RDD-based, micro-batch, processing-time semantics; no
  first-class event-time windows. Superseded by Structured Streaming and a weaker
  choice for time-correct telemetry aggregation. Rejected.

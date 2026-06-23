# ADR-0009: DQ error handling — validate at the boundary, dead-letter failures, never crash

**Status:** Accepted · **Date:** 2026-06-23

## Context

The first DQ app had no error handling: a malformed message crashed the consumer
loop (taking the service down), and a bad-but-parseable event passed through
silently (missing fields skipped, unknown country ids mapped to `UNKNOWN`) with no
record that anything was wrong. For a *data-quality* component, "what happens when
data quality fails" is the central question — and it was unanswered.

## Decision

The DQ component is made resilient and observable:

1. **Validate at the boundary.** Every event is re-validated against its JSON
   schema on the way in — the DQ layer does not trust upstream (even though the
   producer also validates).
2. **Never crash on one record.** JSON decoding and rule application are wrapped;
   a single bad message can never stop the loop. The decision logic lives in a
   pure, tested function `process_event(raw) -> DQResult` that never raises.
3. **Dead-letter queue.** Failures are routed to a Kafka topic **`events.dlq`** as
   `{"reason": <str>, "original": <raw payload>, "failed_at": <epoch_ms>}`. The
   original payload is preserved so the event can be replayed.
4. **Observability.** Each failure is logged as a structured line with its reason;
   a periodic counter line reports `{processed, clean, dead_letter}`.

### Failure taxonomy

| Failure | Reason prefix | Routed to |
|---|---|---|
| Malformed JSON bytes | `decode:` | `events.dlq` |
| Unknown / missing `event-type` | `schema:` | `events.dlq` |
| Schema-invalid (missing field, wrong type) | `schema:` | `events.dlq` |
| Transform raises | `transform:` | `events.dlq` |
| Valid | — | `events.clean` |

### Reprocessing

`events.dlq` **is** the reprocessing path: inspect the dead letters, fix the
offending rule or upstream producer, and replay the `original` payloads back onto
`events.raw`. No data is lost; bad data is quarantined, not dropped.

## Consequences

- A poison message degrades to one DLQ record + one log line, never an outage.
- Data quality is observable (reasons + counts) instead of silent.
- At-least-once delivery (ADR-0002) means a DLQ record may be re-emitted after a
  restart before its offset commits; replay tooling should dedupe. Acceptable here.
- `events.dlq` is auto-created by the broker; no compose change.

## Alternatives rejected

- **Crash-and-alert.** Let the loop die and rely on an external alert/restart. Loses
  in-flight isolation and stops all processing for one bad record. Rejected.
- **Log-only, no DLQ topic.** Logs the failure but provides no machine-readable
  replay path — reprocessing would mean scraping logs. Rejected.
- **Skip silently.** The original behaviour. Hides data-quality problems. Rejected.

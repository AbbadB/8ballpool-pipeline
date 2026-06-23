# ADR-0002: Real Kafka + Spark via Docker Compose (KRaft mode)

**Status:** Accepted · **Date:** 2026-06-23

## Context

The brief allows several self-contained setups and notes that embedded runtimes
"are enough." The role, however, is squarely a Kafka/Spark streaming role, and
the recruiter flagged that real-time pipelines are the heart of the work. A
submission that runs on *real* infrastructure is a stronger, more honest claim
than one that runs only embedded.

## Decision

Run real Kafka and real Spark as services in Docker Compose, started with a
single `docker compose up`. Kafka runs in **KRaft mode** (self-managed metadata,
no separate ZooKeeper), as a single node acting as both broker and controller.
Topics are auto-created with broker defaults (the brief permits this).

## Consequences

- "Works on a real broker/cluster" is demonstrable, not asserted.
- Single broker + replication factor 1 = no fault tolerance, which is fine for a
  local demo and documented as a non-goal.
- Delivery is **at-least-once**: a consumer may reprocess a message after a
  failure before its offset is committed. Downstream aggregates are designed to
  tolerate reprocessing (idempotent grouping), and this is noted as the upgrade
  path to exactly-once.
- One fewer moving part to run and explain than the ZooKeeper-based setup.

## Alternatives rejected

- **Embedded runtimes (Spark local, embedded Kafka).** Lower effort, but a weaker
  signal for a streaming role when a full week is available. Rejected on
  proportionality grounds — the realism is worth it here.
- **ZooKeeper-based Kafka.** Legacy; KRaft is the current standard and simpler to
  operate. Rejected.

# ADR-0001: Country enrichment happens in Spark, not the DQ layer

**Status:** Accepted · **Date:** 2026-06-23

## Context

`match` and `in-app-purchase` events carry no country — only `init` does (per
user). Yet the Pro-tier aggregations require "matches by country" and "country
revenue." Those events must therefore be **enriched** with the player's country,
looked up from their `init` event by user id (a join). The join can live in one
of two places: the DQ / Kafka Streams layer, or the Spark aggregation layer.

## Decision

The enrichment join lives in **Spark**. The DQ layer stays a *generic column
transformer* — uppercase, id→name — exactly as the brief's DQ section describes,
and stays generic/extensible as the brief requires. Spark builds a
`user → (country_name, platform)` dimension from clean `init` events and joins
match/purchase rows to it before aggregating.

## Consequences

- Each component does one thing: DQ = stateless column rules; Spark = relational
  joins + aggregation. Clean, independently testable boundaries.
- The DQ component remains a pure function of a single event, with no per-user
  state — which is what makes it genuinely "generic."
- A match/purchase whose user has no known `init` enriches to `country = UNKNOWN`
  rather than being dropped, keeping counts honest. (Documented edge case.)

## Alternatives rejected

- **Enrich in the DQ / Kafka Streams layer (KStream-KTable join).** Conceptually
  the canonical Kafka Streams pattern, and the elegant answer *in Java/Scala*. But
  (a) Python has no first-class Kafka Streams library, so we would hand-roll a
  state store, changelog and join semantics — more code and more correctness risk
  in the layer with the weakest support; and (b) it pushes the DQ component beyond
  the "column transform" the brief asks for, making it no longer generic.
  Higher ceiling, much lower floor, for no spec credit. Rejected.

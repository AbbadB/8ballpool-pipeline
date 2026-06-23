# ADR-0003: `country` is emitted as an id and resolved to a name in the DQ layer

**Status:** Accepted · **Date:** 2026-06-23

## Context

The DQ brief explicitly asks for an id→name transformation: *"Map a field ID to a
name, e.g. `country_id` to `country_name`."* But the provided `init` schema has a
single field `country` typed as a string (min 1 / max 100 chars) — there is no
`country_id` field. The two cannot both be taken literally.

The producer is ours to write, so we decide what `country` carries.

## Decision

The producer emits `country` as an **id** (e.g. `"1"`, `"2"`). The DQ rule engine
applies a `map_id_to_name` rule that resolves the id against a lookup
(`COUNTRY_LOOKUP`) and writes the result to a new `country_name` field. Unknown
ids resolve to `"UNKNOWN"` rather than being dropped, keeping downstream counts
honest.

## Consequences

- The DQ id→name requirement is satisfied with a *real* transformation, not a
  decorative one.
- All "by country" aggregations group on the resolved `country_name`.
- The assumption is documented in the README so a reviewer sees the reasoning.

## Alternatives rejected

- **Emit real country names; make id→name a no-op.** Satisfies the schema but
  makes the headline DQ requirement meaningless. Rejected.
- **Add a `country_id` field to the producer output.** Violates the schema
  (`additionalProperties: false`, no such field). Rejected.

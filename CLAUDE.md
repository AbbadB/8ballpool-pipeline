# 8 Ball Pool Pipeline — Engineering Conventions (Agentic Harness)

This project is built with an AI agent as a pair-programmer under these rules.
See `docs/decisions/0007-agentic-development.md`.

## The 7 rules
1. **Spec-first.** No code without an approved spec/plan. Architecture decisions
   are the human's; record them as ADRs in `docs/decisions/`.
2. **TDD control loop.** For every unit: write a failing test → run it (see it
   fail) → minimal implementation → run it (see it pass) → commit. No
   implementation before a red test.
3. **Human owns decisions; AI proposes options.** Surface 2-3 approaches with
   trade-offs; the human chooses; record rejected alternatives in the ADR.
4. **Small, reviewable increments.** One unit per commit. No large unreviewed diffs.
5. **Verify before "done."** Never claim passing without showing test output.
6. **Grounded in real artifacts.** Work from `schemas/` and the spec, not assumptions.
7. **Provenance honesty.** Comments/ADRs reflect real reasoning; AI use is disclosed.

## Conventions
- Python 3.11+, `src/` layout, tests in `tests/`, `pytest` from repo root.
- Pure logic (DQ rules, aggregations) lives in `src/eightball/` and is unit-tested
  with no Kafka/Spark running. Kafka/Spark wrappers live in `apps/` and stay thin.
- Field names follow the schemas exactly (hyphenated keys: `event-type`, `user-id`).

## Definition of done (per task)
- New behavior has a test; `pytest` is green; the change is committed.
- If a decision was made, its ADR exists.

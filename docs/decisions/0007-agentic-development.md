# ADR-0007: AI-assisted agentic development under a defined harness

**Status:** Accepted · **Date:** 2026-06-23

## Context

This project was built with an AI agent (Claude Code) as a pair-programmer. The
hiring brief states the team evaluates "approach, structure, and decision-making,"
not just the final result, and the wider 2026 norm is that engineers use AI daily
and are assessed on *how well* they direct it. Hiding AI assistance would be both
dishonest and fragile: the Stage-3 review is a line-by-line defence of the
solution, so any code I cannot explain is a liability.

The decision is therefore *not* whether to use AI, but under what discipline — and
to disclose that discipline openly as part of the engineering story.

## Decision

Work under an explicit harness of seven rules, enforced by repo artifacts:

1. **Spec-first** — no code without an approved spec/plan; architecture decisions
   are mine and recorded as ADRs.
2. **TDD control loop** — failing test → see it fail → minimal implementation →
   see it pass → commit. The test is the guardrail against plausible-but-wrong
   AI output.
3. **Human owns decisions; AI proposes options** — every ADR records the rejected
   alternatives, which is me weighing trade-offs, not the model.
4. **Small, reviewable increments** — one unit per commit; no large unreviewed diffs.
5. **Verify before "done"** — no success claim without shown test output.
6. **Grounded in real artifacts** — work from the actual schemas and spec.
7. **Provenance honesty** — comments/ADRs reflect genuine reasoning; AI use is
   disclosed (this ADR + the README "Development approach" section).

The rules are enforced operationally by `CLAUDE.md` (standing instructions) and a
`.claude/settings.json` test-gate hook that runs `pytest` before any "done."

## Consequences

- The architecture and trade-offs are defensible because I decided and recorded
  each one; the agent accelerated typing and boilerplate.
- The TDD gate means regressions surface immediately, including AI mistakes.
- Tooling is kept deliberately minimal — a `CLAUDE.md` and one hook, nothing more.

## Alternatives rejected

- **Hide AI use / "pretend I didn't."** Dishonest, and exposes me in the
  line-by-line Stage-3 review. Rejected.
- **Maximal AI tooling (custom skills, subagent fleet).** A project unto itself,
  competes with the actual pipeline for the week's time, and risks the harness
  looking more sophisticated than the deliverable — inverting the signal. Added
  exactly the harness that enforces the rules and stopped (proportionality).

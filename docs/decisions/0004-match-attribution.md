# ADR-0004: Matches are counted under both players' countries

**Status:** Accepted · **Date:** 2026-06-23

## Context

The Pro-tier aggregation asks for "the number of matches by country." A match has
two players (`user-a`, `user-b`) who may be from different countries. "Matches by
country" is therefore ambiguous: which country does a cross-country match belong
to?

## Decision

Attribute each match to **both** players' countries — increment the count for
`user-a`'s country and for `user-b`'s country. Implemented by exploding each
match into one row per participating player (`unionByName` of the two player
columns) and joining each to the user dimension.

The reading is: *"matches involving players from country X."*

## Consequences

- The sum of per-country match counts is **≥** the total number of matches (a
  cross-country match contributes to two countries). This is intentional and
  documented so it is not mistaken for a bug.
- For total match volume, use the raw match count, not the by-country sum.

## Alternatives rejected

- **Count under `user-a` only.** Sum equals total matches exactly, but the choice
  of `user-a` over `user-b` is arbitrary and undercounts the opponent's country.
  Rejected.
- **Count under the winner's country.** Sum equals total, but "matches by country"
  meaning the winner is an unusual reading of the requirement. Rejected.

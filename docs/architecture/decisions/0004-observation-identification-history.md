# ADR 0004: Observation identification history

- Status: Accepted
- Date: 2026-07-23

## Context

Taxonomy changes after an observer selects a name. Current display and historical
identification answer different questions.

## Decision

Store current resolved Sporely taxon separately from selected source usage,
original name, source, raw identifier plus namespace, source release,
identification timestamp, and unresolved/provisional/manual state. Resolution
changes never erase the original snapshot.

## Consequences

Observation migrations and UI must support “current name” and “identified as.”
Unresolved identifications remain valid and can be mapped later.

## Rejected alternatives

- Store only current `taxon_id`: loses historical evidence.
- Rewrite observations after taxonomy updates: changes user records silently.
- Store only free text: prevents reliable resolution and linking.

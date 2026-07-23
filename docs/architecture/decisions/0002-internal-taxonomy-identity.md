# ADR 0002: Internal taxonomy identity

- Status: Accepted
- Date: 2026-07-23

## Context

External IDs change, collide across namespaces, and can identify rows, names,
usages, or concepts.

## Decision

The stable registry allocates immutable positive integer Sporely `taxon_id`
values and owns continuity. IDs are never recycled. External IDs are text and
always carry source and namespace. Name equality alone never establishes
identity. Historical source usages remain traceable.

## Consequences

Compilation requires persistent controlled state and explicit mapping evidence.
Splits and merges cannot silently rewrite observation identity.

## Rejected alternatives

- COL/NorTaxa/GBIF IDs as primary keys: external lifecycle controls identity.
- Hashes of names: renames and homonyms break continuity.
- Reusing deleted IDs: corrupts historical meaning.

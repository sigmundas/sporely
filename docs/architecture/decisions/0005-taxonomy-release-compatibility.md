# ADR 0005: Taxonomy release and compatibility model

- Status: Accepted
- Date: 2026-07-23

## Context

Schema contracts, taxonomy content, upstream source releases, and app releases
change on different schedules.

## Decision

Set `TAXONOMY_SCHEMA_VERSION = 2`. Content IDs use `tax-YYYY.MM.DD-NN`.
Source releases remain independently pinned. Published artifacts/manifests are
immutable; corrections receive new IDs. Consumers declare supported schema
ranges and tested content releases. Additive/backward-compatible and breaking
changes follow distinct rollout procedures.

## Consequences

Content can update without app code when schema-compatible. Breaking changes
need compatibility and rollback plans. Manifests are generated, not hand-edited.

## Rejected alternatives

- One version for schema and content: unnecessary app coupling.
- Mutable “latest”: unreproducible releases.
- Infer compatibility from release date: no semantic guarantee.

# ADR 0003: Complete SQLite and compact cloud publication

- Status: Accepted
- Date: 2026-07-23

## Context

Desktop needs complete offline taxonomy and provenance. Supabase needs a small,
fast search slice with safe publication and rollback.

## Decision

SQLite contains compiled concepts, usages, names, identifiers, source releases,
mapping/reconciliation evidence, and provenance. Supabase contains current
searchable scoped taxa, required ancestors, search names, and external IDs used
by product workflows. Full evidence/raw source rows stay out of cloud.

Cloud publication uses validated inactive slots with atomic activation and
rollback. Taxonomy schema version and content release are independent.

## Consequences

Cloud is not a backup or authoritative compiler state. Publication must prove
parity for the shared slice. Stage 6 sizing requires privileged baseline data.

## Rejected alternatives

- Full provenance in Supabase: unnecessary storage and operational cost.
- Append-in-place publication: stale accumulation and partial updates.
- Cloud-only taxonomy: breaks offline and reproducible builds.

# Sporely taxonomy identity registry

The **canonical** append-only registry lives at
[`canonical.jsonl`](canonical.jsonl). It is the single version-controlled
allocator of `sporely_taxon_id`. Every allocation binds one internal Sporely
concept to one external source usage `(source, namespace, identifier)` per
[`docs/identity-contract.md`](../docs/identity-contract.md).

Rules:

* `canonical.jsonl` is append-only. Existing anchor/alias lines are never
  rewritten. Corrections come in as new lines that reference the superseded
  entry.
* Only a maintainer-authored, reviewed compile promotes new entries. Dry-run
  and experimental compiler invocations MUST pass `--registry` pointing at a
  file *outside* the repository (or to another path under this directory that
  is not `canonical.jsonl` — those files are gitignored).
* The `sporely_taxon_id` sequence starts at 1 and increases monotonically.
* External identifiers are stored as text under their exact
  `(source, namespace)`; a name equality never allocates a shared identity.

At Stage 3A.1 the canonical registry is intentionally empty. The 849,992-anchor
dry-run registry generated during Stage 3A was **not** promoted because it
allocated NorTaxa anchors before cross-source mapping and would have committed
permanent duplicate identities for concepts that COL and NorTaxa share.

Populating `canonical.jsonl` therefore has to run through the Stage 3A.1
compilation pipeline (COL anchors → policy-approved aliases → national-only
anchors) once cross-source mapping proposals have been reviewed.

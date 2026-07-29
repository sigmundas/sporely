# Sporely taxonomy identity registry

The **canonical** append-only registry lives as an ordered shard directory:

```
database/taxonomy/registry/canonical/
    manifest.json
    part-0001.jsonl
    part-0002.jsonl
    ...
```

It is the single version-controlled allocator of `sporely_taxon_id`. Every
allocation binds one internal Sporely concept to one external source usage
`(source, namespace, identifier)` per
[`docs/identity-contract.md`](../docs/identity-contract.md).

## Storage layout

* Each `part-NNNN.jsonl` shard is at most **25 MiB**.
* Line boundaries are preserved — no shard ends in the middle of a JSONL
  record.
* Concatenating the shards in **manifest.json → shards[] order** reproduces
  the underlying single-file registry byte-for-byte, including the trailing
  newline. That concatenation SHA-256 is recorded in
  `manifest.json → concatenated_sha256` and is the identity fingerprint of
  the registry as a whole.
* `manifest.json` records the registry schema version, ordered shard
  filenames, per-shard byte size / line count / SHA-256, total byte size,
  total entry count, and the concatenated SHA-256.

## Loading

`IdentityRegistry(path).load()` streams a shard directory in manifest
order when `path` is a directory, otherwise it reads a single JSONL file.
Fail-closed checks on the directory form cover: missing manifest, missing
shard, extra file present in the directory, per-shard size mismatch,
per-shard SHA-256 mismatch, per-shard line-count mismatch, or a mismatched
concatenated SHA-256.

## Promotion and dry-run policy

* `canonical/` and its contents are the only registry payload committed to
  Git.
* Dry-run / experiment registries live outside the repository (under
  `/tmp/`) or as other files under this directory. All non-canonical
  `registry/*.jsonl` and `*.tmp` paths are gitignored.
* `compile_release.py --registry PATH` requires an explicit path; dry runs
  point at a local scratch location so the canonical shard directory is
  never silently mutated.
* Promotion is a two-step maintainer workflow: (1) produce an accepted
  single-file registry via a compile that a reviewer has signed off on,
  and (2) run `identity_registry.shard_registry(source, canonical_dir)` to
  rewrite the shard directory atomically.

## Current canonical registry

The current shard set is generated from the Stage 3A.3 accepted dry-run
registry (COL XR 2026-07-17 + NorTaxa 1.284, no manual mappings) whose
concatenated SHA-256 is:

```
21b5d39d257799fdd4ea758d857fec4d29758b83dd7ea91bf3ecd24e3d1d3077
```

The shard directory is a byte-exact repackaging of that single-file
registry; the compiler treats loading from the shards as equivalent to
loading from the source file.

# W2D input snapshot contract

Status: contract for the anonymised historical-observation export that
feeds the Stage W2D reconciliation engine. This document defines the
JSONL wire shape the engine's CLI consumes; it does **not** authorise a
production export.

## 1. Scope

The reconciliation engine takes a JSONL stream of
`ReconciliationInput` records (see
`database/taxonomy/reconciliation/input_model.py`). This contract binds
any script that produces such a stream — whether from a local desktop
SQLite `observations.sqlite3` or from an anonymised web dump — to the
same shape.

Explicitly out of scope:

* production Supabase (no writer, no reader);
* the user's real observations DB — every export tool must be
  read-only, must accept an explicit path, and must refuse to run
  against production;
* rewriting the input records — this contract only describes the wire
  shape, not the mutation of the underlying DB.

## 2. Filesystem layout

A snapshot export produces a directory (never a single file mounted at
the repository root):

```text
<snapshot-dir>/
    snapshot-manifest.json            provenance header (§4)
    reconciliation-inputs.jsonl       one JSON object per line
    snapshot-inputs.sha256.txt        SHA-256 of reconciliation-inputs.jsonl
```

The `sha256` companion is chained into the `snapshot-manifest.json`
`inputs_sha256` field. Consumers must reject the snapshot if the
recorded hash does not match the file.

## 3. Line format

Each non-empty line of `reconciliation-inputs.jsonl` is a UTF-8-encoded
JSON object whose keys are the `ReconciliationInput` field names.

```jsonc
{
  "observation_id": "opaque-string",           // internal id; MUST NOT be user email / device id
  "manual_identification_flag": false,          // bool
  "stored_scientific_name": "Boletus edulis",   // string | null
  "stored_vernacular_name": null,               // string | null
  "stored_rank": "species",                     // string | null (see §3.1)
  "source_release_or_timestamp": null,          // string | null; free-form provenance
  "signals": [                                  // array of RawSignal
    {
      "kind": "exact",
      "source_system": "col_xr",
      "namespace": "col_usage_id",
      "external_id": "323XQ",
      "origin_field": "observations.ai_selected_taxon_id",
      "raw_value": "323XQ",
      "rule_id": "artsorakel_bare_int_v1",
      "notes": null
    }
  ]
}
```

The first line MAY be a sentinel:

```json
{"__synthetic__": true, "purpose": "human-readable label"}
```

The CLI loader (`cli._iter_input`) skips this line. Every anonymised
export MUST emit the sentinel so the file cannot silently be mistaken
for real user data. A production export SHOULD emit it as well; its
`purpose` may read `"production-snapshot-<release-id>"` or similar.

### 3.1 Whitelisted rank values

`stored_rank`, when non-null, MUST be one of:

* `genus`, `species`, `subspecies`, `variety`, `form`, `aggregate`.

Any value outside the whitelist MUST be coerced to `null`. This is the
same whitelist Stage 3B.3 enforces on the desktop
`observations.taxon_rank_snapshot` column.

## 4. Provenance header

`snapshot-manifest.json` is a UTF-8 JSON object with sorted keys and
this exact set of fields:

```jsonc
{
  "engine_contract_version": "w2d-1.0.0",
  "input_row_count": 337,
  "inputs_sha256": "<sha256 of reconciliation-inputs.jsonl>",
  "snapshot_kind": "desktop_local | web_anonymised_dump",
  "snapshot_source_release_or_git_sha": "sporely-py:<git-sha>",
  "snapshot_id": "<uuid or content hash>",
  "taxonomy_release_id": "tax-2026.08.01-01"
}
```

Optional fields may be added; every field the engine reads is listed
above. The engine refuses to run when `engine_contract_version` is not
a version it recognises.

## 5. Forbidden PII

Every export tool MUST strip or refuse to emit any of the following
fields **anywhere** in the JSONL payload, even inside `RawSignal.notes`
or `stored_*`:

* user email, cloud user id, or account name;
* device identifier, hostname, MAC address;
* GPS coordinates, region id, country code beyond the ISO 3166-1
  alpha-2 code, or any address-line field;
* image paths, filenames, camera EXIF metadata;
* free-form observer comments (`open_comment`, `private_comment`,
  `species_guess` are consumed only as `raw_value` on a text-only
  signal, never as long-form prose);
* creation / update timestamps at wall-clock resolution;
* any field that has been marked private in
  `database/schema.py::init_database`.

`observation_id` MUST be an opaque stable string (a UUIDv4 is
preferred; a hash of the internal id is acceptable). It MUST NOT be a
sequential integer that leaks database size.

Every export tool MUST refuse to emit rows whose `observation_id` is
empty, or which duplicates an already-emitted `observation_id`.

## 6. SHA-256 chaining

The engine's manifest header records `input_source_hash`, which is
independently computed inside the resolver over the normalised
signal tuples (see `manifest._input_source_hash`). It is not required
to match the `inputs_sha256` in the snapshot manifest — they are
computed over different projections of the data.

However, a Stage W2D consumer that wants to verify the snapshot →
manifest chain end-to-end should record both hashes in its own audit
log. The invariant is:

* recomputing `inputs_sha256` over the raw JSONL bytes MUST reproduce
  the value in `snapshot-manifest.json`.
* re-running the resolver over the same JSONL MUST reproduce
  `input_source_hash` in the reconciliation manifest.

Silent mismatch on either side is a hard-fail condition for the
consumer.

## 7. Non-goals

This contract does not describe:

* how observation records are anonymised (see the export script's
  README docstring);
* how a downstream cloud driver applies the resulting reconciliation
  manifest (workstream D, `sporely-web`);
* how the canonical registry is updated (that is a separate maintainer
  workflow).

## 8. Related documents

* `database/taxonomy/docs/w2d-reconciliation-contract.md` — the
  authoritative engine contract.
* `database/taxonomy/scripts/export_observations_snapshot.py` —
  specification-only script (dry-run mode) documenting the safe
  read-only export from a local `observations.sqlite3`.
* `database/taxonomy/docs/identity-contract.md` — global identity
  policy.

## 9. W2D-R additions

The Stage W2D-R work adds an authorised end-to-end recovery path:

* `database/taxonomy/reconciliation/snapshot/pseudonym.py` — HMAC-keyed
  observation-reference pseudonymisation. Key supplied at run time via
  `SPORELY_W2DR_PSEUDONYM_KEY` or `--pseudonym-key-file`. Minimum 32
  bytes; never committed; never logged.
* `database/taxonomy/reconciliation/snapshot/validator.py` — schema +
  privacy validator that refuses prohibited private fields (see §3),
  raw-UUID observation ids, duplicates, and missing snapshot headers.
* `database/taxonomy/reconciliation/snapshot/transformer.py` — offline
  transformer that consumes an authorised raw JSONL export and emits
  the anonymised snapshot JSONL, `.sha256.txt`, and a `.stats.json`
  sidecar. Refuses `--production`.
* `database/taxonomy/docs/w2d-source-recovery-runbook.md` — the
  operator's runbook for producing an anonymised snapshot.

An observation reference in this contract's `observation_id` must be a
keyed pseudonym of the form `obs_<24 hex>`, or (for fixture-only
records) a value prefixed with `synthetic_`. Raw UUIDs are refused by
the validator.

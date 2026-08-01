# W2D-R historical source-recovery runbook

Audience: authorised human operator who holds:

* read access to a disposable local copy of the observations database
  (either a copied `observations.sqlite3` or an already-exported
  Postgres dump) — **never a production endpoint**;
* the pseudonymisation key (32 random bytes, base64-encoded, kept
  outside the repository).

This runbook produces the anonymised snapshot that unblocks the real
Stage W2D historical reconciliation. It never authorises production
access.

Two-step pipeline:

1. **Authorised read-only source export** (out of scope for this
   tooling): the operator produces a raw JSONL export from a local,
   disposable copy of the observations DB, containing only the columns
   inventoried below. The tooling in this repo does **not** talk to
   Postgres, Supabase, or SQLite directly for this step — a
   specification-only helper
   (`database/taxonomy/scripts/export_observations_snapshot.py`)
   documents the read-only projection and refuses to write when
   `--production` is set.
2. **Offline anonymisation + validation** (this repo):
   `database/taxonomy/reconciliation/snapshot/transformer.py transform`
   consumes the raw JSONL, HMAC-keyed pseudonymises `id`, derives
   `RawSignal` records, drops prohibited private fields, and emits an
   anonymised snapshot JSONL that the reconciliation CLI can consume.

## 1. Raw-export projection

The raw export must contain **exactly** these columns for each observation.
Any additional column is refused by the validator (unexpected fields).

| column | type | required |
|---|---|---|
| `id` | text or integer | yes — the transformer pseudonymises this and never persists the raw value |
| `sporely_taxon_id` | integer | optional (may be null) |
| `artsdata_id` | integer | optional |
| `artportalen_id` | integer | optional |
| `inaturalist_taxon_id` | integer | optional |
| `inaturalist_id` | integer | optional; observation id, never creates identity |
| `mushroomobserver_id` | integer | optional; observation id, never creates identity |
| `ai_selected_service` | text | optional |
| `ai_selected_taxon_id` | text | optional |
| `ai_selected_scientific_name` | text | optional |
| `scientific_name_snapshot` | text | optional |
| `taxon_rank_snapshot` | text | optional |
| `genus`, `species`, `common_name`, `species_guess` | text | optional |
| `manual_identification_flag` or `manual_name` | boolean | optional |
| `source_release` | text | optional |

The following fields are **prohibited** in the raw export. If any
appear, the validator will fail:

```
email, email_address, user_email, display_name, user_id, auth_user_id,
profile_id, device_id, session_id, access_token, refresh_token, password,
photo_url, image_url, media_url, storage_path, latitude, longitude,
lat, lon, lng, geom, geohash, locality, place_name, notes,
observation_notes, private_habitat
```

The operator MUST inspect the export column list and a small sample
before running the transformer. Pattern-based validation is a backstop,
not a substitute for human review.

## 2. Pseudonymisation key

Generate once, store outside the repository, share only with authorised
operators:

```bash
openssl rand -base64 32 > ~/.config/sporely/w2dr-pseudonym.key
chmod 600 ~/.config/sporely/w2dr-pseudonym.key
```

The transformer accepts the key either via the environment variable
`SPORELY_W2DR_PSEUDONYM_KEY` or `--pseudonym-key-file <path>`. The key
is never logged, never emitted to the snapshot, and never committed.

The same key applied to the same raw id always yields the same
pseudonym `obs_<24 hex chars>`. Rotating the key breaks the mapping —
plan accordingly if a later migration must join snapshots produced
under different keys.

## 3. Transform + validate

```bash
python -m database.taxonomy.reconciliation.snapshot.transformer transform \
  --raw-export /path/to/local/raw-export.jsonl \
  --output    /path/to/local/snapshot.jsonl \
  --pseudonym-key-file ~/.config/sporely/w2dr-pseudonym.key

python -m database.taxonomy.reconciliation.snapshot.transformer validate \
  --snapshot /path/to/local/snapshot.jsonl \
  --report   /path/to/local/snapshot.validation.json
```

The transformer emits three files next to the snapshot:

* `snapshot.jsonl` — anonymised snapshot with header row
* `snapshot.jsonl.sha256.txt` — SHA-256 of the snapshot
* `snapshot.jsonl.stats.json` — record count, signal counts, prohibited
  fields stripped, raw-export SHA-256

The validator refuses the snapshot if any of the following are present:

* prohibited private field name;
* email-like string in any value;
* media / storage URL in any value;
* coordinate-shaped field key;
* raw UUID under `observation_id`;
* duplicate pseudonymous references;
* unexpected fields;
* signals with missing / mis-typed contract fields;
* missing snapshot header line or `schema_version` mismatch.

## 4. Reconciliation

Once validation is clean:

```bash
python -m database.taxonomy.reconciliation.cli \
  --input   /path/to/local/snapshot.jsonl \
  --output  /path/to/local/reconciliation-real \
  --release-dir database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01 \
  --policy  database/taxonomy/policies/w2d-reconciliation-policy.json
```

Run **twice**, into distinct output directories, and confirm the
manifests are byte-identical (determinism proof).

Label the outputs as `historical anonymized manifest` so they cannot be
confused with the synthetic fixture manifest under
`database/taxonomy/evidence/historical-reconciliation/`.

## 5. Disposable web simulation against the real manifest

The web repo's disposable simulation
(`scripts/taxonomy-v2/run-w2d-migration-simulation.mjs`) accepts
`--manifest <path>` and can be pointed at the real reconciliation
manifest exactly the same way. Do not apply the manifest to production;
the disposable schema is the only authorised target.

## 6. Commit boundary

**Do not commit**:

* the raw export;
* the pseudonymisation key;
* the anonymised snapshot itself (unless a repository-storage exception
  is explicitly approved for a specific investigation);
* the `.stats.json` sidecar when it references SHA-256 hashes of files
  under investigation-privileged directories.

**Do commit**: aggregate evidence — record counts by reconciliation
state, semantic SHA-256 of the reconciliation manifest, verdicts, and
this runbook.

## 7. Refusal cases

The tooling refuses to run when any of the following holds:

* `--production` flag is set on any subcommand;
* pseudonymisation key is missing, malformed, or shorter than 32 bytes;
* raw export contains a prohibited field name;
* snapshot header is missing or references a schema version other than
  `w2d-input-1.0.0`.

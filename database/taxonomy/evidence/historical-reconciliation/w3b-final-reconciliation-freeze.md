# W3-B final reconciliation freeze

Manual review is complete. This stage validates the reviewer's decisions,
applies them as an explicit overlay on the accepted 369-record
reconciliation, re-rehearses the whole install locally, and rebuilds the
real-ID deployment manifest.

## Decisions validation

Path: `~/w2dr/review/manual-decisions.jsonl` (outside Git).
File SHA-256: `02dfd80713afc2ff66cfe7f391df6cd7a8baa6ddc4f320e45d0914040bb87d68`.

| metric | value |
|---|---:|
| review groups covered | **73 / 73** |
| decisions header present | ✓ (manifest_semantic_sha256 recorded) |
| malformed lines | 0 |
| missing decisions | 0 |
| extra decisions | 0 |
| invalid selections (context-only, missing id) | 0 |

Choice breakdown:

| choice | groups | observations |
|---|---:|---:|
| `accepted_1` | 66 | **78** |
| `no_match` | 7 | 7 |

Accepted by rank of resolved concept:

| rank | observations |
|---|---:|
| species | **75** |
| genus | **3** |
| other | 0 |

## Overlay application

Tool: [`database/taxonomy/scripts/apply_manual_review_overlay.py`](../../scripts/apply_manual_review_overlay.py).
Refuses `--production` and any output path under either repo.

Semantics:

* only pseudonymous observation IDs listed inside each decision are
  updated — no global name inference;
* `accepted_*` rewrites the record to `resolved_exact`
  (`resolved_exact_via_synonym_relationship` when the candidate's
  `match_type == nortaxa_synonym_redirect`);
* `no_match` leaves the record `manual_unresolved` with an explicit
  reviewer acknowledgement in `review_reason`;
* the manifest header gains an `overlay` block: kind, decisions SHA,
  input-manifest SHA, and per-state counts.

Byte-identical across two runs:

```
sha256(reconciliation-manifest.json) = 97bd7b19c346e1348e7b9a30a5641bc95760d77a8679d5b14fdaf83ffc4abe58
input-manifest sha256                 = 1beaa33f3891b216d3bc7c6d34cd96df1a936627c5a6f749a515cc75d51c094e
decisions file sha256                 = 02dfd80713afc2ff66cfe7f391df6cd7a8baa6ddc4f320e45d0914040bb87d68
```

**Final state counts:**

| state | count |
|---|---:|
| resolved_exact | **311** |
| unresolved_external_identifier | 21 |
| manual_unresolved | 7 |
| no_identity_evidence | 30 |
| **total** | **369** |

Resolution methods (resolved records):

| method | count |
|---|---:|
| trusted_secondary_provider_mapping (canonical registry) | 233 |
| operator_manual_review | 78 |

## Local rehearsal against the final manifest

Applied through `sporely-web/scripts/taxonomy-v2/run-w2d-migration-simulation.mjs` against the local disposable Supabase.

| dimension | value |
|---|---:|
| identification_snapshot | **369** (immutable) |
| resolution_link | 369 |
| registry_concept | **194** — 39 in_cache/include + 155 out_of_cache/not_evaluated |
| external_mapping | 144 |

`resolution_link` breakdown by state (real DB):

| state | rows |
|---|---:|
| resolved_exact | 311 |
| unresolved_external_identifier | 21 |
| manual_unresolved | 7 |
| no_identity_evidence | 30 |

Invariants verified:

| proof | verdict |
|---|---|
| 369 immutable identification snapshots | ✓ |
| final resolved and null-link counts (311 / 58) | ✓ (78 overlay + 233 base = 311; 21 + 7 + 30 = 58) |
| original observation fields unchanged | ✓ (schema trigger blocks UPDATE of `original_*` while snapshot_locked=true) |
| idempotency | ✓ (`--twice` sim: row counts + snapshot fingerprint stable) |
| rollback | ✓ (induced P0001 → zero orphan rows) |
| registry_concept identity conflict fails closed | ✓ (W3-A2 hardened invariant carries) |
| external_mapping conflict fails closed | ✓ (W2E-A2 hardened invariant carries) |
| search cache unchanged | ✓ (base release `taxon_external_id.jsonl` still 52 881 `col_xr:col_usage_id` rows; supplements added only sparse-registry entries) |

## Rebuilt real-ID deployment manifest

Path (outside Git):
`/tmp/w3b-deployment/deployment-manifest.jsonl`
SHA-256: `32bc2848d1c5aa2c54b0e754bc976ee0db86455c0eb5f56eb1a7529c287eac1c`

| metric | value |
|---|---:|
| matched observations | 369 / 369 |
| resolved links | 311 |
| null (unresolved / manual / no-evidence) | 58 |
| raw-export SHA-256 | `8efbedfaedb3fea94c6d5ebcc4b80eb65c04e535f8b27649dfdcf353e40b17e4` |
| final manifest input-file SHA-256 | `97bd7b19c346e1348e7b9a30a5641bc95760d77a8679d5b14fdaf83ffc4abe58` |
| pseudonym collisions | 0 |
| unmatched raw IDs | 0 |

## Final drift-export SQL

Committed at [`database/taxonomy/scripts/final_drift_export_query.sql`](../../scripts/final_drift_export_query.sql). Single SELECT, read-only, returns exactly the taxonomy columns the fingerprint is computed over.

## Operator commands

Extract the ARRAY body for the SQL:

```bash
python3 -c "
import json
ids = []
with open('/tmp/w3b-deployment/deployment-manifest.jsonl') as f:
    for line in f:
        d = json.loads(line)
        if '__deployment_manifest_header__' in d:
            continue
        ids.append(str(d['real_observation_id']))
print(','.join(ids))
" > /tmp/w3b-deployment/ids.csv
```

Paste `/tmp/w3b-deployment/ids.csv` into the SQL Editor in place of `__IDS__` in
[`final_drift_export_query.sql`](../../scripts/final_drift_export_query.sql), run it, and download the CSV to
`/tmp/w3b-deployment/current-observations.csv`.

Compare against the fingerprint stamped at export time:

```bash
python3 -m database.taxonomy.scripts.build_deployment_manifest \
  --raw-export /Users/sigmundas/Documents/Code/sporely/w2dr/support-export.csv \
  --manifest /tmp/w3b-final/reconciliation-manifest.json \
  --pseudonym-key-file /Users/sigmundas/.config/sporely/w2dr-pseudonym.key \
  --output /tmp/w3b-deployment/deployment-manifest-drift-checked.jsonl \
  --observations-fingerprint /tmp/w3b-deployment/current-observations.csv \
  --allow-output-under /tmp
```

The resulting summary emits `drift_counts` broken down into
`no_drift` / `drifted_since_export` / `observation_missing`. Any row with
`drifted_since_export` must be re-reviewed before staging.

## Safety

* production access: **no**
* production writes: **no**
* staging migrations created or applied: **no**
* client cutover: **no**
* raw-ID deployment manifest committed: **no**
* pseudonym key exposed: **no**
* new upstream download: **no** (drift export runs against the operator's own project via Supabase SQL Editor and is authorised separately)

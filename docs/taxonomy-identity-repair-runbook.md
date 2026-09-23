# Observation taxonomy-identity repair runbook

Operational contract for `database/audit_observation_identity.py`, the Stage 4
audit and repair tool of the taxonomy-v2 closeout. It covers what the tool
classifies, what it is allowed to write, what must be true before it writes
anything to production, and how to undo it.

Read this before running the tool against any database you care about. The
authoritative identity rules it enforces live in
`database/taxonomy/docs/identity-contract.md` and the identity section of
`docs/supabase-sync-contract.md`; this document does not restate them, it says
how the audit applies them.

## Old and new identity semantics

The two readings of the same column, stated side by side, because every
classification below is a judgement about which one a row was written under.

| | Old (pre-Stage-2) | New (Stage 2 onward) |
|---|---|---|
| What `sporely_taxon_id` means | "some integer a taxonomy-ish thing produced" | a Sporely-owned concept id, and only that |
| Proof standard | `value > 0` | a recorded proof token in `taxon_identity_proof` |
| External identifiers | a bare integer, namespace discarded | the tuple `(source_system, namespace, external_id)` plus the verbatim provider string |
| A provider id such as `NBIC:53482` | dropped, or stripped to `53482` and treated as a concept id | parsed, preserved, and resolvable only through a declared namespace bridge |
| Failure to resolve | indistinguishable from "no identification" | the explicit state `external_unresolved`, which still displays its name |
| Name equality | usable evidence for binding a concept | not identity evidence under any circumstances |
| A pre-existing integer | authoritative | `legacy_unverified` — real persisted data, not proof, never asserted to the cloud |

The consequence that matters operationally: **a row written under the old
semantics cannot be upgraded by inspection.** Nothing in the row records which
producer wrote its integer, so the only sound promotion is re-deriving the same
value from a namespaced identifier the row itself carries. Everything else is
reported and left alone. That is why the repairable population is small and
why a large repair count is a signal to stop, not to celebrate.

## What the audit classifies

Every observation gets exactly one value on each of three axes, so each axis
independently sums to the row count.

### `identity_class`

| Class | Meaning | Repairable |
|---|---|---|
| `proven_sporely_identity` | a recorded proof token, and the concept exists in the artifact | no — already correct |
| `proven_external_with_unique_bridge` | a namespaced tuple on the row resolves to exactly one concept | **yes** |
| `unresolved_external_identity` | a namespaced tuple with no authoritative mapping | no |
| `ambiguous_external_identity` | the tuple matches several concepts | no |
| `manual_or_no_identity_evidence` | no identifier at all; a name is not an identity | no |
| `suspicious_numeric_collision` | the stored integer is contradicted by, or collides with, a real external identifier | no |
| `legacy_unverified_identity` | a pre-Stage-2 integer with nothing to re-derive it from | no |
| `stale_sporely_identity` | proven, but names a concept this artifact does not contain | no |

`suspicious_numeric_collision` is the class the whole stage exists for. It
fires on evidence, not on how the number looks: either the row's own provider
identifier resolves to a *different* concept than the stored integer, or the
stored integer is simultaneously a valid concept id and some other registry's
external identifier. Repairing such a row on numeric grounds would silently
retag the observation as a different species.

### `name_class`

| Class | Meaning | Repairable |
|---|---|---|
| `name_intact` | the observation has a name, or never had a provider candidate | no |
| `name_loss_repairable_from_row` | null `genus` and `species` beside a splittable `ai_selected_scientific_name` | **yes** |
| `name_loss_unrepairable_from_row` | the same, but the provider string does not split into a binomial | no |

The name-loss population is counted separately from the identity-leak
population because it is a different defect with a different repair. Name
repair infers no identity: it writes back a string the same row already
carries, using `utils.taxon_text.split_scientific_name_text` — the same rule
the desktop cloud-pull path already applies to this field. A desktop client
therefore already reconstructs these names on pull; the population that stays
broken is the one read directly from the cloud row, which is where the
`Entoloma conferendum` incident was visible.

`common_name` is never restored. Nothing on the row records it, and taking it
from the taxonomy artifact would be inferring a name from an identity the
repair has not proven. Rows needing it are reported, not guessed at.

### `proposed_action`

`no_change_required`, `bind_sporely_identity`, `restore_lost_names`,
`bind_sporely_identity_and_restore_lost_names`, `report_only`. Only the middle
three write anything.

## Running the dry run

Against a desktop database:

```
python -m database.audit_observation_identity \
  --taxonomy <candidate-or-desktop-pack>.sqlite3 \
  --observations ~/…/sporely.sqlite3 \
  --output stage4-observation-identity-audit.json
```

Against cloud rows, which must be exported first (the tool never reads
Supabase — see the gate below):

```
python -m database.audit_observation_identity \
  --taxonomy <candidate-or-desktop-pack>.sqlite3 \
  --cloud-rows cloud-observations-export.json \
  --output stage4-cloud-identity-audit.json
```

`--taxonomy` accepts either compiled shape: the `build_sqlite_candidate.py`
candidate (`taxon_min` + `taxon_external_id_text_min`) or the
`macrofungi_scope.build_desktop` search pack (`taxon` + `external_mapping`).
The namespace-lost `taxon_external_id_min` table is never read.

The output is deterministic: same rows plus same artifact produce byte-identical
JSON. That is what makes "the dry run was reviewed" a statement about what a
later run will do.

## Part B — the production migration gate

Production mutation is a separate reviewed operation. `--apply` writes only to
a local `--observations` database; passing it alongside `--cloud-rows` is
refused outright.

Five conditions, encoded as `ProductionMigrationGate` so they fail closed:

1. **`dry_run_artifact_reviewed`** — a named, archived audit JSON has been read
   by a reviewer, row by row for every row whose `proposed_action` writes.
2. **`counts_reconcile`** — `AuditReport.reconciles()` is true and the three
   axes each sum to the row count. Check it in the artifact, not by eye.
3. **`candidate_release_validated`** — the taxonomy release the repair resolves
   against is validated and identified by `content_release_id`. For this
   closeout that is `tax-2026.09.23-01`, whose build, determinism and coverage
   are recorded in
   `database/taxonomy/evidence/taxonomy-v2-closeout/stage3-candidate-verification.md`.
4. **`rollback_procedure_documented`** — the section below, with the pre-image
   captured.
5. **`integrity_checks_defined`** — the section below, run before and after.

`require_open_gate()` raises `NEEDS_YOU` listing every unmet condition. A gate
that is satisfied by default would enforce nothing, so every field starts false.

### Status as of 2026-09-23

**The production run is blocked: `NEEDS_YOU`.** Condition 1 cannot be met
because no cloud export exists — reading `public.observations` requires an
authorized Supabase session, and the Supabase MCP server is unauthorized in the
sessions this work was done in. The same block is recorded against the Stage 2
Part B evidence requirement in the closeout plan.

To unblock, in an interactive authorized session:

1. export the observation rows (`id`, `genus`, `species`, `common_name`,
   `ai_selected_taxon_id`, `ai_selected_scientific_name`,
   `selected_sporely_taxon_id`, and the `taxon_identity_*` columns) to JSON;
2. run the `--cloud-rows` dry run above against `tax-2026.09.23-01`;
3. review the artifact and record the gate.

## Rollback

The repair is narrow by construction, which is what makes rollback simple: it
writes `genus`, `species`, `sporely_taxon_id` and the seven `taxon_identity_*`
columns, and nothing else. It touches no other table.

Before applying:

1. take a full copy of the database (`VACUUM INTO 'pre-stage4.sqlite3'` locally;
   a point-in-time-recovery marker plus a table snapshot for production);
2. record the pre-image of exactly the repairable rows, keyed by observation id,
   from the dry-run artifact's own `stored_identity` and `stored_names` blocks —
   the artifact **is** the pre-image, which is why it must be archived rather
   than regenerated.

To roll back, restore those ten columns for the listed observation ids from the
archived artifact. Restoring the whole database is also safe but unnecessary and
loses unrelated work done since.

The repair runs in one transaction and rolls back on any error, so a failed run
leaves nothing partially applied.

## Integrity checks

Run before and after; every one must be unchanged.

**Observation content.** Every column of every audited observation other than
the ten listed above. `tests/test_observation_917_and_53482_regression.py`
pins this as an automated check by diffing the full row.

**Media.** Row count and per-row content digest of `images`, including
`filepath`, `cloud_id`, `image_type`, `micro_category`, `sort_order`,
`objective_name`, `scale_microns_per_pixel` and `calibration_id`. Media
associations are keyed on `observation_id` and `calibration_id`, neither of
which the repair can write.

**Measurements.** Row count and content digest of `spore_measurements` and
`spore_annotations`, plus the calibration rows they reference.

**Mosaic / spore atlas.** `mosaic_signature` and the publication-selection
state on the observation row. An unchanged signature must stay unchanged: a
taxonomy repair is not a media change and must not cause a mosaic rebuild.

**Visibility.** `sharing_scope`, `spore_data_visibility` and
`location_precision` are independent of identity and must not move. A repair
that widened an audience would be a privacy incident, not a taxonomy one.

**Historical snapshots.** `taxonomy_v3.identification_snapshot` and
`taxonomy_v3.resolution_link` row counts and contents. A later correct
selection does **not** rewrite history: observation 917's snapshot recorded
`no_identity_evidence` and that remains true of the moment it describes. The
new choice is recorded as the current selected identity beside it.

## Idempotence

A second run over a repaired database proposes nothing and writes nothing: a
bound row now classifies as `proven_sporely_identity`, a restored name as
`name_intact`. The name write additionally carries its own `IS NULL` guard, so
even a stale audit artifact cannot overwrite a name that appeared between the
dry run and the apply. Rows that were already correct before the first run are
never touched by any run.

Re-running is therefore safe, and re-running is the recommended way to verify
an apply: the second audit's counts are the post-condition.

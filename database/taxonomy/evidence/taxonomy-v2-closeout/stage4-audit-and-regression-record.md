# Stage 4 — audit, repair and end-to-end regression record

Closeout Stage 4, 2026-09-23. Operational detail lives in
`docs/taxonomy-identity-repair-runbook.md`; this file records what was built,
what was verified against what, and what remains blocked.

## What was built

| Artifact | Role |
|---|---|
| `database/audit_observation_identity.py` | Part A dry-run audit, the proven-only repair, and the Part B gate as code |
| `docs/taxonomy-identity-repair-runbook.md` | Part B gate conditions, rollback, integrity checks, old/new identity semantics |
| `tests/test_observation_identity_audit.py` | audit determinism, refusal rules, idempotence, transactional rollback, gate |
| `tests/test_observation_917_and_53482_regression.py` | Parts C and D end to end |

The repair writes ten columns and no others: `genus`, `species`,
`sporely_taxon_id` and the seven `taxon_identity_*` columns. No other table is
reachable from it.

## Corrections applied after the first sparring review

Candidate `8021424` was sent back with three bounded safety defects. All three
are fixed; each has a test that fails against the old behaviour.

| Defect | Fix |
|---|---|
| The Part B gate was consulted only by tests — CLI `--apply` called `apply_repairs` directly, bypassing all five conditions | `gate` is now a required argument of `apply_repairs`, which calls `require_open_gate` itself before touching the database. The CLI exposes one flag per condition and defaults closed. `counts_reconcile` is additionally re-checked against the report rather than taken on assertion. |
| The identity write's `WHERE` clause checked only the observation id, so a newer valid identity chosen between audit and apply could be silently overwritten | The update now matches the full pre-image the audit recorded, using `IS` so NULL compares as a value. A mismatch raises `StaleAuditArtifact` and aborts the whole transaction, because the artifact is also the rollback pre-image. Name restoration keeps its `IS NULL` guard and skips instead of aborting — filling a null destroys nothing — but the skip is now counted in `names_skipped_changed_since_audit` rather than silent. |
| `_classify_name` required only `genus` and `species` null, while the stage defines the population as `genus`, `species` **and** `common_name` all null, so proposed writes exceeded the specified population | All three fields must now be null. A row that kept its common name is classified `partial_name_loss_reported` — reported, never repaired — so it is neither repaired out of scope nor hidden inside `name_intact`. The test that previously supplied a non-null `common_name` while permitting repair now asserts the row is reported and its binomial left null. |

## Verified against the real Stage 3 candidate

`desktop-tax-2026.09.23-01.sqlite3`, release `tax-2026.09.23-01`, from the
Stage 3 build recorded in `stage3-candidate-verification.md`:

| Check | Result |
|---|---|
| `resolve(nortaxa, nortaxa_taxon_id, 53482)` | `{7821}` |
| `resolve(nortaxa, nortaxa_taxon_id, 52369)` | `{83668}` |
| `taxon 7821` scientific name | `Entoloma conferendum` |
| vernaculars on 7821 | `stjernesporet rødspore` (nb), `stjernespora raudspore` (nn) |

Pinned by `test_the_real_stage_3_artifact_backs_the_reconstruction`, which
skips when the build output is absent so the suite stays portable. Every other
assertion runs against an in-repo reconstruction of the same four mappings, so
the portable tests and the real artifact are tied together rather than drifting.

Running the audit against that artifact with an observation shaped like 917
classifies it `proven_external_with_unique_bridge` +
`name_loss_repairable_from_row`, candidate `7821`, evidence
`provider_snapshot_identifier: nortaxa/nortaxa_taxon_id/53482`.

## Corrections applied after the second sparring review

Candidate `06a80a7` was sent back with two further defects. Both are fixed.

| Defect | Fix |
|---|---|
| The CLI regenerated a fresh report and applied that; `--gate-evidence` was an unchecked string, so the reviewed artifact was never loaded or compared. A row added after review could be classified and repaired unreviewed, and the per-row guard passed trivially because its pre-image came from the same invocation. | `--apply` now **takes the reviewed artifact** and applies that file. Three independent checks bind the write to it: the artifact's own `digest` (catches post-review edits), `require_artifact_still_describes` (re-audits the live rows and demands the reviewed artifact be reproduced exactly — the only check that can see *added* rows), and the per-row pre-image (catches a change between that re-audit and the write). |
| The stale-name test let a record bind concept 7821 while its name had become *Amanita muscaria*, committing a name/identity mismatch. | The identity pre-image now includes `genus`, `species` and `common_name`, which `docs/supabase-sync-contract.md` couples to the bound concept. A coupled bind-and-restore record whose name moved aborts; a name-only repair still skips, which is safe because filling a null destroys nothing. The old test is split into the two cases. |

Verified end to end through the CLI against the real Stage 3 artifact:

| Scenario | Result |
|---|---|
| dry run | artifact written, digest `ee15b5c734bae162` |
| row 918 added after review, then apply | `NEEDS_YOU — … added observations [918]`, exit 1, both rows untouched |
| reviewed state restored, gated apply | `identities_bound: 1, names_restored: 1`; 917 → `Entoloma conferendum` / `7821` / `external_id_resolution` |
| replay the same artifact | `NEEDS_YOU — … changed [917]` |
| artifact edited to widen a refusal into a repair | `NEEDS_YOU — … does not match its own digest` |
| apply without the gate flags | `NEEDS_YOU — production migration gate is closed` (checked before the artifact is even read) |

## Part C — observation 917

Built on the real schema (`database.schema.init_database`) with a field image,
a microscope image carrying objective/scale/calibration/mount/stain metadata,
three spore measurements, a spore annotation and an active calibration; private
sharing scope and private spore-data visibility.

| Requirement | How it is verified |
|---|---|
| identity fields before/after | full row read; pre-state matches the plan's recorded cloud state |
| current selection through the corrected bridge | audit binds `7821` with `external_id_resolution` proof, source tuple retained |
| immutable W3 snapshot | repair + push emit no write naming `identification_snapshot` or `resolution_link`; `ai_selected_*` history unchanged |
| desktop save/reload | reread through `ObservationDB.get_observation` → `TaxonIdentity.from_row` |
| desktop → cloud | real `push_observation`; exactly one `set_observation_selected_taxon_v2(917, 7821)` |
| cloud → desktop | real `_apply_remote_observation_fields`; identity and repaired name both survive a remote row that still carries nulls |
| media, image metadata, measurements, calibration | per-table content digests, byte-identical across the repair and across the pull |
| mosaic / publication state, visibility | covered by the observation-row column diff; `sharing_scope` and `spore_data_visibility` asserted by name |
| no unrelated content changed | changed columns ⊆ the ten repairable ones |

The pull test is the one that earns its place: it applies a cloud row still
holding the pre-migration nulls, which is exactly the window in which a naive
pull would walk the repair back.

## Part D — `Entoloma conferendum` / `NBIC:53482`

| Step | Result |
|---|---|
| 1 — normalization | `NBIC:53482` → `(nortaxa, nortaxa_taxon_id, 53482)`, raw retained |
| 2 — bridge resolution | `{7821}` against the Stage 3 candidate |
| 3 — saved name | `genus = Entoloma`, `species = conferendum` |
| 4 — vernacular | both Norwegian names present on 7821 |
| 5 — not unidentified | desktop half only: the row carries a binomial, and `external_unresolved` is pinned as a state distinct from `no_identity_evidence` |
| 6 — round trip | repair → push → pull → re-push is a no-op |
| 7 — deprecated / malformed | both sentinel nesting levels dropped for the same taxon; four malformed candidate shapes each leave a committed identification byte-identical |

## Blocked

**Part B production migration: `NEEDS_YOU`.** No cloud export exists, because
reading `public.observations` needs an authorized Supabase session and the
Supabase MCP server was unauthorized in this session. Gate condition 1
(reviewed dry-run artifact) therefore cannot be met, and the gate fails closed.
The unblocking steps are in the runbook.

**Part D step 5, `sporely-web` rendering.** That client lives in another
repository and is out of this worktree's scope. The desktop-side contract it
must honour — unresolved identity is a displayed state, not an absence — is
pinned here.

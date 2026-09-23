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

# NorTaxa self-identity resolution

Written 2026-10-18 against `main` @ `6cd5872`. Revised the same day after an
independent planner review. Nothing in this plan is approved or started by
writing it. No bulk database repair, taxonomy publication or cloud write is
authorized by it.

## What this plan is for

A published NorTaxa-only Sporely concept (an anchor whose canonical source
usage is NorTaxa) does not resolve through its own NorTaxa identifier. For
example, `NBIC:52330` resolves to nothing, although concept `624659`
(*Panaeolina foenisecii*) is the concept anchored by exactly that NorTaxa usage.
The resolver (`database/taxon_lookup.py` `namespaced_external_id_matches`,
`resolve_installed_external_identity`) reads only
`taxon_external_id_text_min`. Anchors carry no bridge evidence class, so
`bridge_emission.py` emits no row for them.

The goal is that a concept resolves through the namespaced identifier of the
source usage that anchors it, without any NorTaxa-to-COL bridge.

**What this does not mean.** A concept's own source identity says which concept
it is. It says nothing about equivalence with any other concept, and so does
not touch `mapping_policy.yml` `automatic_identity`, which governs equivalence
between concepts. This plan changes no bridge, no policy rule and no
supersession, and no concept is merged, aliased or re-keyed by it.

## Evidence (read-only audit, 2026-10-18)

- The installed release `tax-2026.10.07-01` is the sole active production
  release (`docs/plans/completed/2026-10-04-norwegian-vernacular-only-enrichment.md`,
  line 3). Its `taxonomy_meta` says `state=candidate` and `publication=none`
  because those values are copied at freeze time
  (`promote_desktop_bundle.py:259`). The live state is confirmed only by
  `select release_id,status from public.taxonomy_v2_releases` (not run).
- The release has 12,566 NorTaxa anchors, all `taxonomic_status=valid`. Each
  has `canonical_source_system='nortaxa'` and `canonical_external_id=<taxonID>`.
  The namespace is **not stored**, so the column is namespace-lost data:
  `build_sqlite_candidate.py:571` writes `canonical_source_usage["identifier"]`
  and drops `namespace`. The frozen evidence
  (`tax-2026.10.07-01.evidence.tar.gz`, `compiler/source_usages.jsonl`) records
  every one of the 12,566 anchor usages as namespace `nortaxa_taxon_id`.
- **No conflict in the active release.** No `(nortaxa, nortaxa_taxon_id, X)`
  row in `taxon_external_id_text_min` has an `X` equal to an anchor's
  `canonical_external_id`, and no two anchors share one. A union of the two
  sources therefore changes no existing resolution in 10.07.
- The ID registry is append-only, and 10.07 allocated no new IDs. Approved
  supersessions remove the retired anchor from `taxon_min` and emit
  `reviewed_supersession` rows for its NorTaxa tuple.
- The cloud export includes NorTaxa anchors as concepts
  (`database/taxonomy/docs/cloud-export-contract.md:44`). The selected-taxon
  RPC accepts any ID in the active release.
- `PINNED_RELEASE_EXPECTATIONS` (`database/taxonomy/cloud_export.py:469-473`)
  already pins 10.07 by `content_release_id` and `sqlite_sha256`. The install
  receipt (`install_receipt.json` beside the installed SQLite; written by
  `utils/taxonomy_v2.py`) records the same `sqlite_sha256`.
- In the owner's local database (637 observations), about 200 observations
  carry a NorTaxa provider tuple whose own anchor has the same name as the
  observation. **20 observations carry the Artsorakel suggestion's tuple for
  a different taxon than the one the owner chose** (6 different species, 14
  aggregate vs. strict species, e.g. AI *Russula sanguinea* → owner
  *R. emetica*). Those must never be rebound to the suggestion.
- The live picker keeps the two apart. A manual edit after accepting a
  suggestion clears the identity but keeps every `ai_selected_*` field
  (`ui/taxon_input_controller.py:1015-1065`). Two batch tools read
  `ai_selected_taxon_id` as identity evidence:
  - the audit's `provider_snapshot_identifier` path in
    `database/audit_observation_identity.py`;
  - `database/migrate_observations_sporely_id.py`, which calls
    `namespaced_external_id_matches` directly (`:144-160`).
- Nothing handles cloud RPC error 22023 specifically. A refused
  `set_observation_selected_taxon` (`utils/cloud_sync.py:7841-7850`) fails
  that observation, which stays dirty and is retried on each sync.
- The desktop has no forward mapping for an ID a later release supersedes:
  `utils/cloud_sync_impl/identity_state.py:22-47` only checks membership.

## Decisions for the owner

- **D1 — Source of the namespace.**
  - (A, recommended now) Resolve through `taxon_min.canonical_source_system`
    / `canonical_external_id`, but **only** when the installed artifact is the
    pinned one: the receipt's `content_release_id` and `sqlite_sha256` equal
    `PINNED_RELEASE_EXPECTATIONS`. The pin carries the declaration
    `canonical_namespace[nortaxa] = nortaxa_taxon_id`, proved by a test
    against the frozen evidence. Hash checks come from the receipt; nothing
    hashes the 340 MB file at resolve time. Without a receipt (dev or test
    databases, the audit's search-pack format) the path is closed. The audit
    proves the pin itself by hashing its `--taxonomy` input once per run.
  - (B, durable, later) The release emits the declaration (a `taxonomy_meta`
    key) or namespaced self-identity rows. This needs a new release, a cloud
    import and the owner's publication gate. See Follow-ups.
- **D2 — Record the source tuple for picker-selected NorTaxa anchors.** This is
  only needed for a future supersession carry-forward (Stage 6). Recommended:
  record it, as its own stage (Stage 5), without changing the proof or the
  default output of `TaxonIdentity.from_taxonomy_v2_artifact`.
- **D3 — When an AI snapshot is identity evidence.** Recommended: only when the
  owner's genus+species equals `ai_selected_scientific_name` *and* equals the
  resolved concept's canonical name, exactly. Synonym or aggregate (`coll.`,
  `var.`) differences are never agreement.
- **D4 — Web and Android parity.** sporely-web resolves provider IDs through
  `taxonomy_v2_external_ids` only. Parity is a separate sporely-web stage
  after D1(B), or after a web equivalent of D1(A).

## Invariants held by every stage

- Bridges, `mapping_policy.yml`, `concept_supersessions.yml`, the registry and
  every published artifact are unchanged.
- Namespace-lost data is never identity evidence on its own:
  - `taxon_external_id_min` and the integer `norwegian_taxon_id` column are
    never read for identity;
  - `taxon_min.canonical_external_id` is read only through the pinned,
    evidence-proven declaration (D1).
- Out of scope: the name-based `_lookup_external_taxon_text_id_from_db`
  (`database/models.py:1176-1225`) is not identity resolution and is not
  changed.
- A tuple naming more than one concept, by bridge and by self-identity
  combined, resolves to nothing.
- A scientific name, a vernacular name or a number that happens to be equal is
  never identity evidence.
- `tests/fixtures/cloud_sync_taxonomy_identity_golden.json` is unchanged. If a
  stage would change it, the stage stops and reports, because the cloud-sync
  orchestration plan holds that fixture as its invariant.
- No change to `sync_key` or record-identity code. That belongs to the
  canonical sync-key plan (sporely-web `docs/plans/active/2026-10-14-sync-key-identity.md`).
- No bulk repair of existing observations. No sync-behaviour change in
  Stages 1–5.

## Stage 1 — Contract text

Docs only.

- `database/taxonomy/docs/identity-contract.md`: a "same-source canonical
  identity" section stating that:
  - a concept's own anchoring usage resolves that concept only;
  - `canonical_external_id` is namespace-lost and is used only through a
    pinned declaration proved against the release's frozen evidence;
  - it is never equivalence evidence;
  - bridges and supersessions are unchanged.
- D3 stated as a rule: an AI or provider snapshot is a suggestion, and becomes
  identity evidence only under exact three-way name agreement.
- `mapping_policy.yml`: no change. Record that in the stage notes.
- `docs/supabase-sync-contract.md`: no change for Stages 1–5. The key
  vocabulary, the guarded RPC and the pull table are untouched.

## Stage 2 — AI suggestions are not identifications (batch tools)

Self-verifiable. Lands before any resolver change.

- Apply D3 in `database/migrate_observations_sporely_id.py` and in the audit's
  provider-snapshot path. Search for every other reader that derives identity
  from `ai_selected_*`; do not assume these two are the only ones.
- Tests built from the 20 owner cases: different species, `coll.` vs. strict,
  and `var.` all classify as "suggestion, not identification" and are never
  proposed for binding. An agreeing case still binds.

## Stage 3 — Resolver: same-source canonical identity

Tests are self-verifiable. The runtime effect on the live picker is
human-gated (checklist below).

- A **separate** function, for example `source_identity_matches(conn, *,
  source_system, namespace, external_id, release_pin)`, kept out of
  `namespaced_external_id_matches`, so callers opt in deliberately.
  `resolve_installed_external_identity` and the audit's artifact loader use
  the union of the two functions. `migrate_observations_sporely_id.py` opts in
  only after Stage 2.
- The pin is read from the receipt beside `db_path` (plain JSON;
  `database/` gains no import of `utils/`) and compared with
  `PINNED_RELEASE_EXPECTATIONS`, which stays the single source of the ID and
  hash. In the search-pack format the path is closed, and the plan notes that
  the audit's `namespaces_holding_external_id` collision signal does not see
  anchor IDs.
- Provenance names the path. `InstalledTaxonConcept` gains the resolution
  path, and `ui/taxon_input_controller.py:993` stops hardcoding
  `…:taxon_external_id_text_min`. The proof stays `external_id_resolution`.
- Pinned-release probe (it needs the installed artifact; if the artifact is
  absent the probe reports **skipped**, never a pass). It checks:
  - the namespace declaration against the frozen evidence;
  - the bridge-versus-anchor conflict count, which must be 0 (it is 0 today).
- Fixture tests:
  - an anchor resolves through its own tuple;
  - bridge resolutions are unchanged;
  - a bridge to concept A plus an anchor B gives `None`;
  - a missing receipt, an unpinned release or a hash mismatch gives `None`;
  - the `nortaxa_dwc_id` and `col_usage_id` namespaces are unaffected;
  - a superseded anchor falls through to its `reviewed_supersession` bridge.
- Human check:
  1. In the GUI, pick an Artsorakel suggestion for a NorTaxa-only species,
     e.g. *Panaeolina foenisecii*. It binds, and the Red List badge and name
     display are unchanged.
  2. Pick the suggestion, then change the species before saving. The saved
     observation has the owner's species and no identity from the suggestion.

## Stage 4 — Persistence and cloud round trip

Unit tests are self-verifiable. The live round trip is human-gated and not
authorized here.

- Persistence: the typed identity write (`database/models.py` ~1404) is
  unchanged. Test that a self-resolved identity stores the tuple, raw value,
  provenance and proof, and that `is_proven_sporely` is true.
- Push: a self-resolved identity goes through the guarded RPC like any proven
  identity. **No new refusal handling.** The cloud's 10.07 export contains the
  anchors. A refusal keeps today's documented behaviour (the observation fails
  and stays dirty); record that in the stage notes. A release mismatch belongs
  to Stage 6.
- Pull: the cloud echo of the same ID never downgrades the local proven
  identity (contract, "Cloud → desktop taxonomy identity", common rule 1).
- The golden fixture stays byte-identical. Mocked transports prove only how
  the payload is built, never what the server does.
- Human check:
  1. One owner observation with a self-resolved NorTaxa-only identity syncs.
  2. It shows its name on the web.
  3. A second device pulls it as `cloud_selected_unverified` for the same ID.

## Stage 5 — D2: tuple beside picker-selected anchors

Self-verifiable for the data shape; the picker check is human-gated.

- A picker selection of a NorTaxa anchor stores the anchor's own NorTaxa tuple
  beside the Sporely ID. The proof stays `taxonomy_v2_artifact`. The default
  output of `from_taxonomy_v2_artifact` is unchanged.
- Checks:
  - the golden fixture is unchanged;
  - the push payload is unchanged (the RPC carries only the ID);
  - change detection still keys the identity as `sporely:<id>`.

## Stage 6 — Supersession carry-forward

Design decision only. Not implementable until reviewed.

- The gap is real. Re-resolving a stale ID from its stored tuple would create
  an identity the owner never selected, and the next sync would push it.
  Before any implementation, a decision document must settle:
  - the cloud release lagging behind the desktop release, or the reverse
    (RPC 22023);
  - the push overwriting a web-side identity (baseline conflict,
    `identity_push.py:136-160`);
  - whether a supersession rebind counts as owner-selected under
    `docs/supabase-sync-contract.md`;
  - how the change is surfaced to the owner.
- The decision needs a cross-repo review at a higher tier. Any implementation
  updates the identity sections of the sync contract in **both** copies, on
  their own branches, kept out of the orchestration plan.

## Interactions with ongoing work

- **Cloud-sync orchestration**: unmerged branch `plan/cloud-sync-orchestration`
  (2026-10-09), 73 commits behind `main`. It predates the sync-key landing on
  `main` (2026-10-14 to 10-17), so it needs refreshing before it runs; that is
  separate from this plan. Its invariant is the taxonomy identity golden.
  Stages 1–5 leave the identity classification and the golden untouched; the
  golden's helpers do not consult the artifact
  (`tests/test_cloud_sync_taxonomy_identity_golden.py:31-34`). Stage 6 is the
  only stage that could interact with orchestration, and it stays a design
  stage until the owner sequences it against that plan.
- **Extraction run S1–S7**: complete and accepted. No interaction.
- **Sync-key identity** (sporely-web canonical plan): record identity, not
  taxonomy identity. No shared code.
- **`feature/unified-taxon-search`** (unmerged, 7 commits ahead, 66 behind,
  rewrites `search_taxa` in `database/taxon_lookup.py`): it touches only the
  export list next to the resolver functions. Expect a small textual conflict.
  Merge order is the owner's choice.
- **Desktop release gate**: any desktop build carrying Stages 3–5 remains
  subject to the owner's release decisions.

## After this plan

1. Rerun the 637-observation audit (read-only) with Stage 2 and Stage 3
   applied.
2. Prepare a reviewed bulk-repair plan, which needs separate authorization:
   - backup;
   - deterministic dry run;
   - owner approval of the artifact;
   - the existing gated `--apply`;
   - post-apply audit and idempotency check.

## Follow-ups (separate plans)

- D1(B): a release-side namespace declaration or self-identity rows, plus a
  supersession table for the desktop, under the owner's publication gate.
- sporely-web parity (D4).
- The taxonomy reconciliation stage for synonym-authorship normalization
  (second plan).

## Not authorized by this plan

- Bulk repair of observations.
- Publishing any taxonomy release.
- Cloud writes or live Supabase queries by agents.
- Changes to bridges, supersessions or mapping policy.
- Implementing Stage 6.

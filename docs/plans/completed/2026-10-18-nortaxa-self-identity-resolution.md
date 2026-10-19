# NorTaxa self-identity resolution

**Closed 2026-10-19.** Tests 1, 2a and 2b passed, and the deferred NorTaxa
round trip (Test 3) passed as the live canary on observation 1032 (see the
identity backfill plan). The cloud fallback for NorTaxa-only concepts was added
under option A. The Species-dropdown follow-up below is open and recorded in
`INBOX.md`.

Written 2026-10-18 against `main` @ `6cd5872`, then simplified the same day
under the owner's taxonomy principles: Sporely is not a taxonomic authority,
and the solution is kept proportional to the problem. Nothing is approved or
started by writing this plan. No bulk repair, taxonomy publication or cloud
write is authorized by it.

## The user-facing problem

When an observer accepts an Artsorakel suggestion for a species that exists in
the taxonomy only as a NorTaxa concept, the observation is saved as
`external_unresolved`. It gets no Sporely concept, so the identity is not
synced to the cloud. For example, `NBIC:52330` does not resolve, although
concept `624659` (*Panaeolina foenisecii*) is the concept built from exactly
that NorTaxa record.

The cause: `resolve_installed_external_identity` (`database/taxon_lookup.py`)
looks only at approved bridge rows in `taxon_external_id_text_min`. A
NorTaxa-only concept has no bridge row, because it needs none: its own record
is its identity. That record sits in `taxon_min.canonical_source_system` /
`canonical_external_id`.

## The change (one stage)

1. In `resolve_installed_external_identity`, when the approved-bridge lookup
   finds nothing for `(nortaxa, nortaxa_taxon_id, X)`, resolve to the concept
   with `canonical_source_system='nortaxa'` and `canonical_external_id=X`, if
   exactly one exists. Nothing else changes:
   - existing bridge resolutions take precedence;
   - other namespaces are untouched;
   - no name matching and no use of the legacy integer tables.
2. Record the path in provenance (`…:taxon_min.canonical_source_usage` instead
   of the hardcoded `…:taxon_external_id_text_min` at
   `ui/taxon_input_controller.py:993`). This keeps the identity's origin
   visible. The proof stays `external_id_resolution`.
3. One paragraph in `database/taxonomy/docs/identity-contract.md`:
   - a NorTaxa-only concept resolves through its own NorTaxa ID;
   - `canonical_external_id` of a NorTaxa anchor is its `nortaxa_taxon_id`
     (this is how `build_sqlite_candidate.py` writes it);
   - this says nothing about equivalence with any COL concept.

**Out of scope for this stage:** the batch tools
(`database/migrate_observations_sporely_id.py`, the audit). They call the
bridge lookup directly and are unaffected.

## Why this is safe

- **Your identifications are preserved.** The live picker resolves a
  suggestion only when you accept it. Editing the species afterwards clears
  that identity and keeps the AI fields only as a record of the suggestion
  (`ui/taxon_input_controller.py:1015-1065`). Nothing here rebinds an existing
  observation.
- **No equivalence is asserted.** No bridge, mapping policy, supersession or
  published artifact changes.
- **Nothing existing changes in the active release.** In `tax-2026.10.07-01`,
  no approved NorTaxa bridge uses an ID that is also a NorTaxa-only concept's
  own ID, and no two NorTaxa-only concepts share an ID. The frozen release
  evidence records all 12,566 NorTaxa-only concepts as `nortaxa_taxon_id`.
- **No sync behaviour changes.** The cloud's active release (10.07) contains
  these concepts. The resolved identity is an ordinary proven identity, and
  the taxonomy identity golden fixture is unaffected.

## Verification

Self-verifiable:
- Fixture tests:
  - a NorTaxa-only concept resolves through its own ID;
  - a bridged ID still resolves through its bridge;
  - two candidate concepts give no resolution;
  - other namespaces are unchanged;
  - an AI suggestion followed by a species edit saves no identity from the
    suggestion.
- One probe against the installed release, which reports *skipped* when the
  artifact is absent: `NBIC:52330` resolves to 624659, and no approved bridge
  ID collides with a NorTaxa-only concept's own ID.

Human-gated (the user's interaction changes, so this waits for the owner
before commit):
1. Accept the Artsorakel suggestion *Panaeolina foenisecii* on a new
   observation. It binds; the name and Red List badge show as before.
2. Accept a suggestion, then change the species before saving. The saved
   observation keeps your species and no identity from the suggestion.
3. Sync that observation. It shows its name on the web.

## Verification record (2026-10-18)

Owner's desktop app on this branch, observation 1032. Values were read from
the local database after quitting Sporely.

- **Test 1 — passed.** The Artsorakel suggestion `NBIC:52330` bound 624659:
  - proof `external_id_resolution`;
  - provenance `…:taxon_min.canonical_source_usage`;
  - Red List `LC` with snapshot `{"NO": "LC"}`.
- **Test 2a — passed.** After the species was changed by hand, the identity
  was empty and `ai_selected_taxon_id` stayed `NBIC:52330` as history.
- **Test 2b — passed.** An explicit Scientific-dropdown pick bound 40672
  (proof `taxonomy_v2_artifact`, its own tuple).
- **Test 3 (cloud round trip) — deferred, not passed.** It needs the owner's
  authorization for a live sync.

Two defects were found and fixed during these checks:
1. The Artsorakel API carries its ID only as `scientific_name_id`. The copy
   path never read that key, so 48 of the owner's picks stored no ID.
2. The identity commit cleared the suggestion's own Red List snapshot.

Both have regression tests built from the real response shape.

## Interactions

- **Cloud-sync orchestration** (unmerged `plan/cloud-sync-orchestration`):
  none. No sync code changes, and its golden fixture is untouched.
- **`feature/unified-taxon-search`** (unmerged): it touches the export list
  next to this function in `database/taxon_lookup.py`. Expect a small textual
  conflict.

## After this change

1. Rerun the 637-observation audit (read-only). Before any repair, the audit
   and repair tools must treat an AI snapshot as evidence only when your
   genus+species, the AI's name and the concept's name agree exactly. The 20
   observations where you chose a different species, or a strict species
   instead of an aggregate, are never rebound.
2. A reviewed bulk repair, authorized separately: backup, dry run, your
   approval, the existing gated apply, then a re-audit.

## Follow-up found during the manual check (owner-approved for later)

Picking a species from the **Species** field's dropdown stores no identity,
although the list entry (`TaxonChoice`) carries a `taxon_id`
(`ui/taxon_input_controller.py` `on_species_selected` fills in text only).
Only a pick in the **Scientific** dropdown binds. Observers reasonably read
both as a selection. Fix: bind the clicked entry's own concept ID, exactly as
the Scientific pick does. Where the list shows two same-name concepts, bind
only the entry clicked and never choose between them. A separate small stage
after this one, with its own human check.

## Deferred until a user-facing problem is demonstrated

- Web and Android parity. The web resolves provider IDs through approved
  bridges only. Take this up when it shows up for web users.
- Carrying identities forward after a future supersession of a NorTaxa-only
  concept. No supersession is planned.
- A release-side namespace declaration or self-identity rows.
- Broad NorTaxa/COL duplicate and synonym reconciliation.

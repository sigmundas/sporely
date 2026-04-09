# Community Spore Data Plan

Status: active
Owner: desktop + Supabase/web
Last updated: 2026-04-06

This file is the working checklist for community spore-data search, review, and import.
Update this document as work is completed and check items off in place.

## Product direction

Goal: let users discover and import useful spore stats and spore measurements from other users and public sources, while preserving privacy for observation/location details.

Current direction:

- Add a dedicated `Cloud...` button beside `Source:` in the Analysis reference panel.
- Open a full review dialog before import, with QC details.
- Support genus-only search to browse all species in a genus.
- Treat spore data visibility separately from observation visibility — default is **public**.
- Exact location and private observation content always remain protected regardless of spore visibility.

## Core decisions

- [x] Use a dedicated cloud-review dialog instead of extending the `Source` dropdown.
- [x] Keep species search aligned with the existing reference-data search flow.
- [x] Support genus-only search (species is optional).
- [x] Support import after review, not direct fetch-on-select.
- [x] Split privacy into:
  - observation visibility
  - location visibility
  - spore-data visibility (separate field, default public)
- [x] Aim for public spore stats/reference/raw data by default, without exposing exact location or private notes.

## Scope

In scope:

- Desktop search/review/import flow for community spore data.
- Supabase schema and SQL/RPC support for public spore datasets and reference values.
- Per-observation spore-data visibility control in the desktop Analysis tab.
- QC metadata so users can inspect how measurements were produced before import.

Out of scope for first pass:

- Full moderation/reporting tooling.
- Rich social features beyond public/friends data access.
- Automatic trust scoring that hides low-quality data without user review.

## Workstreams

### 1. Product and data model

- [x] Finalize privacy model: observation visibility / location visibility / spore-data visibility are separate fields.
- [x] Define what becomes public spore data: measurements + aggregate stats + calibration summary + method. No location, no private notes, no private observation IDs.
- [x] Contributor identity: `community_contributor_label` SQL helper — public display name or anonymous-stable alias depending on profile settings.
- [x] Local import types: import summary as local reference, use raw points for plot only.

### 2. Supabase schema

- [x] Review current cloud schema and existing observation/image sync model.
- [x] Add `spore_data_visibility` column to `public.observations` (values: `private`, `friends`, `public`; default `public`).
- [x] Add `spore_data_visibility` to local SQLite `observations` table (migrated automatically).
- [x] Confirm fields for: taxon, contributor label, dataset type, mount medium, stain, sample type, contrast, objective, scale/calibration, raw measurements, aggregate stats, QC flags, timestamps.
- [x] Ensure exact GPS and private notes excluded from public RPC outputs.
- [x] Define indexes for taxon search and dataset lookup.
- [x] Prepare and apply migration SQL: `database/supabase_spore_community_batch1.sql` (applied 2026-04-05).

### 3. Supabase SQL and RPC functions

- [x] `search_community_spore_datasets(p_genus, p_species, p_limit)` — species is optional; empty string matches all species in the genus.
- [x] `get_community_spore_dataset(p_observation_id)` — full dataset for review.
- [x] `community_spore_taxon_summary(p_genus, p_species)` — aggregate stats; species optional.
- [x] `search_public_reference_values(p_genus, p_species, p_limit)` — species optional.
- [ ] Return QC metadata in RPC responses.
- [ ] Return only safe/public fields (verify no location/note leakage).
- [ ] Define pagination and sort options (best quality, newest, highest n, official first).
- [ ] Test RPCs against: public datasets, friends/private observations with public spore data, taxa with no public data.

### 4. Quality control model

- [ ] Define QC signals for desktop review:
  - calibrated or not
  - calibration age/date
  - calibration precision / CI if available
  - objective/profile present
  - mount/stain present
  - point geometry present
  - sample size `n`
  - presence of microscopy evidence
  - curated reference vs ad hoc observation
- [ ] Decide whether to compute a server-side quality score.
- [ ] If using a score, define explainable sub-signals shown to the user.

### 5. Desktop app: reference panel entry point

- [x] Add a dedicated `Cloud...` button beside `Source:` in the Analysis reference panel.
- [x] Prefill genus/species from the current reference panel.
- [x] Reuse existing reference species search/completer logic where possible.
- [x] Disable the button gracefully when not logged in, with a useful hint.

### 6. Desktop app: cloud review dialog

- [x] Create `CloudReferenceDialog`.
- [x] Add genus + optional species search (genus-only shows all species in genus).
- [x] Results table shows `Genus species – source label` so species are distinguishable in genus-wide results.
- [x] Add review tabs/panels: Summary, Raw spores, Method, Calibration, Provenance.
- [x] Show aggregate stats: min / median / max, mean where available, n, Q metrics.
- [x] Show QC badges and short explanations.
- [x] Show provenance: contributor, dataset source, date, license, public/private-safe explanation.
- [x] Show calibration details where available.
- [x] Fix crash when closing dialog while search/detail worker thread is running.
- [ ] Decide whether to preview cloud microscopy images in first pass.

### 7. Desktop app: import behavior

- [x] `Import summary as reference`.
- [x] `Use raw points for plot`.
- [x] Preserve provenance for imported summary references (cloud dataset id, contributor, license, QC summary, timestamp in `metadata_json`).
- [x] Preserve temporary provenance for plotted raw-point datasets.
- [x] Give imported cloud summaries a distinct `Cloud: ...` source label.
- [ ] Add stronger visual distinction for cloud-origin imported sources.

### 8. Desktop app: local storage updates

- [x] Extend local reference storage with `metadata_json` for cloud provenance.
- [x] Store: cloud dataset id, contributor label, source/provenance, license, QC summary, imported timestamp.
- [x] Add `spore_data_visibility` to local `observations` table (default `public`, synced to/from cloud).
- [ ] Ensure imported references still work offline.

### 9. Desktop app: spore-data visibility UI

- [x] "Spore data sharing" collapsible section added to the Analysis tab sidebar (below Reference values).
- [x] Three options: Public (share with everyone) / Friends only / Private.
- [x] Default is Public for new observations.
- [x] Setting is saved immediately and marks the observation dirty for sync.
- [x] `spore_data_visibility` is included in the cloud push payload and pulled back on sync.
- [ ] Explain in UI that public spore data does not reveal exact location or private notes (tooltip/note text is present; may want a more prominent callout).

### 10. Security and privacy review

- [x] Review current Supabase RLS policies.
- [ ] Verify that public RPCs never expose: exact GPS, private notes/comments, hidden observation IDs, raw image storage paths.
- [ ] Verify that friends/private observation visibility does not block public spore-data export when `spore_data_visibility = 'public'`.
- [ ] Verify that taxa/location sensitivity can override public spore-data defaults if needed.

### 11. Testing

- [ ] Desktop: genus-only search returns multi-species results correctly.
- [ ] Desktop: species prefill and search behavior.
- [ ] Desktop: empty-state and error-state handling.
- [ ] Desktop: import summary flow.
- [ ] Desktop: raw-point plot-only flow.
- [ ] Desktop: offline behavior after import.
- [ ] Desktop: spore-data visibility change syncs correctly on next push.
- [ ] Supabase: RPC correctness and safe-field filtering.
- [ ] Supabase: genus-only queries return results across all species.
- [ ] Privacy regression tests for location/private fields.

## External coordination

These steps require back-and-forth outside the local repo:

- [x] Run Supabase migration SQL: `database/supabase_spore_community_batch1.sql` applied 2026-04-05.
- [x] `spore_data_visibility` column exists in Supabase `observations` table (batch1).
- [ ] Re-run updated RPC functions with genus-only search fix (`p_species = ''` now matches all species). The three affected functions are in `supabase_spore_community_batch1.sql` — re-apply those CREATE OR REPLACE blocks in the Supabase SQL editor.
- [ ] Run `database/supabase_spore_measurements_sync.sql` in the Supabase SQL editor to add `desktop_id`, `user_id` columns + unique index + RLS policies to `public.spore_measurements`. **Required before measurement sync will work.**
- [ ] Verify `spore_data_visibility` is included in the `observations` PostgREST select response (needed for pull-down).
- [ ] Test RPC outputs with real cloud data including genus-only queries.
- [ ] Iterate on schema/RPC shape based on desktop dialog needs.

Notes:
- Supabase work moves in small batches.
- First runnable draft batch: `database/supabase_spore_community_batch1.sql`
- Batch 1 applied successfully on 2026-04-05 after fixing `max(uuid)` aggregation.
- Desktop dialog is wired to Batch 1 RPCs and stores imported cloud-reference provenance in local `reference_values.metadata_json`.
- Genus-only search requires re-running the updated RPC functions (see above).
- Measurement sync (batch 2): `database/supabase_spore_measurements_sync.sql` — adds `desktop_id`/`user_id` columns + unique index + RLS. Desktop app upserts measurements after each image sync pass.

## Suggested implementation order

- [x] Phase 1: finalize privacy/data model
- [x] Phase 2: Supabase schema + RPCs (Batch 1)
- [x] Phase 3: desktop `Cloud...` button + dialog shell
- [x] Phase 4: desktop review/import flows
- [x] Phase 5: desktop spore-data visibility UI (Analysis tab)
- [ ] Phase 6: RPC genus-only fix deployed to Supabase
- [ ] Phase 7: privacy hardening and testing

## Open questions

- [ ] Should public raw spore data always be anonymous by default, or tied to contributor identity?
- [ ] Should public microscopy evidence be optional, recommended, or required for high-trust QC?
- [ ] Should community aggregates mix all public raw datasets automatically, or only curated ones?
- [ ] Should cloud-imported summaries be editable locally, or locked with provenance preserved?
- [ ] Do we want to support public reference-value submissions separately from raw measurement datasets in v1?

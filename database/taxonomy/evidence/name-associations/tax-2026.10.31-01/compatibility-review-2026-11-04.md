# Compatibility review before desktop 0.9.30 (2026-11-04)

Three findings from promoting `tax-2026.10.31-01` were checked before
publishing desktop 0.9.30, as the owner asked. Nothing in the frozen release
or in production was changed. The production checks were read-only (`BEGIN
READ ONLY`, project `zkpjklzfwzefhjluvhfw`, system identifier
`7606130613603586966`, active release `tax-2026.10.31-01`).

## 1. Associations over bridges an earlier review did not approve

**Resolved: keep them. No genuine exceptions.**

- No association overrides an explicit decision to keep concepts separate:
  - `policies/name_association_policy.json` has no `exclusions`;
  - `policies/manual_mappings.yml` has no rejected or non-exact mapping (5,744, all `approved` / `exact`);
  - no usage under a hold in `policies/source_continuity_holds.json` (130 holds) is associated.
- Every unapproved bridge pair is a `needs_review` member of an evidence-class
  manifest (`evidence/taxonomy-v3/stage0/`, `stage2/`, `stage4p/`). This means the
  automatic bridge lacked enough evidence; it is not a decision that the
  concepts differ. The owner's merge-by-name policy (2026-10-31) accepts
  these weaker associations, and records `authorship_differs`,
  `family_differs` and `broader_concept_signal` as flags, not blocks.
- The two named cases are the same species:
  - NorTaxa 56227 *Craterellus tubaeformis* (Fr.) Quél. → 620306
    *Craterellus tubaeformis* (Fr.) Quél. (COL). Same name and authorship;
    only the family placement differs (Cantharellaceae vs Hydnaceae). The
    cloud resolves 56227 to 620306 too.
  - Dyntaxa 236654 *Pholiotina vexans* (P. D. Orton) Bon → 617026
    *Conocybe vexans* P.D. Orton. The same species: Bon's name is a
    recombination of Orton's.

## 2. Six NorTaxa ids that resolve to another concept

**Resolved: legitimate synonym resolutions. No code change.**

Each id is a NorTaxa *synonym*, and its own concept is a synonym-status
concept. NorTaxa 1.292 (`acceptedNameUsageID`) names the accepted id, and the
id resolves to exactly the concept that carries that accepted id:

| NorTaxa id | Name (NorTaxa synonym) | NorTaxa accepted id | Resolves to | Cloud |
|---|---|---|---|---|
| 62451 | *Clavulina cristata* var. *incarnata* | 300363 | 614211 *Clavulina incarnata* | 614211 |
| 67495 | *Ascocoryne solitaria* | 223201 | 154532 *Ascocoryne albida* | 154532 |
| 67718 | *Belonidium leucostomum* | 300252 | 113094 *Solenopezia leucostoma* | out of cloud scope |
| 69728 | *Arctopeltis thuleana* | 300345 | 51346 *Polyozosia thuleana* | out of cloud scope |
| 70320 | *Lecanora thuleana* | 300345 | 51346 *Polyozosia thuleana* | out of cloud scope |
| 70995 | *Megalospora marginiflexa* | 70986 | 114873 *Megaloblastenia marginiflexa* | out of cloud scope |

- The live `resolve_taxon_external_id_v2` (a `STABLE` SQL read) returns the
  same concept as the desktop for the two ids in cloud scope. It returns no row
  for the other four, whose targets are lichens or other taxa outside the
  macrofungi scope; the web leaves those unresolved, as it did before.
- The `-11` desktop resolved these ids to the synonym-status concept. The
  fix proposed earlier (let a concept's own anchor win over association rows)
  would bring that back and make the desktop disagree with the cloud. It is
  not needed.
- **Saved observations:** none use these ids or their own concepts (0 in production
  across `resolved_sporely_taxon_id`, `selected_sporely_taxon_id`,
  `taxon_identity_external_id`, `artsdata_id` and `ai_selected_taxon_id`; 0
  in the owner's desktop database). Stored bindings are never remapped.
- **Artsorakel:** a synonym id is resolved to the accepted concept, which is
  the intended reading.
- **External publishing:** Artsobservasjoner ids come only from
  `id_role = 'accepted'` rows (`database/models.py`
  `_adb_ids_for_concepts`), so a synonym id is never published.
- `tests/test_nortaxa_self_identity_resolution.py` pins each id, its accepted
  id and its target.

## 3. Older species lists

**Resolved for 0.9.30.** `VernacularDB.suggest_scientific_names` and
`TaxonLookupService.suggest_common_names` now show an associated concept as
its primary, the way the unified picker does
(`taxon_search.association_primaries`, `_collapse_associated`):

- A member's row under the primary's name becomes the primary's row.
- A row under another name becomes a synonym row of the primary carrying
  `provenance = name_association:<id>`, so a pick records the association.
- The main-window taxon input (`TaxonInputController`) and the reference
  dialogs (`ui/add_reference_dialog.py`, `ui/cloud_reference_dialog.py`)
  use these two functions.
- Same-name concepts that no association joins (for example *Mycena
  pearsoniana*, COL 34794 and NorTaxa 625327) are still listed separately,
  with their sources.
- A saved observation bound to a member concept keeps it.

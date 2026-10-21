# Proposed tests for the taxonomy build (not implemented; revision 4)

## Existing coverage

- `test_stage_3a_2` covers the homonym guard and the absence of duplicate ids
  after aliasing.
- `test_bridge_emission` covers:
  - chain refusal;
  - unique resolution of emitted tuples;
  - the homonym bucket;
  - the Stage 2 manifest validator.
- `test_build_sqlite_candidate::test_hierarchy_only_when_resolved` checks only
  that no parent dangles. It does not catch the NorTaxa parent defect.
- `audit_stage6p_release.py` is an evidence script and is not in the build.

`release_thresholds.yml` is not enforced.

The audit already runs `scripts/test_manifests_validator.py`, which calls the
repository's real validator read-only: 18 checks pass.

## Hard failures

| Test | Asserts |
|---|---|
| H1 `test_national_concept_parent_resolves` | NorTaxa parent references resolve. The code is in `nortaxa-parent-link-defect.md`; this fails today. |
| H2 `test_every_retired_id_maps_to_live_survivor` | The supersession map exists, its survivor is live, and there is no chain. |
| H3 `test_no_concept_has_two_nortaxa_accepted_ids` | Passes today. |
| H4 `test_every_nortaxa_binding_has_external_id` | Passes today; a regression guard. |
| H5 `test_batch_manifest_applied_exactly` | Approved members equal the applied records; each still satisfies R1–R8; manifests pass `verify_supersession_manifest_approvals` unchanged. |
| H6 `test_superseded_rows_rekeyed` | Stage 6P accounting, including red list and **parent changes**: each parent change must appear in the approved re-parenting list. |
| H7 `test_adb_lookup_never_returns_sporely_id` | Fix on a separate branch. |
| H8 `test_authorship_comparator_selftest` | 66 cases in both orders (rev 4 adds `Smith: Jones`, comma tails, separators, identical qualified forms and malformed parentheses), including Fr./Fritsch, Fr./Frank, Bres./Bresinsky, Lind/Lindau, `Scop.: Fr.` vs `(Scop.) Fr.`, ex-authors, misapplied names, `nom. prov.`, `ined.` and `?`. The sha256 equals the pin. |
| H9 `test_reparenting_matches_manifest` | Replaces "no children". Using repaired source parents, every child whose parent retires is listed as an approved operation `(child, retiree, survivor)`. The cloud gate requires zero cloud references to retirees. |
| H10 `test_artsobs_lookup_not_degraded` | Rank-verified coverage of the regression observation corpus is at least the baseline: concept-verified 84.6% of desktop observations that have a stored id. Name-lookup success, 81.2%, is reported separately and is not a concept metric. Wrong-rank ids never count. |
| H11 `test_no_unmanifested_name_binding` | No `cross_source_automatic_exact_*` alias is applied unless it is a manifest member, a legacy binding (P8) or a manual mapping. |
| H12 `test_no_qualified_or_provisional_name_in_batch` | No batch member carries a qualifier or provisional marker in its authorship, **or a blocked nomenclatural status field** (NorTaxa `nomenclaturalStatus`, COL `col:nameStatus`), on either side. |
| H13 `test_synonym_inheritance_scoped` | A `synonym_of_accepted` usage is bound to a COL concept only through an anchor, an approved manifest member or a ratified legacy host, and only within the pinned source release (P9). The fixture includes a new synonym on a legacy_unratified host, which must not be bound. |
| H14 `test_manifest_pins_match_compiled_inputs` | Every applied manifest's pins (release, archives, policy, rule and comparator sha256) equal the compiled inputs. A mismatch refuses the build. |
| H16 `test_inherited_usages_not_bridged` | (Revision 5, S1.) For every ledger record with `inherited_usages: names_only`, only the anchor usage becomes `reviewed_supersession`. No `authoritative_bridge:%` row exists for its other usages, and those usages keep their class, status and `superseded_from`. For S1: 988 bridges, 0 for the 1,175 synonyms. |
| H15 `test_stage_dependency_closure` | For the release's own stage, every child of a retiree is either retired by the end of that stage or listed as an approved operation. For A0 and A1 stages the list must be empty. |

## Tracked thresholds

Baseline `tax-2026.10.07-01`. An increase is `review_required`.

| Code | Baseline |
|---|---|
| Same-name same-rank cross-source pairs | 6,040 |
| Same-name same-rank COL groups | 154 |
| Same-name same-rank NorTaxa groups | 17 |
| Identical nb label groups | 5,533 |
| Parentless NorTaxa concepts | 12,566 |
| Vernacular twins | 1,145 |
| Legacy_unratified bindings | 4,122 (rev 4 full evaluator) |
| Wrong-rank Artsobservasjoner keys | 141 species name keys |
| iNaturalist id contradicting COL | 104 |
| Placeholders | 70 |

## Fixtures

Synthetic cases:
- a homonym (Stereum durum);
- sanctioning and structural pairs (Hygrocybe conica; Helvella crispa
  `Scop.: Fr.` vs `(Scop.) Fr.`);
- `ss. … non`;
- `nom. prov.`;
- `ined.`;
- an ex-author pair;
- prefix collisions;
- a genus/section pair;
- a placeholder;
- a provisionally accepted COL duplicate;
- a national genus with a species (H1, H9);
- an infraspecific id sharing its species' name (H10).

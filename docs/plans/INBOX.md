# Development inbox

Quick observations captured while using Sporely.
Items here are not approved implementation plans.
Add a note under the nearest heading, or use “Unsorted / needs review” when its home is unclear.

## Cloud sync / recovery

- Add a real Reset Cloud Sync / Reset Cloud Link tool, or remove that instruction from the account-mismatch message until the tool exists.
- Replace “Unable to save cloud login” for account-link protection failures with wording that identifies the account mismatch.
- Consider a non-modal pending-cloud-media indicator on close; do not add a blocking reminder dialog.
- Run live cloud-lock QA with two disposable Sporely Cloud accounts; verify account-mismatch blocking and the Reset Cloud Link flow.
- Verify Profile parity between desktop and web for `username`, `display_name`, `bio`, `avatar_url`, and `profile_email`.
- Review E2 anchor-promotion residual risks before changing that pipeline: cross-device reservation adoption, dangling reserved keys, and URL encoding assumptions.
- Decide whether to repair the known `G_conflicting_intent` row and historical duplicate observation/image records outside the extraction plan.
- Harden standalone migration tooling if it becomes necessary; it is explicitly outside the cloud-sync extraction plan.
- Design a safe fix for the observation-deletion ordering and original-media coverage noted in `docs/cloud-media-incident-audit.md`.

Source: former `PLAN.md`, the active cloud-sync extraction plan, and the cloud-media incident audit.

## Images / galleries / publishing

- In Prepare Images, editing a raw image and then zooming can revert the preview to the old, unedited JPEG.
- Prepare Images thumbnails do not update after edits.
- Returning to Live Lab after editing a “from raw” image leaves the main preview stale until another image is selected.
- Add multi-select auto white balance, calculated independently for each selected image.
- Apply one custom/picked white balance to all selected images.
- Hide the “stain” pill on field images in Measure/observation main-image surfaces.
- Fix missing scale bars on microscope images published to iNaturalist/Artsobservasjoner.
- Ensure the publication plate respects disabled image bubbles.
- Fix Android-imported JPEG portrait rotation in thumbnails and Measure.
- Define HEIC behavior: HEIC as import source, JPEG/PNG as local working form, and cloud derivatives from the best decoded pixels when practical.
- Replace remaining generated-media heuristics with explicit provenance tags in a dedicated artifact-model plan.
- Make the thumbnail gallery height adjustable; prevent cropped/hidden thumbnails in Prepare Images; allow thumbnails around 100 px.

Source: former `PLAN.md`.

## UI / usability

- In Analysis, default “Orient” and “Uniform scale” to on.
- Fix selection/highlight artifacts in AI suggestions and the Observations table; align them with the Measurements table.
- Make room for measure-type radio-button labels.
- Consider renaming “Reference shape” to “Shape”.
- Camera Import naming: fix “Intestion”, rename “Sync shot” to “Camera time offset”, and rename “Microscope sessions” to “Live lab sessions”.
- Camera Import layout: order Import folder, Camera time offset, Live lab sessions, Actions; update button hints.
- Add richer manual reassignment tools for unmatched images.
- Add fine-tuning for multi-line measurement segments.
- Add a hint bar at the bottom of Measure.
- Add Cmd/Ctrl-click additive selection and histogram additive selection in Analysis.
- Continue the Slate Lab / Clinical Nocturne migration in Live Lab, Ingestion Hub, Calibration, and remaining dialogs; consolidate remaining inline style sheets.
- Add a desktop menu link to Pro information/payment on `sporely.no`; do not embed checkout.

Source: former `PLAN.md`; unmatched-image work also appears in `docs/hardware-sync.md`.

## Taxonomy

- Consider the deferred observer-safe `Help → Taxonomy` menu described in `database/taxonomy/docs/ui-menu-recommendation.md`; keep acquisition, compilation, promotion, mapping edits, and deletion CLI-only.
- Reconcile NorTaxa/COL duplicate candidates (Taxonomy v3 Group B): 7,338 NorTaxa concepts in 7,423 same-name pairs with a separate COL concept in `tax-2026.09.26-02`; the COL side is in the cloud scope for 2,132 pairs, the NorTaxa side never. Names and ids sit on the NorTaxa concept, so web users find no common name for species such as Cantharellus cibarius. Name equality only sizes this; identity needs reviewed evidence. Counts: `database/taxonomy/evidence/taxonomy-v3/stage0/coverage-report.json`.
- Group-A bridge coverage (Taxonomy v3): 19,807 NorTaxa ids on COL concepts only through automatic exact matches, so production cannot resolve them (7,099 in the cloud scope: 1,888 `shared_synonymy`, 5,211 without a published cross-reference). Review and emit approved bridges from the Stage 0 candidate manifests.
- National scientific names (Taxonomy v3): show the approved NorTaxa (later Dyntaxa) preferred scientific name alongside canonical COL; search accepts both; identity never changes.
- Historical unresolved-observation repair (Taxonomy v3): after new Group-A bridges are active, idempotently re-resolve cloud `external_unresolved` observations from preserved source + namespace + external id.
- Review the 448 Artportalen overlay cases left out of `tax-2026.09.26-02` (`database/taxonomy/evidence/artportalen-overlay/`).
- Decide whether the cloud should carry Artportalen/iNaturalist publishing ids as a namespaced channel; today the scoped export suppresses the legacy integer channel.
- Consider Dyntaxa as a national source so Swedish names and Artportalen ids stop depending on the frozen legacy database.
- Decide whether `TaxonChoice` should expose external IDs directly or through a richer match object.
- Add list-returning iNaturalist and Artportalen ID lookup APIs; both identifier types can map to multiple local concepts.
- Define accepted-backbone versus Artportalen-only tie-breaking for duplicate scientific names.
- Decide normalization rules for exact vernacular matching.
- Add an on-demand Artsdatabanken red-list resolver and caching policy.
- Verify AI Photo ID uses a local iNaturalist ID before name matching and that desktop/web apply compatible lookup rules.

Source: former `PLAN.md` and the legacy-database lookup audit `docs/taxonomy-lookup-status.md` (removed 2026-09-26; see Git history).

## AI identification / crop

- Verify the current Supabase AI-crop fields and desktop/web crop sync.
- Verify Artsorakel/iNaturalist result persistence and dropdown behavior.
- Verify Review, Import Review, and Find Detail share one AI Photo ID state model.
- Confirm AI crop is used only for AI requests, not gallery display or R2 originals.
- Prevent replay of stale or tombstoned-image AI runs as current suggestions.
- Define retention before production: remove stale runs after 30 days or retain at most 2–3 stale rows per observation/service; long term, prefer one current row plus short-lived debug history.

Do not crop R2 originals, make gallery display depend on AI crop, or add a separate crop table unless the current model fails.

Source: former `PLAN.md`.

## Reference data / community data

- Return QC metadata in community-data RPC responses.
- Make cloud-origin imported sources more visually distinct in the reference panel.
- Complete the public reference dataset model before broadly publishing comparison plots.

The scoped reference-library work, including its deferred editor, quick-add, revision, cloud-sync, and public-rendering work, remains in the active reference-library plan.

## Testing / infrastructure

- Add export coverage for observations, images, measurements, calibrations, reference data, and image files; verify `app_settings.json` and full profile state are excluded.
- Verify local database values take priority over file EXIF in Prepare Images and the Measure Info box.
- Fix the cloud-synced image warning overlay in Prepare Images.
- Introduce Ruff.
- Consider mypy after the codebase has stable, useful annotations.
- Broaden focused coverage for cloud conflicts, local media signatures, image crop math, `utils/r2_storage.py`, SQLite migrations, and `database/models.py`.
- Test metadata auto-merge and true conflict-dialog triggers.
- Reconcile old “cloud deletion conflict” tests with tombstone behavior.
- Add direct coverage for deterministic pagination ordering, metadata-only anchor byte-fetch prevention, broader pull-only zero-write surfaces, affirmative measurement/calibration identity repair, cross-restart retryability, and snapshot schema compatibility.

Source: former `PLAN.md` and the active cloud-sync extraction plan.

## Hardware / ingestion

- Design the field-device temporal anchor for DSLR/phone batches.
- Add Artsobservasjoner support for per-image note upload.

Source: `docs/hardware-sync.md`.

## Web / infrastructure

- Add an offline queue for upload failures in field conditions.
- Consider a cloud summary RPC/view for observation/image change summaries.
- Future web-native analysis ideas: responsive Plotly L × W plots, thumbnail-linked outlier review, mobile/desktop layouts, a public dataset explorer, taxon summaries, literature reference entry, browser measurement, and possible Pyodide sharing of Python/Numpy logic.
- Privacy/social follow-ups: verify live owner/friend/stranger/blocked/banned RLS and feed behavior, strip GPS EXIF from public media, add an iNaturalist export deep link, and generate Bluesky share cards.

Source: former `PLAN.md`.

## Unsorted / needs review

- Confirm whether Worker secrets/routes and `SUPABASE_URL`, JWT overrides, `MEDIA_PUBLIC_BASE_URL`, and the `sporely-media` R2 binding are already deployed.
- Re-check whether old R2 migration notes are obsolete after the Supabase baseline reset.
- Re-check whether old Phase 7 SQL notes are obsolete after the Supabase baseline reset.
- Verify whether the earlier AI crop backlog has already been completed before promoting any item.

Source: former `PLAN.md`; status could not be established safely from this repository alone.


## Cloud media recovery: handle local image whose matching cloud image is soft-deleted

Reproduce with observation 604 / local image 3258 / cloud image 3694. Determine when recovery should explicitly restore the existing cloud identity versus preserve deletion and report a conflict. Do not silently create a duplicate image.

# Parse references/citations from bibtex
Example:
@book{guzman1983genus,
  title={The Genus Psilocybe: A Systematic Revision of the Known Species Including the History, Distribution and Chemistry of the Hallucinogenic Species},
  author={Guzm{\'a}n, Gast{\'o}n},
  series={Beihefte zur Nova Hedwigia},
  volume={74},
  year={1983},
  publisher={J. Cramer},
  address={Vaduz},
  pages={1--439}
}

# Selecting a taxon for comparison
Manual testing confirms the deferred reference-taxon mismatch bug.
Reproduction:
- observation has species A;
- open Add Reference → Add new;
- choose species B from an AI suggestion in Reference taxon;
- enter literature data for species B;
- Add to plot / save;
- current code shows the legacy dialog saying species B differs from the observation taxon and asks whether this is a historical/synonym name.
This dialog must not appear in this workflow.
Reference taxon is an independent comparison/reference target. Choosing another species there is normal and must never be treated as an attempted change to the observation's identification.
Fix the guard condition, not merely the wording.
Required behavior:
- Add Reference / Enter manually + different Reference taxon → no mismatch confirmation.
- the new TaxonTreatment / measurement set is normalized to the selected Reference taxon;
- name_as_published remains the publication's actual name;
- the observation's taxon/identity remains untouched;
- observation-drift protection must still work if the observation itself changes while the dialog is open.
Preserve the legacy mismatch protection only in any flow that genuinely edits or derives from the observation's own identification.
Add a regression test specifically using an AI suggestion as the Reference taxon and a different observation species. Assert no QMessageBox question is invoked and the resulting reference is stored under the AI-selected reference taxon while the observation identity is unchanged.



## taxon-dialog fix
Two more manual-test findings in the Analysis reference panel.
1. Reference list wastes available vertical space
With 4+ references, the fourth row requires scrolling even though there is a large unused area immediately below the Reference values section.
I checked the current layout:
- ComparisonListWidget has setMinimumHeight(160);
- reference_section is Expanding;
- but its containing top_sections is QSizePolicy.Maximum;
- left_layout.addWidget(top_sections, 0) is followed by left_layout.addStretch(1).
So the surrounding layout keeps the comparison section close to its size hint and gives the remaining height to an empty stretch.
Fix the layout so Reference values consumes available vertical space before its internal scrollbar is needed. Do not simply increase a hard-coded fixed height. On a window like the attached screenshot, four or five rows should all be visible without scrolling; scrolling should remain available for genuinely long lists.
Add a UI/layout regression test or renderer scenario with at least 5 comparison rows.
2. A range reference is being drawn as an ellipse
In the same test, the first three Funga Nordica references render correctly as range rectangles, but the fourth (P. fimicola, Funga Nordica 2008) renders as an ellipse.
The comparison-list subtitle is also different:
- correct rows show range · <raw expression>;
- the problematic row shows only Funga Nordica (2008).
Investigate the actual persisted measurement set / observation-reference snapshot and resolved reference_series payload for this row. Do not fix this cosmetically in the matplotlib layer.
If it contains literature L/W range bounds, it must travel through the same normalized range/summary translation as the other references and render:
- solid/translucent core rectangle for typical/core bounds;
- dotted outer rectangle when exceptional min/max exist;
- no confidence ellipse invented from literature ranges.
Determine why this reference is taking the legacy/ellipse path and fix that source-path inconsistency. Add a regression using four references where the fourth is a normalized literature range.
Keep PR #7 unmerged and include these fixes together with the already identified Reference-taxon mismatch-popup fix.

## hint bar must not scroll away
Problem
In both dialogs:
- Edit reference
- Add reference
the bottom hint/help bar is currently inside the scrollable content area.
That means if the user scrolls upward, the hint bar disappears from view.
This is wrong. The hint bar is part of the dialog chrome / action area, not part of the document body.
Required behavior
Move the hint/help bar so it is anchored with the bottom action area, alongside / just above the button row, and always visible regardless of scroll position.
Applies to:
1. Edit reference dialog
2. Add reference dialog
Expected layout
The dialog should behave like this:
- Scrollable area: the actual form content only
- Fixed bottom area:
  - hint/help/status bar
  - action buttons (Save, Cancel, Delete / Add to diagram, Save to library, etc.)
In other words:
- scrolling should move the form fields and content,
- but not the hint bar,
- and not the action buttons.
Why
The hint text is contextual guidance for the current input state.
If it disappears when the form scrolls, it becomes much less useful.
Acceptance criteria
- In Edit reference, scroll to the top and bottom: the hint bar remains visible at the bottom.
- In Add reference, scroll to the top and bottom: the hint bar remains visible at the bottom.
- The hint bar still updates correctly with focus / validation / parse context.
- The action buttons remain fixed as before (or are fixed together with the hint bar if they were not already).
- No overlap, clipping, or double scrollbars introduced.
- The available content area shrinks appropriately so the fixed footer does not cover content.
Implementation preference
Use a layout where:
- the main content area is the only scroll container,
- the footer/action region is separate and fixed within the dialog,
- the hint bar belongs to that footer region.
Please include
- the code changes,
- focused tests if practical,
- and a short note on which widgets/layout containers were changed.



## Reference taxon UI
- **Referansetakson** ([add_reference_dialog.py:751-848](ui/add_reference_dialog.py#L751-L848)) is a plain editable dropdown with no taxonomy search. It lists "Use observation taxon" plus the AI suggestions. Typing "Slekt art" + Enter is just split into two words.
- **AI suggestions and typed names carry no taxon ID** (`taxon_id: None`). That's what the amber "Ingen taksonidentifikator er angitt" warning means. A reference made that way isn't linked to the taxonomy, so it won't line up with species pages or default sharing, which need a registry species.
- **Takson** ([reference_entry_editor.py:648-658](ui/reference_entry_editor.py#L648-L658)) is just a read-only copy of the choice above. It adds nothing.
- **Navn som publisert** is a required field in the data model, prefilled with the chosen name. It's only useful for an old name or spelling variant.

**What I'd suggest: one searchable species field**

1. **A single field "Art / Species"** replaces Referansetakson and Takson. It reuses the app's existing taxonomy search (`TaxonInputController`, already used for observation identification):
   - **Before you type:** the dropdown shows *Observasjonens takson* and the *AI-forslag*, with the AI names looked up in the taxonomy so they carry a real taxon ID.
   - **As you type:** it searches all species by scientific *and* vernacular name, so "Flat…" or "Pholiota sq…" both work. Each row shows the scientific name with the Norwegian name beside it, and a synonym shows "syn. → accepted name" and links to the accepted species.
   - **Choosing a row** sets the taxon ID, so there's no unlinked reference and the amber warning only appears if you deliberately type something not in the taxonomy.
2. **The vernacular name** appears next to the chosen species, read-only, and is searchable in the same field. A separate vernacular input would just be a second way to set the same thing.
3. **Navn som publisert** stays, because the publication's own name matters scientifically (e.g. *Pholiota squarrosipes* published under an older combination). It becomes a small optional line under the species field:
   - It's filled in automatically with the name you picked.
   - If you picked a synonym row, it's filled with that synonym, while the reference links to the accepted species.
   - You only touch it for spelling variants or old combinations.

Your rule still holds: the observation's own identification is never changed. The AI only gives starting points, and you can pick any species.


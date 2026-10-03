# Mosaic publication-selection fix

Source handoff:
`docs/plans/active/mosaic-fix.md`

The source handoff remains authoritative for policy, regressions,
acceptance criteria, and scope exclusions. This document only decides where the
implementation/review boundaries fall. Where the two disagree on *what* the
policy is, the source handoff wins; where they disagree on *how the work is
cut into stages*, this document wins.

## The invariant being defended

> **Source-image publication selection does not control analytical measurement
> selection.**

`artsobs_publish_excluded_image_ids_<observation>` answers "should this source
image itself be uploaded to the external publishing target?". It does not
answer "should measurements made on this image participate in observation-level
generated summary media?".

This policy has been reverted by agents several times. The staging below is
shaped by that fact: the coupling removal and the evidence that makes it hard to
reintroduce land together, and the audit of *other* consumers is deliberately
kept separate so a reviewer is not asked to judge the fix and the sweep at once.

## Stage-size heuristic

Used to decide the boundaries below, and to be used again if a stage has to be
split during execution:

> If a stage can be described in one sentence, has one principal architectural
> concern, and its acceptance evidence can be reviewed without mentally holding
> several unrelated changes, it is probably a good stage.

Corollaries applied here:

- Split when two pieces have meaningfully different failure modes, acceptance
  criteria, or kinds of evidence.
- Keep tightly coupled changes together when splitting them would create an
  artificial intermediate state (for example: a code fix whose partner test
  still asserts the old contract).
- Investigation lives inside the stage that needs it, unless the investigation
  result genuinely determines the shape of later stages.
- Human verification need is decided by the Sparrer per stage; it is not a
  reason to draw a boundary.

## Stage map

| Stage | One-sentence description | Depends on |
| --- | --- | --- |
| 1 | Remove the source-image publication filter from mosaic measurement selection, and make the reversed contract explicit in tests and a local comment. | — |
| 2 | Prove the mosaic cache/signature is independent of source-image publication selection while remaining sensitive to real measurement and mosaic-setting changes. | 1 |
| 3 | Audit every other consumer of the publication exclusion set and correct only those that clearly violate the invariant. | 1 |
| 4 | Record the invariant in durable repository engineering documentation. | 3 |

No separate final independent-review stage is defined. Stage 3 is already an
adversarial sweep over the integrated result by someone who must justify each
retained use of the exclusion set, so a further fresh-critic stage would be
ritual rather than value. If Stage 3 returns a large or contested set of
changes, the Sparrer may add one then.

---

## Stage 1 — Restore measurement selection independence

### Goal

Make observation-level spore mosaic measurement selection independent of which
source images the user selected for external publication, and leave behind
evidence that states the contract loudly enough that a future agent does not
re-add the filter.

### Scope

- `ui/observations_tab.py`: remove the use of `_publish_excluded_image_ids()`
  from `_prepare_publish_mosaic_inputs()` so the mosaic starts from the
  observation's eligible measurements.
- Preserve every use of the exclusion set that genuinely decides which source
  image files are uploaded, notably `_collect_artsobs_image_paths()` and
  publication UI state.
- Reverse the test in `tests/test_publish_media_stage2.py` that currently
  asserts the wrong contract (approximately
  `test_analysis_and_publish_use_same_ordered_mosaic_inputs`). Rename or split
  it so the policy is visible in the test name, for example
  `test_source_image_publish_exclusion_does_not_filter_mosaic_measurements`.
- Add a regression matching the real failure shape: one observation, 61 eligible
  spore measurements across several microscope source images, one source image
  selected for upload, 3 of the measurements on that image. Assert both sides in
  one place: the uploaded source-image list is the selected subset, and the
  mosaic input still contains all 61 eligible measurements.
- Keep or add a test proving legitimate measurement filtering still works
  (calibration rows excluded, measurement category respected, incomplete
  geometry rejected).
- Add the contract comment where the temptation to reuse
  `_publish_excluded_image_ids()` is strongest, with the substance given in the
  source handoff, worded to fit the surrounding code.
- A tiny rename or helper extraction is acceptable if it materially clarifies
  the boundary between "which source image files are uploaded" and "which
  measurements form the analytical summary".

The code change and the test reversal are one stage on purpose: splitting them
would leave the repository in a state where the suite asserts a contract the
code no longer implements.

### Investigation before editing

Trace and report, before changing anything:

- `_prepare_publish_mosaic_inputs()`, `_collect_artsobs_image_paths()`,
  `_collect_publish_selected_image_rows()`, `_publish_excluded_image_ids()`,
  `_prepare_publish_media_assets()` in `ui/observations_tab.py`.
- `filter_mosaic_measurements()`, `prepare_ordered_mosaic_inputs()`,
  `mosaic_dependencies()` in `utils/publish_media.py`.
- `tests/test_publish_media_stage2.py`.

Search for `_publish_excluded_image_ids`, `artsobs_publish_excluded_image_ids`,
`excluded_image_ids`, `_prepare_publish_mosaic_inputs`. Record every hit with a
one-line judgement: source-upload decision (correct), measurement selection
(wrong), or deferred to Stage 3. Do not act on the Stage 3 hits here.

### Acceptance criteria

1. Unchecking a source image still prevents that source image from being
   externally uploaded.
2. Measurements belonging to an unchecked source image remain eligible for the
   generated spore mosaic.
3. A 61-measurement observation cannot collapse to 3 mosaic spores solely
   because one source image is selected for publication.
4. Legitimate measurement/category/geometry filtering is unchanged.
5. The test that encoded the wrong contract has been reversed or replaced, and
   the new test's name states the policy.
6. At least one test proves both sides of the contract simultaneously, so a
   future "fix" cannot restore mosaic membership by re-uploading excluded
   source images.
7. The contract comment is present at the mosaic preparation path.
8. No iNaturalist Stage 2 API behavior changes; no Stage 3 UX work starts.

### Verification

Focused suites covering publish media and mosaic preparation, run with the
project virtual environment (`.venv/bin/pytest`) from the worktree root. Include
the previously wrong test explicitly, since its contract is intentionally
changing. Do not run broad suites.

### Out of scope

Cache/signature work (Stage 2), plate/export and other derived-media consumers
(Stage 3), documentation beyond the local code comment (Stage 4), and every
exclusion listed under "Scope exclusions" in the source handoff.

---

## Stage 2 — Mosaic cache and signature independence

### Goal

Establish that source-image publication selection does not participate in the
mosaic cache key, so toggling which source images are uploaded neither changes
the mosaic nor invalidates a valid cached one, while genuine measurement and
mosaic-setting changes still invalidate correctly.

### Scope

- Inspect the mosaic dependency/signature path (`mosaic_dependencies()` and its
  callers) and determine whether publication exclusion state entered the
  signature indirectly through the pre-filtered measurement list, or directly.
- If Stage 1 already removed the dependency, prove it rather than change it.
- Add or adjust the cache/signature regression tests that pin both directions:
  publication-selection changes do not alter the signature; measurement and
  mosaic-setting changes do.
- Decide and justify whether the cached output contract requires a
  renderer/cache version increment. Default is no. If the answer is yes, state
  the concrete reason in the stage report.

This is separate from Stage 1 because its failure mode is different — a stale or
over-invalidated cached image rather than a wrong mosaic — its evidence is
signature-level rather than membership-level, and the version-bump question is
an architectural decision a reviewer should weigh on its own.

### Investigation before editing

Read `mosaic_dependencies()` and the cache key/versioning code around it, plus
the existing publish-media cache tests, before deciding whether this stage is a
proof or a fix.

### Acceptance criteria

1. Changing only which source images are selected for direct upload produces the
   same mosaic and does not invalidate the cache.
2. Changing the eligible measurement set or mosaic settings still invalidates.
3. Tests pin both directions.
4. Any renderer/cache version change is justified in writing, or explicitly
   recorded as unnecessary.

### Verification

Focused publish-media cache and mosaic signature suites.

### Out of scope

Cache architecture redesign, cross-observation cache behavior, and anything not
reachable from the mosaic signature path.

---

## Stage 3 — Audit derived-media consequences

### Goal

Determine, for every remaining consumer of source-image publication exclusion,
whether it decides source-image upload (legitimate) or measurement inclusion in
an observation-level analytical summary (the same bug), and correct only the
clear violations.

### Scope

- Plate/export paths that accept `excluded_image_ids`. Similar naming is not
  evidence of the same bug: omitting a source-image panel from a plate is
  plausibly correct, omitting analytical measurements from an observation-level
  summary is not.
- Annotated-image and other source-image-derived media, where a derivative may
  legitimately correspond one-to-one with a source image.
- Any hit Stage 1 flagged as deferred.

Produce a short table: call site, what the exclusion currently means there,
verdict (correct / violates the invariant / ambiguous), action taken. Each
correction gets its own regression test. Ambiguous cases are documented as
follow-up, not fixed on speculation — and an ambiguity that would require new
measurement-selection state to resolve is out of scope by policy, since
measurement exclusion requires its own explicit filter state.

This is a separate stage because its output is an audit judgement over several
unrelated call sites, which a reviewer should be able to evaluate without also
re-deciding the core fix.

### Investigation before editing

The audit is the stage. Reuse Stage 1's search results rather than repeating the
archaeology; extend the search only where Stage 1's map is incomplete.

### Acceptance criteria

1. Every consumer of the publication exclusion set has a recorded verdict.
2. Retained uses are justified by what they actually control.
3. Any correction is accompanied by a regression test.
4. Ambiguities are documented as follow-up work, not silently fixed or silently
   dropped.
5. No new measurement-selection UI, privacy control, or publish-media rewrite.

### Verification

Focused suites for each path actually touched, plus nearby external-publishing
behavior. If no code changes, the audit table is the deliverable and the Stage 1
and 2 suites remain green.

### Out of scope

Redesigning derived media generally; the source handoff's full exclusion list.

---

## Stage 4 — Record the durable invariant

### Goal

Put the invariant where an agent working in this area is reasonably likely to
read it, in the style of the existing precedent distinguishing external
publication image exclusion from cloud-storage exclusion.

### Scope

- Choose one primary durable location: publish-media architecture documentation,
  `docs/engineering-history.md`, or another stable contract document. State the
  invariant there:

  > **Source publication selection ≠ analytical measurement selection.**
  > `artsobs_publish_excluded_image_ids_<obs>` controls which source images are
  > sent to an external publication target. It must not alter the measurements
  > used to generate observation-level analytical summary media such as the
  > spore mosaic. Measurement exclusion requires separate explicit
  > measurement/filter state.

- Add a concise version to `AGENTS.md` only if it has a suitable invariants /
  "do not violate" section, and without implementation history.
- Fold in anything Stage 3's audit established about the boundary between
  source-image-derived media and observation-level analytical media.
- Do not scatter the same paragraph across multiple documents.

This stage is last because Stage 3 determines how broadly the invariant can be
stated. It is genuine independent work with its own acceptance criterion — the
policy has been reverted repeatedly, so tests alone are known to be
insufficient — not a container for unfinished pieces from earlier stages. If an
earlier stage is incomplete, it stays open rather than draining into here.

### Investigation before editing

Locate the existing external-publication vs cloud-storage exclusion precedent
and match its placement and style.

### Acceptance criteria

1. The invariant appears in exactly one durable primary location, discoverable
   from the publish-media docs.
2. Any `AGENTS.md` addition is short and fits an existing invariants section.
3. The wording covers observation-level analytical summary media generally, not
   just the spore mosaic.
4. No duplicated paragraphs across documents.

### Verification

Documentation only; no test run required beyond confirming earlier stages remain
green if any code was touched.

### Out of scope

Rewriting publish-media documentation, and recording implementation history in
`AGENTS.md`.

---

## Handoff expectations

Per stage, report: files and symbols touched, what was reused, verification
command and result, deviations with reasons, and the candidate commit SHA.
Stage 1 additionally reports the exact root cause and the full map of exclusion
set uses with verdicts; Stage 2 reports the cache/version decision and its
reason; Stage 3 reports the audit table and any deferred ambiguity.

Repository Git policy (`AGENTS.md`) permits committing and pushing verified
stage work; it does not permit committing a stage whose verification failed.
Do not start iNaturalist Stage 3 work at any point in this plan.

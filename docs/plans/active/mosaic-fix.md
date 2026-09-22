Work in the Sporely repository.

The relevant iNaturalist publication work has been merged into main (PR #2);
inspect the current main implementation rather than an old feature worktree.

The iNaturalist Stage 2 candidate accepted at the time this handoff was written
was `7c8196b`, which is now reachable from main.

First read the repository `AGENTS.md` and follow its rules. In particular, do not push or merge unless repository policy explicitly permits it.

This is **not Stage 3 iNaturalist work**.

This is a separate publish-media policy regression that has apparently been reintroduced several times by agents. The goal is not merely to patch the current symptom; the goal is to make the intended policy difficult to accidentally reverse again.

## The policy

This is the invariant:

> **Source-image publication selection does not control analytical measurement selection.**

More concretely:

`artsobs_publish_excluded_image_ids_<observation>` answers:

> Should this source image itself be uploaded to the external publishing target?

It does **not** answer:

> Should measurements made on this image participate in observation-level generated summary media?

Therefore:

* unchecking a source microscope image must prevent that source image from being uploaded;
* measurements made on that image remain part of the observation's measurement dataset;
* those measurements must still participate in generated observation-level summary media such as the **spore mosaic**, subject only to the mosaic's actual measurement/category/filter rules;
* the source-image publication checkbox must not silently act as a measurement filter.

If Sporely ever needs a way to exclude measurements from a mosaic, that requires its **own explicit measurement/filter state**. Do not reuse the source-image publication exclusion set for that purpose.

This policy is deliberate.

Do not reinterpret it as a privacy safeguard and restore the coupling.

## Real failure that exposed this

During real iNaturalist append testing, local observation `835` had approximately:

* 61 spore measurements total;
* only a small subset of microscope source images selected for external publication, to avoid uploading duplicate source photos;
* generated spore mosaic contained only **3 spores**.

Investigation showed the likely reason:

1. source-image publishing selection excludes some microscope images;
2. `_prepare_publish_mosaic_inputs()` reads `_publish_excluded_image_ids(...)`;
3. measurements whose `image_id` belongs to one of those excluded source images are dropped;
4. the mosaic therefore contains only measurements from source images still selected for upload.

That behavior is wrong according to the policy above.

The iNaturalist append implementation itself worked; this is a pre-existing/general publish-media boundary exposed by the append workflow.

## Important: there is currently a test enforcing the wrong policy

Inspect:

`tests/test_publish_media_stage2.py`

At the time of this handoff, a test named approximately:

`test_analysis_and_publish_use_same_ordered_mosaic_inputs`

does roughly this:

1. sets up measurements `[2, 3, 4]`;
2. confirms the publish mosaic receives `[2, 3, 4]`;
3. changes:

```python
publish._publish_excluded_image_ids = lambda _observation_id: {20}
```

4. then asserts the resulting mosaic rows collapse to only:

```python
[2]
```

That assertion explicitly tells future agents that source-image publication exclusion is supposed to remove mosaic measurements.

**That test is enforcing the wrong contract and must be changed.**

Do not work around the test. Reverse the contract it asserts.

## Investigation before editing

Before changing code, trace the relevant paths and report what you find.

At minimum inspect:

* `ui/observations_tab.py`

  * `_prepare_publish_mosaic_inputs()`
  * `_collect_artsobs_image_paths()`
  * `_collect_publish_selected_image_rows()`
  * `_publish_excluded_image_ids()`
  * `_prepare_publish_media_assets()`
* `utils/publish_media.py`

  * `filter_mosaic_measurements()`
  * `prepare_ordered_mosaic_inputs()`
  * `mosaic_dependencies()`
* `tests/test_publish_media_stage2.py`
* relevant plate/export tests if they share the same exclusion set;
* documentation describing publication image selection.

Also search the repository for:

* `_publish_excluded_image_ids`
* `artsobs_publish_excluded_image_ids`
* `excluded_image_ids`
* `_prepare_publish_mosaic_inputs`

The purpose is to identify everywhere the source-image publication exclusion is being interpreted as something broader than “do not upload this source image.”

Do not blindly remove every use of the exclusion set. Some uses are correct.

## Correct uses of source-image publication exclusion

The exclusion set is appropriate when deciding which **source images** are actually sent externally.

Examples include:

* `_collect_artsobs_image_paths()`;
* source-image lists handed to an uploader;
* publication UI state indicating which source images will be sent.

Preserve those behaviors.

If a user unchecks source image X, source image X must still not be uploaded.

## Incorrect use to remove

The publication exclusion set must **not filter the observation's measurements when generating the spore mosaic**.

Current code approximately does:

```python
excluded_image_ids = ...
source_measurements = []

for measurement in MeasurementDB.get_measurements_for_observation(observation_id):
    if measurement["image_id"] not in excluded_image_ids:
        source_measurements.append(measurement)
```

and then sends `source_measurements` into:

```python
prepare_ordered_mosaic_inputs(...)
```

That is the coupling to remove.

The mosaic should instead start from the observation's eligible measurements and let its actual measurement rules determine inclusion:

* measurement category;
* renderability/complete points;
* whatever explicit Analysis/mosaic measurement filters are already part of the mosaic contract;
* canonical ordering.

A source-image upload checkbox is not one of those filters.

## Keep measurement/category semantics intact

Do not turn this into “always render every database measurement no matter what.”

Existing legitimate filtering remains valid, such as:

* excluding calibration measurements;
* selected measurement category;
* invalid/incomplete measurement geometry;
* explicit mosaic/Analysis measurement filters if such a mechanism currently exists.

The invariant is narrower:

> **Do not derive measurement inclusion from source-image publication selection.**

## Derived media scope

The immediate confirmed bug is the **spore mosaic / thumbnail gallery generated from measurements**.

Fix that boundary first.

Do not casually redesign every other kind of derived media.

In particular, distinguish:

1. **source-image-derived media**, where a derivative may correspond directly to one source image, such as an annotated copy of that image;
2. **observation-level analytical summary media**, such as a mosaic composed from many measurements.

This task establishes the policy for the latter.

If annotated-image behavior raises a separate ambiguity, document it in the handoff rather than expanding this fix without evidence.

## Regression tests — critical

The most important deliverable is a test suite that makes the intended policy obvious to a future coding agent.

### 1. Reverse the existing wrong test

Change the existing test so source-image publication exclusion does **not** alter mosaic measurement membership.

Conceptually:

```python
publish._publish_excluded_image_ids = lambda _observation_id: {20}

_settings, rows_after_source_exclusion, _images, _render = (
    ObservationsTab._prepare_publish_mosaic_inputs(publish, 77)
)

assert [row["id"] for row in rows_after_source_exclusion] == [2, 3, 4]
```

The exact fixture may differ.

Rename or split the test if necessary so the policy is stated in the test name.

Prefer an explicit name such as:

```python
test_source_image_publish_exclusion_does_not_filter_mosaic_measurements
```

Do not leave this buried only in a generic ordering test.

### 2. Add a regression matching the real failure shape

Add a test conceptually representing:

* one observation;
* 61 eligible spore measurements;
* measurements distributed over several microscope images;
* only one source image selected for upload;
* that one image contains only 3 of the measurements.

Expected:

* selected source-image upload list contains only the intended source image;
* generated spore mosaic input still contains **all 61 eligible measurements**;
* mosaic inclusion is not reduced to 3.

The test does not need literally 61 hand-written dictionaries if a helper can generate them cleanly, but using `61` and `3` would be useful because it captures the real regression unmistakably.

The important assertion is the separation:

```text
source images uploaded: selected subset
mosaic measurements: complete eligible observation measurement set
```

### 3. Test both sides of the contract in one place if practical

A particularly strong regression would prove simultaneously:

```text
image 20 excluded from source upload
measurements belonging to image 20 still present in mosaic input
```

This prevents a future agent from “fixing” the mosaic by also accidentally re-uploading excluded source images.

### 4. Preserve genuine mosaic filters

Have at least one test demonstrating that a real measurement/category rule still filters correctly.

For example:

* spores remain;
* calibration rows remain excluded;

or whatever existing test already proves this.

The new invariant must not disable legitimate measurement filtering.

## Code comment / local contract

Put a short but forceful comment near the code where the temptation to reuse `_publish_excluded_image_ids()` is strongest.

Something along these lines:

```python
# IMPORTANT CONTRACT:
# Source-image publication selection controls whether the source image itself
# is uploaded. It does NOT control which measurements participate in
# observation-level generated summary media such as the spore mosaic.
#
# Do not filter mosaic measurements using _publish_excluded_image_ids().
# Measurement inclusion requires its own explicit measurement/filter state.
```

Adjust wording/style to fit the repository, but retain the substance.

The goal is that a future agent reading only this function understands why the obvious-looking image filter must not be added.

## Repository documentation / agent guidance

This policy has apparently been reverted several times, so tests alone are not enough.

Find the most appropriate durable engineering-contract location.

Good candidates may include:

* repository `AGENTS.md`;
* publish-media architecture documentation;
* `docs/engineering-history.md`;
* another stable contract document.

Do not scatter the same paragraph everywhere, but put it somewhere agents are reasonably likely to read.

Add an explicit invariant:

> **Source publication selection ≠ analytical measurement selection.**
>
> `artsobs_publish_excluded_image_ids_<obs>` controls which source images are sent to an external publication target. It must not alter the measurements used to generate observation-level analytical summary media such as the spore mosaic. Measurement exclusion requires separate explicit measurement/filter state.

There is already a similar architectural precedent in the repository distinguishing external-publication image exclusion from cloud-storage exclusion. Follow that style if appropriate.

If `AGENTS.md` has a suitable “do not violate” / invariants section, add a concise version there as well, but do not bloat AGENTS with implementation history.

## Naming / boundary clarity

While keeping the patch modest, inspect whether nearby names are encouraging the mistake.

Desired conceptual boundary:

* `_collect_artsobs_image_paths()` / publication selection:
  **which source image files are uploaded**
* `_prepare_publish_mosaic_inputs()`:
  **which observation measurements form the analytical summary**

Do not pass source-upload exclusion state into the second concept.

If a tiny rename or helper extraction makes this distinction substantially clearer, it is acceptable.

Do not perform a broad publish architecture refactor just to improve naming.

## Cache/signature implications

Inspect the mosaic cache/signature path carefully.

If the current mosaic dependency/signature includes source-publication exclusion state indirectly because the measurement list was filtered before computing dependencies, then correcting the measurement set should naturally make the cache reflect the full eligible measurement set.

Verify that:

* changing only which source images are selected for direct upload must **not** produce a semantically different mosaic;
* therefore source-image publication checkbox state should not unnecessarily invalidate or alter the mosaic cache if the underlying measurements/mosaic settings are unchanged;
* genuine measurement or mosaic-setting changes must still invalidate appropriately.

Add or adjust a cache/signature regression if needed.

Do not increment renderer/cache versions unless the actual cached output contract requires it. Explain if you think it does.

## Plate/export investigation

Some plate/export code may also accept `excluded_image_ids`.

Do not assume it is wrong just because the variable name is similar.

Determine whether that exclusion means:

* omit source-image panels from a plate — possibly correct;
* omit analytical measurements from an observation-level summary — potentially the same bug.

Report what you find.

Only change additional paths when they clearly violate the invariant established above.

## Acceptance criteria

The task is complete when all of these are true:

1. unchecking a source image prevents that source image from being externally uploaded;
2. measurements belonging to that source image remain eligible for the generated spore mosaic;
3. a 61-measurement observation cannot collapse to 3 mosaic spores solely because only one source image is selected for publication;
4. legitimate measurement/category filtering remains intact;
5. the existing test that enforced the wrong behavior has been reversed/replaced;
6. a clearly named regression test states the invariant;
7. code near the mosaic preparation path contains the contract comment;
8. durable repository documentation states the invariant;
9. no iNaturalist Stage 2 API behavior is changed;
10. no Stage 3 UX work is started.

## Scope exclusions

Do not implement:

* iNaturalist photo deduplication;
* remote/local photo mapping;
* Stage 3 publish action wording;
* new measurement-selection UI;
* new privacy controls;
* broad cloud-sync changes;
* a general publish-media rewrite.

If you discover a separate design issue, document it for follow-up rather than silently expanding scope.

## Verification

Run at least the focused suites covering:

* publish media;
* mosaic preparation;
* publish media cache;
* plate/export if touched;
* nearby external-publishing behavior.

Also run any existing test that currently encodes the old image-exclusion-to-mosaic coupling, since that contract must intentionally change.

No real external API request is needed for this task.

If practical, add a compact diagnostic test/fixture corresponding to local observation 835's failure shape:

```text
61 eligible spores across multiple microscope source images
only one source image selected for direct publication
3 spores happen to belong to that source image
expected mosaic measurement count = 61, not 3
```

## Handoff

When finished, stop.

Report:

* exact root cause found;
* every place source-image exclusion was affecting mosaic/derived measurement selection;
* which uses of source-image exclusion were retained because they genuinely control direct source upload;
* tests that previously encoded the wrong policy;
* new invariant tests;
* documentation/AGENTS changes;
* any plate/export ambiguity discovered but deliberately left out of scope;
* cache/signature implications;
* test command and result;
* candidate commit SHA.

Commit the fix on the working feature branch.

Do not push unless repository policy explicitly permits agents to push.

Do not merge to `main`.

Do not start iNaturalist Stage 3.

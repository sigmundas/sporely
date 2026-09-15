I’d do this in **three stages**. They are small enough to stay in one feature branch, but separating them keeps the dangerous part—changing remote iNaturalist state—from getting mixed up with the stale-link recovery.

### Stage 1 — Make the iNaturalist link truthful

Goal: fix your immediate problem without changing normal upload behavior.

Change the current rule from:

`inaturalist_id exists → refuse upload`

to:

`inaturalist_id exists → verify remote observation`

Add a small iNaturalist helper around the existing API code, probably alongside `INaturalistUploader` in `utils/artsobs_uploaders.py` or, preferably, extract the iNat-specific API methods into a focused helper if that class starts getting unwieldy.

Desired states:

| Local state         | iNaturalist response | Sporely behavior                   |
| ------------------- | -------------------- | ---------------------------------- |
| no `inaturalist_id` | —                    | Normal new publish                 |
| ID present          | 200                  | Existing publication               |
| ID present          | 404                  | Treat link as stale                |
| ID present          | 410 Gone             | Treat link as deleted/stale        |
| ID present          | auth/network/5xx     | Do **not** change ID; report error |

For 404/410, publishing should offer:

**“The linked iNaturalist observation no longer exists. Publish this find as a new iNaturalist observation?”**

On confirmation:

1. Keep the old ID temporarily.
2. Create the new iNaturalist observation using the normal publisher.
3. Upload the selected media.
4. Only after success, replace `inaturalist_id` with the newly returned ID.
5. Refresh the publication link in the Observations table.

If creation fails, the existing local ID remains untouched.

Tests for this stage should cover at least:

* linked observation returns 200 → don't create a duplicate;
* linked observation returns 404 → republish allowed;
* linked observation returns 410 → republish allowed;
* replacement publish succeeds → new ID stored;
* replacement publish fails → old ID retained;
* network/auth failure during verification → no local mutation.

**Acceptance test:** your current broken find can be selected, published to iNaturalist, Sporely notices that the old observation is gone, and creates a fresh one.

---

### Stage 2 — Add images to an existing iNaturalist observation

Goal: make deleting an entire iNaturalist observation unnecessary when only the plot/image was wrong.

Extend `INaturalistUploader` with an explicit operation along the lines of:

```text
add_images(observation_id, image_paths, access_token, progress_cb)
```

It should reuse exactly the upload mechanism Sporely already uses after creating an observation:

`POST /v1/observation_photos`

with the existing `observation_id`.

Then change the Publish flow for an observation whose iNat link verifies successfully.

Instead of:

> Upload failed: this observation already has an ID in iNaturalist.

the iNaturalist publish action becomes something like:

> **Add selected images to existing iNaturalist observation**

Use the same media-preparation pipeline Sporely already uses, including the publication checkboxes, generated spore plot, scale bars, plate, etc. The important thing is that **only the currently selected publishing media are sent**.

Do not change the observation metadata in this stage. Adding photos should mean adding photos, not silently rewriting species, notes, location, etc.

Tests:

* existing iNat ID + one selected new image → one `observation_photos` call;
* multiple selected images → all uploaded;
* no selected images → useful validation message;
* partial media-upload failure → observation ID remains valid;
* media preparation settings still apply;
* existing iNaturalist photos are never deleted.

**Acceptance test:** make a corrected spore plot in Sporely, select it, Publish → iNaturalist, and it appears as an additional photo on the already-published iNat observation.

---

### Stage 3 — Clean up the UX and make the state explicit

This should be relatively small, but I would do it after the API behavior is proven.

For iNaturalist, the Publish menu/action should reflect the actual state:

* **Not published** → `Publish to iNaturalist`
* **Published and reachable** → `Add images to iNaturalist`
* **Linked ID is stale** → `Republish to iNaturalist`

I would **not** put remote existence checks into every Observations-table refresh. That would make ordinary browsing dependent on the network and hammer iNaturalist unnecessarily. Verify when the user actually invokes an iNaturalist action.

The existing `iNat` link in the publication column can remain as-is.

I would also add one deliberate manual escape hatch, probably in the row/context menu:

**Clear iNaturalist link…**

with a confirmation explaining that it only removes Sporely's stored link and **does not delete anything from iNaturalist**.

That is useful even after automatic recovery exists, because external systems can get into odd states.

### Explicitly out of scope for this pass

I would keep these out so this remains a safe, compact change:

* synchronizing all Sporely images against all iNaturalist images;
* automatically deleting/replacing iNaturalist photos;
* tracking individual iNaturalist photo IDs;
* automatically updating taxon, location, description, date, etc. on an existing iNat observation;
* detecting iNaturalist-side image changes;
* making iNaturalist a general two-way sync target.

Those require a real per-media synchronization model. We don't need that to solve the problem you actually hit.

### Branch / implementation shape

I’d use one branch, something like:

```bash
git switch main
git pull --ff-only
git switch -c feature/inaturalist-republish-media
```

with three logical commits:

1. `inat: detect stale observation links and allow republish`
2. `inat: add media to existing observations`
3. `ui: expose iNaturalist publish/repair states`

The key existing areas are `utils/artsobs_uploaders.py`, `ui/observations_tab.py`, `ObservationDB.set_inaturalist_id(...)`, and the publishing tests. The current hard block in `_observation_has_existing_upload()` is the first thing to replace rather than work around.

I would have the agent complete **Stage 1 and its tests before touching Stage 2**. That gives you a working fix for the observation you already deleted even if the additional-media work uncovers complications.

---

## Stage 1 record — 2026-09-15 (implemented)

**Status:** implemented on `feature/inaturalist-republish-media`. Stage 2 not started.

### What the old flow did

`ObservationsTab._observation_has_existing_upload()` returned `True` for any
stored `inaturalist_id > 0`, and that answer was used as a hard block in three
places: the publish-menu action enablement and hint text
(`_update_publish_controls`), the batch gate in `publish_selected_observations`,
and the per-observation guard in `upload_observation_to_artsobs`. The first two
disabled the iNaturalist action outright, so a user whose remote observation had
been deleted could never even reach the publish code path.

### Design

- `INaturalistUploader.check_observation_link(observation_id, cookies)` in
  `utils/artsobs_uploaders.py` performs a single `GET /v1/observations/{id}`
  using the same bearer-token convention as `upload()`, and returns a frozen
  `RemoteLinkStatus` (`live` / `missing` / `unverified`).
- `_existing_upload_blocks_publish(uploader_key)` separates "a remote id is
  stored" from "that id may block publishing". Only iNaturalist is exempt; every
  other target keeps its previous behavior.
- `ObservationsTab._resolve_inaturalist_link_before_publish()` runs the check
  once, when the publish action is invoked and after the OAuth token is
  obtained. No remote check happens during table refreshes.

### Verified iNaturalist API behavior

`GET https://api.inaturalist.org/v1/observations/{id}` answers a deleted or
never-existing observation with **HTTP 200 and `{"total_results": 0,
"results": []}`**, not 404/410 (probed 2026-09-15 against ids `1` and
`999999999`). The empty result set is therefore the real "gone" signal; 404 and
410 are handled as well because alternate/older endpoints use them.

### Outcome table

| Remote answer | Behavior |
| --- | --- |
| 200 with matching result | `live` — refuse, keep the "already has an ID" message; no create request |
| 200 with empty `results` | `missing` — prompt to republish |
| 404 / 410 | `missing` — prompt to republish |
| 401 / 403 / 429 / 5xx / other non-200 | `unverified` — refuse, keep the stored id, report the failure |
| network error, timeout, unreadable payload, wrong id echoed back | `unverified` — same as above |

On `missing` the user is asked "The linked iNaturalist observation no longer
exists. Publish this find as a new iNaturalist observation?". Declining returns
`(False, None, None)` with no mutation and no create request. Confirming runs
the ordinary create-observation plus selected-media path; the stored
`inaturalist_id` is replaced only by the existing success branch
(`ObservationDB.set_inaturalist_id`), so a failed replacement leaves the old id
in place.

### Residual risk

A false `missing` is possible in principle if iNaturalist ever hides a live
observation from the authenticated lookup. The consequence is bounded: the user
must confirm, and the worst case is a duplicate remote observation. Sporely
never deletes local data or remote photos on this path.

### Deferred to later stages

Adding media to a live observation (Stage 2), publish-action wording per state
and a manual "Clear iNaturalist link" escape hatch (Stage 3).

### Stage 1 correction — 2026-09-15

Three boundary issues found in review of `c2213d0`.

**A. "Both" regression.** Relaxing the stored-id gate also made the combined
"Both" action eligible. Because `_publish_selected_observations_both()`
publishes `web` first and `inat` second, a live existing iNaturalist link would
have been discovered only after the Artsobservasjoner record was already
created. "Both" now keeps the original stored-id block, in the action
enablement and as a runtime guard in `_publish_selected_observations_both()`;
the individual iNaturalist action stays relaxed so stale-link repair works.

**B. Create-success / media-failure window.** `INaturalistUploader.upload()`
raised on the first image failure, discarding an observation id that already
existed remotely. The caller then kept its stale local id, so a retry created a
second replacement observation. The image loop now records the first failure in
`raw["image_upload_error"]` and returns the new id, reusing the partial-success
contract `utils/artsobservasjoner_submit.py` already uses and
`upload_observation_to_artsobs()` already handles. Create failure still raises,
so a failure before any remote observation exists leaves the old id untouched.

Artsobservasjoner web queues failed images for retry, so its media failures stay
non-final and its reporting is unchanged. iNaturalist has no such queue, so a
media failure there is reported as a warning and carried to batch callers as the
third element of `upload_observation_to_artsobs()`'s return tuple: `ok=True`
with a message now means partial success.

**C. Translations.** Five new source strings registered in the three Sporely
`.ts` catalogs via `tools/update_translations.sh`. The `.qm` files are unchanged
because the new strings are untranslated and `lrelease` omits them; they fall
back to English until a translator fills them in.

### Stage 1 batch-semantics correction — 2026-09-15

`_publish_selected_observations()` classified every `ok=False` result as a
failure and reported a partial success only when nothing else had failed.

`upload_observation_to_artsobs()` now documents four outcomes in its docstring,
and the batch wrapper sorts results into three buckets:

| Result tuple | Bucket |
| --- | --- |
| `(True, id, None)` | success |
| `(True, id, message)` | success **and** partial (remote record exists, media incomplete) |
| `(False, None, message)` | failed |
| `(False, None, None)` | skipped - the user declined the stale-link republish |

Only `_fail()` produces the failure shape and it always sets a message, so the
bare `(False, None, None)` from the decline branch is unambiguous.

Batch summaries:

| Outcome mix | Summary |
| --- | --- |
| all clean | success, unchanged |
| partial only | warning, "…with warnings" plus the first partial detail |
| hard failure only | error, unchanged |
| partial + hard failure | failure summary plus first error **and** first partial detail |
| decline only | info, "Publishing to {target} was cancelled." - not a failure |
| success + decline | success; the decline is not counted in the failure count |

One new source string, "Publishing to {target} was cancelled.", registered via
`tools/update_translations.sh`.

---

## Stage 2 record — 2026-09-15 (implemented)

**Status:** implemented on `feature/inaturalist-republish-media`. Base
`352a639`. Stage 3 not started.

### Existing flow that was changed

`upload_observation_to_artsobs()` resolves auth per uploader, and for `inat`
calls `_resolve_inaturalist_link_before_publish()` **before** any media work.
That check returned `(proceed, message, level)` and answered a live link with
`proceed=False` plus "Upload failed: this observation already has an ID in
{service}." — the refusal Stage 2 replaces. Media preparation
(`_prepare_publish_media_assets`, fed by `_collect_artsobs_image_paths`) runs
afterwards, inside the big `try`, and produces `upload_image_paths`; the worker
thread then calls `uploader.upload(...)`.

### Chosen create-vs-append control flow

`_resolve_inaturalist_link_before_publish()` now returns a frozen
`InatPublishDecision` (`ui/observations_tab.py`) carrying a mode
(`INAT_PUBLISH_MODE_CREATE` / `INAT_PUBLISH_MODE_APPEND`) and, for append, the
existing remote id. A live link yields append mode; missing still prompts for a
republish in create mode; unverified and a declined republish are unchanged.

The append id is held in `inat_append_observation_id` and drives three places:
the worker calls `add_images()` instead of `upload()`, the stored id is not
rewritten, and the result is reported with append wording. No other uploader
sees any change.

The confirmation deliberately happens **after** media preparation, not at the
link check, so the count in the prompt is the prepared-media count rather than
the gallery count.

### API method added

`INaturalistUploader.add_images(observation_id, image_paths, cookies,
progress_cb=None) -> UploadResult`, using the same bearer-token convention as
`upload()`. It POSTs `/v1/observation_photos` with
`observation_photo[observation_id]` per image and never POSTs `/observations`.
`upload()` keeps its own docstring saying it always means create; it does not
inspect any stored id. The per-image POST loop both methods share was extracted
as `_post_observation_photos()`.

`add_images()` raises only when nothing was attempted (missing token, non-
positive id, empty path list). Otherwise it returns
`UploadResult(sighting_id=observation_id, raw={observation_id,
images_requested, images_uploaded, image_upload_error?})`, which distinguishes
all three outcomes the caller needs.

### How prepared media reach the append

Unchanged pipeline. `_collect_artsobs_image_paths()` →
`_prepare_publish_media_assets()` → `upload_image_paths` → `add_images()`. No
second image-generation path exists, and the publishing checkboxes, annotated
images, measurement plots, thumbnail gallery, plate, scale bar and copyright
treatment all apply exactly as they do for a create.

### Result semantics

| `add_images` outcome | Reported as |
| --- | --- |
| all images attached | `(True, existing_id, None)`, success status |
| some attached, then one failed | `(True, existing_id, warning)`, partial success — the Stage 1 shape |
| nothing attached (first failed) | `(False, None, message)`, warning-level failure |
| `add_images` raised | `(False, None, message)` via the existing exception handler |
| append confirmation declined | `(False, None, None)`, the Stage 1 "cancelled" skip |
| no images selected | `(False, None, message)`, warning-level failure, nothing posted |

The stored `inaturalist_id` is never written on any of these paths, so
`ObservationDB.set_inaturalist_id()` (which marks the row dirty for cloud sync)
is not called at all in append mode.

### Duplicate-media limitation

Sporely persists no local-image ↔ iNaturalist `photo`/`observation_photo`
mapping, and the default publishing selection is *every* non-excluded field or
microscope image (`_collect_artsobs_image_paths`). Publishing a live-linked
find would therefore re-upload everything already published. Nothing
deduplicates, skips by filename, deletes, or replaces remote photos.

That risk is surfaced by `_confirm_inaturalist_media_append()`, which states
the prepared image count, the remote observation id, that only the current
selection is sent, that Sporely does not track what was already uploaded so
earlier images will be added again as duplicates, and that nothing else on the
observation changes. It defaults to No. Users narrow the selection through the
existing per-observation publish exclusion (`_publish_excluded_image_ids`).

### Stage 1 behavior preserved

Stale (`missing`) links still prompt and republish as a new observation,
replacing the id only on success. Unverified links still refuse with no create,
no append and no mutation. Observations with no stored id still take the
ordinary create path. `Both` still refuses outright for any stored `web` or
`inat` id, in both the action enablement and the runtime guard in
`_publish_selected_observations_both()`. The four batch result semantics are
unchanged; an append simply produces the success, partial or cancelled shapes
the batch wrapper already sorts.

### Design issues found, not silently fixed

1. **Batch appends prompt per observation.** The individual iNaturalist action
   is relaxed for stored ids, so selecting several live-linked observations
   produces one confirmation each. That is safe and explicit but tedious; a
   single selection-level prompt belongs with Stage 3's state-aware UI.
2. **Publish menu wording is still create-shaped.** The action still reads
   "iNaturalist" and the hint still says "Publish directly to …", even though
   invoking it on a live link appends. That is exactly Stage 3's scope and was
   left alone.
3. **A prepare-time warning outranks the append success message.** The
   pre-existing `publish_warning_text` branch reports "Upload completed with
   warnings" before the append-specific wording. Truthful, less specific;
   unchanged to keep the patch narrow.

### Files changed

- `utils/artsobs_uploaders.py` — `_post_observation_photos()`, `add_images()`,
  `upload()` docstring; `upload()`'s photo loop now delegates.
- `ui/observations_tab.py` — `InatPublishDecision` + mode constants;
  `_resolve_inaturalist_link_before_publish()` return type and live branch;
  `_confirm_inaturalist_media_append()`; append plumbing, id preservation and
  reporting in `upload_observation_to_artsobs()`;
  `_existing_upload_blocks_publish()` docstring.
- `tests/test_inaturalist_add_media_to_existing.py` — new.
- `tests/test_inaturalist_stale_link_republish.py` — `_RecordingUploader` gained
  `add_images`; `_build_env` gained configurable base/prepared media paths and
  records `_prepare_publish_media_assets` kwargs; the live-link test now pins
  "no duplicate create" instead of the removed refusal message.
- `i18n/Sporely_{nb_NO,sv_SE,de_DE}.ts` — six new source strings registered via
  `tools/update_translations.sh`. The `.qm` files are unchanged because the new
  strings are untranslated and `lrelease` omits them.

### Verification

`pytest tests/test_inaturalist_add_media_to_existing.py
tests/test_inaturalist_stale_link_republish.py
tests/test_artsobservasjoner_submit.py tests/test_publish_media_cache.py
tests/test_publish_media_stage2.py tests/test_publish_plate_export.py
tests/test_publish_targets.py` — 105 passed. No test makes a real iNaturalist
request.

The acceptance test is human-gated: a real append against a live iNaturalist
observation cannot be verified from here.

### Deferred to Stage 3

State-aware publish action wording ("Publish" / "Add images" / "Republish"), a
selection-level append confirmation, and the manual "Clear iNaturalist link"
escape hatch.

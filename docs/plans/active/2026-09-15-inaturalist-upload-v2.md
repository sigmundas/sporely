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

### Stage 2 correction — 2026-09-15

Two correctness boundaries found in review of `b40edad`. The Stage 2
architecture was accepted unchanged.

**A. Append required create-only metadata it never sends.**
`upload_observation_to_artsobs()` validated GPS coordinates and the observation
date near the top, before the iNaturalist create-vs-append decision existed. An
otherwise valid media append was therefore refused over fields `add_images()`
does not transmit and does not update remotely.

The two checks are now evaluated into a single deferred
`create_metadata_failure` tuple at the original position. Every target except
"iNaturalist with a stored id" still fails there, in the order it always did, so
no other publish path changed. Only that one case defers, and the refusal is
re-applied immediately after the auth/link chain once `decision.mode` is known.
A create or a stale-link republish therefore still fails exactly as before; only
the point of enforcement moved. `target_key` moved up because the deferral test
needs it, and the date check merged into the GPS check as an `elif`, which
preserves the existing message precedence when both are missing.

The create payload is now built only in create mode (`observation_payload` is
`None` for an append), so the append no longer constructs notes, spore
statistics, taxon, coordinates or habitat data that `add_images()` never reads.
That is also what the new tests exercise: building the payload evaluates
`float(lat)`, which would raise on a missing coordinate, so an append that
succeeds with no GPS proves the payload was not built.

Ordering note: the link check still runs first, so an unverified link or a
declined republish on a record that also lacks GPS/date reports the Stage 1 link
outcome rather than a metadata refusal. Both are covered by tests.

**B. Partial-append wording overstated the failure.**
`_post_observation_photos()` stops at the first failure, so with three images
and a failure on the second, the third is never attempted. "Added 1 of 3
images … the rest failed" claimed otherwise. The message now reads "Added
{done} of {total} images to {target} observation {id}. An image failed to
upload, so any images after it were not attempted. The observation's other
details and its earlier photos are unchanged." The stop-at-first-failure API
behavior is unchanged.

**Tests.** `tests/test_inaturalist_add_media_to_existing.py` gained a
create-only-metadata section: append proceeds without GPS, without latitude,
without date and with both missing (5 parametrised cases); create still fails
for missing GPS/date; stale-link republish still fails rather than bypassing
validation; an unverified link still reports the link failure; a declined
republish is still a plain skip; and an append with no GPS still refuses an
empty selection. The partial-append test and the batch partial test now pin the
corrected wording.

One source string replaced, one obsolete entry removed, via
`tools/update_translations.sh`; `.qm` unchanged (the new string is
untranslated).

**Verification.** Same suite as above: 115 passed.

### Stage 2 correction 2 — 2026-09-15

One remaining create-only dependency found in review of `eb787fa`.

The iNaturalist branch still called `_resolve_inaturalist_taxon_id(obs)` as its
first statement, before the create-vs-append decision existed. That helper reads
the local taxonomy tables and, on a miss, parses the Artsdatabanken taxon file,
so a media-only append was coupled to state `add_images()` never transmits.

The call moved into an `else` on the decision branch, so it runs for create and
for a confirmed stale-link republish exactly as before, and not at all for an
append, an unverified-link refusal or a declined republish (both of which return
before it). `taxon_id` stays `None` in append mode, which is harmless because the
create payload is not built there.

Nothing else changed: no new strings, no API change, no flow redesign.

**Tests.** `test_append_never_resolves_a_taxon` replaces the helper with one
that raises, then asserts the append still succeeds, `add_images()` ran, no
create happened and the stored id is unchanged - proving the coupling is gone
rather than merely unexercised. `test_create_and_republish_still_resolve_a_taxon`
asserts the helper is called exactly once on both create paths and that the new
id is stored. `test_refused_or_declined_publish_resolves_no_taxon` covers the
unverified and declined cases with the same raising helper.

**Verification.** Same suite: 119 passed.

---

## Stage 3 record — 2026-09-16 (implemented)

**Status:** implemented on `feature/inaturalist-republish-media`. Base `7c8196b`.
Stage 3 is the last stage of this plan.

### UI wording that was there

| Surface | Before |
| --- | --- |
| publish menu action | `iNaturalist` (the raw `INaturalistUploader.label`), no tooltip |
| publish button hint, single target | "Publish directly to {target}. Saved login will be used automatically…" |
| publish button hint, multiple targets | "Choose where to publish: {targets}…" |
| `Both` when disabled | no explanation at all |
| stale-link confirmation | title "Stale iNaturalist link"; "The linked iNaturalist observation no longer exists." - no id |
| append confirmation | title "Add images to iNaturalist"; already named the count, the id and the duplicate risk |
| publication column | `iNat` link to `https://www.inaturalist.org/observations/{id}` |
| row context menu | none existed |

Every one of those said or implied "create", even though invoking the action on
a live link had appended media since Stage 2.

### Chosen local-state wording

Only two states are knowable without a remote request, so only two are named:

| Local state | Action text | Tooltip / status tip |
| --- | --- | --- |
| no stored `inaturalist_id` | **Publish to iNaturalist** | "Publish this find as a new iNaturalist observation." |
| stored `inaturalist_id` | **Update iNaturalist…** | "Check the linked iNaturalist observation, then add the selected images or offer to republish if the link is stale." |

"Update" is truthful because it commits to changing the linked observation
without claiming which change: the ellipsis marks that something is decided
after the click, and the tooltip names all three outcomes (add images,
republish, refuse). The rejected wording was "Add images to iNaturalist", which
asserts the remote observation is reachable - a claim Stage 1 exists precisely
because Sporely cannot make from a stored id.

`_sync_inaturalist_action_wording()` does the retitling from
`_selection_has_existing_upload_for_uploader("inat")`, a local DB read, and is
called only from `_update_publish_controls()`. No remote request is issued to
produce a label.

Because the action's visible text is now state-dependent, `_uploader_label()`
would have started returning "Update iNaturalist…" inside sentences that name
the service ("Publishing to {targets}…"). The labels the menu was built with are
therefore kept in `_publish_action_base_labels` and preferred there, so every
existing status message is unchanged.

The single-target publish-button hint also switches to the update hint when
iNaturalist is the only enabled target and the selection is already linked;
otherwise "Publish directly to {target}…" is unchanged.

### Transient last-known state

**None was introduced.** No session cache, no timestamp, no persisted remote
health. The two-state wording above needs no memory of an earlier check, so
adding one would only create a second, decaying source of truth about remote
state. `check_observation_link()` remains the only authority and runs only when
the user invokes the action.

### "Clear iNaturalist link…"

The observations table had no row/context menu, so one was added:
`QTableWidget.setContextMenuPolicy(Qt.CustomContextMenu)` →
`_show_observation_context_menu()`, whose only entry is **Clear iNaturalist
link…**. It is enabled only when the selection contains a stored link, and
carries a tooltip either way. The publish menu was rejected as the home because
it is replaced by a direct-click button when only one publish target is enabled,
which would have made the action unreachable in exactly the single-target
iNaturalist setup that needs it most.

The confirmation names the remote id(s), then states that the link is removed
"from Sporely only", that Sporely "does not delete or modify anything on
iNaturalist: the observation and its photos stay exactly as they are", and that
the find is afterwards treated as unpublished so publishing again would create a
new observation. It defaults to No.

### Persistence semantics when clearing

`ObservationDB.set_inaturalist_id(observation_id, None)` was traced before being
used: it nulls the column and calls `_touch_observation(..., mark_dirty=True)`,
the same bookkeeping the publish success path uses when it *writes* an id. It is
the smallest existing truthful path, so no new setter was added.
`schedule_metadata_cloud_sync(observation_id)` is then called, matching
`_ensure_selection_publish_target()` and the `web` publish branch;
`inaturalist_id` is already in the cloud-sync observation field lists
(`utils/cloud_sync.py`), so the cleared value propagates as ordinary metadata.

Rendering is refreshed the same way the publish path does it:
`_find_table_row_for_observation()` → `_render_publish_cell()` with the reloaded
row, then `_update_publish_controls()`. The `iNat` link disappears and the
action text falls back to "Publish to iNaturalist".

**No iNaturalist request is made on this path.** The method touches
`ObservationDB`, the table and the status line only; it never obtains a token,
never calls `get_uploader`, and never reaches `check_observation_link()` or
`add_images()`. Two tests enforce this rather than assert it: the fake
environment installs an uploader whose `check_observation_link` raises and
replaces `utils.artsobs_uploaders.requests.get/post` with tripwires.

### Confirmation consistency

Both confirmations now lead with the remote id and name one operation only:

* append - "Add the {count} selected image(s) to the existing iNaturalist
  observation {id}?" plus the unchanged duplicate warning and "Nothing else on
  the iNaturalist observation is changed." Title "Add images to iNaturalist".
  Unchanged from Stage 2; the duplicate warning was **not** shortened.
* republish - "The linked iNaturalist observation {id} no longer exists. /
  Publish this find as a new iNaturalist observation? / Sporely's stored link is
  replaced only if the new observation is created successfully." Title changed
  from "Stale iNaturalist link" to **Republish to iNaturalist**, and the id was
  added.

Both still default to No. A test asserts the republish message contains no
duplicate warning, so the two operations cannot silently blur.

### Batch prompts: deliberately left per-observation

Not consolidated. Whether an observation appends or republishes is knowable only
after its own `check_observation_link()`, so a single up-front prompt would
either guess, or force the batch loop into a check-everything-then-act two-phase
state machine - the complexity the stage brief rules out. A mixed selection
(no-id, live, stale, unverifiable) has no honest one-sentence summary. The
per-observation confirmations stay, with the improved wording; only the actions'
titles and hints changed.

### Files changed

- `ui/observations_tab.py`
  - `__init__`: `_publish_action_base_labels`.
  - table construction: `setContextMenuPolicy` + `customContextMenuRequested`.
  - `_build_publish_menu()`: records base labels, `setToolTipsVisible(True)`.
  - `_uploader_label()`: prefers the base label.
  - new `_inaturalist_action_label()`, `_inaturalist_action_hint()`,
    `_sync_inaturalist_action_wording()`.
  - `_update_publish_controls()`: calls the wording sync in both the
    no-selection and normal paths; iNaturalist-only hint branch; `Both` tooltip
    in the enabled and disabled cases.
  - new `_show_observation_context_menu()`, `_selected_inaturalist_links()`,
    `_confirm_clear_inaturalist_link()`,
    `_clear_inaturalist_link_for_selection()`.
  - `_resolve_inaturalist_link_before_publish()`: republish confirmation title
    and text only.
- `tests/test_inaturalist_publish_state_ux.py` — new, 23 tests.
- `i18n/Sporely_{nb_NO,sv_SE,de_DE}.ts` — 18 new source strings, 2 obsolete
  removed, via `tools/update_translations.sh`. The `.qm` files are unchanged
  because the new strings are untranslated and `lrelease` omits them; they fall
  back to English.

No behavior in `utils/artsobs_uploaders.py`, the Stage 1/2 decision logic, the
append/create flow, the batch result semantics or the `Both` gate was changed.

### Verification

`pytest tests/test_inaturalist_publish_state_ux.py
tests/test_inaturalist_add_media_to_existing.py
tests/test_inaturalist_stale_link_republish.py
tests/test_artsobservasjoner_submit.py tests/test_publish_media_cache.py
tests/test_publish_media_stage2.py tests/test_publish_plate_export.py
tests/test_publish_targets.py tests/test_observations_tab_cloud_sync.py` —
**218 passed**. No test makes a real iNaturalist request.

The new file covers: create-oriented label/hint without a stored id;
check-oriented label/hint with one, asserted not to say "Add images"; the
iNaturalist-only button hint; `_uploader_label()` stability; no remote call
during `_update_publish_controls`/`_sync_inaturalist_action_wording`/
`_selected_inaturalist_links`; empty selection resetting the label; live → append
mode; the append confirmation's count/id/duplicate text; missing → republish
confirmation naming the id and carrying no duplicate warning; declined republish
as a bare skip; unverified → warning with the detail and no republish offer;
clear-link confirmed (id nulled, dirty/cloud-sync bookkeeping, UI refresh,
status text, no API call); clear-link declined (nothing changes); the
"not modified on iNaturalist" wording; multi-row clearing skipping unlinked
rows; clear-link with nothing linked; the context menu's contents and
enablement in both states; the `iNat` link still rendering for a stored id and
disappearing once cleared; `Both` still disabled for a stored id with the new
explanation, and still enabled for an unpublished find.

### Human-gated verification

No test instantiates the real `ObservationsTab`, so these need the running app:

1. Select a find with **no** iNaturalist link — the publish menu entry reads
   *Publish to iNaturalist*; hovering it shows the create hint.
2. Select a find **with** a link — the entry reads *Update iNaturalist…* with
   the check/add/repair hint, and is not greyed out.
3. Browse and re-sort the table with linked finds selected and deselected —
   no network activity, no delay, no iNaturalist errors.
4. Right-click a linked row — *Clear iNaturalist link…* is present and enabled;
   right-click an unlinked row — it is greyed out with the explanation.
5. Clear a link, decline the dialog — the `iNat` link stays.
6. Clear it again and confirm — the `iNat` link disappears, the action text
   falls back to *Publish to iNaturalist*, and the observation still opens on
   iNaturalist through the URL you noted from the dialog (proving nothing was
   deleted remotely).
7. Click the `iNat` link on a linked row — it opens the observation and changes
   nothing.
8. Invoke *Update iNaturalist…* on a live link — the Stage 2 append
   confirmation appears, defaulting to No.
9. Invoke it on a deleted remote observation — the *Republish to iNaturalist*
   confirmation names the old id.
10. Select a linked find with both targets enabled — *Both* is greyed out and
    its tooltip points at the individual iNaturalist action.

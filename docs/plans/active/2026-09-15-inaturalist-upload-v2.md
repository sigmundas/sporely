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

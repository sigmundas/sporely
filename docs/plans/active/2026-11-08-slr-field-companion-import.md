# SLR field capture with the phone as companion

Status: proposed, not started. Stage F0 (decisions and a read-only spike) is next.
Owner request: 2026-11-08.

## Goal

In the field the user shoots with an SLR and carries the phone. For each find,
the phone records an observation (a few photos, GPS, the species guess) with the
Sporely mobile app. Back home, the SD card goes into Kameraimport, and the
desktop places each SLR photo in the observation it belongs to:

- SLR photos taken close in time to a synced phone observation go into that
  observation as field photos.
- SLR photos that match no observation are grouped by time, the same way the
  mobile import groups photos (photos less than the gap apart, 1 minute by
  default, are one find), and offered as new observations.

No new capture screen is needed on the phone. The phone's existing capture flow
is the record of what the SLR captured.

## What already exists

- **Kameraimport field matching** (`ui/ingestion_hub_tab.py`,
  `utils/temporal_matcher.py`): an SD-card photo outside every microscope
  session is matched to an observation when it is close to that
  observation's time window. The window is built in
  `TemporalMatcher._observation_window_for_observation`: it runs from the
  earliest to the latest capture time of the observation's non-microscope
  images. Without image capture times, it is the observation datetime alone.
  The tolerance is the setting `ingestion_hub_field_match_tolerance_seconds`,
  60 s by default. It has no UI.
- **Clock offset:** a manual offset, and an automatic try of ±1, 2 and 3 hours
  when nothing matches (`IngestionHubTab._auto_apply_clock_offset_if_helpful`).
  Sync Shot (a QR clock photographed by the camera) gives an exact offset, but
  it is shown on the desktop screen, so it suits the microscope, not the forest.
- **Mobile time grouping** (`sporely-web/src/screens/import_review.js`,
  `_groupSourceItems`): photos sorted by capture time, and a new group starts
  when the gap to the previous photo exceeds the gap setting. The gap is
  `getPhotoGapMinutes()`, 1 minute by default, clamped to 10 s–120 min, and
  stored per device in localStorage.
- **Already-imported filter:** Kameraimport hides photos whose capture time
  already exists on the observation.

## Gaps

1. **Phone photo capture times probably do not reach the desktop.** The mobile
   app writes `observations.captured_at` but no per-image
   `observation_images.captured_at`. On pull, the desktop fills a missing image
   capture time from the observation datetime (sync contract, "Existing
   cloud-backed image"). Each pulled observation's window therefore shrinks to
   one moment, and with a 60 s tolerance, SLR photos taken a few minutes into a
   find will not match. The window can also be rebuilt from the EXIF of the
   downloaded files, but only if the mobile upload keeps EXIF. F0 checks this.
2. **No fine clock offset.** SLR clocks drift by seconds to minutes, and the
   automatic offset only tries whole hours. A 90 s error breaks matching at a
   1 minute gap.
3. **No time grouping on the desktop.** Unmatched photos are listed one by one;
   nothing turns them into proposed observations.
4. **Matching is per photo, not per find.** One SLR burst that straddles the
   edge of a window can be split between an observation and "unmatched". The
   unit should be the group: a group that overlaps a window goes to it as a
   whole.
5. **Kameraimport cannot create observations.** It only adds photos to
   existing ones.

## Decisions for the owner (before F1)

1. **Source of phone capture times** (gap 1):
   - **A. Desktop only:** read EXIF capture times from downloaded cloud images
     and use them for the window, without writing them back to the cloud.
     This only works if mobile uploads keep EXIF; F0 checks that.
   - **B. Mobile writes `observation_images.captured_at`** for each photo. This
     is a cross-repo change (sporely-web) and a sync-contract change: update
     `docs/supabase-sync-contract.md` in both copies. It is also more robust,
     since it doesn't depend on EXIF surviving.
   - Recommendation: B if F0 shows EXIF is stripped, otherwise A first, with B
     as a follow-up.
2. **Gap default and storage:** a desktop setting, 1 minute by default with the
   same 10 s–120 min range as mobile, kept on the desktop only. The mobile
   value lives in localStorage and is not synced.
3. **New observations from unmatched groups:** created as local drafts with
   SLR GPS if the camera recorded it, otherwise with no location. Their date is
   the first photo's capture time. They sync like any desktop observation once
   saved.
4. **Phone as clock reference:** should the mobile app get a "Show clock QR"
   screen, so the SLR can photograph the phone once per trip? This would reuse
   the Sync Shot QR format. It is optional and cross-repo; F3 can work without
   it.

## Stages

Each stage is one PR, following AGENTS.md: Codex review, and before/after
screenshots for visible changes.

### F0 — Spike (read-only)

On a test profile copied from real data, with images left out per the
test-profile rule, plus a few downloaded cloud images:

- For pulled observations created on mobile, compare local
  `images.captured_at` with the observation datetime.
- Check whether the downloaded files carry EXIF `DateTimeOriginal`.
- Check whether the cloud rows carry `observation_images.captured_at`. This is
  read-only and needs no approval.

Output: a short note in this plan answering decision 1, plus the typical
spread of phone photos within one find.

### F1 — Phone capture times in the observation window

Implement decision 1:

- **For A:** `_observation_window_for_observation` prefers the file's EXIF
  capture time over a capture time that the pull injected from the observation
  datetime. Don't overwrite the stored value with a value the sync contract
  would then push.
- **For B:** the mobile upload writes per-image `captured_at`; the desktop pull
  keeps it; both copies of the contract are updated.

Tests: matcher unit tests with pulled-style image rows.

### F2 — Group-based matching and grouping

- Add a gap setting.
- Group the scanned photos by capture time, porting the mobile
  `_groupSourceItems` rule.
- Match groups, not single photos, against observation windows. If a group
  overlaps two windows, split it at the boundary and flag it.
- Leave microscope session matching unchanged: photos inside an offline
  session keep matching per photo.

Tests: `TemporalMatcher` unit tests for grouping, straddling, overlap and the
gap clamp.

### F3 — Fine clock offset

- Estimate the SLR offset by aligning SLR groups to phone windows: choose the
  offset in a bounded range, for example ±15 min after the hour candidates,
  that maximizes the number of matched groups while keeping the total
  distance small.
- Show the result as a suggestion that the user accepts, not a silent change.
- Optional: the phone clock QR from decision 4.

Tests: synthetic drifts of 0 s, 45 s, 4 min and 1 h + 30 s.

### F4 — Kameraimport UI for field trips

Inside the existing Kameraimport tab, with no new dialog, per the redesign's
fewer-dialogs direction:

- matched groups listed under their observation;
- unmatched groups shown as "New observation" proposals;
- move, split and merge groups;
- create the new observations on import, using the existing observation
  creation path;
- English strings, translated to nb_NO, sv_SE and de_DE.

Verification: qtbot tests on the real tab, a renderer scenario with
dark-theme and nb_NO screenshots, and the owner's layout opinion before merge.

Coordinate with the UI redesign plan
(`docs/plans/active/2026-11-08-desktop-ui-redesign.md`): Kameraimport is a
staging screen there, so F4 should use the redesign's shared components if
they have landed.

### Manual tests (human-gated, after F4)

1. A real field trip: phone observations plus SLR photos, synced, then the SD
   card imported. Did each SLR photo land in the right find?
2. Check that the new observations made from unmatched groups read correctly
   as finds to you as a mycologist.
3. If B is chosen: check cross-client sync of the per-image capture times with
   a real account.

## Out of scope

- A GPS track from the phone to geotag SLR photos. This would be a natural
  follow-up once groups map to observations.
- Live tethering or Wi-Fi transfer from the SLR.
- Changes to the mobile capture flow, other than optional decision 4 and
  per-image `captured_at` under B.

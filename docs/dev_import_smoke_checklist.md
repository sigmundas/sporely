# Import-Path Smoke Checklist

**Purpose:** Manual smoke-test matrix for the staged image import work (Stages 1–6).  
**Last updated:** 2026-06-09 (Stage 6.1 corrective pass — watcher timeout retry semantics fixed)

---

## Live Lab

| # | Scenario | Expected outcome | Status |
|---|----------|-----------------|--------|
| LL-1 | RAW-only file copied into watched folder | File stabilises → emitted → rendered → imported; `raw_processing` in `lab_metadata`; `_seen_source_paths` updated | ☐ |
| LL-2 | RAW + JPEG pair, preference = Prefer RAW | RAW rendered and imported; JPEG suppressed via companion-group dedup; `source_kind = raw` | ☐ |
| LL-3 | RAW + JPEG pair, preference = Camera JPEG | JPEG imported; RAW not rendered; `source_kind = camera_jpeg`; no `raw_processing` in metadata | ☐ |
| LL-4 | Slow RAW copy (>10 s stabilise) → times out → file later stabilises → new watcher event fires | Timeout logged as WARNING; path **not** in `_handled_paths`; later watcher event retries and imports successfully | ☐ |
| LL-5 | Slow RAW copy times out → user clicks Rescan folder | Timeout logged; path not in `_seen_source_paths`; Rescan picks it up and imports once | ☐ |
| LL-6 | RAW render failure + companion JPEG present | Fallback to JPEG; `fallback_used=True`; `fallback_reason` set; no `raw_processing` in `lab_metadata` | ☐ |
| LL-7 | Already-imported file appears again (watcher duplicate event) | `_handled_paths` (watcher) and `_seen_source_paths` (session) both block second import | ☐ |
| LL-8 | Moved-in complete file (on_moved event) | Emitted once; imported; `_handled_paths` updated | ☐ |

---

## Prepare Images

| # | Scenario | Expected outcome | Status |
|---|----------|-----------------|--------|
| PI-1 | RAW-only file selected | Candidate staged with `source_kind = raw`; rendered to working JPEG; `raw_processing` in `lab_metadata` | ☐ |
| PI-2 | RAW + JPEG pair, preference = Prefer RAW | RAW selected; JPEG recorded as `camera_jpeg_path`; `has_raw_companion = True` | ☐ |
| PI-3 | Bad RAW + good JPEG in same batch | RAW candidate fails; fallback to JPEG; `fallback_used=True`; batch continues; other candidates unaffected | ☐ |
| PI-4 | HEIC file | Converted to JPEG working copy; `source_kind = heic`; no `raw_processing` | ☐ |
| PI-5 | Continue filters failed rows | Failed candidates remain in list with `status = failed`; user can retry or skip; successful candidates commit normally | ☐ |

---

## Ingestion Hub

| # | Scenario | Expected outcome | Status |
|---|----------|-----------------|--------|
| IH-1 | RAW + JPEG pair matched to microscope session | Candidate built with `raw_path` and `camera_jpeg_path`; `source_kind = raw`; `raw_processing` in committed `lab_metadata` | ☐ |
| IH-2 | Metadata survives commit | `microscope`, `session`, `user`, `objective`, `contrast`, `mount_medium`, `stain`, `sample_type` all present after `ImageDB.add_image` | ☐ |
| IH-3 | Add-all continues after one failed candidate | Failed candidate marked `status = failed`; remaining candidates committed; no exception propagation | ☐ |
| IH-4 | Sync Shot still works | QR-code-matched image committed with correct observation linkage; `raw_processing` preserved if RAW-backed | ☐ |

---

## Calibration

| # | Scenario | Expected outcome | Status |
|---|----------|-----------------|--------|
| CA-1 | RAW calibration source | RAW rendered to derivative; calibration worker receives derivative (raster) path only; `source_path` (RAW) stored as provenance | ☐ |
| CA-2 | RAW + JPEG pair | Preferred path selected per policy; `fallback_used` / `fallback_reason` set if RAW fails | ☐ |
| CA-3 | Failed RAW + valid raster batch | Failed RAW candidate skipped; raster candidates proceed; calibration result valid | ☐ |
| CA-4 | Automatic calibration worker path is raster derivative | Worker receives `working_path` (JPEG/PNG), not the original RAW; derivative dimensions recorded in `processing_metadata` | ☐ |

---

## Watcher Timeout Retry Semantics (key invariants)

These invariants were verified and corrected in Stage 6.1 (2026-06-09):

### `_handled_paths` — watcher-level dedup (NewImageHandler)

- **Successful emit** adds the path to `_handled_paths`. Later `on_created`/`on_moved` events for the same path are skipped.
- **Timeout does NOT add the path to `_handled_paths`**. The path remains retryable by any later watcher event.
- A timeout is logged as a WARNING. Repeated timeout warnings for the same path within the cooldown window (`WATCHER_TIMEOUT_LOG_COOLDOWN_SECONDS`, default 60 s) are suppressed to avoid log noise, but the retry is never blocked.

### `_seen_source_paths` — session-level dedup (LiveLabTab)

- Populated only when `_queue_companion_source` is called (i.e., only after a successful watcher emit).
- A timed-out path is never emitted, so it is never added to `_seen_source_paths`.

### Rescan folder

- `rescan_watch_folder()` bypasses the watcher entirely and calls `_queue_companion_source` directly.
- It is only blocked by `_seen_source_paths` (session-level dedup).
- A previously timed-out file that was never emitted will be picked up by Rescan.
- An already-imported file will be skipped by Rescan (it is in `_seen_source_paths`).

### Recovery paths for a timed-out file

1. **Automatic** — if the file later stabilises and the watchdog fires another `on_created`/`on_moved` event, the watcher will retry stability checking and emit if successful.
2. **Manual** — the user clicks **Rescan folder** to force a directory scan.

---

## Notes

- All paths above use `prepare_local_ingest_image` → `LocalIngestResult` as the shared ingest façade.
- `raw_processing` is only written to `lab_metadata` when a RAW file is successfully rendered. Fallback JPEG paths explicitly pop `raw_processing` from `lab_metadata` (see `image_import_candidates.py` line 981).
- Calibration workers always receive the raster derivative (`working_path`), never the original RAW. The RAW path is stored as provenance only.

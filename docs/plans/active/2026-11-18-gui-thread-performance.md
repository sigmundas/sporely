# GUI-thread performance

Source: external review (its P1–P8), verified on 2026-11-18 against `main` at
`a2cc84f`. Line numbers are as of that commit.

Verification tier: correctness is agent-verified (tests). **Performance on
real data is human-gated** under AGENTS.md. Each stage therefore ends with a
short before/after timing checklist for the owner, and it is not committed
until the owner confirms the change feels right on their data.

## Verified findings

| # | Verdict | Where it really is | Fix size |
|---|---|---|---|
| P6 EXIF parsed twice | Confirmed | `utils/exif_reader.py:262` `get_image_metadata` → `get_image_datetime` (:190) and `get_gps_coordinates` (:228) each call uncached `get_exif_data` (:21). Callers: `observations_tab.py:13669, 17870, 19295`. | Small |
| P5 reference picker N+1 | Half true | `ui/add_reference_dialog.py:397` `default_my_observation_candidates`: per row `get_measurements_for_observation` (:425) + `get_observation` (:434), so ~2N+1 queries. `reference_picker_drawer.py` **does not exist**; the claimed second re-fetch is unverified. | Small |
| P4 gallery thumbnails | Partly | `utils/thumbnail_generator.py:148` `get_thumbnail_path`: one connection, up to 3 SELECTs per image (not 3 connections), on the GUI thread via `image_gallery_widget.py:2719` `_load_pixmap`. A 512-entry pixmap cache already exists (:642). Reusable worker: `_ObservationGalleryLoader(QThread)` (:519). | Small (bulk path lookup); medium (async decode) |
| P8 startup imports | Confirmed | `main.py:78` imports `MainWindow` at module level, before `QApplication` (:235) and the splash (:317). That pulls matplotlib (`main_window.py:281-285`), numpy, `utils.cloud_sync`. Existing lazy pattern: `utils/rawpy_import.py`. | Medium |
| P7 analysis plot | Confirmed | `main_window.py:17773` `update_graph_plots`: `figure.clear()` + re-add subplots each call; `canvas.draw()` at :17899 (needed for the tight-bbox layout pass) and :17912. A second function draws twice at :19081/:19094. `draw_idle` already used at :8296. | Medium |
| P3 network on GUI thread | Partly | 7 `requests` calls in `observations_tab.py`; **2 already run in `INatAIGuessWorker`** (:1740). GUI-thread ones: `_taxon_id_from_nbic` (:1451), `_inat_taxon_id` (:1463), `_gbif_taxon_id` (:1511), `_artskart_taxon_id` (:1540), `_artsobs_picker_search` (:11043). `_finish_manual_refresh_flow` (:4050) runs `_upload_pending_artsobs_web_images` (:4615) synchronously, one upload per observation group (not per image). | Medium |
| P1 RAW import on GUI thread | Partly; wrong location | The cited slot `MainWindow.load_image` is dead code (see stale-code plan). Real paths: `ImageImportDialog.add_images` (`image_import_dialog.py:4201`) calls `prepare_image_import_candidates` in a loop on the GUI thread; `MainWindow.load_image_record` (`main_window.py:13550`) calls `prepare_local_ingest_image` (:13566) synchronously on every record load. | Medium |
| P2 Prepare Images apply | Partly | LANCZOS resize in `_resample_import_image` (`observations_tab.py:13819`), from `_apply_import_results_to_observation` (:14213). `generate_all_sizes` (:14461, :14555) re-reads the saved file (`thumbnail_generator.py:38`). The `processEvents` claim is not true for this path. | Medium |

The review's companion refactor R2 ("move SQL and HTTP out of widgets") is
partly true: 32 `.execute(` calls across `main_window.py` (15),
`observations_tab.py` (12) and `image_import_dialog.py` (5), and temp-table map
builders at `observations_tab.py:6150-6311`. Its "17 cloud-client constructions"
did not reproduce (about 10). Do not run R2 as its own project; extract only
what a stage below needs.

## Stages (each its own PR, in this order)

1. **P6 — parse EXIF once.** `get_image_metadata` reads EXIF once and passes it
   to the datetime/GPS helpers (keep the public helpers' signatures). Add an
   `(path, mtime)` cache only if the owner's timing shows it is still needed.
2. **P5 + P4 path lookup — batch queries.** One grouped query for reference
   candidates; resolve thumbnail paths for a whole gallery load in one query.
   Leave pixmap decoding where it is.
3. **P8 — defer heavy imports.** Import `MainWindow` inside `main()` after the
   splash is shown; lazy-import matplotlib where the analysis tab first needs
   it. Check the frozen app still starts (owner check; no PyInstaller run by the
   agent unless asked).
4. **P7 — update the plot in place.** Keep figure and axes; update artists; one
   draw where the tight-bbox pass needs it, `draw_idle` otherwise. Renderer
   screenshots before and after must match.
5. **P3 — network off the GUI thread.** Move the five lookups and the Artsobs
   upload into workers using the existing `INatAIGuessWorker` pattern; keep the
   UI disabled/busy while they run. Touches Artsobs credentials, so read the
   flow end to end first.
6. **P1 — RAW decode off the GUI thread.** Make `prepare_local_ingest_image`
   usable from a worker and call it from one in `load_image_record` and
   `add_images`. Put the worker at the shared helper level so it survives
   redesign Stage 9 (which retires `ImageImportDialog`).
7. **P2 — Prepare Images apply.** Generate thumbnails from the in-memory resized
   image instead of re-reading the file; then move the loop into a worker.
   Coordinate with redesign Stage 9 before investing in the worker half.
8. (Optional) **P4 async decode** in the gallery, if stage 2 is not enough.

Stages 1–4 are independent and small to medium. Stages 5–7 change threading
and should go one at a time with focused `qtbot` tests.

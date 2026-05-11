# Sporely Desktop — History & Debugging Notes

## Phase 7: Transparency, Social Trails, and Privacy Slots
*Implemented Q2 2026*

### Change of Plans: Open Science First
We moved from a "Safety First" (hidden by default) model to an **"Open Science First"** (transparent by default) model to improve data density.
- **Drafts are Public by default.** This allows the community to see the "Live Stream" of science happening via a subtle "Draft" badge in the feed.
- **Privacy Slots Introduced.** If a user wants total secrecy, they toggle it to **Private** or **Fuzzed Location**, which consumes 1 of 20 Free Tier "Privacy Slots". 
- **Visibility Schema Overhaul:** 
  - Legacy `'draft'` visibility value shifted to `'private'`. 
  - `is_draft` (boolean) now handles workflow independently.
  - `location_precision` (`'exact'` vs `'fuzzed'`) separated from general visibility.

### Completed Phase 7 Milestones
- Added `is_draft` and `location_precision` columns to `observations` table (SQLite & Supabase).
- Created `follows` table for social trails (`user_id`, `target_type`, `target_id`).
- Updated Postgres trigger `enforce_non_public_observation_limit()` to watch for `visibility != 'public'` OR `location_precision = 'fuzzed'`.
- Updated `observations_community_view` to return exact GPS by default.
- Refactored UI layouts across desktop/web to support the split Draft/Privacy scope.
- Preserved local `private` semantics while translating legacy cloud `draft` rows backward securely.

## R2 Media Migration
*Migrated Q2 2026*
- Shifted media from Supabase Storage `observation-images` to Cloudflare R2 bucket `sporely-media`.
- Deployed Cloudflare Upload Worker at `upload.sporely.no` enforcing ES256 JWT auth and updating user storage quotas.

## Refactor Notes & Lessons Learned
- **Splitter Collapse Traps:** Historically, fixed minimum widths on QSplitter children caused sidebars to get stuck. Standardized on `QSizePolicy.Ignored` horizontally with minimal guardrail limits.
- **Snapshot Staleness:** An issue in cloud pull where the stored snapshot missed the newly assigned `desktop_id`, causing the *next* sync to falsely read a conflict (shifting from `cloud:<id>` to `desktop:<id>`). Fixed by refreshing the snapshot *after* all ID injections.
- **Thread Garbage Collection:** `QThread` objects were prematurely collected because custom signals (`search_done`) shadowed built-in thread signals. Fixed by blocking local cleanup until `QThread.finished` properly emits.
- **OAuth Event Loop Blocking:** The local HTTP server used for iNaturalist OAuth2 callbacks blocks the main thread. To keep the PySide6 UI responsive (and allow cancellations), a `tick_callback` (which calls `QApplication.processEvents()`) must be propagated through the `authorize()` signature down to the server's `wait_for_callback` loop.
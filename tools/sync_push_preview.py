"""Preview what a global Sync would push, without touching the real database.

Copies the live observation database with SQLite's backup API (the source is
opened read-only), points the app's database path at the copy, runs the same
local pre-push re-dirty scans ``push_all`` runs, then lists every push
candidate. No network calls. Writes only to the copy.

Calibrations are compared with the cloud's list during a real sync, so only
their local count is shown. ``images_without_cloud_id`` is an upper bound:
storage-intent rules can exclude some of them.

Usage (from the sporely-py worktree, Sporely closed or open):
  PYTHONPATH=. .venv/bin/python tools/sync_push_preview.py <scratch dir>
"""
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

scratch = Path(sys.argv[1])
live = Path(os.path.expanduser("~/Library/Application Support/Sporely/mushrooms.db"))
copy = scratch / f"sync-preview-{datetime.now():%Y%m%d-%H%M%S}.db"
src = sqlite3.connect(f"file:{str(live).replace(' ', '%20')}?mode=ro", uri=True)
dst = sqlite3.connect(copy)
src.backup(dst)
dst.close()
src.close()

import database.schema as schema  # noqa: E402

real_images_dir = schema.get_images_dir()
schema.get_database_path = lambda: copy
schema.get_images_dir = lambda: real_images_dir

from utils import cloud_sync  # noqa: E402
from utils.cloud_sync_impl import pending_images  # noqa: E402
from utils.cloud_sync_impl.child_identity import redirty_interrupted_child_creates  # noqa: E402
from utils.cloud_sync_impl.observation_delete import pending_observation_deletes  # noqa: E402


def dirty_ids():
    conn = sqlite3.connect(copy)
    rows = conn.execute(
        "SELECT id FROM observations WHERE cloud_id IS NULL OR sync_status = 'dirty'"
    ).fetchall()
    conn.close()
    return {r[0] for r in rows}


before = dirty_ids()
redirty_interrupted_child_creates()
cloud_sync._mark_cloud_observations_dirty_for_media_changes()
cloud_sync._mark_cloud_observations_dirty_for_image_capture_time_changes()
due, reason = pending_images._cloud_pending_image_repair_scan_due()
if due:
    pending_images._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
    )
after = dirty_ids()

conn = sqlite3.connect(copy)
conn.row_factory = sqlite3.Row
report = {
    "copy": str(copy),
    "already_pending": sorted(before),
    "added_by_pre_push_scans": sorted(after - before),
    "pending_image_scan_due": [due, reason],
    "pending_observation_deletes": sorted(pending_observation_deletes()),
    "pending_image_tombstones": conn.execute(
        "SELECT COUNT(*) FROM image_tombstones WHERE delete_synced_at IS NULL").fetchone()[0],
    "local_calibrations": conn.execute("SELECT COUNT(*) FROM calibrations").fetchone()[0],
    "observations": [],
}
for obs_id in sorted(after):
    row = conn.execute(
        "SELECT id, genus, species, cloud_id, sync_status, is_draft, sporely_taxon_id, "
        "taxon_identity_proof FROM observations WHERE id = ?", (obs_id,)).fetchone()
    images = conn.execute(
        "SELECT image_type, cloud_id IS NULL AS new FROM images WHERE observation_id = ?",
        (obs_id,)).fetchall()
    report["observations"].append({
        **dict(row),
        "images_total": len(images),
        "images_without_cloud_id": sum(1 for i in images if i["new"]),
    })
conn.close()
print(json.dumps(report, indent=1, default=str))

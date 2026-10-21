#!/usr/bin/env python3
"""Add table cloud_scope(sid) to the audit work DB: the concepts the
global_macrofungi_policy_v1 cloud export carries for the audited release.

Reuses database/taxonomy/evidence/taxonomy-v3/audit_stage0_coverage.cloud_scope
(the same predicate the Stage 2 Group-B audit used). Opens the release
read-only (mode=ro URI). Writes only to --workdb.

  python -B scripts/compute_cloud_scope.py --repo <worktree> \
      --release <copy of release sqlite> --workdb <workdb.sqlite3>
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.dont_write_bytecode = True


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--repo", required=True, type=Path)
    p.add_argument("--release", required=True, type=Path)
    p.add_argument("--workdb", required=True, type=Path)
    a = p.parse_args(argv)
    sys.path.insert(0, str(a.repo))
    sys.path.insert(0, str(a.repo / "database/taxonomy/evidence/taxonomy-v3"))
    import audit_stage0_coverage as stage0  # noqa: E402

    conn = sqlite3.connect(f"file:{a.release}?mode=ro", uri=True)
    scope = stage0.cloud_scope(
        conn, a.repo / "database/taxonomy/policies/global-macrofungi-scope.yml")
    conn.close()
    db = sqlite3.connect(a.workdb)
    db.execute("drop table if exists cloud_scope")
    db.execute("create table cloud_scope(sid integer primary key)")
    db.executemany("insert into cloud_scope values(?)", [(s,) for s in sorted(scope)])
    db.commit()
    print("cloud_scope", len(scope))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

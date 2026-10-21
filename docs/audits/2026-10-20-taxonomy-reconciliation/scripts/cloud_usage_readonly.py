#!/usr/bin/env python3
"""READ-ONLY aggregate of production cloud observation references per Sporely
taxon id. Follows the existing read-only path used by
database/taxonomy/evidence/taxonomy-v3/vernacular-production-publication-2026-10-07/
stage4b-deploy/check_production.py: docker postgres:17 psql against
~/.config/sporely/production-db.env, wrapped in BEGIN READ ONLY ... ROLLBACK.

Only aggregate counts per sporely_taxon_id are fetched (no user ids, names,
locations or text). Output: JSON lines {kind, sid, n} at --out, plus totals.

  python -B scripts/cloud_usage_readonly.py --out <scratch>/cloud_usage.jsonl
  python -B scripts/cloud_usage_readonly.py --load <scratch>/cloud_usage.jsonl --workdb <workdb>
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

ENV = Path.home() / ".config/sporely/production-db.env"
PROJECT_REF = "zkpjklzfwzefhjluvhfw"

SQL = """
SELECT json_build_object('kind','total','sid',NULL,'n',count(*)) FROM public.observations;
SELECT json_build_object('kind','selected','sid',sid,'n',n) FROM (SELECT selected_sporely_taxon_id sid, count(*) n
  FROM public.observations WHERE selected_sporely_taxon_id IS NOT NULL GROUP BY 1) q;
SELECT json_build_object('kind','resolved','sid',sid,'n',n) FROM (SELECT resolved_sporely_taxon_id sid, count(*) n
  FROM public.observations WHERE resolved_sporely_taxon_id IS NOT NULL GROUP BY 1) q;
SELECT json_build_object('kind','active_release','sid',NULL,'n',NULL,'release',release_id)
  FROM public.taxonomy_v2_releases WHERE status='active';
"""


def fetch(out: Path) -> None:
    url = next(x.split("=", 1)[1].strip().strip('"').strip("'")
               for x in ENV.read_text().splitlines() if x.startswith("DATABASE_URL="))
    u = urlsplit(url)
    assert PROJECT_REF in (u.username or "") or PROJECT_REF in (u.hostname or "")
    p = subprocess.run(
        ["docker", "run", "--rm", "-i", "--env-file", str(ENV), "postgres:17", "sh", "-c",
         'psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -Atf -'],
        input="BEGIN READ ONLY;\n" + SQL + "\nROLLBACK;\n", text=True, capture_output=True)
    if p.returncode:
        raise SystemExit(p.stderr[-2000:])
    out.write_text(p.stdout)
    print("wrote", out, len(p.stdout.splitlines()), "rows")


def load(src: Path, workdb: Path) -> None:
    db = sqlite3.connect(workdb)
    db.execute("drop table if exists obs_cloud")
    db.execute("create table obs_cloud(kind text, sid integer, n integer, extra text)")
    for line in src.read_text().splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        db.execute("insert into obs_cloud values(?,?,?,?)",
                   (r["kind"], r.get("sid"), r.get("n"), r.get("release")))
    db.commit()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path)
    ap.add_argument("--load", type=Path)
    ap.add_argument("--workdb", type=Path)
    a = ap.parse_args(argv)
    if a.out:
        fetch(a.out)
    if a.load:
        load(a.load, a.workdb)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

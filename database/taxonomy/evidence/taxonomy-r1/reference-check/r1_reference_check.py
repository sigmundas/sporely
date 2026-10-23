"""READ-ONLY reference check for the R1 retired ids (desktop copies + cloud).

Groups: ``r1`` (the 1,160 R1 retirees), ``earlier`` (the 1,353 shipped
retirees), ``held`` (the 18 reference-held rel3-verified NorTaxa concepts,
still live). Aggregate counts per taxon id only; no user ids, names,
locations or free text.

    python r1_reference_check.py sql --candidate <sqlite> --out <file.sql>
    python r1_reference_check.py desktop --candidate <sqlite> --mushrooms <copy> --reference <copy> --out <json>
    python r1_reference_check.py cloud --sql <file.sql> --out <jsonl>

The cloud step runs the generated SELECT-only SQL inside ``BEGIN READ ONLY ...
ROLLBACK`` through docker ``postgres:17`` psql with
``--env-file ~/.config/sporely/production-db.env`` (the repository's existing
read-only path). The env file is passed to docker, never read or printed here.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
from collections import Counter
from pathlib import Path

_HERE = Path(__file__).resolve().parent
ENV = Path.home() / ".config/sporely/production-db.env"


def groups(candidate: Path) -> list[tuple[str, int, int, str]]:
    """(group, retired_or_held_id, survivor_or_None, nortaxa_id)."""
    conn = sqlite3.connect(f"file:{candidate}?mode=ro&immutable=1", uri=True)
    rows = conn.execute("SELECT retired_taxon_id, current_taxon_id, supersession_id "
                        "FROM taxon_supersession_min ORDER BY 1").fetchall()
    out = []
    for retired, current, sid in rows:
        group = "r1" if str(sid).startswith("taxonomy-r1-") else "earlier"
        out.append((group, int(retired), int(current), ""))
    holds = json.loads((_HERE.parent / "reference-holds.json").read_text(encoding="utf-8"))["holds"]
    for nt in sorted(holds, key=int):
        row = conn.execute("SELECT taxon_id FROM taxon_min WHERE canonical_source_system='nortaxa' "
                           "AND canonical_external_id=?", (nt,)).fetchone()
        out.append(("held", int(row[0]), 0, nt))
    conn.close()
    return out


CHECKS = [
    ("observations.selected_sporely_taxon_id", "live_dependency",
     "FROM public.observations t JOIN r ON t.selected_sporely_taxon_id = r.id"),
    ("observations.resolved_sporely_taxon_id", "live_dependency",
     "FROM public.observations t JOIN r ON t.resolved_sporely_taxon_id = r.id"),
    ("observations.taxon_identity(sporely namespace)", "live_dependency",
     "FROM public.observations t JOIN r ON t.taxon_identity_namespace = 'sporely_taxon_id' "
     "AND t.taxon_identity_external_id = r.id::text"),
    ("observation_reference_uses(via observation)", "live_dependency",
     "FROM public.observation_reference_uses u JOIN public.observations t ON t.id = u.observation_id "
     "JOIN r ON r.id IN (t.selected_sporely_taxon_id, t.resolved_sporely_taxon_id)"),
    ("taxonomy_v3.resolution_link.resolved_sporely_taxon_id", "live_dependency",
     "FROM taxonomy_v3.resolution_link t JOIN r ON t.resolved_sporely_taxon_id = r.id"),
    ("public.reference_taxon_treatments.taxon_id", "live_dependency",
     "FROM public.reference_taxon_treatments t JOIN r ON t.taxon_id = r.id::text"),
    ("public.reference_curated_forks.sporely_taxon_id", "live_dependency",
     "FROM public.reference_curated_forks t JOIN r ON t.sporely_taxon_id = r.id"),
    ("private.shared_reference_contributions.sporely_taxon_id", "live_dependency",
     "FROM private.shared_reference_contributions t JOIN r ON t.sporely_taxon_id = r.id"),
    ("private.curated_reference_treatment_taxa.sporely_taxon_id", "live_dependency",
     "FROM private.curated_reference_treatment_taxa t JOIN r ON t.sporely_taxon_id = r.id"),
    ("private.curated_reference_publication_taxa.sporely_taxon_id", "live_dependency",
     "FROM private.curated_reference_publication_taxa t JOIN r ON t.sporely_taxon_id = r.id"),
    ("taxonomy_v3.identification_snapshot(original nortaxa tuple, held)", "historical_evidence",
     "FROM taxonomy_v3.identification_snapshot t JOIN r ON r.nt <> '' AND t.original_source_system = 'nortaxa' "
     "AND t.original_source_namespace = 'nortaxa_taxon_id' AND t.original_external_id = r.nt"),
    ("public.taxonomy_v2_taxa(active release)", "generated_release_row",
     "FROM public.taxonomy_v2_taxa t JOIN r ON t.sporely_taxon_id = r.id AND t.release_id = "
     "(SELECT release_id FROM public.taxonomy_v2_releases WHERE status = 'active')"),
]


def build_sql(candidate: Path) -> str:
    values = ",\n".join(f"  ('{g}', {i}, {s}, '{nt}')" for g, i, s, nt in groups(candidate))
    unions = "\nUNION ALL\n".join(
        f"SELECT '{name}' AS source, '{cls}' AS class, r.grp, count(*) AS rows, "
        f"count(DISTINCT r.id) AS ids {frm} GROUP BY r.grp"
        for name, cls, frm in CHECKS)
    per_id = "\nUNION ALL\n".join(
        f"SELECT '{name}' AS source, r.grp, r.id, count(*) AS n {frm} GROUP BY r.grp, r.id"
        for name, cls, frm in CHECKS if cls == "live_dependency")
    cte = f"WITH r(grp, id, survivor, nt) AS (VALUES\n{values})\n"
    return (
        "-- Generated by r1_reference_check.py. READ ONLY; aggregate counts only.\n"
        "SELECT json_build_object('kind', 'meta', 'active_release', "
        "(SELECT release_id FROM public.taxonomy_v2_releases WHERE status = 'active'), "
        "'observations', (SELECT count(*) FROM public.observations));\n"
        + cte + "SELECT json_build_object('kind', 'check', 'source', source, 'class', class, "
        "'grp', grp, 'rows', rows, 'ids', ids)\nFROM (\n" + unions + "\n) q;\n"
        + cte + "SELECT json_build_object('kind', 'per_id', 'source', source, 'grp', grp, 'id', id, 'n', n)"
        "\nFROM (\n" + per_id + "\n) q;\n")


def run_cloud(sql_path: Path, out: Path) -> None:
    sql = "BEGIN READ ONLY;\n" + sql_path.read_text(encoding="utf-8") + "\nROLLBACK;\n"
    p = subprocess.run(
        ["docker", "run", "--rm", "-i", "--env-file", str(ENV), "postgres:17", "sh", "-c",
         'psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -Atf -'],
        input=sql, text=True, capture_output=True)
    if p.returncode:
        # psql errors name the failing statement, not the connection string.
        raise SystemExit("cloud check failed: " + p.stderr[-1500:].replace(str(ENV), "<env>"))
    out.write_text(p.stdout, encoding="utf-8")
    print("wrote", out, len(p.stdout.splitlines()), "rows")


def desktop(candidate: Path, mushrooms: Path, reference: Path, out: Path) -> None:
    ids = {i: g for g, i, _s, _nt in groups(candidate)}
    md = sqlite3.connect(f"file:{mushrooms}?mode=ro&immutable=1", uri=True)
    rv = sqlite3.connect(f"file:{reference}?mode=ro&immutable=1", uri=True)
    checks = {
        "observations.sporely_taxon_id": (md, "SELECT sporely_taxon_id FROM observations"),
        "observations.taxon_identity(sporely namespace)": (
            md, "SELECT taxon_identity_external_id FROM observations "
                "WHERE taxon_identity_namespace = 'sporely_taxon_id'"),
        "reference_taxon_treatments.taxon_id": (rv, "SELECT taxon_id FROM reference_taxon_treatments"),
        "curated_reference_forks.sporely_taxon_id": (rv, "SELECT sporely_taxon_id FROM curated_reference_forks"),
    }
    result = {"total_observations": md.execute("SELECT count(*) FROM observations").fetchone()[0],
              "checks": {}, "per_id": {}}
    for name, (conn, sql) in checks.items():
        hits = Counter()
        for (value,) in conn.execute(sql):
            try:
                tid = int(value)
            except (TypeError, ValueError):
                continue
            if tid in ids:
                hits[(ids[tid], tid)] += 1
        per_group = {"r1": 0, "earlier": 0, "held": 0}
        for (g, _tid), n in hits.items():
            per_group[g] += n
        result["checks"][name] = per_group
        for (g, tid), n in hits.items():
            result["per_id"].setdefault(name, []).append({"grp": g, "id": tid, "n": n})
    out.write_text(json.dumps(result, indent=1) + "\n", encoding="utf-8")
    print(json.dumps(result["checks"]))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=("sql", "desktop", "cloud"))
    ap.add_argument("--candidate", type=Path)
    ap.add_argument("--mushrooms", type=Path)
    ap.add_argument("--reference", type=Path)
    ap.add_argument("--sql", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    if a.mode == "sql":
        a.out.write_text(build_sql(a.candidate), encoding="utf-8")
    elif a.mode == "desktop":
        desktop(a.candidate, a.mushrooms, a.reference, a.out)
    else:
        run_cloud(a.sql, a.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

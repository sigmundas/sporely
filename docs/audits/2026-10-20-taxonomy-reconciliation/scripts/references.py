#!/usr/bin/env python3
"""Review finding 8 / owner task 4: every reference to a proposed retired
Sporely id, per manifest partition.

Sources (all read-only copies):
  * taxonomy SQLite (release copy): taxon_min.parent_taxon_id, vernacular_min,
    scientific_name_min, taxon_external_id_min, taxon_external_id_text_min,
    taxon_redlist_min (+ survivor red-list collisions per assessment area)
  * desktop mushrooms.db copy: observations.sporely_taxon_id and the
    observation_reference_uses rows of those observations
  * desktop reference_values.db copy: reference_taxon_treatments.taxon_id,
    curated_reference_forks.sporely_taxon_id
  * cloud snapshot loaded into the work DB (obs_cloud: selected / resolved),
    taken 2026-10-20 by cloud_usage_readonly.py
Cloud tables with no snapshot are not queried; instead this script writes
cloud_reference_check.sql (BEGIN READ ONLY ... ROLLBACK) for an operator.

  python -B scripts/references.py --manifests <audit>/manifests --release <release copy> \
      --workdb <workdb> --desktop <mushrooms.db copy> --reference-db <reference_values.db copy> \
      --out <out>/references.json --sql-out <audit>/scripts/cloud_reference_check.sql
"""
from __future__ import annotations

import argparse
import collections
import json
import sqlite3
from pathlib import Path

def load_partitions(mdir: Path) -> dict[str, dict[int, int]]:
    """retired -> survivor per partition, from manifests/index.json (rev 3:
    validator-format JSON manifests and the two non-validator JSONL groups)."""
    index = json.loads((mdir / "index.json").read_text())
    out = {}
    for name, info in sorted(index["partitions"].items()):
        path = mdir / info["path"]
        m = {}
        if info["validator_format"]:
            doc = json.loads(path.read_text())
            for row in doc["members"]:
                r = dict(zip(doc["columns"], row))
                m[r["nortaxa_sporely_taxon_id"]] = r["col_sporely_taxon_id"]
        else:
            for line in path.open():
                r = json.loads(line)
                m[r["retired_sporely_taxon_id"]] = r["survivor_sporely_taxon_id"]
        out[name] = m
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifests", required=True, type=Path)
    ap.add_argument("--release", required=True, type=Path)
    ap.add_argument("--workdb", required=True, type=Path)
    ap.add_argument("--desktop", required=True, type=Path)
    ap.add_argument("--reference-db", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--sql-out", required=True, type=Path)
    ap.add_argument("--s1", type=Path, help="data/s1-candidate.json: append the S1-only section (rev 5)")
    a = ap.parse_args(argv)
    parts = load_partitions(a.manifests)
    rel = sqlite3.connect(f"file:{a.release}?mode=ro", uri=True)
    work = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    desk = sqlite3.connect(f"file:{a.desktop}?mode=ro", uri=True)
    ref = sqlite3.connect(f"file:{a.reference_db}?mode=ro", uri=True)

    def per_id(conn, sql):
        c = collections.Counter()
        for k, n in conn.execute(sql):
            if k is not None and str(k).strip().lstrip("-").isdigit():
                c[int(k)] += n
        return c

    tables = {
        "taxonomy.child_concepts(parent_taxon_id)": per_id(rel, "select parent_taxon_id, count(*) from taxon_min group by 1"),
        "taxonomy.vernacular_min": per_id(rel, "select taxon_id, count(*) from vernacular_min group by 1"),
        "taxonomy.scientific_name_min": per_id(rel, "select taxon_id, count(*) from scientific_name_min group by 1"),
        "taxonomy.taxon_external_id_min": per_id(rel, "select taxon_id, count(*) from taxon_external_id_min group by 1"),
        "taxonomy.taxon_external_id_text_min": per_id(rel, "select taxon_id, count(*) from taxon_external_id_text_min group by 1"),
        "taxonomy.taxon_redlist_min": per_id(rel, "select taxon_id, count(*) from taxon_redlist_min group by 1"),
        "desktop.observations.sporely_taxon_id": per_id(desk, "select sporely_taxon_id, count(*) from observations group by 1"),
        "desktop.observations.taxon_identity_external_id(namespace sporely_taxon_id)": per_id(desk, """select
            taxon_identity_external_id, count(*) from observations
            where taxon_identity_namespace = 'sporely_taxon_id' group by 1"""),
        "desktop.observation_reference_uses(via observation)": per_id(desk, """select o.sporely_taxon_id, count(*)
            from observation_reference_uses u join observations o on o.id = u.observation_id group by 1"""),
        "desktop.reference_taxon_treatments.taxon_id": per_id(ref, "select taxon_id, count(*) from reference_taxon_treatments group by 1"),
        "desktop.curated_reference_forks.sporely_taxon_id": per_id(ref, "select sporely_taxon_id, count(*) from curated_reference_forks group by 1"),
        "cloud_snapshot_2026-10-20.observations.selected_sporely_taxon_id": per_id(work, "select sid, n from obs_cloud where kind='selected'"),
        "cloud_snapshot_2026-10-20.observations.resolved_sporely_taxon_id": per_id(work, "select sid, n from obs_cloud where kind='resolved'"),
    }
    handling = {
        "taxonomy.child_concepts(parent_taxon_id)": "blocking: compiler cannot re-parent; partition rule excludes retirees with children",
        "taxonomy.vernacular_min": "re-keyed by the compiler (supersession)",
        "taxonomy.scientific_name_min": "re-keyed by the compiler (supersession)",
        "taxonomy.taxon_external_id_min": "re-keyed by the compiler (supersession; accepted NorTaxa id -> reviewed_supersession row)",
        "taxonomy.taxon_external_id_text_min": "re-keyed by the compiler (supersession)",
        "taxonomy.taxon_redlist_min": "re-keyed by the compiler; survivor collisions need review",
        "desktop.observations.sporely_taxon_id": "needs desktop retired-id resolution (read time) and optional owner-run repair",
        "desktop.observation_reference_uses(via observation)": "follows the observation; repair must not orphan snapshot_json",
        "desktop.observations.taxon_identity_external_id(namespace sporely_taxon_id)": "unresolved Sporely-namespace identity (pulled id); needs retired->survivor resolution",
        "desktop.reference_taxon_treatments.taxon_id": "user reference data: needs re-key or read-time resolution (syncs to cloud)",
        "desktop.curated_reference_forks.sporely_taxon_id": "user reference data: needs re-key or read-time resolution",
        "cloud_snapshot_2026-10-20.observations.selected_sporely_taxon_id": "blocking for cloud import (09.30 repair refuses a selected retired id)",
        "cloud_snapshot_2026-10-20.observations.resolved_sporely_taxon_id": "operator repair before import (09.30 pattern)",
    }
    # red-list collisions: survivor already assessed in the same area & release
    rl = collections.defaultdict(set)
    for sid, area, release in rel.execute(
            "select taxon_id, assessment_area, source_release from taxon_redlist_min where taxon_id is not null"):
        rl[sid].add((area, release))

    out = {"cloud_snapshot": "2026-10-20 read-only aggregate (cloud_usage_readonly.py); active release tax-2026.10.07-01",
           "handling": handling, "partitions": {}}
    for name, mapping in parts.items():
        res = {"retired_concepts": len(mapping)}
        for t, counter in tables.items():
            rows = sum(counter.get(r, 0) for r in mapping)
            ids = sum(1 for r in mapping if counter.get(r, 0))
            res[t] = {"rows": rows, "retired_ids_referenced": ids}
        res["taxonomy.taxon_redlist_min.survivor_collisions"] = sum(
            1 for old, new in mapping.items() if rl.get(old) and (rl[old] & rl.get(new, set())))
        out["partitions"][name] = res
    obs_ids = set(tables["desktop.observations.sporely_taxon_id"])
    rel_ids = {r[0] for r in rel.execute("select taxon_id from taxon_min")}
    cloud_ids = set(tables["cloud_snapshot_2026-10-20.observations.selected_sporely_taxon_id"]) | \
        set(tables["cloud_snapshot_2026-10-20.observations.resolved_sporely_taxon_id"])
    out["observed_ids_absent_from_release"] = {
        "desktop": sorted(obs_ids - rel_ids),
        "cloud": sorted(cloud_ids - rel_ids),
    }
    a.out.write_text(json.dumps(out, indent=1, sort_keys=True))

    # ---- read-only SQL for the cloud (revision 4): every table/column holding a
    # taxon id, each classified. Not run by the audit (no production access).
    nt_ident = dict(work.execute("select sid, ident from taxa where src='nortaxa'"))
    values = ",\n".join(
        f"  ('{name}', {old}, {new}, {repr(str(nt_ident[old])) if old in nt_ident else 'NULL'})"
        for name, m in parts.items() for old, new in sorted(m.items()))
    LIVE, REG, HIST, GEN = ("live_dependency", "registry_append_only",
                            "historical_evidence", "generated_release_row")
    checks = [
        # (label, class, table, join condition)
        ("observations.selected_sporely_taxon_id", LIVE, "public.observations", "t.selected_sporely_taxon_id = r.retired"),
        ("observations.resolved_sporely_taxon_id", LIVE, "public.observations", "t.resolved_sporely_taxon_id = r.retired"),
        ("observations.taxon_identity(sporely ns)", LIVE, "public.observations",
         "t.taxon_identity_namespace = 'sporely_taxon_id' AND t.taxon_identity_external_id = r.retired::text"),
        ("observations.taxon_identity(nortaxa tuple; resolution changes, incl. NULL selected/resolved)", LIVE,
         "public.observations", "t.taxon_identity_source_system = 'nortaxa' AND t.taxon_identity_namespace = "
         "'nortaxa_taxon_id' AND t.taxon_identity_external_id = r.nortaxa_id"),
        ("observation_reference_uses(via observation)", LIVE,
         "public.observation_reference_uses u JOIN public.observations t ON t.id = u.observation_id",
         "r.retired IN (t.selected_sporely_taxon_id, t.resolved_sporely_taxon_id)"),
        ("taxonomy_v3.resolution_link.resolved_sporely_taxon_id", LIVE, "taxonomy_v3.resolution_link", "t.resolved_sporely_taxon_id = r.retired"),
        ("private.shared_reference_contributions.sporely_taxon_id", LIVE, "private.shared_reference_contributions", "t.sporely_taxon_id = r.retired"),
        ("public.reference_taxon_treatments.taxon_id(text)", LIVE, "public.reference_taxon_treatments", "t.taxon_id = r.retired::text"),
        ("public.reference_curated_forks.sporely_taxon_id", LIVE, "public.reference_curated_forks", "t.sporely_taxon_id = r.retired"),
        ("private.curated_reference_treatment_taxa.sporely_taxon_id", LIVE, "private.curated_reference_treatment_taxa", "t.sporely_taxon_id = r.retired"),
        ("private.curated_reference_publication_taxa.sporely_taxon_id", LIVE, "private.curated_reference_publication_taxa", "t.sporely_taxon_id = r.retired"),
        ("taxonomy_v3.registry_concept.sporely_taxon_id (retiree)", REG, "taxonomy_v3.registry_concept", "t.sporely_taxon_id = r.retired"),
        ("taxonomy_v3.registry_concept.sporely_taxon_id (survivor present)", REG, "taxonomy_v3.registry_concept", "t.sporely_taxon_id = r.survivor"),
        ("taxonomy_v3.external_mapping.sporely_taxon_id", REG, "taxonomy_v3.external_mapping", "t.sporely_taxon_id = r.retired"),
        ("public.taxonomy_v2_concepts.sporely_taxon_id", GEN, "public.taxonomy_v2_concepts", "t.sporely_taxon_id = r.retired"),
        ("public.taxonomy_v2_taxa.sporely_taxon_id (active)", GEN, "public.taxonomy_v2_taxa", "t.sporely_taxon_id = r.retired AND t.release_id = (SELECT release_id FROM public.taxonomy_v2_releases WHERE status = 'active')"),
        ("public.taxonomy_v2_taxa.parent_sporely_taxon_id (active)", GEN, "public.taxonomy_v2_taxa", "t.parent_sporely_taxon_id = r.retired AND t.release_id = (SELECT release_id FROM public.taxonomy_v2_releases WHERE status = 'active')"),
        ("public.taxonomy_v2_scientific_names (active)", GEN, "public.taxonomy_v2_scientific_names", "t.sporely_taxon_id = r.retired AND t.release_id = (SELECT release_id FROM public.taxonomy_v2_releases WHERE status = 'active')"),
        ("public.taxonomy_v2_vernacular_names (active)", GEN, "public.taxonomy_v2_vernacular_names", "t.sporely_taxon_id = r.retired AND t.release_id = (SELECT release_id FROM public.taxonomy_v2_releases WHERE status = 'active')"),
        ("public.taxonomy_v2_external_ids (active)", GEN, "public.taxonomy_v2_external_ids", "t.sporely_taxon_id = r.retired AND t.release_id = (SELECT release_id FROM public.taxonomy_v2_releases WHERE status = 'active')"),
        ("public.taxonomy_v2_legacy_external_ids (active)", GEN, "public.taxonomy_v2_legacy_external_ids", "t.sporely_taxon_id = r.retired AND t.release_id = (SELECT release_id FROM public.taxonomy_v2_releases WHERE status = 'active')"),
        ("public.taxonomy_v2_redlist (active)", GEN, "public.taxonomy_v2_redlist", "t.sporely_taxon_id = r.retired AND t.release_id = (SELECT release_id FROM public.taxonomy_v2_releases WHERE status = 'active')"),
        ("private.retired_resolution_repair_items.superseded_sporely_taxon_id", HIST, "private.retired_resolution_repair_items", "t.superseded_sporely_taxon_id = r.retired"),
        ("private.retired_resolution_repair_items.survivor_sporely_taxon_id", HIST, "private.retired_resolution_repair_items", "t.survivor_sporely_taxon_id = r.retired"),
        ("private.retired_resolution_repair_registry_additions.sporely_taxon_id", HIST, "private.retired_resolution_repair_registry_additions", "t.sporely_taxon_id = r.retired"),
        ("private.taxon_identity_repair_items.sporely_taxon_id", HIST, "private.taxon_identity_repair_items", "t.sporely_taxon_id = r.retired"),
        ("private.taxon_identity_repair_reference_actions.old_sporely_taxon_id", HIST, "private.taxon_identity_repair_reference_actions", "t.old_sporely_taxon_id = r.retired"),
        ("private.taxon_identity_repair_reference_actions.new_sporely_taxon_id", HIST, "private.taxon_identity_repair_reference_actions", "t.new_sporely_taxon_id = r.retired"),
    ]
    unions = "\nUNION ALL\n".join(
        f"SELECT '{label}' AS source, '{cls}' AS class, r.part, count(*) AS rows, count(DISTINCT r.retired) AS ids\n"
        f"  FROM {tbl} JOIN retired r ON {cond} GROUP BY r.part"
        for label, cls, tbl, cond in checks)
    unions = unions.replace("FROM public.observations JOIN", "FROM public.observations t JOIN") \
        .replace("FROM taxonomy_v3.resolution_link JOIN", "FROM taxonomy_v3.resolution_link t JOIN")
    for _, _, tbl, _ in checks:
        if " " not in tbl:
            unions = unions.replace(f"FROM {tbl} JOIN", f"FROM {tbl} t JOIN")
    sql = (
        "-- Generated by docs/audits/2026-10-20-taxonomy-reconciliation/scripts/references.py (rev 4).\n"
        "-- READ ONLY. Aggregate counts only. Not run by the audit; every count it\n"
        "-- returns is UNVERIFIED until an authorised operator runs it.\n"
        "-- class: live_dependency      -> must be handled (remap/repair) before the retiring release\n"
        "--        registry_append_only -> retiree rows stay; survivor must be present\n"
        "--        generated_release_row-> present in the active release by design; must be absent\n"
        "--                                (taxa/parents re-parented) in the candidate release import\n"
        "--        historical_evidence  -> audit history; no action, but report any hit\n"
        "BEGIN READ ONLY;\n"
        f"WITH retired(part, retired, survivor, nortaxa_id) AS (VALUES\n{values}\n)\n"
        "SELECT json_build_object('source', source, 'class', class, 'part', part, 'rows', rows, 'ids', ids) FROM (\n"
        f"{unions}\n) q;\nROLLBACK;\n")
    if a.s1:
        sql += s1_section(json.loads(a.s1.read_text())["members"], work)
    a.sql_out.write_text(sql)
    for name, res in out["partitions"].items():
        print(name, json.dumps(res))
    print("absent", out["observed_ids_absent_from_release"])
    return 0


def s1_section(members: list[dict], work: sqlite3.Connection) -> str:
    """Revision 5: S1-only read-only checks, parameterised by the frozen S1
    list (retiree, survivor, NorTaxa accepted taxonID) and the NorTaxa
    synonym taxonIDs allocated to those retirees. Each row reports
    (check, class, expected on the 2026-10-20 baseline, rows, distinct ids).
    'expected' is what the audit can support; anything marked unverified has
    never been run."""
    ret = {x["retired"] for x in members}
    syn = sorted((ident, sid) for ident, sid in work.execute(
        "select ident, sid from usages where src='nortaxa' and alias_reason='synonym_of_accepted'") if sid in ret)
    s1v = ",\n".join(f"  ({x['retired']}, {x['survivor']}, '{x['nortaxa_taxon_id']}')" for x in members)
    ntv = ",\n".join([f"  ('{x['nortaxa_taxon_id']}', 'accepted', {x['retired']})" for x in members] +
                      [f"  ('{i}', 'synonym', {sid})" for i, sid in syn])
    ACT = "(SELECT release_id FROM public.taxonomy_v2_releases WHERE status = 'active')"
    leaf = lambda col, idcol="r.retired": (
        f"{col}::text ~ ('\"(sporely_)?taxon_id\"[[:space:]]*:[[:space:]]*\"?' || {idcol}::text || '([^0-9]|$)')")
    checks = [
        # (label, class, expected, FROM ... , join condition, distinct-id expression)
        ("S1.observations.selected_sporely_taxon_id", "live_dependency", "0 (snapshot verified 0)", "public.observations t", "t.selected_sporely_taxon_id = r.retired", "r.retired"),
        ("S1.observations.resolved_sporely_taxon_id", "live_dependency", "0 (snapshot verified 0)", "public.observations t", "t.resolved_sporely_taxon_id = r.retired", "r.retired"),
        ("S1.observations.taxon_identity(sporely ns)", "live_dependency", "0 (unverified)", "public.observations t", "t.taxon_identity_namespace = 'sporely_taxon_id' AND t.taxon_identity_external_id = r.retired::text", "r.retired"),
        ("S1.observations.taxon_identity(nortaxa accepted id)", "live_dependency", ">=1 (desktop mirror shows 1 external_unresolved on 55146; unverified)", "public.observations t JOIN s1_nt n ON n.kind = 'accepted' AND t.taxon_identity_source_system = 'nortaxa' AND t.taxon_identity_namespace = 'nortaxa_taxon_id' AND t.taxon_identity_external_id = n.nortaxa_id", "r.retired = n.retired", "r.retired"),
        ("S1.observations.taxon_identity(nortaxa synonym id)", "live_dependency", "unverified", "public.observations t JOIN s1_nt n ON n.kind = 'synonym' AND t.taxon_identity_source_system = 'nortaxa' AND t.taxon_identity_namespace = 'nortaxa_taxon_id' AND t.taxon_identity_external_id = n.nortaxa_id", "r.retired = n.retired", "r.retired"),
        ("S1.observations.taxon_identity(nortaxa S1 tuple) with a selected id", "live_dependency", "0 (unverified): external_unresolved rows carry no selection", "public.observations t JOIN s1_nt n ON t.taxon_identity_source_system = 'nortaxa' AND t.taxon_identity_namespace = 'nortaxa_taxon_id' AND t.taxon_identity_external_id = n.nortaxa_id AND t.selected_sporely_taxon_id IS NOT NULL", "r.retired = n.retired", "r.retired"),
        ("S1.observations.ai_selected_taxon_id(NBIC provider id)", "live_dependency", "unverified (desktop: 21 NBIC ids, all accepted ids)", "public.observations t JOIN s1_nt n ON t.ai_selected_taxon_id IN (n.nortaxa_id, 'NBIC:' || n.nortaxa_id)", "r.retired = n.retired", "r.retired"),
        ("S1.observation_identifications.top_taxon_id(legacy; id space unverified)", "live_dependency", "unverified", "public.observation_identifications t JOIN s1_nt n ON t.top_taxon_id IN (n.nortaxa_id, 'NBIC:' || n.nortaxa_id)", "r.retired = n.retired", "r.retired"),
        ("S1.observation_reference_uses(via observation)", "live_dependency", "0 (unverified)", "public.observation_reference_uses u JOIN public.observations t ON t.id = u.observation_id", "r.retired IN (t.selected_sporely_taxon_id, t.resolved_sporely_taxon_id)", "r.retired"),
        ("S1.taxonomy_v3.resolution_link.resolved_sporely_taxon_id", "live_dependency", "0 (unverified)", "taxonomy_v3.resolution_link t", "t.resolved_sporely_taxon_id = r.retired", "r.retired"),
        ("S1.public.reference_taxon_treatments.taxon_id(text)", "live_dependency", "0 (unverified)", "public.reference_taxon_treatments t", "t.taxon_id = r.retired::text", "r.retired"),
        ("S1.public.reference_curated_forks.sporely_taxon_id", "live_dependency", "0 (unverified)", "public.reference_curated_forks t", "t.sporely_taxon_id = r.retired", "r.retired"),
        ("S1.private.shared_reference_contributions.sporely_taxon_id", "live_dependency", "0 (unverified)", "private.shared_reference_contributions t", "t.sporely_taxon_id = r.retired", "r.retired"),
        ("S1.private.curated_reference_treatment_taxa.sporely_taxon_id", "live_dependency", "0 (unverified)", "private.curated_reference_treatment_taxa t", "t.sporely_taxon_id = r.retired", "r.retired"),
        ("S1.private.curated_reference_publication_taxa.sporely_taxon_id", "live_dependency", "0 (unverified)", "private.curated_reference_publication_taxa t", "t.sporely_taxon_id = r.retired", "r.retired"),
        ("S1.public.observation_reference_uses.snapshot_json(taxon_id leaf)", "historical_evidence", "0 (unverified)", "public.observation_reference_uses t", leaf("t.snapshot_json"), "r.retired"),
        ("S1.public.reference_curated_forks.source_envelope_json(taxon_id leaf)", "historical_evidence", "0 (unverified)", "public.reference_curated_forks t", leaf("t.source_envelope_json"), "r.retired"),
        ("S1.private.curated_reference_publications.snapshot_json(taxon_id leaf)", "historical_evidence", "0 (unverified)", "private.curated_reference_publications t", leaf("t.snapshot_json"), "r.retired"),
        ("S1.private.shared_reference_contribution_revisions.envelope_json(taxon_id leaf)", "historical_evidence", "0 (unverified)", "private.shared_reference_contribution_revisions t", leaf("t.envelope_json"), "r.retired"),
        ("S1.private.reference_curation_submission_versions.candidate_json(taxon_id leaf)", "historical_evidence", "0 (unverified)", "private.reference_curation_submission_versions t", leaf("t.candidate_json"), "r.retired"),
        ("S1.taxonomy_v3.identification_snapshot(original nortaxa tuple)", "historical_evidence", "unverified", "taxonomy_v3.identification_snapshot t JOIN s1_nt n ON t.original_source_system = 'nortaxa' AND t.original_namespace = 'nortaxa_taxon_id' AND t.original_external_id = n.nortaxa_id", "r.retired = n.retired", "r.retired"),
        ("S1.taxonomy_v3.identification_snapshot.original_legacy_taxon_id(id space unverified)", "historical_evidence", "unverified", "taxonomy_v3.identification_snapshot t JOIN s1_nt n ON t.original_legacy_taxon_id = n.nortaxa_id", "r.retired = n.retired", "r.retired"),
        ("S1.private.retired_resolution_repair_items(superseded or survivor)", "historical_evidence", "0 (unverified)", "private.retired_resolution_repair_items t", "r.retired IN (t.superseded_sporely_taxon_id, t.survivor_sporely_taxon_id)", "r.retired"),
        ("S1.private.retired_resolution_repair_registry_additions.sporely_taxon_id", "historical_evidence", "0 (unverified)", "private.retired_resolution_repair_registry_additions t", "t.sporely_taxon_id = r.retired", "r.retired"),
        ("S1.private.taxon_identity_repair_items.sporely_taxon_id", "historical_evidence", "0 (unverified)", "private.taxon_identity_repair_items t", "t.sporely_taxon_id = r.retired", "r.retired"),
        ("S1.private.taxon_identity_repair_reference_actions(old or new)", "historical_evidence", "0 (unverified)", "private.taxon_identity_repair_reference_actions t", "r.retired IN (t.old_sporely_taxon_id, t.new_sporely_taxon_id)", "r.retired"),
        ("S1.public.taxa.taxon_id(legacy table; id space unverified)", "historical_evidence", "unverified", "public.taxa t", "t.taxon_id = r.retired", "r.retired"),
        ("S1.public.taxa_vernacular.taxon_id(legacy table; id space unverified)", "historical_evidence", "unverified", "public.taxa_vernacular t", "t.taxon_id = r.retired", "r.retired"),
        ("S1.taxonomy_v3.registry_concept(retiree present)", "registry_append_only", "988 (unverified)", "taxonomy_v3.registry_concept t", "t.sporely_taxon_id = r.retired", "r.retired"),
        ("S1.taxonomy_v3.registry_concept(survivor present)", "registry_append_only", "988 (unverified)", "taxonomy_v3.registry_concept t", "t.sporely_taxon_id = r.survivor", "r.retired"),
        ("S1.taxonomy_v3.external_mapping(on retiree)", "registry_append_only", "unverified", "taxonomy_v3.external_mapping t", "t.sporely_taxon_id = r.retired", "r.retired"),
        ("S1.taxonomy_v3.external_mapping(nortaxa S1 tuple -> other than retiree)", "registry_append_only", "0 (unverified)", "taxonomy_v3.external_mapping t JOIN s1_nt n ON t.source_system = 'nortaxa' AND t.namespace = 'nortaxa_taxon_id' AND t.external_id = n.nortaxa_id", "r.retired = n.retired AND t.sporely_taxon_id <> r.retired", "r.retired"),
        ("S1.public.taxonomy_v2_concepts(retiree)", "generated_release_row", "0: retirees are outside the cloud scope (unverified)", "public.taxonomy_v2_concepts t", "t.sporely_taxon_id = r.retired", "r.retired"),
        ("S1.public.taxonomy_v2_taxa(active, retiree or parent)", "generated_release_row", "0 (unverified)", "public.taxonomy_v2_taxa t", f"r.retired IN (t.sporely_taxon_id, t.parent_sporely_taxon_id) AND t.release_id = {ACT}", "r.retired"),
        ("S1.public.taxonomy_v2_taxa(active, survivor present)", "generated_release_row", "499 survivors in cloud scope (unverified)", "public.taxonomy_v2_taxa t", f"t.sporely_taxon_id = r.survivor AND t.release_id = {ACT}", "r.retired"),
        ("S1.public.taxonomy_v2_scientific_names(active, retiree)", "generated_release_row", "0 (unverified)", "public.taxonomy_v2_scientific_names t", f"t.sporely_taxon_id = r.retired AND t.release_id = {ACT}", "r.retired"),
        ("S1.public.taxonomy_v2_vernacular_names(active, retiree)", "generated_release_row", "0 (unverified)", "public.taxonomy_v2_vernacular_names t", f"t.sporely_taxon_id = r.retired AND t.release_id = {ACT}", "r.retired"),
        ("S1.public.taxonomy_v2_external_ids(active, retiree)", "generated_release_row", "0 (unverified)", "public.taxonomy_v2_external_ids t", f"t.sporely_taxon_id = r.retired AND t.release_id = {ACT}", "r.retired"),
        ("S1.public.taxonomy_v2_external_ids(active, nortaxa tuple on S1 ids)", "generated_release_row", "0 (unverified)", f"public.taxonomy_v2_external_ids t JOIN s1_nt n ON t.source_system = 'nortaxa' AND t.namespace = 'nortaxa_taxon_id' AND t.external_id = n.nortaxa_id AND t.release_id = {ACT}", "r.retired = n.retired", "r.retired"),
        ("S1.public.taxonomy_v2_legacy_external_ids(active, retiree)", "generated_release_row", "0 (unverified)", "public.taxonomy_v2_legacy_external_ids t", f"t.sporely_taxon_id = r.retired AND t.release_id = {ACT}", "r.retired"),
        ("S1.public.taxonomy_v2_redlist(active, retiree)", "generated_release_row", "0 (unverified)", "public.taxonomy_v2_redlist t", f"t.sporely_taxon_id = r.retired AND t.release_id = {ACT}", "r.retired"),
    ]
    unions = "\nUNION ALL\n".join(
        f"SELECT '{label}' AS source, '{cls}' AS class, '{exp}' AS expected, count(*) AS rows, count(DISTINCT {idx}) AS ids\n"
        f"  FROM {frm} JOIN s1 r ON {cond}"
        for label, cls, exp, frm, cond, idx in ((l.replace(";", ","), c, e.replace(";", ","), f, k, i)
                                                for l, c, e, f, k, i in checks))
    gates = f"""
-- S1 consistency gates (cloud side). Expected values are the 2026-10-20 snapshot; all unverified until run.
SELECT json_build_object('gate', 'CG0 active release', 'expected', 'tax-2026.10.07-01', 'value', {ACT});
SELECT json_build_object('gate', 'CG1 selected ids absent from the active release', 'expected', 0, 'value',
  (SELECT count(DISTINCT o.selected_sporely_taxon_id) FROM public.observations o WHERE o.selected_sporely_taxon_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa x WHERE x.sporely_taxon_id = o.selected_sporely_taxon_id AND x.release_id = {ACT})));
SELECT json_build_object('gate', 'CG2 resolved ids absent from the registry', 'expected', 0, 'value',
  (SELECT count(DISTINCT o.resolved_sporely_taxon_id) FROM public.observations o WHERE o.resolved_sporely_taxon_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM taxonomy_v3.registry_concept c WHERE c.sporely_taxon_id = o.resolved_sporely_taxon_id)));
SELECT json_build_object('gate', 'CG3 resolved ids outside the active release (drift check)', 'expected', '87 distinct at snapshot', 'value',
  (SELECT count(DISTINCT o.resolved_sporely_taxon_id) FROM public.observations o WHERE o.resolved_sporely_taxon_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa x WHERE x.sporely_taxon_id = o.resolved_sporely_taxon_id AND x.release_id = {ACT})));
SELECT json_build_object('gate', 'CG4 resolved ids 634896-634916 (absent from the desktop release)', 'expected', '20 ids, none is an S1 id', 'value',
  (SELECT json_agg(DISTINCT o.resolved_sporely_taxon_id) FROM public.observations o WHERE o.resolved_sporely_taxon_id BETWEEN 634896 AND 634916),
  'in_registry', (SELECT count(*) FROM taxonomy_v3.registry_concept c WHERE c.sporely_taxon_id BETWEEN 634896 AND 634916));
-- CG5b (template, run with the cloud ids printed by s1_desktop_gates.py --print-g4-cloud-ids, not embedded because
--   they identify the owner's rows): SELECT id, selected_sporely_taxon_id, taxon_identity_state
--   FROM public.observations WHERE id IN (<G4 cloud ids>)   expected: selected_sporely_taxon_id IS NULL
--   for every row, otherwise the desktop fallback push (identity_push.py:130-144) would clear it.
SELECT json_build_object('gate', 'CG5 inconsistent rows: external_unresolved state with a selected id', 'expected', 0, 'value',
  (SELECT count(*) FROM public.observations o WHERE o.taxon_identity_state = 'external_unresolved' AND o.selected_sporely_taxon_id IS NOT NULL));
"""
    return (
        "\n-- ======================================================================\n"
        "-- S1-ONLY SECTION (revision 5). Generated by references.py --s1 from data/s1-candidate.json.\n"
        "-- READ ONLY, SELECT only, aggregate counts. Parameterised by the frozen S1 list:\n"
        f"-- {len(members)} retirees (retired, survivor, NorTaxa accepted taxonID) and {len(syn)} NorTaxa synonym taxonIDs.\n"
        "-- Every value is UNVERIFIED until an authorised operator runs it; 'expected' states the audit baseline.\n"
        "BEGIN READ ONLY;\n"
        f"WITH s1(retired, survivor, nortaxa_id) AS (VALUES\n{s1v}\n),\n"
        f"s1_nt(nortaxa_id, kind, retired) AS (VALUES\n{ntv}\n)\n"
        "SELECT json_build_object('source', source, 'class', class, 'expected', expected, 'rows', rows, 'ids', ids) FROM (\n"
        f"{unions}\n) q;\n{gates}ROLLBACK;\n")


if __name__ == "__main__":
    raise SystemExit(main())

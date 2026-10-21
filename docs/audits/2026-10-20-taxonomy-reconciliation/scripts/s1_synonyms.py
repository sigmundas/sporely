#!/usr/bin/env python3
"""Revision 5, S1 task 2: synonym inheritance under supersession.

Part A counts every source usage allocated to an S1 retiree and grades the
synonym usages (nomenclatural status, comparator self-class).
Part B proves the effect of the CURRENT compiler block and of the PROPOSED
change, in memory, on a small S1 sample:
  * the current block is executed verbatim: its source lines are read from
    database/taxonomy/scripts/compile_release.py at run time and exec'd;
  * the proposed block is the same code with the change in
    s1-synonym-proposal (see report), defined below;
  * the outputs are fed to the REAL builder function
    build_sqlite_candidate._build_into (current compiler; proposed compiler)
    and to a scratch copy of it whose only change is the `note` composition
    (proposed compiler + optional note provenance), writing three small
    SQLite files under --work-dir. Nothing in the repository is modified.
Part C applies both decision rules to ALL S1 usages with the real
BridgeEmissionPolicy.is_eligible.

  python -B scripts/s1_synonyms.py --repo <worktree> --workdb <workdb> --release <release copy> \
     --evidence-dir <extracted evidence> --s1 <data/s1-candidate.json> --work-dir <scratch> --out <json>
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import importlib.util
import json
import shutil
import sqlite3
import sys
import textwrap
import types
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import authorship as au  # noqa: E402
import rules  # noqa: E402

START = "emitted_sporely_id = allocation.sporely_taxon_id"
END = 'alias_reason = "reviewed_supersession"'
NOTE_LINE = 'note = (u.get("alias_reason") or "") or None'
NOTE_PATCH = ('note = ";".join(x for x in ((u.get("alias_reason") or ""), '
              '(f"inherited_via_supersession:{u[\'superseded_from_sporely_taxon_id\']}" '
              'if u.get("inheritance") == "inherited_via_supersession" else ""), '
              '(f"nomenclatural_status:{u[\'nomenclatural_status\']}" if u.get("nomenclatural_status") else "")) if x) or None')


def current_block(repo: Path) -> tuple[str, str]:
    src = (repo / "database/taxonomy/scripts/compile_release.py").read_text().splitlines()
    i = next(n for n, l in enumerate(src) if START in l)
    j = next(n for n in range(i, len(src)) if END in src[n])
    block = textwrap.dedent("\n".join(src[i:j + 1]))
    return block, f"compile_release.py:{i + 1}-{j + 1}"


def proposed(alloc, superseded_to_current, bridge_evidence_class, alias_reason, REVIEWED):
    """Proposed change: only the superseded concept's own anchor (the reviewed
    manifest member) is promoted. Every other usage keeps its own evidence
    class and is marked inherited."""
    emitted_sporely_id, emitted_binding, superseded_from, inheritance = \
        alloc.sporely_taxon_id, alloc.kind, None, ""
    if alloc.sporely_taxon_id in superseded_to_current:
        superseded_from = alloc.sporely_taxon_id
        emitted_sporely_id = superseded_to_current[superseded_from]
        emitted_binding = "alias"
        if alloc.kind == "anchor":
            bridge_evidence_class = REVIEWED
            if not alias_reason:
                alias_reason = "reviewed_supersession"
        else:
            inheritance = "inherited_via_supersession"
    return emitted_sporely_id, emitted_binding, superseded_from, bridge_evidence_class, alias_reason, inheritance


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for x in ("repo", "workdb", "release", "evidence-dir", "s1", "work-dir", "out"):
        ap.add_argument("--" + x, required=True, type=Path)
    a = ap.parse_args(argv)
    scripts = a.repo / "database/taxonomy/scripts"
    sys.path.insert(0, str(scripts))
    import bridge_emission as be  # noqa: E402
    import build_sqlite_candidate as bsc  # noqa: E402
    policy = be.BridgeEmissionPolicy.load(a.repo / "database/taxonomy/policies/mapping_policy.yml")
    REVIEWED = be.EVIDENCE_CLASS_REVIEWED_SUPERSESSION

    s1 = json.loads(a.s1.read_text())["members"]
    to_current = {x["retired"]: x["survivor"] for x in s1}
    w = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    rel = sqlite3.connect(f"file:{a.release}?mode=ro", uri=True)
    nt = {r[0]: (r[1], r[2]) for r in w.execute("select taxon_id, nom_status, status from nt_raw")}

    # ---------------- Part A: usages allocated to S1 retirees
    usages = [r for r in w.execute("select sid, src, ns, ident, name, authorship, status, binding, bec, alias_reason "
                                   "from usages") if r[0] in to_current]
    by_kind = collections.Counter((r[1], r[7], r[8], r[9]) for r in usages)
    syn = [r for r in usages if r[9] == "synonym_of_accepted"]
    nom = collections.Counter(nt.get(r[3], ("?", "?"))[0] for r in syn)
    selfcls = collections.Counter(au.compare(r[5], r[5]) for r in syn)
    part_a = {
        "usages_by_src_binding_class_reason": {"|".join(k): v for k, v in sorted(by_kind.items())},
        "synonym_usages": len(syn),
        "retirees_with_synonyms": len({r[0] for r in syn}),
        "synonym_nomenclatural_status": dict(nom),
        "synonym_blocked_status": sum(n for s, n in nom.items() if s in rules.NT_BLOCKED_NOM),
        "synonym_comparator_self_class": dict(selfcls),
        "synonym_qualified": selfcls.get("qualified_usage", 0),
        "synonym_unpublished_or_doubtful": selfcls.get("unpublished_or_doubtful", 0),
        "synonym_any_flag": sum(1 for r in syn if nt.get(r[3], ("",))[0] in rules.NT_BLOCKED_NOM
                                or au.compare(r[5], r[5]) in ("qualified_usage", "unpublished_or_doubtful")),
    }
    # context: the shipped release already promotes synonyms of its 1,353 supersessions
    part_a["shipped_release_context"] = {
        "workdb_usages_synonym_promoted_to_reviewed_supersession": w.execute(
            "select count(*) from usages where bec='reviewed_supersession' and alias_reason='synonym_of_accepted'").fetchone()[0],
        "release_text_bridge_rows_reviewed_supersession_by_role": dict(rel.execute(
            "select id_role, count(*) from taxon_external_id_text_min "
            "where note='authoritative_bridge:reviewed_supersession' group by 1").fetchall()),
    }

    # ---------------- Part C: full-S1 decision with the real policy
    block, where = current_block(a.repo)
    def run_current(sid, kind, bec, reason):
        ns = {"allocation": types.SimpleNamespace(sporely_taxon_id=sid, kind=kind),
              "superseded_to_current": to_current, "bridge_evidence_class": bec,
              "alias_reason": reason, "EVIDENCE_CLASS_REVIEWED_SUPERSESSION": REVIEWED}
        exec(block, ns)
        return ns["emitted_sporely_id"], ns["emitted_binding"], ns["superseded_from"], \
            ns["bridge_evidence_class"], ns["alias_reason"], ""
    part_c = {}
    for label, fn in (("current", run_current),
                      ("proposed", lambda sid, kind, bec, reason: proposed(
                          types.SimpleNamespace(sporely_taxon_id=sid, kind=kind), to_current, bec, reason, REVIEWED))):
        c = collections.Counter()
        for r in usages:
            _, binding, _, bec, reason, inh = fn(r[0], r[7], r[8], r[9])
            emitted = binding != "anchor" and policy.is_eligible(bec)
            c[f"{'synonym' if r[9] == 'synonym_of_accepted' else 'accepted_anchor'}|bridge_emitted={emitted}|{bec}"] += 1
        part_c[label] = dict(sorted(c.items()))

    # ---------------- Part B: real builder on a sample
    syn_by_ret = collections.defaultdict(list)
    for r in syn:
        syn_by_ret[r[0]].append(r)
    pick = [624178]
    for want in ("illegitimate", "qualified_usage", "unpublished_or_doubtful"):
        for sid, rs in sorted(syn_by_ret.items()):
            if sid in pick:
                continue
            if any(nt.get(r[3], ("",))[0] == want or au.compare(r[5], r[5]) == want for r in rs):
                pick.append(sid)
                break
    keep = set(pick) | {to_current[s] for s in pick}
    ev = a.evidence_dir / "compiler"
    taxa_rows, su_rows = [], []
    needles = tuple(f'"sporely_taxon_id": {s},' for s in keep)
    with (ev / "taxa.jsonl").open() as f:
        for line in f:
            if any(n in line for n in needles):
                row = json.loads(line)
                if row["sporely_taxon_id"] in keep and row["sporely_taxon_id"] not in to_current:
                    taxa_rows.append(row)
    with (ev / "source_usages.jsonl").open() as f:
        for line in f:
            if any(n in line for n in needles):
                row = json.loads(line)
                if row["sporely_taxon_id"] in keep:
                    su_rows.append(row)
    nom_by_ident = {k: v[0] for k, v in nt.items()}
    results = {}
    for label in ("current", "proposed_compiler_unchanged_builder", "proposed"):
        out_rows = []
        for u in su_rows:
            u = dict(u)
            sid = u["sporely_taxon_id"]
            args = (sid, u["identity_binding"], u["bridge_evidence_class"], u["alias_reason"])
            if label == "current":
                e_sid, e_bind, s_from, bec, reason, inh = run_current(*args)
            else:
                e_sid, e_bind, s_from, bec, reason, inh = proposed(
                    types.SimpleNamespace(sporely_taxon_id=sid, kind=u["identity_binding"]), to_current,
                    u["bridge_evidence_class"], u["alias_reason"], REVIEWED)
                u["inheritance"] = inh
                if u["source_code"] == "nortaxa":
                    u["nomenclatural_status"] = nom_by_ident.get(u["source_usage"]["identifier"], "")
            u.update({"sporely_taxon_id": e_sid, "identity_binding": e_bind, "superseded_from_sporely_taxon_id": s_from,
                      "bridge_evidence_class": bec, "alias_reason": reason})
            out_rows.append(u)
        d = a.work_dir / f"mini-{label}"
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)
        (d / "taxa.jsonl").write_text("".join(json.dumps(r) + "\n" for r in taxa_rows))
        (d / "source_usages.jsonl").write_text("".join(json.dumps(r) + "\n" for r in out_rows))
        (d / "vernacular.jsonl").write_text("")
        man = json.loads((ev / "manifest.json").read_text())
        man["state"] = "candidate"
        (d / "manifest.json").write_text(json.dumps(man))
        (d / "registry.placeholder").write_text("audit sample; registry hash only\n")
        if label != "proposed":
            builder = bsc
        else:
            text = (scripts / "build_sqlite_candidate.py").read_text()
            assert text.count(NOTE_LINE) == 1
            p = a.work_dir / "build_sqlite_candidate_proposed.py"
            p.write_text(text.replace(NOTE_LINE, NOTE_PATCH))
            spec = importlib.util.spec_from_file_location("bsc_proposed", p)
            builder = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(builder)
        db = d / "sample.sqlite3"
        try:
            builder._build_into(tmp_db=db, manifest=man, taxa_path=d / "taxa.jsonl",
                                source_usages_path=d / "source_usages.jsonl", vernacular_path=d / "vernacular.jsonl",
                                registry_path=d / "registry.placeholder", manifest_path=d / "manifest.json",
                                release_dir=d, bridge_policy=policy)
            build_error = None
        except Exception as exc:  # report, do not hide
            build_error = f"{type(exc).__name__}: {exc}"
        res = {"build_error": build_error}
        if db.exists():
            c = sqlite3.connect(db)
            res["taxon_min_ids"] = [r[0] for r in c.execute("select taxon_id from taxon_min order by 1")]
            res["bridge_rows"] = [list(r) for r in c.execute(
                "select taxon_id, external_id, id_role, note from taxon_external_id_text_min "
                "where note like 'authoritative_bridge:%' order by 1,2")]
            res["nortaxa_int_rows"] = [list(r) for r in c.execute(
                "select taxon_id, external_id, id_role, note from taxon_external_id_min "
                "where source_system='artsdatabanken' order by 1,2")]
            res["scientific_names"] = [list(r) for r in c.execute(
                "select taxon_id, scientific_name, is_preferred_name, source, note from scientific_name_min order by 1,2")]
            c.close()
        results[label] = res
    part_b = {"current_block_source": where,
              "current_block_sha256": hashlib.sha256(block.encode()).hexdigest(),
              "sample_retirees": pick, "sample_survivors": [to_current[s] for s in pick],
              "results": results}
    out = {"part_a_counts": part_a, "part_b_sample_build": part_b, "part_c_full_s1_decision": part_c}
    a.out.write_text(json.dumps(out, ensure_ascii=False, indent=1, sort_keys=True) + "\n")
    print(json.dumps({"part_a": part_a, "part_c": part_c, "sample": pick,
                      "build_errors": {k: v["build_error"] for k, v in results.items()},
                      "bridges": {k: v.get("bridge_rows") for k, v in results.items()}}, indent=1, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

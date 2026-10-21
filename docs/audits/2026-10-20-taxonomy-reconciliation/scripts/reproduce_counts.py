#!/usr/bin/env python3
"""Section 2: reproduce, with one explicit definition each, the previously
reported populations, and measure how they overlap.

  python -B scripts/reproduce_counts.py --workdb <workdb> --release <release copy> \
      --repo <worktree> --out <out>/section2_counts.json

Name equality throughout is taxon_min.canonical_scientific_name byte
equality (as the earlier ad-hoc SQL used); `nkey` (NFC + casefold) is
reported alongside where it differs.
"""
from __future__ import annotations

import argparse
import glob
import json
import sqlite3
from pathlib import Path

Q = {
    "concepts_by_source_status": """
        select source_system, taxonomic_status, count(*), sum(parent_taxon_id is null)
        from r.taxon_min group by 1, 2""",
    # (1) 5,959 / 79 / 5: names carried by more than one concept, ranks ignored
    "multi_concept_names_by_source_mix": """
        with g as (select canonical_scientific_name n, count(*) c,
                          count(distinct source_system) s, max(source_system) mx
                   from r.taxon_min group by 1 having count(*) > 1)
        select case when s = 2 then 'col_xr+nortaxa' else mx || '_only' end, count(*), sum(c)
        from g group by 1""",
    # (2) 3,748 / 1,876 / ...: NorTaxa x COL pairs, same name AND same rank, by rank
    "nortaxa_col_pairs_same_name_same_rank_by_rank": """
        select n.taxon_rank, count(*), count(distinct n.taxon_id), count(distinct c.taxon_id)
        from r.taxon_min n join r.taxon_min c
          on c.canonical_scientific_name = n.canonical_scientific_name
         and c.taxon_rank = n.taxon_rank and c.source_system = 'col_xr'
        where n.source_system = 'nortaxa' group by 1 order by 2 desc""",
    # same, but joined on name only and grouped by the NorTaxa rank (gives family 307)
    "nortaxa_col_pairs_same_name_any_rank_by_nortaxa_rank": """
        select n.taxon_rank, count(*), count(distinct n.taxon_id), count(distinct c.taxon_id)
        from r.taxon_min n join r.taxon_min c
          on c.canonical_scientific_name = n.canonical_scientific_name and c.source_system = 'col_xr'
        where n.source_system = 'nortaxa' group by 1 order by 2 desc""",
    "nortaxa_col_pairs_same_name_any_rank_total": """
        select count(*), count(distinct n.taxon_id), count(distinct c.taxon_id),
               count(distinct n.canonical_scientific_name), sum(n.taxon_rank <> c.taxon_rank)
        from r.taxon_min n join r.taxon_min c
          on c.canonical_scientific_name = n.canonical_scientific_name and c.source_system = 'col_xr'
        where n.source_system = 'nortaxa'""",
    # (3) 1,145: same name+rank pairs sharing an nb/no vernacular (case-insensitive)
    "vernacular_twin_pairs_by_rank": """
        with p as (select n.taxon_id nid, c.taxon_id cid, n.taxon_rank rk
                   from r.taxon_min n join r.taxon_min c
                     on c.canonical_scientific_name = n.canonical_scientific_name
                    and c.taxon_rank = n.taxon_rank and c.source_system = 'col_xr'
                   where n.source_system = 'nortaxa')
        select rk, count(*), count(distinct nid), count(distinct cid) from p
        where exists (select 1 from r.vernacular_min vn join r.vernacular_min vc
                        on lower(vc.vernacular_name) = lower(vn.vernacular_name)
                       and vc.language_code in ('nb','no')
                      where vn.taxon_id = p.nid and vc.taxon_id = p.cid
                        and vn.language_code in ('nb','no'))
        group by 1""",
    # (4) 154: COL same name + same rank groups; and ignoring rank
    "col_same_name_same_rank_groups": """
        select count(*), sum(c) from (select count(*) c from r.taxon_min
        where source_system = 'col_xr' group by canonical_scientific_name, taxon_rank having c > 1)""",
    "col_same_name_any_rank_groups": """
        select count(*), sum(c) from (select count(*) c from r.taxon_min
        where source_system = 'col_xr' group by canonical_scientific_name having c > 1)""",
    # (5) 29: NorTaxa same-name groups ignoring rank; same rank; placeholders
    "nortaxa_same_name_any_rank_groups": """
        select count(*), sum(c) from (select count(*) c from r.taxon_min
        where source_system = 'nortaxa' group by canonical_scientific_name having c > 1)""",
    "nortaxa_same_name_same_rank_groups": """
        select count(*), sum(c) from (select count(*) c from r.taxon_min
        where source_system = 'nortaxa' group by canonical_scientific_name, taxon_rank having c > 1)""",
    "nortaxa_incertae_sedis_placeholder_concepts": """
        select count(*) from r.taxon_min where source_system = 'nortaxa'
        and canonical_scientific_name = 'Incertae sedis'""",
    # (6) 12,566 parentless NorTaxa concepts; how many parents would resolve
    "parentless_by_source": """
        select source_system, sum(parent_taxon_id is null), count(*) from r.taxon_min group by 1""",
    "nortaxa_parent_reference_resolution": """
        select case when u.sid is null then 'parent_not_bound'
                    when pt.src = 'nortaxa' then 'parent_is_nortaxa_concept'
                    else 'parent_bound_to_col_concept' end, count(*)
        from taxa t left join usages u on u.src = 'nortaxa' and u.ident = t.parent_id
             left join taxa pt on pt.sid = u.sid
        where t.src = 'nortaxa' group by 1""",
    # overlaps
    "overlap_mixed_names_that_are_also_col_internal_dups": """
        select count(*) from (select canonical_scientific_name n from r.taxon_min group by 1
          having count(distinct source_system) = 2) m
        where (select count(*) from r.taxon_min where canonical_scientific_name = m.n
               and source_system = 'col_xr') > 1""",
    "overlap_mixed_names_that_are_also_nortaxa_internal_dups": """
        select count(*) from (select canonical_scientific_name n from r.taxon_min group by 1
          having count(distinct source_system) = 2) m
        where (select count(*) from r.taxon_min where canonical_scientific_name = m.n
               and source_system = 'nortaxa') > 1""",
    "overlap_col_same_rank_groups_with_nortaxa_twin": """
        select count(*) from (select canonical_scientific_name n, taxon_rank rk from r.taxon_min
          where source_system = 'col_xr' group by 1, 2 having count(*) > 1) g
        where exists (select 1 from r.taxon_min t where t.source_system = 'nortaxa'
                      and t.canonical_scientific_name = g.n and t.taxon_rank = g.rk)""",
    "compiler_cross_source_proposals_for_live_nortaxa_concepts": """
        select p.proposal_class, count(*) from taxa t join proposals p on p.nt_ident = t.ident
        where t.src = 'nortaxa' group by 1""",
    "nortaxa_usages_already_bound_to_col_concepts": """
        select u.bec, count(*) from usages u join taxa t on t.sid = u.sid
        where u.src = 'nortaxa' and t.src = 'col_xr' group by 1""",
}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdb", required=True, type=Path)
    ap.add_argument("--release", required=True, type=Path)
    ap.add_argument("--repo", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    db = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    db.execute(f"attach database 'file:{a.release}?mode=ro' as r")
    out = {k: db.execute(q).fetchall() for k, q in Q.items()}
    # Stage 2 Group-B lineage: 7,423 pairs in tax-2026.09.26-02
    stage2_pairs = set()
    for f in glob.glob(str(a.repo / "database/taxonomy/evidence/taxonomy-v3/stage2/group-b-*.manifest.json")):
        m = json.loads(Path(f).read_text())
        i_n, i_c = m["columns"].index("nortaxa_taxon_id"), m["columns"].index("col_usage_id")
        stage2_pairs |= {(str(x[i_n]), str(x[i_c])) for x in m["members"]}
    sup = db.execute("""select s.superseded_sid, u.ident, s.cur_ident from supersessions s
        join usages u on u.sid in (select sid from taxa where src='col_xr' and ident=s.cur_ident)
         and u.src='nortaxa' and u.superseded_from = s.superseded_sid and u.bec='reviewed_supersession'""").fetchall()
    sup_pairs = {(n, c) for _, n, c in sup}
    live = set(db.execute("""select n.ident, c.ident from taxa n join taxa c on c.nkey = n.nkey and c.name = n.name
        and c.src='col_xr' where n.src='nortaxa'""").fetchall())
    out["stage2_group_b_lineage"] = {
        "stage2_pairs_tax_2026_09_26_02": len(stage2_pairs),
        "supersessions_total": db.execute("select count(*) from supersessions").fetchone()[0],
        "supersessions_whose_pair_was_group_b": len(sup_pairs & stage2_pairs),
        "live_name_equal_pairs_now": len(live),
        "live_pairs_in_stage2": len(live & stage2_pairs),
        "live_pairs_not_in_stage2": sorted(live - stage2_pairs)[:20],
        "stage2_minus_superseded_equals_live": len(stage2_pairs - sup_pairs) == len(live),
    }
    a.out.write_text(json.dumps(out, indent=1, ensure_ascii=False))
    for k, v in out.items():
        print(k, v if not isinstance(v, list) or len(v) < 15 else v[:15])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

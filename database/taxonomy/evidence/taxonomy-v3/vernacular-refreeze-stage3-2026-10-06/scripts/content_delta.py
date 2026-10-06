"""Stage 3B refreeze — semantic content delta, superseded d0e5de92 set vs new frozen set.
Inputs: compiler/ extracted from both evidence archives (cmp/old, cmp/new) and both
decompressed SQLites. cwd = sporely-py. Usage: python content_delta.py <cmp_dir> <out.json>"""
import collections, gzip, json, sqlite3, sys, shutil
from pathlib import Path
C = Path(sys.argv[1]); OUT = Path(sys.argv[2])
OLDF = Path.home()/'sporely-scratch/vernacular-2026-10-06/stage2/finalA/tax-2026.10.04-01.sqlite3.gz'
NEWF = Path.home()/'sporely-scratch/vernacular-2026-10-06-r2/final/tax-2026.10.06-01.sqlite3.gz'
def L(p): return [json.loads(l) for l in open(p)]
def key(r): return (r['sporely_taxon_id'], r['language'], r['vernacular_name'], r['source_code'], json.dumps(r['core_row_id'], sort_keys=True))
res = {}
o, n = L(C/'old/compiler/vernacular.jsonl'), L(C/'new/compiler/vernacular.jsonl')
oi, ni = {key(r): r for r in o}, {key(r): r for r in n}
res['compiler_vernacular'] = {'old_rows': len(o), 'new_rows': len(n),
    'removed': sorted([list(k[:4]) for k in oi.keys()-ni.keys()]), 'added': sorted([list(k[:4]) for k in ni.keys()-oi.keys()])}
churn = collections.Counter(); other = []
for k in oi.keys() & ni.keys():
    a, b = oi[k], ni[k]
    diff = {f for f in set(a)|set(b) if a.get(f) != b.get(f)}
    if not diff: continue
    if diff == {'association_evidence_sha256'}: churn[a['source_code']+'/'+a['language']] += 1
    else: other.append([list(k[:4]), sorted(diff)])
res['compiler_vernacular']['evidence_sha_only_churn'] = dict(churn)
res['compiler_vernacular']['other_field_changes'] = other
def ev(p):
    c = collections.Counter(); g = []
    for e in L(p):
        c[(e['evidence_class'], e.get('projection_status'))] += 1
        if e['evidence_class'] == 'withheld_col_vernacular_association':
            g.append([e['target_sporely_taxon_id'], e['language'], e['vernacular_name'], [x['source_evidence']['source_usage']['identifier'] for x in e['national_withheld_associations']]])
    return {f'{a}|{b}': v for (a, b), v in sorted(c.items())}, sorted(g)
(co, _), (cn, gn) = ev(C/'old/compiler/vernacular_evidence.jsonl'), ev(C/'new/compiler/vernacular_evidence.jsonl')
res['evidence_class_counts'] = {k: [co.get(k, 0), cn.get(k, 0)] for k in sorted(set(co)|set(cn)) if co.get(k, 0) != cn.get(k, 0)}
res['col_echo_guard_withheld'] = gn
def sq(gz, dst):
    with gzip.open(gz) as s, open(dst, 'wb') as d: shutil.copyfileobj(s, d, 1 << 20)
    db = sqlite3.connect(dst)
    v = {(a, b, c): (p, s) for a, b, c, p, s in db.execute('select taxon_id,language_code,vernacular_name,is_preferred_name,source from vernacular_min')}
    tabs = [r[0] for r in db.execute("select name from sqlite_master where type='table' order by name")]
    other = {t: db.execute(f'select count(*) from "{t}"').fetchone()[0] for t in tabs}
    return v, other, db
vo, to, dbo = sq(OLDF, C/'old.sqlite3'); vn, tn, dbn = sq(NEWF, C/'new.sqlite3')
res['sqlite_vernacular'] = {'old': len(vo), 'new': len(vn), 'removed': sorted([list(k)+list(vo[k]) for k in vo.keys()-vn.keys()]),
    'added': sorted([list(k)+list(vn[k]) for k in vn.keys()-vo.keys()]),
    'changed': sorted([[list(k), vo[k], vn[k]] for k in vo.keys() & vn.keys() if vo[k] != vn[k]])}
res['sqlite_table_counts_changed'] = {t: [to.get(t), tn.get(t)] for t in sorted(set(to)|set(tn)) if to.get(t) != tn.get(t)}
same = {}
for t in sorted(set(to) & set(tn)):
    if t == 'vernacular_min': continue
    q = f'select * from "{t}" order by 1,2' if dbo.execute(f'select count(*) from pragma_table_info("{t}")').fetchone()[0] > 1 else f'select * from "{t}" order by 1'
    same[t] = list(dbo.execute(q)) == list(dbn.execute(q))
res['sqlite_other_tables_identical'] = same
cols = [r[1] for r in dbo.execute('pragma table_info(taxon_min)') if r[1] != 'sporely_content_release_id']
q = 'select ' + ','.join(cols) + ' from taxon_min'
res['taxon_min_identical_except_release_id'] = sorted(dbo.execute(q)) == sorted(dbn.execute(q))
res['taxonomy_meta_diff'] = sorted(set(dbn.execute('select * from taxonomy_meta')) ^ set(dbo.execute('select * from taxonomy_meta')))
OUT.write_text(json.dumps(res, ensure_ascii=False, indent=2, sort_keys=True, default=list) + '\n')
print(json.dumps({k: v for k, v in res.items()}, ensure_ascii=False, default=list)[:4000])

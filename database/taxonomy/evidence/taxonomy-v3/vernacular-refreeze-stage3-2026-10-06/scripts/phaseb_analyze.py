"""Stage 3B refreeze (2026-10-06) — Phase B analysis.

`inputs <run>` writes probes-input.json / dbchecks-input.json.
`analyze <run>` evaluates W1/scoped exports, frozen SQLite and the rolled-back
local import log; writes export-verification.json and search-probes.json in <run>.
cwd = sporely-py.
"""
import collections, gzip, hashlib, json, sqlite3, sys
from pathlib import Path

sys.path.insert(0, str(Path('database/taxonomy/scripts').resolve()))
import vernacular_projection as vp  # noqa: E402

S = Path.home() / 'sporely-scratch/vernacular-2026-10-06-r2'
S_OLD = Path.home() / 'sporely-scratch/vernacular-2026-10-06'
EV = Path('database/taxonomy/evidence/taxonomy-v3')
BUNDLE = S_OLD / 'bundled-2026.09.30-01.sqlite3'
PROBES = [('grønnkremle', 61673), ('lys høstmorkel', 14541), ('dunpipe', 14834), ('dråpeknippesopp', 103130),
          ('snylterørsopp', 52936), ('tårekjuke', 54615), ('gul ekornnøtt', 60559), ('rognerust', 100280),
          ('gråfiolett køllesopp', 146835), ('grønn navlesopp', 79753), ('silkerødspore', 96276),
          ('Russula aeruginea', 61673), ('taigarødtuppsopp', 58814), ('rosenekornnøtt', 60590),
          ('furustokklav', 103256), ('blekgul køllesopp', 82459)]
#: Owner-withheld (D1): the query must NOT return the strict target.
NEGATIVE = [('sørlig rødtuppsopp', 58815), ('sørleg rødtuppsopp', 58815), ('knolltrevlesopp', 19080)]
COLLECTIVE_NAMES = [('nb', 'silkerødspore-gruppen'), ('nn', 'silkeraudspore-gruppa'),
                    ('nb', 'silkerødskivesopp'), ('nn', 'silkeraudskivesopp')]


def sha(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for c in iter(lambda: f.read(1 << 20), b''):
            h.update(c)
    return h.hexdigest()


def rows(p):
    with open(p) as f:
        return [json.loads(l) for l in f if l.strip()]


def withheld_cases():
    d = json.load(open(EV / 'norwegian-vernacular-2026-10-04-owner-reviewed/exception-source-usages.json'))
    usages = d if isinstance(d, list) else next(v for v in d.values() if isinstance(v, list))
    out = []
    for u in usages:
        if 'qualified_or_aggregate_source' in u['reasons']:
            for lang, name in u['vernacular_names']:
                for t in u['full_bundle_target_ids']:
                    out.append({'source_scientific_name': u['source_scientific_name'],
                                'source_record': u['source_record_identifier']['value'], 'reasons': u['reasons'],
                                'target': t, 'language': lang, 'name': name})
    return out


def inputs(run):
    probes = [{'query': q, 'expected_target': t} for q, t in PROBES] + [{'query': 'silkerødspore-gruppen', 'expected_target': None}] + [{'query': q, 'expected_target': None, 'forbidden_target': t} for q, t in NEGATIVE]
    (run / 'probes-input.json').write_text(json.dumps(probes, ensure_ascii=False, indent=2) + '\n')
    names = sorted({c['name'] for c in withheld_cases()} | {n for _, n in COLLECTIVE_NAMES} | {'silkerødspore', 'silkeraudspore', 'rognerust', 'gråfiolett køllesopp', 'taigarødtuppsopp', 'rosenekornnøtt', 'rosenikornnøtt', 'furustokklav', 'blekgul køllesopp'})
    (run / 'dbchecks-input.json').write_text(json.dumps([{'id': n, 'name': n} for n in names], ensure_ascii=False, indent=2) + '\n')


def analyze(run):
    db = sqlite3.connect(f'file:{run}/frozen.sqlite3?mode=ro', uri=True)
    old = sqlite3.connect(f'file:{BUNDLE}?mode=ro', uri=True)
    norm = vp.norm
    def vmap(c):
        return {(a, b, n): (bool(p), s) for a, b, n, p, s in c.execute('select taxon_id,language_code,vernacular_name,is_preferred_name,source from vernacular_min')}
    fz, ob = vmap(db), vmap(old)
    w1v = rows(run / 'w1/vernacular.jsonl'); scv = rows(run / 'scoped/vernacular.jsonl')
    k = lambda r: (r['taxon_id'], r['language_code'], r['vernacular_name'])
    w1k = {k(r): r for r in w1v}; sck = {k(r): r for r in scv}
    w1n = {(a, b, norm(c)) for a, b, c in w1k}; scn = {(a, b, norm(c)) for a, b, c in sck}; fzn = {(a, b, norm(c)) for a, b, c in fz}
    scoped_taxa = {x['taxon_id']: x for x in rows(run / 'scoped/taxon.jsonl')}
    selectable = {i for i, t in scoped_taxa.items() if t.get('scope_state') == 'include'}
    w1_taxa = {x['taxon_id'] for x in rows(run / 'w1/taxon.jsonl')}
    counts = {'w1': {x['name']: x['row_count'] for x in json.load(open(run / 'w1/taxonomy_export_manifest.json'))['files']},
              'scoped': {x['name']: x['row_count'] for x in json.load(open(run / 'scoped/taxonomy_export_manifest.json'))['files']}}
    res = {'counts': counts}
    # W1 == frozen SQLite exactly; scoped == frozen restricted to selectable concepts.
    res['w1_equals_frozen_sqlite_exactly'] = set(w1k) == set(fz) and all((w1k[x]['is_preferred_name'], w1k[x]['source']) == fz[x] for x in fz)
    res['scoped_equals_frozen_scoped_exactly'] = set(sck) == {x for x in fz if x[0] in selectable} and all((sck[x]['is_preferred_name'], sck[x]['source']) == fz[x] for x in sck)
    # Norwegian additions.
    ev = rows(run / 'vernacular_evidence.jsonl')
    added = [r for r in ev if r.get('projection_status') == 'added']
    ak = [(r['target_sporely_taxon_id'], r['language'], r['vernacular_name']) for r in added]
    cls = collections.Counter(r['evidence_class'] for r in added)
    res['norwegian_additions'] = {'automatic': cls['automatic_vernacular_enrichment'], 'reviewed': cls['owner_reviewed_vernacular_association'],
        'affected_concepts': len({a[0] for a in ak}), 'in_w1': sum(a in w1k for a in ak), 'in_scoped': sum(a in sck for a in ak),
        'not_in_scoped': sorted([list(a) for a in ak if a not in sck])}
    # Stage 1B COL recoveries.
    audit = list(map(json.loads, gzip.open(EV / 'vernacular-removal-audit-2026-10-04/removed-row-classification.jsonl.gz', 'rt')))
    gaps = [(r['target_sporely_taxon_id'], r['language'], norm(r['vernacular_name'])) for r in audit if r['classification'] == 'current_evidence_exists_but_compiler_does_not_recover_it']
    unsupported = [r for r in audit if not r['current_pinned_target_association_exists']]
    un = [(r['target_sporely_taxon_id'], r['language'], norm(r['vernacular_name'])) for r in unsupported]
    res['stage1b_recoveries'] = {'total': len(gaps), 'concepts': len({g[0] for g in gaps}), 'in_w1': sum(g in w1n for g in gaps), 'in_frozen_sqlite': sum(g in fzn for g in gaps)}
    res['unsupported_historical'] = {'total': len(unsupported), 'present_in_frozen_sqlite': sum(u in fzn for u in un),
        'present_in_w1': sum(u in w1n for u in un), 'present_in_scoped': sum(u in scn for u in un)}
    # Stage 1 rows = old bundle - removed + additions (Stage 1 verification.json).
    s1 = json.load(open(EV / 'vernacular-compiler-stage1-2026-10-04/verification.json'))
    removed = {tuple(r) for r in s1['removed_rows']}; adds = {tuple(r) for r in s1['additions']}
    stage1 = (set(ob) - removed) | adds
    s1pref = {x: ob[x][0] for x in stage1 if x in ob}
    for ch in s1['preferred_name_status_changes']:
        s1pref[tuple(ch['row'])] = ch['after']
    res['stage1_rows'] = {'reconstructed': len(stage1), 'expected': 13271, 'missing_from_frozen_sqlite': len(stage1 - set(fz)),
        'missing_from_w1': len(stage1 - set(w1k)), 'scoped_stage1_rows': len({x for x in stage1 if x[0] in selectable}),
        'scoped_missing': len({x for x in stage1 if x[0] in selectable} - set(sck))}
    # Preferred-name drift vs accepted Stage 1B preferred evidence.
    delta = json.load(open(EV / 'vernacular-compiler-stage1b-2026-10-04/preferred-name-delta.json'))
    dmap = {tuple(d['row']): d for d in delta['vs_old_bundle']}
    drift = []
    for x in set(ob) & set(fz):
        exp = dmap[x]['after'] if x in dmap else ob[x][0]
        if dmap.get(x) and dmap[x]['before'] != ob[x][0]:
            drift.append({'row': x, 'kind': 'delta_before_mismatch'})
        if fz[x][0] != exp or w1k[x]['is_preferred_name'] != exp or (x in sck and sck[x]['is_preferred_name'] != exp):
            drift.append({'row': x, 'expected': exp, 'frozen': fz[x][0]})
    s1drift = [x for x, p in s1pref.items() if x in fz and fz[x][0] != p]
    res['preferred_names'] = {'compared_rows_vs_old_bundle': len(set(ob) & set(fz)), 'delta_rows': len(dmap),
        'drift_vs_stage1b_evidence': len(drift), 'drift_examples': drift[:5], 'stage1_rows_compared': len(s1pref),
        'drift_vs_stage1_flags': len(s1drift), 'stage1b_vs_stage1_delta_recorded': len(delta['vs_stage1'])}
    # Entoloma strict/collective.
    def on(keys, tid, lang, name):
        return (tid, lang, name) in keys
    ent = {'strict_id': 96276, 'collective_id': 625268,
           'strict_names_frozen': sorted(n for (t, l, n) in fz if t == 96276 and l in ('nb', 'nn', 'no')),
           'collective_names_frozen': sorted(n for (t, l, n) in fz if t == 625268),
           'collective_in_w1_taxa': 625268 in w1_taxa, 'collective_in_scoped_taxa': 625268 in scoped_taxa,
           'strict_in_scoped': 96276 in selectable}
    for where, keys in (('frozen', fz), ('w1', w1k), ('scoped', sck)):
        ent[f'collective_names_on_strict_{where}'] = [n for l, n in COLLECTIVE_NAMES if on(keys, 96276, l, n)]
    res['entoloma'] = ent
    # Qualified/aggregate withheld cases.
    wh = []
    for c in withheld_cases():
        key = (c['target'], c['language'], c['name'])
        hits = {w: [list(x) + list(keys[x] if isinstance(keys[x], tuple) else [keys[x]['source']]) for x in keys if x[2] == c['name'] and x[1] == c['language']]
                for w, keys in (('frozen', fz), ('w1', w1k), ('scoped', sck))}
        wh.append({**c, 'on_target_frozen': key in fz, 'on_target_w1': key in w1k, 'on_target_scoped': key in sck,
                   'name_anywhere_frozen': [h[0] for h in hits['frozen']], 'withheld_evidence': any(
                       r.get('projection_status') == 'withheld' and r['vernacular_name'] == c['name'] and r['language'] == c['language'] for r in ev)})
    res['qualified_withheld'] = wh
    # Duplicate source-native concepts carrying the probed names.
    res['probe_names_on_other_concepts'] = {q: sorted({t for (t, l, n) in fz if n == q and t != tid}) for q, tid in PROBES}
    # Cloud import log.
    log = (run / 'local-validation.log').read_text().splitlines()
    pr = [json.loads(l.split('|', 1)[1]) for l in log if l.startswith('STAGE3_PROBE|')]
    out = []
    for p in pr:
        r = p['results'] or []
        if p['expected_target'] is None:
            forbidden = dict(NEGATIVE).get(p['query'])
            ids = [x['taxon_id'] for x in r]
            out.append({'query': p['query'], 'expected_target': None, 'forbidden_target': forbidden, 'result_ids': ids,
                        'results': [[x['taxon_id'], x['match_type'], x['canonical_scientific_name'], x['vernacular_name']] for x in r],
                        'pass': (forbidden not in ids) if forbidden else None}); continue
        hit = [x for x in r if x['taxon_id'] == p['expected_target']]
        want = 'canonical_exact' if p['query'][0].isupper() else 'vernacular_exact'
        out.append({'query': p['query'], 'expected_target': p['expected_target'],
                    'expected_match_type': want, 'found': bool(hit), 'match_type': hit[0]['match_type'] if hit else None,
                    'rank_of_target': [x['taxon_id'] for x in r].index(p['expected_target']) + 1 if hit else None,
                    'result_count': len(r), 'canonical': (hit[0]['canonical_scientific_name'], hit[0]['canonical_external_id']) if hit else None,
                    'vernacular_label': hit[0]['vernacular_name'] if hit else None,
                    'other_result_ids': [x['taxon_id'] for x in r if x['taxon_id'] != p['expected_target']],
                    'pass': bool(hit) and hit[0]['match_type'] == want})
    res['probes'] = out
    db_checks = {json.loads(l.split('|', 1)[1])['id']: json.loads(l.split('|', 1)[1])['rows'] for l in log if l.startswith('STAGE3_DBCHECK|')}
    res['db_vernacular_checks'] = {n: [[r['sporely_taxon_id'], r['language_code'], r['source']] for r in v] for n, v in db_checks.items()}
    res['db_counts'] = next(json.loads(l.split('|', 1)[1]) for l in log if l.startswith('STAGE3_COUNTS|'))
    res['db_role'] = next(l.split('|', 1)[1] for l in log if l.startswith('STAGE3_ROLE|'))
    res['db_taxa_present'] = next(json.loads(l.split('|', 1)[1]) for l in log if l.startswith('STAGE3_TAXON|'))['ids']
    res['db_collective_on_strict'] = [r for n, v in db_checks.items() for r in v if r['sporely_taxon_id'] == 96276 and n in {x for _, x in COLLECTIVE_NAMES}]
    res['db_withheld_on_target'] = [c for c in withheld_cases() if any(r['sporely_taxon_id'] == c['target'] and r['language_code'] == c['language'] for r in db_checks.get(c['name'], []))]
    res['file_hashes'] = {f'{d}/{p.name}': sha(p) for d in ('w1', 'scoped') for p in sorted((run / d).iterdir()) if p.is_file()}
    res['file_hashes']['import.sql'] = sha(run / 'import.sql')
    res['file_hashes']['validation-rollback.sql'] = sha(run / 'validation-rollback.sql')
    res['rollback'] = json.load(open(run / 'rollback-verification.json'))
    res['probe_results_sha256'] = hashlib.sha256(json.dumps(pr, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    (run / 'analysis.json').write_text(json.dumps(res, ensure_ascii=False, sort_keys=True, indent=2, default=list) + '\n')
    (run / 'search-probes-raw.json').write_text(json.dumps(pr, ensure_ascii=False, sort_keys=True, indent=1) + '\n')
    print(json.dumps({k: res[k] for k in ['counts', 'w1_equals_frozen_sqlite_exactly', 'scoped_equals_frozen_scoped_exactly', 'stage1b_recoveries', 'unsupported_historical', 'stage1_rows', 'db_counts']}, ensure_ascii=False))
    print(json.dumps({k: v for k, v in res['norwegian_additions'].items() if k != 'not_in_scoped'}), len(res['norwegian_additions']['not_in_scoped']))
    print(json.dumps({k: v for k, v in res['preferred_names'].items() if k != 'drift_examples'}))
    for p in out: print('PROBE', p['query'], p['pass'], p.get('match_type'), p.get('rank_of_target'), p.get('other_result_ids'))
    print('ENT', json.dumps(ent, ensure_ascii=False)); print('DBCOLL', res['db_collective_on_strict'], 'DBWITHHELD', res['db_withheld_on_target'], 'TAXA', res['db_taxa_present'], res['db_role'])


if __name__ == '__main__':
    {'inputs': inputs, 'analyze': analyze}[sys.argv[1]](Path(sys.argv[2]))

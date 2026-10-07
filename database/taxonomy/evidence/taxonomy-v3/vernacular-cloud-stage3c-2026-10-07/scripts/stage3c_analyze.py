"""Stage 3C (2026-10-07) cloud validation of frozen fda89f37 — inputs + analysis.

`inputs <run>`  writes probes-input.json / dbchecks-input.json / dbtaxa-input.json.
`analyze <run>` evaluates frozen SQLite, W1 + scoped exports, bundled 2026.09.30-01,
Stage 1B evidence and the rolled-back local import log; writes <run>/analysis.json.
Extends vernacular-refreeze-stage3-2026-10-06/scripts/phaseb_analyze.py. cwd = sporely-py.
"""
import collections, gzip, hashlib, json, sqlite3, sys, tarfile
from pathlib import Path
import yaml

sys.path.insert(0, str(Path('database/taxonomy/scripts').resolve()))
import vernacular_projection as vp  # noqa: E402

EV = Path('database/taxonomy/evidence/taxonomy-v3')
F = Path.home() / 'sporely-scratch/vernacular-2026-10-06-r2/final'
BUNDLE = Path.home() / 'sporely-scratch/vernacular-2026-10-06/bundled-2026.09.30-01.sqlite3'
BUNDLE_SQLITE_SHA = None  # read from repo manifest at runtime
RID = 'tax-2026.10.06-01'
# (query, expected strict COL target, expected matched language). Targets established from
# frozen SQLite (see report): every name is a recorded NorTaxa name; no fuzzy matching exists.
PROBES = [('grønnkremle', 61673, 'nb'), ('grønkremle', 61673, 'nn'), ('lys høstmorkel', 14541, 'nb'),
          ('lys haustmorkel', 14541, 'nn'), ('dunpipe', 14834, 'nb'), ('dråpeknippesopp', 103130, 'nb'),
          ('snylterørsopp', 52936, 'nb'), ('tårekjuke', 54615, 'nb'), ('gul ekornnøtt', 60559, 'nb'),
          ('oliven rådyrslørsopp', 618912, 'nb'), ('slank vedkorallsopp', 23749, 'nb'),
          ('norsk flammekorallsopp', 58693, 'nb'), ('sumpreddikhette', 34794, 'nb'), ('rognerust', 100280, 'nb'),
          ('gråfiolett køllesopp', 146835, 'nb'), ('grønn navlesopp', 79753, 'nb'), ('grøn navlesopp', 79753, 'nn'),
          ('taigarødtuppsopp', 58814, 'nb'), ('rosenekornnøtt', 60590, 'nb'), ('blekgul køllesopp', 82459, 'nb'),
          ('silkerødspore', 96276, 'nb'), ('gullkorallsopp', 58668, 'nb')]
#: Must NOT return the forbidden strict concept.
NEGATIVE = [('sørlig rødtuppsopp', 58815), ('sørleg rødtuppsopp', 58815), ('knolltrevlesopp', 19080),
            ('silkerødspore-gruppen', 96276), ('silkeraudspore-gruppa', 96276),
            ('falsk lindekorallsopp', 58651), ('blek rødtuppsopp', 58832)]
QUAL = {  # strict COL concept -> (qualified/collective NorTaxa-native concept, names that must stay off the strict concept)
    96276: (625268, [('nb', 'silkerødspore-gruppen'), ('nn', 'silkeraudspore-gruppa'), ('nb', 'silkerødskivesopp'), ('nn', 'silkeraudskivesopp')]),
    19080: (625094, [('nb', 'knolltrevlesopp'), ('nn', 'knolltrevlesopp'), ('nb', 'vanlig knolltrevlesopp'), ('nn', 'vanleg knolltrevlesopp')]),
    58815: (626348, [('nb', 'sørlig rødtuppsopp'), ('nn', 'sørleg rødtuppsopp')]),
    58651: (626338, [('nb', 'falsk lindekorallsopp')]),
    58832: (621902, [('nb', 'blek rødtuppsopp')]),
    58814: (621458, []),
}
NORDIC = ('nb', 'nn', 'no')


def sha(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for c in iter(lambda: f.read(1 << 20), b''):
            h.update(c)
    return h.hexdigest()


def rows(p):
    with open(p) as f:
        return [json.loads(l) for l in f if l.strip()]


def jgz(p):
    with gzip.open(p, 'rt') as f:
        return json.load(f)


def inputs(run):
    probes = [{'query': q, 'expected_target': t, 'expected_language': l} for q, t, l in PROBES]
    probes += [{'query': q, 'expected_target': None, 'forbidden_target': t} for q, t in NEGATIVE]
    (run / 'probes-input.json').write_text(json.dumps(probes, ensure_ascii=False, indent=2) + '\n')
    names = sorted({n for _, (_, ns) in QUAL.items() for _, n in ns} | {'taigarødtuppsopp', 'rosenekornnøtt', 'rosenikornnøtt',
                    'blekgul køllesopp', 'gullkorallsopp', 'gullkorallsopp [GAMMELT]', 'silkerødspore', 'silkeraudspore'})
    (run / 'dbchecks-input.json').write_text(json.dumps([{'id': n, 'name': n} for n in names], ensure_ascii=False, indent=2) + '\n')
    ids = sorted({t for _, t, _ in PROBES} | set(QUAL) | {v[0] for v in QUAL.values()} | {60590, 58668})
    (run / 'dbtaxa-input.json').write_text(json.dumps(ids) + '\n')


def counter(it):
    c = collections.Counter(it)
    return {'|'.join(map(str, k)): v for k, v in sorted(c.items(), key=lambda kv: tuple(map(str, kv[0])))}


def fp_vern(keys):  # keys: dict (tid, lang, name) -> (pref, source)
    out = sorted(keys.items(), key=lambda kv: (kv[0][0], kv[0][1].encode(), kv[0][2].encode()))
    s = '\n'.join(f'{t}|{l}|{n}|{"true" if p else "false"}|{src or ""}' for (t, l, n), (p, src) in out)
    return hashlib.md5(s.encode()).hexdigest()


def analyze(run):
    norm = vp.norm
    res = {}
    db = sqlite3.connect(f'file:{run}/frozen.sqlite3?mode=ro', uri=True)
    repo_manifest = json.load(open('database/reference_data/generated/taxonomy_v2/manifest.json'))
    res['bundled'] = {'release': repo_manifest['content_release_id'], 'path': str(BUNDLE),
                      'sqlite_sha256': sha(BUNDLE), 'repo_manifest_sqlite_sha256': repo_manifest['sqlite_sha256']}
    assert res['bundled']['sqlite_sha256'] == repo_manifest['sqlite_sha256']
    old = sqlite3.connect(f'file:{BUNDLE}?mode=ro', uri=True)

    def vmap(c):
        return {(a, b, n): (bool(p), s) for a, b, n, p, s in c.execute('select taxon_id,language_code,vernacular_name,is_preferred_name,source from vernacular_min')}
    fz, ob = vmap(db), vmap(old)
    w1v = rows(run / 'w1/vernacular.jsonl'); scv = rows(run / 'scoped/vernacular.jsonl')
    k = lambda r: (r['taxon_id'], r['language_code'], r['vernacular_name'])
    w1k = {k(r): (r['is_preferred_name'], r['source']) for r in w1v}
    sck = {k(r): (r['is_preferred_name'], r['source']) for r in scv}
    fzn = {(a, b, norm(c)) for a, b, c in fz}; w1n = {(a, b, norm(c)) for a, b, c in w1k}; scn = {(a, b, norm(c)) for a, b, c in sck}
    scope = json.load(open(run / 'scoped/scope-manifest.json'))
    rule = {r['taxon_id']: r for r in scope['winning_rule_by_taxon']}
    included = set(scope['included_taxon_ids']); ancestors = set(scope['required_ancestor_ids'])
    tsrc = dict(db.execute('select taxon_id, source_system from taxon_min'))
    tname = dict(db.execute('select taxon_id, canonical_scientific_name from taxon_min'))
    res['counts'] = {'w1': {x['name']: x['row_count'] for x in json.load(open(run / 'w1/taxonomy_export_manifest.json'))['files']},
                     'scoped': {x['name']: x['row_count'] for x in json.load(open(run / 'scoped/taxonomy_export_manifest.json'))['files']},
                     'frozen_sqlite_vernacular': len(fz), 'bundled_sqlite_vernacular': len(ob),
                     'scope_aggregate': scope['aggregate_counts'], 'scope_evaluated_taxa': len(rule),
                     'frozen_taxa': len(tsrc), 'frozen_taxa_by_source_system': counter((v,) for v in tsrc.values())}
    # B. breakdowns and drop explanation
    br = lambda keys: counter((l, s, p) for (t, l, n), (p, s) in keys.items())
    res['breakdown_lang_source_pref'] = {'frozen_sqlite': br(fz), 'w1': br(w1k), 'scoped': br(sck)}
    res['nordic_breakdown'] = {w: counter((l, s, p) for (t, l, n), (p, s) in keys.items() if l in NORDIC) for w, keys in (('frozen_sqlite', fz), ('w1', w1k), ('scoped', sck))}
    res['w1_equals_frozen_sqlite_exactly'] = w1k == fz
    res['scoped_equals_frozen_restricted_to_included'] = sck == {x: v for x, v in fz.items() if x[0] in included}
    def why(t):
        if t in included:
            return 'UNEXPLAINED_included_taxon'
        if t not in rule:
            return f'not_scope_evaluated:canonical_source_system={tsrc.get(t)} (policy source.system=col_xr; macrofungi_scope.load_taxa)'
        if t in ancestors:
            return f'required_ancestor_classification_only (rule={rule[t]["rule"]}; build_export writes vernacular only for included)'
        r = rule[t]
        return f'state={r["state"]}|rule={r["rule"]}|reason={r["reason"]}'
    dropped = [x for x in w1k if x not in sck]
    res['scope_drops'] = {'total': len(dropped), 'by_rule': counter((why(x[0]),) for x in dropped),
                          'nordic_by_rule': counter((why(x[0]),) for x in dropped if x[1] in NORDIC),
                          'unexplained': [list(x) for x in dropped if x[0] in included][:20],
                          'added_in_scoped_not_in_w1': len([x for x in sck if x not in w1k])}
    # B. enrichments
    ev = rows(run / 'vernacular_evidence.jsonl')
    added = [r for r in ev if r.get('projection_status') == 'added']
    ak = [(r['target_sporely_taxon_id'], r['language'], r['vernacular_name'], r['evidence_class']) for r in added]
    res['enrichments'] = {
        'by_class': counter((c,) for *_, c in ak), 'total': len(ak), 'concepts': len({a[0] for a in ak}),
        'in_frozen_sqlite': sum(a[:3] in fz for a in ak), 'in_w1': sum(a[:3] in w1k for a in ak),
        'target_selectable': sum(a[0] in included for a in ak),
        'selectable_and_in_scoped': sum(a[0] in included and a[:3] in sck for a in ak),
        'selectable_but_missing_in_scoped': [list(a) for a in ak if a[0] in included and a[:3] not in sck],
        'out_of_scope_by_rule': counter((why(a[0]),) for a in ak if a[0] not in included),
        'reviewed_rows': [[*a[:3], a[0] in included, a[:3] in sck, tname.get(a[0])] for a in ak if a[3] == 'owner_reviewed_vernacular_association'],
    }
    # Guard
    guard = [r for r in ev if r.get('evidence_class') == 'withheld_col_vernacular_association']
    res['col_echo_guard'] = [[r['target_sporely_taxon_id'] if 'target_sporely_taxon_id' in r else r.get('full_bundle_target_ids'), r['language'], r['vernacular_name'],
                              any((t, r['language'], r['vernacular_name']) in fz for t in ([r['target_sporely_taxon_id']] if r.get('target_sporely_taxon_id') else r.get('full_bundle_target_ids', [])))] for r in guard]
    # B/E. Stage 1B COL recoveries
    audit = [json.loads(l) for l in gzip.open(EV / 'vernacular-removal-audit-2026-10-04/removed-row-classification.jsonl.gz', 'rt')]
    gaps = [(r['target_sporely_taxon_id'], r['language'], norm(r['vernacular_name'])) for r in audit if r['classification'] == 'current_evidence_exists_but_compiler_does_not_recover_it']
    guarded = {(19080, 'nb', norm('knolltrevlesopp')), (58815, 'nb', norm('sørlig rødtuppsopp')), (58815, 'nn', norm('sørleg rødtuppsopp'))}
    rec = jgz(EV / 'vernacular-compiler-stage1b-2026-10-04/recovered-current-col-associations.json.gz')
    res['col_recovery'] = {'audit_gaps': len(gaps), 'stage1b_recovered_list': len(rec), 'concepts': len({g[0] for g in gaps}),
        'guarded_among_gaps': sorted(map(list, guarded & set(gaps))),
        'in_frozen_sqlite': sum(g in fzn for g in gaps), 'in_w1': sum(g in w1n for g in gaps),
        'target_selectable': sum(g[0] in included for g in gaps),
        'selectable_and_in_scoped': sum(g[0] in included and g in scn for g in gaps),
        'selectable_missing_in_scoped': [list(g) for g in gaps if g[0] in included and g not in scn][:20],
        'out_of_scope_by_rule': counter((why(g[0]),) for g in gaps if g[0] not in included),
        'current_source_recovery_gaps_in_frozen': sum(g not in fzn for g in gaps)}
    # E. unsupported historical
    un = [(r['target_sporely_taxon_id'], r['language'], norm(r['vernacular_name'])) for r in audit if not r['current_pinned_target_association_exists']]
    res['unsupported_historical'] = {'total': len(un), 'present_frozen_sqlite': sum(u in fzn for u in un),
                                     'present_w1': sum(u in w1n for u in un), 'present_scoped': sum(u in scn for u in un)}
    # E. spelling replacements (historical spelling replaced by current-source spelling)
    rep = [r for r in audit if r['classification'] == 'current_evidence_recovered_as_case_or_spacing_variant']
    def names_on(keys, t, l, nm):
        return sorted(n for (a, b, n) in keys if a == t and b == l and norm(n) == nm)
    by_tl = collections.defaultdict(list)
    for (a, b, n) in fz:
        by_tl[(a, b)].append(n)
    rp = []
    for r in rep:
        t, l, h = r['target_sporely_taxon_id'], r['language'], r['vernacular_name']
        cur = r.get('current_source_spelling_variants_retained') or []
        onf = sorted(n for n in by_tl.get((t, l), []) if norm(n) == norm(h))
        rp.append({'target': t, 'language': l, 'historical': h, 'current': cur, 'frozen_names': onf,
                   'historical_variant_present': h in onf and h not in cur, 'current_present': all(c in onf for c in cur) and bool(cur),
                   'duplicate': len(onf) > 1,
                   'scoped_names': sorted(n for n in onf if (t, l, n) in sck) if t in included else None})
    res['spelling_replacements'] = {'pairs': len(rp), 'current_present': sum(x['current_present'] for x in rp),
        'historical_variant_present': sum(x['historical_variant_present'] for x in rp), 'duplicates': sum(x['duplicate'] for x in rp),
        'selectable': sum(x['scoped_names'] is not None for x in rp),
        'selectable_scoped_single': sum(x['scoped_names'] is not None and len(x['scoped_names']) == 1 for x in rp),
        'problems': [x for x in rp if x['historical_variant_present'] or not x['current_present'] or x['duplicate']][:20],
        'pairs_sha256': hashlib.sha256(json.dumps(rp, ensure_ascii=False, sort_keys=True).encode()).hexdigest()}
    (run / 'spelling-replacements.json').write_text(json.dumps(rp, ensure_ascii=False, indent=1) + '\n')
    # E. preferred names split by layer
    common = set(ob) & set(fz)
    comp = [(x, ob[x][0], fz[x][0]) for x in common if ob[x][0] != fz[x][0]]
    delta = jgz(EV / 'vernacular-compiler-stage1b-2026-10-04/preferred-name-delta.json.gz')
    dmap = {tuple(d['row']): d['after'] for d in delta['vs_old_bundle']}
    res['preferred_names'] = {
        'compiler_level_vs_bundled_2026.09.30-01': {'common_rows': len(common), 'flag_changes': len(comp),
            'true_to_false': sum(1 for _, a, b in comp if a and not b), 'false_to_true': sum(1 for _, a, b in comp if b and not a),
            'by_language': counter((x[1],) for x, _, _ in comp), 'nordic_changes': [[*x, a, b] for x, a, b in comp if x[1] in NORDIC][:50],
            'nordic_change_count': sum(x[1] in NORDIC for x, _, _ in comp),
            'explained_by_stage1b_delta': sum(dmap.get(x) == b for x, _, b in comp),
            'unexplained_by_stage1b_delta': [[*x, a, b] for x, a, b in comp if dmap.get(x) != b][:20],
            'unexplained_count': sum(dmap.get(x) != b for x, _, b in comp)},
        'export_level_vs_frozen_sqlite': {'w1_flag_diffs': sum(w1k[x][0] != fz[x][0] for x in fz if x in w1k),
            'scoped_flag_diffs': sum(sck[x][0] != fz[x][0] for x in sck if x in fz),
            'w1_source_diffs': sum(w1k[x][1] != fz[x][1] for x in fz if x in w1k)}}
    # D. qualified / collective
    q = {}
    for strict, (qual, names) in QUAL.items():
        q[strict] = {'strict_name': tname.get(strict), 'strict_source': tsrc.get(strict), 'qualified_concept': qual,
            'qualified_name': tname.get(qual), 'qualified_source': tsrc.get(qual),
            'strict_selectable': strict in included, 'qualified_selectable': qual in included,
            'qualified_scope_reason': why(qual), 'qualified_names_frozen': sorted([l, n] for (t, l, n) in fz if t == qual and l in NORDIC),
            'strict_nordic_frozen': sorted([l, n, *fz[(t, l, n)]] for (t, l, n) in fz if t == strict and l in NORDIC),
            'strict_nordic_scoped': sorted([l, n, *sck[(t, l, n)]] for (t, l, n) in sck if t == strict and l in NORDIC),
            'leaks': {w: [[l, n] for l, n in names if (strict, l, n) in keys] for w, keys in (('frozen', fz), ('w1', w1k), ('scoped', sck))}}
    res['qualified'] = q
    # strict 58651 'gullkorallsopp [GAMMELT]' origin
    res['gullkorallsopp_gammelt'] = {'frozen': [list(x) + list(v) for x, v in fz.items() if x[2].startswith('gullkorallsopp')],
                                    'bundled': [list(x) + list(v) for x, v in ob.items() if x[2].startswith('gullkorallsopp')]}
    # F. identity invariants frozen vs bundled
    cols = 'taxon_id,parent_taxon_id,canonical_source_system,canonical_external_id,canonical_scientific_name,taxon_rank,taxonomic_status,source_system,norwegian_taxon_id,swedish_taxon_id,inaturalist_taxon_id,genus,specific_epithet'
    T = lambda c: {r[0]: r for r in c.execute(f'select {cols} from taxon_min')}
    ft, otx = T(db), T(old)
    res['identity_vs_bundled'] = {'bundled_taxa': len(otx), 'frozen_taxa': len(ft), 'missing_in_frozen': len(set(otx) - set(ft)),
        'new_in_frozen': len(set(ft) - set(otx)), 'identity_column_diffs': sum(ft[i] != otx[i] for i in otx if i in ft),
        'identity_diff_examples': [[otx[i], ft[i]] for i in otx if i in ft and ft[i] != otx[i]][:5]}
    for tbl, c2 in (('taxon_external_id_min', 'taxon_id,source_system,external_id,id_role,is_preferred,external_name,note'),
                    ('taxon_external_id_text_min', 'taxon_id,source_system,namespace,external_id,id_role,is_preferred,external_name,note'),
                    ('scientific_name_min', '*'), ('taxon_redlist_min', '*')):
        a = set(old.execute(f'select {c2} from {tbl}')); b = set(db.execute(f'select {c2} from {tbl}'))
        if c2 == '*':
            a = {r[1:] for r in a}; b = {r[1:] for r in b}
        pre = set(otx)
        res['identity_vs_bundled'][tbl] = {'bundled': len(a), 'frozen': len(b), 'removed': len(a - b),
            'added_on_preexisting': len({r for r in b - a if r[0] in pre}), 'added_on_new': len({r for r in b - a if r[0] not in pre})}
    res['identity_vs_bundled']['taxonomy_meta'] = {'bundled': dict(old.execute('select key,value from taxonomy_meta')) if True else None,
                                                   'frozen': dict(db.execute('select key,value from taxonomy_meta'))}
    # source_usages / registry / supersessions / policies (frozen evidence vs repo HEAD + internal consistency)
    man = json.load(open(F / 'manifest.json'))
    with tarfile.open(F / man['compiler_evidence']['artifact']) as t:
        su = [json.loads(l) for l in t.extractfile('compiler/source_usages.jsonl')]
        inp = {n: json.load(t.extractfile(f'inputs/{n}.json')) for n in ('concept_supersessions', 'manual_mappings', 'mapping_policy', 'registry-manifest', 'previous-publication-manifest')}
    repo_reg = json.load(open('database/taxonomy/registry/canonical/manifest.json'))
    yml = lambda n: yaml.safe_load(open(f'database/taxonomy/policies/{n}.yml'))
    ids_frozen = set(ft)
    res['identity_sources'] = {
        'registry_manifest_equals_repo': inp['registry-manifest'] == repo_reg,
        'registry_concatenated_sha256': [man['registry_concatenated_sha256'], repo_reg.get('concatenated_sha256'), repo_manifest.get('registry_concatenated_sha256')],
        'concept_supersessions_equal_repo': inp['concept_supersessions'] == yml('concept_supersessions'),
        'manual_mappings_equal_repo': inp['manual_mappings'] == yml('manual_mappings'),
        'mapping_policy_equal_repo': inp['mapping_policy'] == yml('mapping_policy'),
        'previous_publication_manifest_equals_repo_bundle_manifest': inp['previous-publication-manifest'] == repo_manifest,
        'source_usages': len(su), 'identity_binding': counter((u.get('identity_binding'),) for u in su),
        'aliases': sum(bool(u.get('alias_reason')) for u in su), 'superseded_from': sum(u.get('superseded_from_sporely_taxon_id') is not None for u in su),
        'bound_ids_missing_in_frozen_sqlite': sum(1 for u in su if u.get('sporely_taxon_id') is not None and u['sporely_taxon_id'] not in ids_frozen),
        'bound_ids_absent_from_bundled': sum(1 for u in su if u.get('sporely_taxon_id') is not None and u['sporely_taxon_id'] not in otx),
        'source_usages_sha256': hashlib.sha256(json.dumps(su, sort_keys=True).encode()).hexdigest()}
    # canonical identity: each COL-canonical taxon's canonical_external_id equals its anchor source_usage identifier
    anchor = {u['sporely_taxon_id']: u['source_usage']['identifier'] for u in su if u.get('identity_binding') == 'anchor' and u.get('sporely_taxon_id') is not None}
    res['identity_sources']['anchor_vs_canonical_external_id_mismatch'] = sum(1 for i, x in anchor.items() if i in ft and ft[i][3] != x)
    # F. frozen vs exported (W1 & scoped taxa/external ids) and imported (DB fingerprints)
    w1t = {r['taxon_id']: r for r in rows(run / 'w1/taxon.jsonl')}
    sct = {r['taxon_id']: r for r in rows(run / 'scoped/taxon.jsonl')}
    idk = lambda r: (r['taxon_id'], r.get('parent_taxon_id'), r['canonical_source_system'], r['canonical_external_id'], r['canonical_scientific_name'])
    res['identity_export'] = {'w1_taxa_vs_frozen_diffs': sum(1 for i, r in w1t.items() if idk(r) != (ft[i][0], ft[i][1], ft[i][2], ft[i][3], ft[i][4])),
        'w1_taxa_missing': len(set(ft) - set(w1t)), 'scoped_taxa_vs_w1_diffs': sum(1 for i, r in sct.items() if idk(r) != idk(w1t[i])),
        'scoped_taxa_expected': len(included | ancestors), 'scoped_taxa': len(sct), 'scoped_taxa_set_ok': set(sct) == (included | ancestors)}
    exk = lambda r: (r['taxon_id'], r['source_system'], r.get('namespace'), str(r['external_id']), r['id_role'], bool(r['is_preferred']))
    w1e = {exk(r) for r in rows(run / 'w1/taxon_external_id.jsonl')}
    sce = {exk(r) for r in rows(run / 'scoped/taxon_external_id.jsonl')}
    fze = {(a, b, c, str(d), e, bool(f)) for a, b, c, d, e, f in db.execute('select taxon_id,source_system,namespace,external_id,id_role,is_preferred from taxon_external_id_text_min')}
    res['identity_export'].update({'w1_external_ids_equal_frozen_text_table': w1e == fze, 'scoped_external_ids_equal_w1_restricted': sce == {x for x in w1e if x[0] in included}})
    # expected DB fingerprints from scoped export
    vfp = fp_vern(sck)
    tl = sorted(sct.values(), key=lambda r: r['taxon_id'])
    tfp = hashlib.md5('\n'.join(f"{r['taxon_id']}|{'' if r.get('parent_taxon_id') is None else r['parent_taxon_id']}|{r['canonical_source_system']}|{r['canonical_external_id']}|{r['canonical_scientific_name']}" for r in tl).encode()).hexdigest()
    el = sorted(sce, key=lambda x: (x[0], x[1].encode(), (x[2] or '').encode(), x[3].encode(), x[4].encode()))
    efp = hashlib.md5('\n'.join(f'{a}|{b}|{c}|{d}|{e}|{"true" if f else "false"}' for a, b, c, d, e, f in el).encode()).hexdigest()
    res['expected_db_fingerprints'] = {'vernacular': vfp, 'taxa': tfp, 'external_ids': efp}
    # DB log
    log = (run / 'local-validation.log').read_text().splitlines()
    P = lambda tag: [l.split('|', 1)[1] for l in log if l.startswith(tag + '|')]
    dbfp = dict(x.split('|', 1) for x in P('STAGE3_FP'))
    res['db_fingerprints'] = dbfp
    res['db_fingerprints_match_scoped_export'] = {k2: dbfp.get(k2) == v for k2, v in res['expected_db_fingerprints'].items()}
    res['db_counts'] = json.loads(P('STAGE3_COUNTS')[0]); res['db_role'] = P('STAGE3_ROLE')[0]
    tv = json.loads(P('STAGE3_TAXVERN')[0]); res['db_taxa_present'] = tv['ids']
    dbn = {(r[0], r[1], r[2]): (r[3], r[4]) for r in tv['nordic']}
    for strict, v in q.items():
        v['db_strict_nordic'] = sorted([l, n, *dbn[(t, l, n)]] for (t, l, n) in dbn if t == strict)
        v['leaks']['imported_db'] = [[l, n] for l, n in QUAL[strict][1] if (strict, l, n) in dbn]
        v['strict_in_db'] = strict in tv['ids']; v['qualified_in_db'] = v['qualified_concept'] in tv['ids']
    res['db_name_checks'] = {json.loads(x)['id']: json.loads(x)['rows'] for x in P('STAGE3_DBCHECK')}
    vrows = {json.loads(x)['query']: json.loads(x)['rows'] for x in P('STAGE3_VROWS')}
    pr = [json.loads(x) for x in P('STAGE3_PROBE')]
    inp_p = {p['query']: p for p in json.load(open(run / 'probes-input.json'))}
    out = []
    for p in pr:
        spec = inp_p[p['query']]; r = p['results'] or []
        vr = {(a, b, c): (d, e) for a, b, c, d, e in vrows[p['query']]}
        items = []
        for i, x in enumerate(r, 1):
            m = vr.get((x['taxon_id'], x['matched_language'], x['matched_name']))
            items.append({'rank': i, 'taxon_id': x['taxon_id'], 'scientific_name': x['canonical_scientific_name'], 'canonical': f"{x['canonical_source_system']}:{x['canonical_external_id']}",
                          'matched_name': x['matched_name'], 'matched_language': x['matched_language'], 'match_type': x['match_type'],
                          'matched_is_preferred': m[0] if m else None, 'matched_source': m[1] if m else None, 'display_vernacular': x['vernacular_name']})
        ids = [x['taxon_id'] for x in r]
        e = {'query': p['query'], 'result_count': len(r), 'results': items[:8]}
        # exact rows that exist anywhere for this name (selectable or not) — shows competing / out-of-scope concepts
        e['exact_rows_frozen_sqlite'] = sorted([t, l, *fz[(t, l, n)], tname.get(t), t in included] for (t, l, n) in fz if n.lower() == p['query'].lower() and l in NORDIC)
        if spec.get('expected_target'):
            tgt = spec['expected_target']; hit = [x for x in items if x['taxon_id'] == tgt]
            e.update({'expected_target': tgt, 'expected_language': spec['expected_language'], 'rank_of_target': hit[0]['rank'] if hit else None,
                      'match_type': hit[0]['match_type'] if hit else None, 'matched_language': hit[0]['matched_language'] if hit else None,
                      'competing_exact_other_concepts': [x for x in items if x['taxon_id'] != tgt and x['match_type'] in ('vernacular_exact', 'canonical_exact', 'scientific_alias_exact')],
                      'pass': bool(hit) and hit[0]['rank'] == 1 and hit[0]['match_type'] == 'vernacular_exact' and hit[0]['matched_language'] == spec['expected_language']})
        else:
            e.update({'forbidden_target': spec['forbidden_target'], 'pass': spec['forbidden_target'] not in ids, 'result_ids': ids})
        out.append(e)
    res['probes'] = out
    res['no_includes_nb_nn'] = {'nn_only_exact_hits': [[e['query'], e['matched_language']] for e in out if e.get('expected_language') == 'nn'],
                                'pass': all(e['matched_language'] == 'nn' and e['pass'] for e in out if e.get('expected_language') == 'nn')}
    res['probe_results_sha256'] = hashlib.sha256(json.dumps(pr, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    res['file_hashes'] = {f'{d}/{p.name}': sha(p) for d in ('w1', 'scoped') for p in sorted((run / d).iterdir()) if p.is_file()}
    for n in ('import.sql', 'validation-rollback.sql', 'frozen.sqlite3', 'archive-members.json', 'scope-validation.json', 'desktop.json'):
        res['file_hashes'][n] = sha(run / n)
    res['rollback'] = json.load(open(run / 'rollback-verification.json'))
    (run / 'analysis.json').write_text(json.dumps(res, ensure_ascii=False, sort_keys=True, indent=1, default=list) + '\n')
    print('written', sha(run / 'analysis.json'))


if __name__ == '__main__':
    {'inputs': inputs, 'analyze': analyze}[sys.argv[1]](Path(sys.argv[2]))

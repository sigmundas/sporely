#!/usr/bin/env python3
"""Read-only Norwegian vernacular simulation; never compiles a release.

Inputs include an existing normalized NorTaxa source, a read-only decompressed
bundle, and a read-only production taxonomy snapshot. Outputs are audit evidence.
"""
import argparse
import collections
import csv
import gzip
import hashlib
import io
import json
import re
import sqlite3
import sys
import unicodedata
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
TAX = REPO / 'database/taxonomy'
sys.path.insert(0, str(TAX / 'scripts'))
from cross_source_mapping import _canonical_name, _ACCEPTED_STATUSES
sys.path.insert(0, str(TAX))
import macrofungi_scope

LANGUAGES = {'nb', 'nn', 'no'}
QUALIFIED = re.compile(r'\b(?:sensu|auct|pro\s*parte|s\s*\.\s*l\s*\.|s\s*\.\s*str\s*\.|agg(?:regate)?\.?|complex|misapplied|cf\.|aff\.)|\b(?:p\s*\.\s*p\s*\.)', re.I)
SPLIT = re.compile(r'\b(?:sensu lato|sensu stricto|pro parte|misapplied|species complex|species aggregate|cryptic species|split into|split from|split off|merged into|merged with|lumped with|taxonomic split|taxonomic merge)\b', re.I)

RULE_CLASS = 'vernacular_only_exact_name_rank_full_universe_unique_current_v2'

def lines(path):
    with path.open() as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()

def write(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2) + '\n')

def author_kind(a, b):
    def simple(s):
        return re.sub(r'[^\w]', '', unicodedata.normalize('NFKD', s).casefold())
    if a == b:
        return 'same'
    if not a or not b:
        return 'missing_on_one_or_both_sides'
    if simple(a) == simple(b):
        return 'punctuation_spacing_case_or_diacritics'
    def sanction(s):
        return re.sub(r':[^)]*(?=\)|$)', '', s)
    if simple(sanction(a)) == simple(sanction(b)):
        return 'sanctioning_citation'
    def ex(s):
        return re.sub(r'\bex\s+[^),]+', '', s, flags=re.I)
    if simple(ex(a)) == simple(ex(b)):
        return 'ex_authorship_form'
    return 'other_difference_including_abbreviations'

def apply_owner_reviews(document, rows, taxa, bundle_names, bundle_preferred, meta):
    """Apply pinned per-name metadata decisions to evidence, never identity.

    The automatic result is computed first and is not reclassified by review.
    A reviewed target must reconstruct from both its stable ID and COL usage.
    Collective constraints preserve existing linkage instead of inventing a
    canonical aggregate or flattening it onto the strict species.
    """
    pins = document['pins']
    assert pins['content_release_id'] == meta['content_release_id']
    assert pins['nortaxa_archive_sha256'] == meta['source_release[nortaxa].archive_sha256']
    assert pins['col_archive_sha256'] == meta['source_release[col_xr].archive_sha256']
    by_col = collections.defaultdict(list)
    for tid, t in taxa.items():
        if t['canonical_source_system'] == 'col_xr':
            by_col[t['canonical_external_id']].append(tid)
    reviewed_rows = []
    unresolved = []
    used = set()
    for review in document['reviews']:
        assert review['review_status'] == 'approved_for_vernacular_only'
        assert review['identity_effect'] == 'none' and review['external_identifier_emission'] is False
        assert review['review_id'] not in used
        used.add(review['review_id'])
        tid = review['target_sporely_taxon_id']
        target = taxa.get(tid)
        if not target or by_col[review['target_col_usage_id']] != [tid] or target['canonical_scientific_name'] != review['target_canonical_scientific_name']:
            unresolved.append({'review_id': review['review_id'], 'reason': 'target_not_uniquely_reconstructable', 'target_sporely_taxon_id': tid})
            continue
        wanted = {tuple(n) for n in review['vernacular_names']}
        candidates = [r for r in rows if r['source_record_identifier'] == review['source_record_identifier'] and (r['language'], r['vernacular_name']) in wanted]
        assert {(r['language'], r['vernacular_name']) for r in candidates} == wanted, review['review_id']
        for row in candidates:
            assert row['source_taxon_identifier'] == review['source_taxon_identifier']
            assert row['pinned_source_release'] == review['source_release']
            assert row['source_scientific_name'] == review['source_scientific_name']
            for field in ('source_authorship', 'source_status', 'source_nomenclatural_status'):
                if field in review:
                    assert row[field] == review[field], (review['review_id'], field)
            assert row['source_rank'] == target['taxon_rank'] == 'species'
            assert row['source_classification']['kingdom'] == 'Fungi'
            assert not row['reviewed_decision_conflicts'], 'Owner vernacular review cannot silently override an identity decision'
            diag = [d for d in row['authorship_diagnostics'] if d['target_sporely_taxon_id'] == tid]
            assert len(diag) == 1 and diag[0]['target_authorship'] == review['target_authorship']
            assert row['disposition'] == 'exception', 'These approvals cover conservative exceptions only'
            assert row['source_record_identifier']['value'] != '53666', 'Collective Entoloma source cannot be sent to strict species'
            row['automatic_disposition'] = row['disposition']
            row['automatic_rule_evidence_class'] = row['rule_evidence_class']
            row['automatic_exception_reasons'] = list(row['reasons'])
            row['disposition'] = 'reviewed_enrichment'
            row['rule_evidence_class'] = review['rule_evidence_class']
            row['review_id'] = review['review_id']
            row['reviewer'] = review['reviewer']
            row['review_rationale'] = review['rationale']
            row['review_evidence_references'] = review['evidence_references']
            row['reasons'] = ['owner_reviewed_vernacular_only_exception']
            row['target_sporely_taxon_id'] = tid
            row['target_canonical_scientific_name'] = target['canonical_scientific_name']
            row['target_authorship'] = review['target_authorship']
            row['projected_is_preferred'] = bool(row['is_preferred']) and not any(lang == row['language'] for lang, name in bundle_preferred[tid])
            row['existing_bound_sporely_taxon_id_unchanged'] = row['existing_bound_sporely_taxon_id']
            reviewed_rows.append(row)
    collective_evidence = []
    for constraint in document.get('semantic_constraints', []):
        strict = constraint['strict_target_sporely_taxon_id']
        collective = constraint['source_bound_sporely_taxon_id']
        assert strict != collective and strict in taxa and collective in taxa
        assert all(tuple(n) in bundle_names[strict] for n in constraint['strict_vernaculars'])
        assert all(tuple(n) in bundle_names[collective] and tuple(n) not in bundle_names[strict] for n in constraint['collective_vernaculars'])
        matching = [r for r in rows if r['source_record_identifier'] == constraint['source_record_identifier']]
        assert matching
        for row in matching:
            assert row['disposition'] == 'exception' and row['existing_bound_sporely_taxon_id'] == collective
            assert row['source_authorship'] == constraint['source_authorship']
            row['semantic_constraint_id'] = constraint['constraint_id']
            row['source_concept_semantics'] = 'collective_aggregate'
            row['source_semantic_display_name'] = constraint['collective_display_name']
            row['preserved_source_bound_sporely_taxon_id'] = collective
            row['forbidden_strict_target_sporely_taxon_id'] = strict
            row['reasons'] = sorted(set(row['reasons'] + ['collective_concept_must_remain_distinct']))
        collective_evidence.append({**constraint, 'strict_existing_vernaculars_confirmed': True, 'collective_existing_vernaculars_confirmed': True, 'projection_to_strict_species': False})
    return reviewed_rows, unresolved, collective_evidence

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--normalized', type=Path, required=True)
    p.add_argument('--bundle', type=Path, required=True)
    p.add_argument('--live-snapshot', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--owner-reviews', type=Path)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(f'file:{args.bundle.resolve()}?mode=ro', uri=True)
    c.row_factory = sqlite3.Row
    c.execute('pragma query_only=on')
    meta = dict(c.execute('select key,value from taxonomy_meta'))
    live = json.loads(args.live_snapshot.read_text())
    assert live['status'] == 'active' and live['release_id'] == meta['content_release_id']
    scope = set(live['taxon_ids'])
    taxa = {r['taxon_id']: dict(r) for r in c.execute('select * from taxon_min')}
    assert scope <= taxa.keys(), 'Live scope contains concepts absent from bundle'
    no = [v for v in lines(args.normalized / 'vernacular.jsonl') if v['language'] in LANGUAGES]
    wanted = {v['core_row_id']['value'] for v in no}
    sources = {r['core_row_id']['value']: r for r in lines(args.normalized / 'taxa.jsonl') if r['core_row_id']['value'] in wanted}
    source_ids = {r['taxon_id']['value'] for r in sources.values()}
    raw_sources = {}
    csv.field_size_limit(10 * 1024 * 1024)
    with zipfile.ZipFile(TAX / 'sources/nortaxa/1.284/archive.zip') as z:
        for row in csv.DictReader(io.TextIOWrapper(z.open('taxon.txt'), encoding='utf-8-sig'), delimiter='\t', quoting=csv.QUOTE_NONE):
            if row['taxonID'] in source_ids:
                raw_sources[row['taxonID']] = row
    assert source_ids <= raw_sources.keys()
    bundle_names = collections.defaultdict(set)
    bundle_preferred = collections.defaultdict(set)
    for r in c.execute("select * from vernacular_min where language_code in ('nb','nn','no')"):
        bundle_names[r['taxon_id']].add((r['language_code'], r['vernacular_name']))
        if r['is_preferred_name']:
            bundle_preferred[r['taxon_id']].add((r['language_code'], r['vernacular_name']))
    live_names = collections.defaultdict(set)
    for tid, lang, name, preferred in live['norwegian_vernaculars']:
        live_names[tid].add((lang, name))
    scope_policy = macrofungi_scope.load_policy(TAX / 'policies/global-macrofungi-scope.yml')
    scope_taxa, by_col = macrofungi_scope.load_taxa(c)
    scope_rules = macrofungi_scope.resolve_rules(scope_policy, by_col)
    scope_states = macrofungi_scope.evaluate(scope_taxa, scope_rules, scope_policy.get('source_characteristic_exclusions', []))
    selectable = {tid for tid in scope if scope_states[tid]['state'] == 'include'}
    ancestor_only = scope - selectable
    discrepancies = [{'target_sporely_taxon_id': tid, 'bundle_only': sorted(bundle_names[tid] - live_names[tid]), 'live_only': sorted(live_names[tid] - bundle_names[tid]), 'required_ancestor_only': tid in ancestor_only} for tid in sorted(scope) if live_names[tid] != bundle_names[tid]]
    assert all(live_names[tid] == bundle_names[tid] for tid in selectable), 'Selectable live/bundle Norwegian names diverge'
    canonical = {tid: t for tid, t in taxa.items() if t['canonical_source_system'] == 'col_xr'}
    by_name = collections.defaultdict(list)
    for tid, t in canonical.items():
        if _canonical_name(t['taxonomic_status']) in _ACCEPTED_STATUSES:
            by_name[_canonical_name(t['canonical_scientific_name'])].append(tid)
    relevant_targets = {tid for s in sources.values() for tid in by_name[_canonical_name(s['scientific_name'])]}
    registry = {}
    for part in sorted((TAX / 'registry/canonical').glob('part-*.jsonl')):
        for r in lines(part):
            if r.get('__registry_header__'):
                continue
            if r['source'] == 'nortaxa' and r['identifier'] in source_ids or r['source'] == 'col_xr':
                registry[(r['source'], r['namespace'], r['identifier'])] = r['sporely_taxon_id']
    manual = json.loads((TAX / 'policies/manual_mappings.yml').read_text())['mappings']
    supersessions = json.loads((TAX / 'policies/concept_supersessions.yml').read_text())['supersessions']
    def resolve_target(target):
        if 'sporely_taxon_id' in target:
            return target['sporely_taxon_id']
        u = target.get('source_usage', target)
        return registry.get((u['source'], u['namespace'], u['identifier']))
    decisions = collections.defaultdict(list)
    for m in manual:
        u = m['source_usage']
        if u['source'] == 'nortaxa' and u['identifier'] in source_ids:
            decisions[u['identifier']].append(m)
    super_by_id = collections.defaultdict(list)
    current = {}
    for s in supersessions:
        super_by_id[s['superseded_sporely_taxon_id']].append(s)
        if s['review_status'] == 'approved':
            current[s['superseded_sporely_taxon_id']] = resolve_target(s['current_source_usage'])
    raw_targets = {}
    archive = TAX / 'sources/col_xr/2026-07-17-XR/archive.zip'
    print('Reading pinned COL metadata for', len(relevant_targets), 'target concepts', flush=True)
    col_ids = {canonical[tid]['canonical_external_id']: tid for tid in relevant_targets}
    with zipfile.ZipFile(archive) as z, z.open('NameUsage.tsv') as f:
        header = f.readline().decode().rstrip('\n').split('\t')
        ix = {k: i for i, k in enumerate(header)}
        for raw in f:
            parts = raw.decode().rstrip('\n').split('\t')
            if parts[0] not in col_ids:
                continue
            row = {k: parts[i] if i < len(parts) else '' for k, i in ix.items()}
            raw_targets[col_ids[parts[0]]] = row
    assert relevant_targets <= raw_targets.keys()
    stage2 = collections.defaultdict(list)
    for path in [TAX / 'evidence/taxonomy-v3/stage2/group-b-no-published-cross-reference.manifest.json', TAX / 'evidence/taxonomy-v3/stage2/group-b-not-one-to-one.manifest.json']:
        d = json.loads(path.read_text())
        for member in d['members']:
            row = dict(zip(d['columns'], member))
            stage2[row['nortaxa_taxon_id']].append({'path': str(path.relative_to(REPO)), 'evidence_class': d.get('evidence_class'), 'col_sporely_taxon_id': row['col_sporely_taxon_id']})
    output = []
    for v in no:
        s = sources[v['core_row_id']['value']]
        sid = s['taxon_id']['value']
        key = (v['language'], v['vernacular_name'])
        all_targets = sorted(by_name[_canonical_name(s['scientific_name'])])
        rank_targets = [tid for tid in all_targets if _canonical_name(taxa[tid]['taxon_rank']) == _canonical_name(s['rank'])]
        scoped = [tid for tid in rank_targets if tid in selectable]
        allocated = registry.get(('nortaxa', 'nortaxa_taxon_id', sid))
        bound = current.get(allocated, allocated)
        reviewed = [m for m in decisions[sid] if m['review_status'] in ('approved', 'rejected')]
        approved_targets = {resolve_target(m['target']) for m in reviewed if m['review_status'] == 'approved' and m['relationship'] == 'exact'}
        if approved_targets:
            assert len(approved_targets) == 1
            bound = next(iter(approved_targets))
        existing_target = bound if bound in canonical else None
        reachable_bundle = existing_target is not None and key in bundle_names[existing_target]
        reachable_cloud = reachable_bundle and existing_target in selectable and key in live_names[existing_target]
        # Existing matching metadata is unchanged even if there is no identity bridge.
        matching_cloud = [tid for tid in scoped if key in live_names[tid]]
        if matching_cloud:
            reachable_cloud = True
        matching_bundle = [tid for tid in rank_targets if key in bundle_names[tid]]
        reachable_bundle = reachable_bundle or bool(matching_bundle)
        reasons = []
        warnings = []
        differences = []
        conflict_names = []
        classification = []
        reviewed_conflicts = []
        if not scoped:
            if not all_targets:
                reasons.append('no_exact_accepted_canonical_name')
            elif not rank_targets:
                reasons.append('rank_mismatch')
            else:
                reasons.append('outside_current_cloud_scope')
        if len(scoped) > 1:
            reasons.append('multiple_canonical_targets_in_scope')
        if len(rank_targets) != 1:
            reasons.append('multiple_canonical_targets_in_full_release' if rank_targets else 'no_eligible_full_release_target')
        if _canonical_name(s['taxonomic_status']) not in _ACCEPTED_STATUSES:
            reasons.append('source_not_accepted_current')
        accepted = s.get('accepted_name_usage_id')
        if accepted and accepted['value'] != sid:
            reasons.append('source_points_to_other_accepted_usage')
        if QUALIFIED.search(' '.join([s['scientific_name'], s['authorship'], s['rank'], s['taxonomic_status']])):
            reasons.append('qualified_or_aggregate_source')
        assert 'nomenclatural_status' in s, 'Re-normalize source with raw nomenclaturalStatus preservation'
        nomen_status = s['nomenclatural_status']
        assert nomen_status == raw_sources[sid].get('nomenclaturalStatus', ''), 'Normalized nomenclatural warning changed'
        if _canonical_name(nomen_status) not in ('', 'conserved', 'sanctioned'):
            reasons.append('source_nomenclatural_status_ambiguous')
        if not s['classification'].get('kingdom'):
            reasons.append('source_kingdom_missing')
        elif _canonical_name(s['classification']['kingdom']) != 'fungi':
            reasons.append('source_not_fungi')
        # Eligibility evidence is evaluated against the full accepted canonical
        # name/rank universe; the cloud scope controls counting/projection only.
        for tid in rank_targets:
            raw = raw_targets[tid]
            differences.append({'target_sporely_taxon_id': tid, 'target_authorship': raw['col:authorship'], 'difference': author_kind(s['authorship'], raw['col:authorship'])})
            if QUALIFIED.search(' '.join([raw['col:scientificName'], raw['col:authorship'], raw['col:namePhrase']])) or SPLIT.search(' '.join([raw['col:remarks'], raw['col:nameRemarks']])):
                reasons.append('qualified_or_split_prone_target')
            if re.search(r'misappl|pro.?parte|illeg|invalid|unpublished|notvalidlypublished|inval|nudum|dubious|doubtful|orthographic', raw['col:nameStatus'], re.I) or re.search(r'\bnom\.\s*(?:illeg|inval|nud)|\blater homonym\b', raw['col:nameRemarks'] + ' ' + raw['col:remarks'], re.I):
                reasons.append('target_nomenclatural_warning')
            compared = []
            for level in ('kingdom', 'phylum', 'class', 'order', 'family', 'genus'):
                a = s['classification'].get(level, '')
                b = raw.get('col:' + level, '')
                # Incertae sedis is absence of a resolved placement, not a
                # contradictory named clade; retain it as a diagnostic.
                if re.search(r'incertae sedis|unassigned|unclassified|unknown', a + ' ' + b, re.I):
                    warnings.append('unresolved_classification_' + level)
                    continue
                if level == 'genus' and not b:
                    b = raw.get('col:genericName', '')
                if a and b:
                    compared.append(level)
                    if _canonical_name(a) != _canonical_name(b):
                        classification.append({'target_sporely_taxon_id': tid, 'level': level, 'source': a, 'target': b})
            if 'kingdom' not in compared:
                reasons.append('target_kingdom_unconfirmed')
            for lang, name in sorted(bundle_names[tid]):
                if lang == v['language'] and _canonical_name(name) != _canonical_name(v['vernacular_name']):
                    conflict_names.append({'target_sporely_taxon_id': tid, 'language': lang, 'existing_name': name, 'existing_is_preferred': (lang, name) in bundle_preferred[tid]})
            for m in reviewed:
                mt = resolve_target(m['target'])
                if m['review_status'] == 'rejected' or m['relationship'] != 'exact' or mt != tid:
                    reviewed_conflicts.append({'decision_id': m['mapping_id'], 'review_status': m['review_status'], 'relationship': m['relationship'], 'decision_target': mt, 'candidate_target': tid})
            for sup in super_by_id.get(allocated, []):
                st = resolve_target(sup['current_source_usage'])
                if sup['review_status'] in ('approved', 'rejected') and (sup['review_status'] == 'rejected' or st != tid):
                    reviewed_conflicts.append({'decision_id': sup['supersession_id'], 'review_status': sup['review_status'], 'decision_target': st, 'candidate_target': tid})
        if any(d['level'] == 'kingdom' for d in classification):
            reasons.append('kingdom_disagreement')
        if any(d['level'] != 'kingdom' for d in classification):
            if _canonical_name(s['rank']) == 'species':
                warnings.append('below_kingdom_classification_disagreement')
            else:
                reasons.append('higher_classification_disagreement')
        if conflict_names:
            warnings.append('existing_same_language_vernacular_preserved')
        if reviewed_conflicts:
            reasons.append('reviewed_decision_disagrees')
        if approved_targets and scoped and set(scoped) != approved_targets:
            reasons.append('reviewed_association_takes_precedence')
        for m in reviewed:
            if m['relationship'] != 'exact' and m['review_status'] == 'approved':
                reasons.append('reviewed_non_exact_relationship')
        reasons = sorted(set(reasons))
        if reachable_cloud:
            disposition = 'already_reaches_cloud_unchanged'
        elif not reasons:
            disposition = 'automatic_enrichment'
        else:
            disposition = 'exception' if scoped or existing_target in selectable else 'outside_scope_or_no_exact_target'
        target = (matching_cloud or scoped or ([existing_target] if existing_target else []))
        tid = target[0] if len(target) == 1 else None
        native_present = bound in taxa and key in bundle_names[bound]
        output.append({
            'vernacular_name': v['vernacular_name'], 'language': v['language'], 'is_preferred': v['is_preferred'],
            'source': 'nortaxa', 'pinned_source_release': s['source_release'], 'source_record_identifier': s['core_row_id'], 'source_taxon_identifier': s['taxon_id'],
            'source_scientific_name': s['scientific_name'], 'source_authorship': s['authorship'], 'source_rank': s['rank'], 'source_status': s['taxonomic_status'], 'source_classification': s['classification'],
            'source_nomenclatural_status': nomen_status,
            'source_provenance': v['provenance'], 'source_usage_provenance': s['provenance'],
            'target_sporely_taxon_id': tid, 'target_canonical_scientific_name': taxa[tid]['canonical_scientific_name'] if tid else None,
            'target_authorship': raw_targets.get(tid, {}).get('col:authorship'), 'scoped_target_ids': scoped, 'full_bundle_target_ids': rank_targets,
            'allocated_source_sporely_taxon_id': allocated, 'existing_bound_sporely_taxon_id': bound,
            'currently_reaches_bundled_canonical': reachable_bundle, 'currently_reaches_cloud_canonical': reachable_cloud,
            'currently_present_on_source_bound_concept': native_present,
            'disposition': disposition, 'rule_evidence_class': RULE_CLASS,
            'reasons': reasons if disposition != 'automatic_enrichment' else ['exact_name_rank_full_release_unique_current_no_contradiction'],
            'projected_is_preferred': bool(v['is_preferred']) and not any(lang == v['language'] for t in rank_targets for lang, name in bundle_preferred[t]),
            'existing_preferred_vernaculars_preserved': True,
            'warnings': sorted(set(warnings)), 'authorship_diagnostics': differences,
            'classification_disagreements': classification, 'existing_vernacular_conflicts': conflict_names,
            'reviewed_decision_conflicts': reviewed_conflicts, 'existing_reviewed_decisions': [m['mapping_id'] for m in reviewed],
            'stage2_evidence': stage2[sid], 'identity_effect': 'none', 'external_identifier_emission': False,
        })
    output.sort(key=lambda r: (r['source_record_identifier']['value'], r['language'], r['vernacular_name'], r['source_provenance']['row_index']))
    def census(rows):
        return {'source_vernacular_rows': len(rows), 'distinct_name_language_pairs': len({(r['language'], r['vernacular_name']) for r in rows}), 'source_usages': len({r['source_record_identifier']['value'] for r in rows})}
    auto = [r for r in output if r['disposition'] == 'automatic_enrichment']
    exc = [r for r in output if r['disposition'] == 'exception']
    additions = {(r['target_sporely_taxon_id'], r['language'], r['vernacular_name']) for r in auto}
    auto_source_per_target = collections.defaultdict(set)
    for r in auto:
        auto_source_per_target[r['target_sporely_taxon_id']].add(r['source_taxon_identifier']['value'])
    assert all(len(ids) == 1 for ids in auto_source_per_target.values()), 'Competing source usages require review'
    assert all(len(r['full_bundle_target_ids']) == 1 for r in auto), 'Automatic association must be unique across full canonical universe'
    assert all(not r['reviewed_decision_conflicts'] and not any(d['level'] == 'kingdom' for d in r['classification_disagreements']) for r in auto)
    assert all(not r['classification_disagreements'] or r['source_rank'] == 'species' for r in auto)
    conservative_exception_census = census(exc)
    reviewed_additions = []
    unresolved_reviews = []
    collective_evidence = []
    if args.owner_reviews:
        reviewed_additions, unresolved_reviews, collective_evidence = apply_owner_reviews(
            json.loads(args.owner_reviews.read_text()), output, taxa, bundle_names, bundle_preferred, meta,
        )
    exc = [r for r in output if r['disposition'] == 'exception']
    rows_by_disposition = collections.Counter(r['disposition'] for r in output)
    reviewed_keys = {(r['target_sporely_taxon_id'], r['language'], r['vernacular_name']) for r in reviewed_additions}
    assert not (reviewed_keys & additions)
    exception_usages = collections.defaultdict(list)
    for r in exc:
        exception_usages[r['source_record_identifier']['value']].append(r)
    exception_groups = []
    for sid, rows in sorted(exception_usages.items()):
        exception_groups.append({'source_record_identifier': rows[0]['source_record_identifier'], 'source_scientific_name': rows[0]['source_scientific_name'], 'source_rank': rows[0]['source_rank'], 'vernacular_names': sorted({(r['language'], r['vernacular_name']) for r in rows}), 'reasons': sorted({reason for r in rows for reason in r['reasons']}), 'scoped_target_ids': rows[0]['scoped_target_ids'], 'full_bundle_target_ids': rows[0]['full_bundle_target_ids']})
    reason_combinations = collections.Counter(';'.join(r['reasons']) for r in exception_groups)
    by_rank = {}
    for rank in sorted({r['source_rank'] for r in output}):
        rows = [r for r in output if r['source_rank'] == rank]
        by_rank[rank] = dict(collections.Counter(r['disposition'] for r in rows))
    regression = {}
    for name in ('Russula aeruginea', 'Henningsomyces puber', 'Helvella crispa', 'Arrhenia chlorocyanea', 'Gerronema', 'Gymnosporangium cornutum', 'Alloclavaria purpurea', 'Entoloma sericellum'):
        rows = [r for r in output if r['source_scientific_name'] == name]
        regression[name] = [{k: r[k] for k in ('vernacular_name', 'language', 'disposition', 'reasons', 'warnings', 'scoped_target_ids', 'full_bundle_target_ids', 'authorship_diagnostics', 'classification_disagreements', 'existing_vernacular_conflicts', 'automatic_exception_reasons', 'target_sporely_taxon_id', 'source_concept_semantics', 'preserved_source_bound_sporely_taxon_id', 'forbidden_strict_target_sporely_taxon_id') if k in r} for r in rows]
    assert any(r['vernacular_name'] == 'grønnkremle' and r['disposition'] == 'automatic_enrichment' for r in output)
    assert any(r['vernacular_name'] == 'grønn navlesopp' and r['disposition'] == 'already_reaches_cloud_unchanged' for r in output)
    assert any(r['vernacular_name'] == 'lys høstmorkel' and 'multiple_canonical_targets_in_scope' in r.get('automatic_exception_reasons', r['reasons']) for r in output)
    assert all(r['disposition'] == 'exception' and 'multiple_canonical_targets_in_full_release' in r['reasons'] for r in output if r['source_scientific_name'] == 'Gerronema')
    for name in ('Gymnosporangium cornutum', 'Alloclavaria purpurea'):
        assert any(r['source_scientific_name'] == name and r['disposition'] == 'automatic_enrichment' and 'below_kingdom_classification_disagreement' in r['warnings'] for r in output)
    pinpaths = [TAX / 'sources/nortaxa/1.284/archive.zip', archive, TAX / 'policies/manual_mappings.yml', TAX / 'policies/concept_supersessions.yml', TAX / 'policies/mapping_policy.yml', TAX / 'policies/global-macrofungi-scope.yml', REPO / 'database/reference_data/generated/taxonomy_v2/tax-2026.09.30-01.sqlite3.gz', TAX / 'registry/canonical/manifest.json']
    pins = {str(path.relative_to(REPO)): digest(path) for path in pinpaths}
    assert pins[str(archive.relative_to(REPO))] == meta['source_release[col_xr].archive_sha256']
    assert pins['database/taxonomy/sources/nortaxa/1.284/archive.zip'] == meta['source_release[nortaxa].archive_sha256']
    assert digest(args.bundle) == json.loads((REPO / 'database/reference_data/generated/taxonomy_v2/manifest.json').read_text())['sqlite_sha256']
    if args.owner_reviews:
        assert json.loads(args.owner_reviews.read_text())['pins']['bundle_gz_sha256'] == pins['database/reference_data/generated/taxonomy_v2/tax-2026.09.30-01.sqlite3.gz']
    summary = {
        'release_id': meta['content_release_id'], 'rule_evidence_class': RULE_CLASS, 'pins': pins, 'compiler_manifest_sha256': meta['compiler_manifest_sha256'], 'registry_sha256': meta['registry_sha256'],
        'audit_program_sha256': digest(Path(__file__)),
        'normalizer_sha256': digest(TAX / 'scripts/national_source.py'),
        'normalized_taxa_sha256': digest(args.normalized / 'taxa.jsonl'),
        'normalized_vernacular_sha256': digest(args.normalized / 'vernacular.jsonl'),
        'live_snapshot_sha256': digest(args.live_snapshot), 'live_scope_concepts': len(scope), 'selectable_cloud_concepts': len(selectable), 'required_ancestor_concepts': len(ancestor_only), 'live_norwegian_rows': sum(map(len, live_names.values())), 'bundle_cloud_discrepancies': discrepancies,
        'total_nortaxa_norwegian': census(output), 'by_disposition': dict(rows_by_disposition),
        'fungal_source_norwegian': census([r for r in output if _canonical_name(r['source_classification'].get('kingdom', '')) == 'fungi']),
        'already_reaches_bundled_canonical': census([r for r in output if r['currently_reaches_bundled_canonical']]),
        'already_reaches_cloud_canonical': census([r for r in output if r['currently_reaches_cloud_canonical']]),
        'not_reaching_cloud_canonical': census([r for r in output if not r['currently_reaches_cloud_canonical']]),
        'stranded_on_source_bound_concept': census([r for r in output if r['currently_present_on_source_bound_concept'] and not r['currently_reaches_cloud_canonical']]),
        'automatic': census(auto), 'new_export_rows': len(additions), 'canonical_concepts_affected': len({r[0] for r in additions}),
        'conservative_exceptions_before_owner_review': conservative_exception_census,
        'reviewed_additions': census(reviewed_additions), 'reviewed_new_export_rows': len(reviewed_keys),
        'total_new_export_rows_with_reviews': len(additions | reviewed_keys),
        'canonical_concepts_with_reviews': len({r[0] for r in additions | reviewed_keys}),
        'unresolved_review_targets': unresolved_reviews,
        'owner_reviews_sha256': digest(args.owner_reviews) if args.owner_reviews else None,
        'remaining_open_species': sorted({r['source_scientific_name'] for r in exc if r['source_rank'] == 'species'}),
        'collective_semantic_constraints': collective_evidence,
        'exceptions': census(exc), 'exception_reason_counts_rows': dict(collections.Counter(reason for r in exc for reason in r['reasons'])),
        'exception_reason_counts_source_usages': {reason: len({r['source_record_identifier']['value'] for r in exc if reason in r['reasons']}) for reason in sorted({reason for r in exc for reason in r['reasons']})},
        'exception_reason_combinations_source_usages': dict(reason_combinations),
        'classification_disagreement_source_usages_by_level': {level: len({r['source_record_identifier']['value'] for r in exc if any(d['level'] == level for d in r['classification_disagreements'])}) for level in ('kingdom', 'phylum', 'class', 'order', 'family', 'genus')},
        'existing_reviewed_decision_disagreement_rows': sum(bool(r['reviewed_decision_conflicts']) for r in output),
        'outside_scope_reason_counts_rows': dict(collections.Counter(reason for r in output if r['disposition'] == 'outside_scope_or_no_exact_target' for reason in r['reasons'])),
        'automatic_authorship_counts': dict(collections.Counter(d['difference'] for r in auto for d in r['authorship_diagnostics'])),
        'automatic_classification_warning_rows': sum(bool(r['classification_disagreements']) for r in auto),
        'automatic_classification_warning_source_usages': len({r['source_record_identifier']['value'] for r in auto if r['classification_disagreements']}),
        'by_rank': by_rank, 'regressions': regression,
        'stage2_no_cross_reference_automatic_rows': sum(bool(r['stage2_evidence']) for r in auto),
        'invariants': {'sqlite_opened_read_only': True, 'selectable_live_bundle_norwegian_names_equal': True, 'no_identity_or_external_id_outputs': True, 'no_release_build': True, 'automatic_full_release_unique': True, 'preferred_vernaculars_preserved': True},
    }
    write(args.output / 'summary.json', summary)
    # The all-source ledger includes plants and other out-of-scope records.
    # Compress it; keep the bounded automatic/exception manifests readable.
    with (args.output / 'all-source-associations.jsonl.gz').open('wb') as raw:
        with gzip.GzipFile(fileobj=raw, mode='wb', mtime=0, filename='') as f:
            for r in output:
                f.write((json.dumps(r, ensure_ascii=False, sort_keys=True) + '\n').encode())
    write(args.output / 'automatic-associations.json', auto)
    write(args.output / 'reviewed-associations.json', reviewed_additions)
    write(args.output / 'unresolved-reviewed-targets.json', unresolved_reviews)
    write(args.output / 'collective-concept-preservation.json', collective_evidence)
    write(args.output / 'exceptions.json', exc)
    write(args.output / 'exception-source-usages.json', exception_groups)
    write(args.output / 'classification-disagreements.json', [r for r in auto + reviewed_additions + exc if r['classification_disagreements']])
    write(args.output / 'vernacular-conflicts.json', [r for r in auto + reviewed_additions + exc if r['existing_vernacular_conflicts']])
    write(args.output / 'multiple-targets.json', [r for r in reviewed_additions + exc if len(r['full_bundle_target_ids']) > 1])
    write(args.output / 'qualified-or-split-cases.json', [r for r in reviewed_additions + exc if any('qualified' in reason or 'nomenclatural' in reason for reason in r.get('automatic_exception_reasons', r['reasons']))])
    write(args.output / 'named-regressions.json', regression)
    write(args.output / 'live-taxonomy-snapshot.json', live)
    print(json.dumps({k: summary[k] for k in ('by_disposition', 'automatic', 'new_export_rows', 'canonical_concepts_affected', 'exceptions', 'exception_reason_counts_source_usages')}, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()

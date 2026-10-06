"""Vernacular-only evidence and projection. Never allocates or binds identities."""
from __future__ import annotations

import collections
import hashlib
import json
import re
from pathlib import Path

from cross_source_mapping import _canonical_name as norm, _ACCEPTED_STATUSES

AUTOMATIC = 'automatic_vernacular_enrichment'
REVIEWED = 'owner_reviewed_vernacular_association'
NATIVE = 'source_native_concept_no_col_mapping'
RULE = 'vernacular_only_exact_name_rank_full_universe_unique_current_v2'
QUALIFIED = re.compile(r'\b(?:coll\.?(?=\s|$)|sensu\b|auct\.?(?=\s|$)|pro\s*parte|s\s*\.\s*l\s*\.(?!\s*(?-i:[A-ZÀ-ÞŁŠŽČ]))|s\s*\.\s*str\s*\.|agg(?:regate)?\.?(?=\s|$)|aggregate\b|complex\b|misapplied\b|cf\.|aff\.)|\b(?:p\s*\.\s*p\s*\.)(?!\s*(?-i:[A-ZÀ-ÞŁŠŽČ]))', re.I)
QUALIFIED_REASON = 'qualified_or_aggregate_source'
COL_ECHO_WITHHELD = 'withheld_col_vernacular_association'
COL_ECHO_REASON = 'col_vernacular_matches_withheld_qualified_national_association'
SPLIT = re.compile(r'\b(?:sensu lato|sensu stricto|pro parte|misapplied|species complex|species aggregate|cryptic species|split into|split from|split off|merged into|merged with|lumped with|taxonomic split|taxonomic merge)\b', re.I)


class ProjectionError(ValueError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def identity_fingerprint(taxa, bindings):
    digest = hashlib.sha256()
    for rows in (taxa, bindings):
        for row in rows:
            digest.update(canonical(row).encode())
            digest.update(b'\n')
    return digest.hexdigest()


def usage(record):
    return {'source': record.source_code, 'namespace': record.taxon_id_namespace, 'identifier': record.taxon_id_value}


def usage_key(ref):
    return ref['source'], ref['namespace'], ref['identifier']


def annotation(record):
    raw = record.raw
    fields = {'scientific_name': record.scientific_name, 'authorship': record.authorship,
              'rank': record.rank, 'status': record.taxonomic_status,
              **raw.get('concept_annotation', {})}
    # Keep the whole annotated field, not merely its stripped name/string.
    return {key: value for key, value in fields.items()
            if value and (QUALIFIED.search(value) or SPLIT.search(value))}


def semantic_evidence(record):
    """Conservative concept snapshot; release and author typography are diagnostic."""
    return {'source_usage': usage(record), 'scientific_name': record.scientific_name,
            'rank': record.rank, 'concept_annotation': annotation(record),
            'source_status': record.taxonomic_status,
            'nomenclatural_status': record.raw.get('nomenclatural_status', ''),
            'accepted_usage_id': record.accepted_name_usage_id,
            'kingdom': record.kingdom(),
            'raw_concept_annotation': record.raw.get('concept_annotation', {})}


def target_evidence(taxon, record):
    return {'sporely_taxon_id': taxon['sporely_taxon_id'],
            'canonical_source_usage': taxon['canonical_source_usage'],
            **semantic_evidence(record)}


def read_reviews(path):
    if path is None:
        return {'format': 'sporely-vernacular-concept-reviews-v1', 'reviews': []}
    document = json.loads(path.read_text(encoding='utf-8'))
    if document.get('format') != 'sporely-vernacular-concept-reviews-v1':
        raise ProjectionError('review ledger requires normalized concept evidence')
    seen = set()
    source_seen = set()
    for review in document['reviews']:
        if review['review_id'] in seen or usage_key(review['source_evidence']['source_usage']) in source_seen:
            raise ProjectionError('duplicate vernacular concept approval')
        seen.add(review['review_id'])
        source_seen.add(usage_key(review['source_evidence']['source_usage']))
        if (review.get('evidence_class') != REVIEWED or review.get('identity_effect') != 'none'
                or review.get('external_identifier_emission') is not False
                or not all(review.get(f) for f in ('reviewer', 'rationale', 'evidence_references'))):
            raise ProjectionError('invalid vernacular-only review provenance')
        for field in ('source_evidence', 'target_evidence'):
            required = {'source_usage', 'scientific_name', 'rank', 'concept_annotation',
                        'source_status', 'nomenclatural_status', 'accepted_usage_id', 'kingdom',
                        'raw_concept_annotation'}
            if field == 'target_evidence':
                required |= {'sporely_taxon_id', 'canonical_source_usage'}
            if not required <= review[field].keys():
                raise ProjectionError('review lacks required concept evidence')
            if review.get(field + '_sha256') != fingerprint(review[field]):
                raise ProjectionError('review concept evidence fingerprint mismatch')
    return document


def _conflicts(source, target_id, mappings, supersessions, bindings, context=None):
    key = usage_key(usage(source))
    by_usage = context['by_usage'] if context else {usage_key(u['source_usage']): u['sporely_taxon_id'] for u in bindings}
    if context:
        mappings = context['mappings'].get(key, [])
        supersessions = context['supersessions'].get(by_usage.get(key), [])
    result = []
    for mapping in mappings:
        if mapping.source_usage != key or mapping.review_status not in ('approved', 'rejected'):
            continue
        selected = mapping.target_sporely_taxon_id
        if selected is None and mapping.target_source_usage:
            selected = by_usage.get(mapping.target_source_usage)
        if mapping.review_status == 'rejected' or mapping.relationship != 'exact' or selected != target_id:
            result.append(mapping.mapping_id)
    bound = by_usage.get(key)
    for decision in supersessions:
        if decision.superseded_sporely_taxon_id == bound and decision.review_status in ('approved', 'rejected'):
            if decision.review_status == 'rejected' or by_usage.get(decision.current_source_usage) != target_id:
                result.append(decision.supersession_id)
    return sorted(result)


def automatic_assessment(source, candidates, records, mappings, supersessions, bindings, context=None):
    reasons, warnings, classification, authors = [], [], [], []
    if len(candidates) != 1:
        reasons.append('multiple_canonical_targets_in_full_release' if candidates else 'no_exact_canonical_target')
    if norm(source.taxonomic_status) not in _ACCEPTED_STATUSES:
        reasons.append('source_not_accepted_current')
    if source.accepted_name_usage_id and source.accepted_name_usage_id['value'] != source.taxon_id_value:
        reasons.append('source_points_to_other_accepted_usage')
    if annotation(source):
        reasons.append('qualified_or_aggregate_source')
    if 'nomenclatural_status' not in source.raw:
        reasons.append('source_nomenclatural_evidence_missing')
    elif norm(source.raw['nomenclatural_status']) not in ('', 'conserved', 'sanctioned'):
        reasons.append('source_nomenclatural_status_ambiguous')
    if norm(source.kingdom()) != 'fungi':
        reasons.append('source_not_fungi')
    conflicts = []
    for target in candidates:
        record = records[usage_key(target['canonical_source_usage'])]
        tid = target['sporely_taxon_id']
        authors.append({'target_sporely_taxon_id': tid, 'source_authorship': source.authorship,
                       'target_authorship': record.authorship, 'equality_required': False})
        if annotation(record):
            reasons.append('qualified_or_split_prone_target')
        if 'nomenclatural_status' not in record.raw or 'concept_annotation' not in record.raw:
            reasons.append('target_concept_evidence_missing')
        text = record.raw.get('nomenclatural_status', '')
        remarks = ' '.join(record.raw.get('concept_annotation', {}).values())
        if (re.search(r'misappl|pro.?parte|illeg|invalid|unpublished|notvalidlypublished|inval|nudum|dubious|doubtful|orthographic', text, re.I)
                or re.search(r'\bnom\.\s*(?:illeg|inval|nud)|\blater homonym\b', remarks, re.I)):
            reasons.append('target_nomenclatural_warning')
        if norm(record.kingdom()) != 'fungi':
            reasons.append('target_kingdom_unconfirmed_or_disagrees')
        for level in ('kingdom', 'phylum', 'class', 'order', 'family', 'genus'):
            a, b = source.classification.get(level, ''), record.classification.get(level, '')
            if re.search(r'incertae sedis|unassigned|unclassified|unknown', a + ' ' + b, re.I):
                warnings.append('unresolved_classification_' + level)
            elif a and b and norm(a) != norm(b):
                classification.append({'level': level, 'source': a, 'target': b, 'target_sporely_taxon_id': tid})
        conflicts.extend(_conflicts(source, tid, mappings, supersessions, bindings, context))
    if any(d['level'] == 'kingdom' for d in classification):
        reasons.append('kingdom_disagreement')
    if any(d['level'] != 'kingdom' for d in classification):
        (warnings if norm(source.rank) == 'species' else reasons).append(
            'below_kingdom_classification_disagreement' if norm(source.rank) == 'species' else 'higher_classification_disagreement')
    if conflicts:
        reasons.append('contradictory_reviewed_decision')
    return {'reasons': sorted(set(reasons)), 'warnings': sorted(set(warnings)),
            'classification_disagreements': classification, 'authorship_diagnostics': authors,
            'reviewed_decision_conflicts': sorted(set(conflicts))}


def project(*, records, taxa, bindings, vernaculars, reviews, mappings, supersessions, eligible_target_ids=None):
    """Return current-source-derived names and durable per-association evidence."""
    record_index = {usage_key(usage(r)): r for r in records}
    core_index = {(r.source_code, r.core_row_id_namespace, r.core_row_id_value): r for r in records}
    taxa_index = {t['sporely_taxon_id']: t for t in taxa}
    identity_before = identity_fingerprint(taxa, bindings)
    by_name = collections.defaultdict(list)
    targets_per_usage = collections.defaultdict(list)
    for t in taxa:
        targets_per_usage[usage_key(t['canonical_source_usage'])].append(t['sporely_taxon_id'])
        if t['canonical_source_code'] == 'col_xr' and norm(t['taxonomic_status']) in _ACCEPTED_STATUSES:
            by_name[(norm(t['scientific_name']), norm(t['rank']))].append(t)
    bound = {(u['source_code'], u['core_row_id']['namespace'], u['core_row_id']['value']): u['sporely_taxon_id']
             for u in bindings if u.get('core_row_id')}
    review_index = {usage_key(r['source_evidence']['source_usage']): r for r in reviews['reviews']}
    context = {'by_usage': {usage_key(u['source_usage']): u['sporely_taxon_id'] for u in bindings},
               'mappings': collections.defaultdict(list), 'supersessions': collections.defaultdict(list)}
    for mapping in mappings:
        context['mappings'][mapping.source_usage].append(mapping)
    for decision in supersessions:
        context['supersessions'][decision.superseded_sporely_taxon_id].append(decision)
    validations = []
    valid_reviews = {}
    for review in reviews['reviews']:
        source = record_index.get(usage_key(review['source_evidence']['source_usage']))
        target = taxa_index.get(review['target_evidence']['sporely_taxon_id'])
        target_record = record_index.get(usage_key(target['canonical_source_usage'])) if target else None
        reasons = []
        if source is None:
            reasons.append('source_concept_removed')
        elif semantic_evidence(source) != review['source_evidence']:
            reasons.append('source_concept_evidence_changed')
        if target is None or target_record is None:
            reasons.append('target_concept_removed')
        elif target_evidence(target, target_record) != review['target_evidence']:
            reasons.append('target_concept_evidence_changed')
        elif targets_per_usage[usage_key(target['canonical_source_usage'])] != [target['sporely_taxon_id']]:
            reasons.append('target_not_uniquely_reconstructable')
        if source and target and _conflicts(source, target['sporely_taxon_id'], mappings, supersessions, bindings, context):
            reasons.append('contradictory_reviewed_decision')
        for constraint in reviews.get('semantic_constraints', []):
            ref = constraint['source_taxon_identifier']
            source_ref = review['source_evidence']['source_usage']
            if (source_ref['source'] == 'nortaxa' and source_ref['namespace'] == ref['namespace']
                    and source_ref['identifier'] == ref['value']
                    and review['target_evidence']['sporely_taxon_id'] == constraint['strict_target_sporely_taxon_id']):
                reasons.append('collective_to_strict_association_forbidden')
        # Never resolve a missing/replaced reviewed target by its name.
        status = 'invalidated' if reasons else 'reused'
        validations.append({'review_id': review['review_id'], 'status': status, 'reasons': reasons,
                            'source_usage': review['source_evidence']['source_usage'],
                            'reviewed_target_sporely_taxon_id': review['target_evidence']['sporely_taxon_id']})
        if not reasons:
            valid_reviews[usage_key(review['source_evidence']['source_usage'])] = review
    evidence = []
    automatic_sources_per_target = collections.defaultdict(set)
    projected = [dict(v) for v in vernaculars]
    # Source-native COL copies must not suppress independently derivable
    # national associations or their preferred status/provenance. SQLite uses
    # the existing exact-key dedup rule with national-source precedence.
    seen = {(v['sporely_taxon_id'], v['language'], v['vernacular_name'])
            for v in projected if v['source_code'] != 'col_xr'}
    preferred = {(v['sporely_taxon_id'], v['language']) for v in projected if v['is_preferred'] and v['source_code'] != 'col_xr'}
    def base(source, v, tid, evidence_class, assessment=None):
        t = taxa_index[tid]
        r = record_index[usage_key(t['canonical_source_usage'])]
        return {'evidence_class': evidence_class, 'rule': RULE if evidence_class == AUTOMATIC else None,
                'source_evidence': semantic_evidence(source), 'source_authorship': source.authorship,
                'source_release': source.source_release, 'source_provenance': source.raw.get('provenance', {}),
                'vernacular_provenance': v.get('provenance', {}),
                'target_evidence': target_evidence(t, r), 'target_authorship': r.authorship,
                'target_sporely_taxon_id': tid, 'language': v['language'], 'vernacular_name': v['vernacular_name'],
                'is_preferred': bool(v['is_preferred']), 'identity_effect': 'none',
                'external_identifier_emission': False, **(assessment or {})}
    # Every original projection must resolve to current pinned source evidence.
    original_evidence = {}
    for v in projected:
        source = core_index[(v['source_code'], v['core_row_id']['namespace'], v['core_row_id']['value'])]
        tid = v['sporely_taxon_id']
        for constraint in reviews.get('semantic_constraints', []):
            ref = constraint['source_taxon_identifier']
            if (source.source_code == 'nortaxa' and source.taxon_id_namespace == ref['namespace']
                    and source.taxon_id_value == ref['value']
                    and tid == constraint['strict_target_sporely_taxon_id']):
                raise ProjectionError('collective source-native metadata cannot project onto strict target')
        native = taxa_index[tid]['canonical_source_code'] != 'col_xr'
        e = base(source, v, tid, NATIVE if native else 'current_source_vernacular_projection')
        e['qualified_source_native'] = bool(annotation(source)) if native else False
        e['projection_status'] = 'existing_current_source_projection'
        evidence.append(e)
        v['association_evidence_sha256'] = fingerprint(e)
        original_evidence[id(v)] = e
    # Aggregated COL vernaculars may echo the same national source; they must
    # not override that source's explicit concept qualifier. Keyed by
    # (target, language, normalized name); an owner review lifts the guard.
    guarded = collections.defaultdict(list)
    # Current source vernacular entries, including source-native qualified
    # concepts, remain intact. Only the additional association can fail.
    input_entries = collections.defaultdict(list)
    for v in vernaculars:
        input_entries[(v['source_code'], v['core_row_id']['namespace'], v['core_row_id']['value'])].append(v)
    for core, entries in sorted(input_entries.items()):
        source = core_index[core]
        if source.source_code != 'nortaxa':
            continue
        candidates = sorted(by_name[(norm(source.scientific_name), norm(source.rank))], key=lambda t:t['sporely_taxon_id'])
        assessment = automatic_assessment(source, candidates, record_index, mappings, supersessions, bindings, context)
        review = review_index.get(usage_key(usage(source)))
        selected_review = valid_reviews.get(usage_key(usage(source)))
        # A review is a concept decision (current names stay derivable), limited
        # to the languages it lists.
        approved = ({lang for lang, _ in selected_review['vernacular_names']}
                    if selected_review else set())
        if QUALIFIED_REASON in assessment['reasons']:
            for v in entries:
                if v['language'] in ('nb', 'nn', 'no') and v['language'] not in approved:
                    for t in candidates:
                        guarded[(t['sporely_taxon_id'], v['language'], norm(v['vernacular_name']))].append(
                            (source, v, assessment))
        for v in entries:
            if v['language'] not in ('nb', 'nn', 'no'):
                continue
            tid = None
            evidence_class = AUTOMATIC
            if review:
                if selected_review and v['language'] in approved:
                    tid = selected_review['target_evidence']['sporely_taxon_id']
                    evidence_class = REVIEWED
            elif not assessment['reasons']:
                tid = candidates[0]['sporely_taxon_id']
            if tid is None:
                # A valid native collective concept is not failed enrichment.
                native_id = bound.get(core)
                if native_id in taxa_index and taxa_index[native_id]['canonical_source_code'] != 'col_xr' and annotation(source):
                    continue
                evidence.append({'evidence_class': 'withheld_vernacular_association',
                                 'source_evidence': semantic_evidence(source), 'source_release': source.source_release,
                                 'language': v['language'], 'vernacular_name': v['vernacular_name'],
                                 'full_bundle_target_ids': [t['sporely_taxon_id'] for t in candidates],
                                 **assessment, 'projection_status': 'withheld'})
                continue
            if eligible_target_ids is not None and tid not in eligible_target_ids:
                continue
            key = tid, v['language'], v['vernacular_name']
            if key in seen:
                continue
            e = base(source, v, tid, evidence_class, assessment)
            e['full_bundle_target_ids'] = [t['sporely_taxon_id'] for t in candidates]
            e['projection_status'] = 'added'
            e['is_preferred'] = bool(v['is_preferred']) and (tid, v['language']) not in preferred
            if evidence_class == AUTOMATIC and len(e['full_bundle_target_ids']) != 1:
                raise ProjectionError('automatic target is not full-universe unique')
            if evidence_class == AUTOMATIC:
                automatic_sources_per_target[tid].add(usage_key(usage(source)))
            if selected_review:
                e['review_id'] = selected_review['review_id']
                e['review_evidence_sha256'] = fingerprint(selected_review)
            evidence.append(e)
            projected.append({**v, 'sporely_taxon_id': tid, 'is_preferred': e['is_preferred'],
                              'association_evidence_sha256': fingerprint(e)})
            seen.add(key)
    if guarded:
        kept, removed = [], set()
        for v in projected:
            key = (v['sporely_taxon_id'], v['language'], norm(v['vernacular_name']))
            if v['source_code'] != 'col_xr' or key not in guarded or id(v) not in original_evidence:
                kept.append(v)
                continue
            col_e = original_evidence[id(v)]
            removed.add(id(col_e))
            evidence.append({
                'evidence_class': COL_ECHO_WITHHELD, 'projection_status': 'withheld',
                'reasons': [COL_ECHO_REASON], 'target_sporely_taxon_id': v['sporely_taxon_id'],
                'language': v['language'], 'vernacular_name': v['vernacular_name'],
                'source_evidence': col_e['source_evidence'], 'col_evidence': col_e,
                'national_withheld_associations': sorted((
                    {'source_evidence': semantic_evidence(s), 'source_authorship': s.authorship,
                     'source_release': s.source_release, 'language': nv['language'],
                     'vernacular_name': nv['vernacular_name'], 'national_reasons': a['reasons']}
                    for s, nv, a in guarded[key]), key=canonical),
                'identity_effect': 'none', 'external_identifier_emission': False})
        projected = kept
        evidence = [e for e in evidence if id(e) not in removed]
    evidence.sort(key=canonical)
    validations.sort(key=lambda r:r['review_id'])
    if any(len(sources) != 1 for sources in automatic_sources_per_target.values()):
        raise ProjectionError('competing automatic source concepts require review')
    if identity_before != identity_fingerprint(taxa, bindings):
        raise ProjectionError('vernacular projection mutated identity artifacts')
    return projected, evidence, validations


def changes(current, previous, validations):
    """Previous evidence is comparison input only, never projection input."""
    def keyed(rows):
        return {(r['target_sporely_taxon_id'], r['language'], r['vernacular_name']):r
                for r in rows if r.get('projection_status') != 'withheld' and 'target_sporely_taxon_id' in r}
    now, old = keyed(current), keyed(previous)
    def ref(row):
        return {k:row[k] for k in ('target_sporely_taxon_id','language','vernacular_name','evidence_class')}
    report = {'additions':[ref(now[k]) for k in sorted(now.keys()-old.keys())],
              'removals':[ref(old[k]) for k in sorted(old.keys()-now.keys())],
              'changed_targets':[], 'preferred_name_status_changes':[],
              'movements_between_strict_and_qualified_concepts':[],
              'reviewed_approvals_reused':[v for v in validations if v['status']=='reused'],
              'reviewed_approvals_invalidated':[v for v in validations if v['status']=='invalidated']}
    for key in sorted(now.keys() & old.keys()):
        if now[key]['is_preferred'] != old[key]['is_preferred']:
            report['preferred_name_status_changes'].append({'row':ref(now[key]),'before':old[key]['is_preferred'],'after':now[key]['is_preferred']})
    def group(rows):
        groups=collections.defaultdict(list)
        for r in rows.values():
            groups[(usage_key(r['source_evidence']['source_usage']),r['language'],r['vernacular_name'])].append(r)
        return groups
    ng, og = group(now), group(old)
    for key in sorted(ng.keys() & og.keys()):
        a,b=og[key],ng[key]
        before=sorted({r['target_sporely_taxon_id'] for r in a})
        after=sorted({r['target_sporely_taxon_id'] for r in b})
        change={'source_usage':a[0]['source_evidence']['source_usage'],'language':key[1],'vernacular_name':key[2], 'before':before,'after':after}
        if before!=after:
            report['changed_targets'].append(change)
        strict_before={(bool(r['source_evidence']['concept_annotation']), bool(r['target_evidence']['concept_annotation'])) for r in a}
        strict_after={(bool(r['source_evidence']['concept_annotation']), bool(r['target_evidence']['concept_annotation'])) for r in b}
        if strict_before!=strict_after:
            report['movements_between_strict_and_qualified_concepts'].append(change)
    source_now={usage_key(r['source_evidence']['source_usage']) for r in current}
    source_old={usage_key(r['source_evidence']['source_usage']) for r in previous}
    target_now={r['target_sporely_taxon_id'] for r in current if 'target_sporely_taxon_id' in r}
    target_old={r['target_sporely_taxon_id'] for r in previous if 'target_sporely_taxon_id' in r}
    # Callers replace these with complete current concept inventories, so a
    # concept losing its final vernacular is not mistaken for removal.
    report['source_concepts_removed']=[list(k) for k in sorted(source_old-source_now)]
    report['target_concepts_removed']=sorted(target_old-target_now)
    return report

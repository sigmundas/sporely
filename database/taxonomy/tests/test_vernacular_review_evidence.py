"""Safety checks for the evidence-only owner review simulation."""
import collections
import copy
import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / 'evidence/taxonomy-v3/audit_norwegian_vernacular_enrichment.py'
SPEC = importlib.util.spec_from_file_location('vernacular_review_audit', SCRIPT)
subject = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(subject)


@pytest.fixture
def inputs():
    identifier = {'namespace': 'nortaxa_dwc_id', 'value': 'source-1'}
    release = {'version': '1.284', 'issued_date': '2026-07-17'}
    review = {
        'review_id': 'review-1', 'review_status': 'approved_for_vernacular_only',
        'identity_effect': 'none', 'external_identifier_emission': False,
        'target_sporely_taxon_id': 10, 'target_col_usage_id': 'COL-1',
        'target_canonical_scientific_name': 'Example species', 'target_authorship': 'Author',
        'source_record_identifier': identifier, 'source_taxon_identifier': identifier,
        'source_release': release, 'source_scientific_name': 'Example species',
        'source_status': 'valid', 'source_authorship': 'Author sensu auct.',
        'source_nomenclatural_status': 'illegitimate',
        'vernacular_names': [['nb', 'reviewed name']],
        'rule_evidence_class': 'owner_reviewed_vernacular_only_target_selection',
        'reviewer': 'Owner', 'rationale': 'Owner approval', 'evidence_references': ['approval'],
    }
    row = {
        'source_record_identifier': identifier, 'source_taxon_identifier': identifier,
        'pinned_source_release': release, 'source_scientific_name': 'Example species',
        'source_rank': 'species', 'source_classification': {'kingdom': 'Fungi'},
        'source_status': 'valid', 'source_authorship': 'Author sensu auct.',
        'source_nomenclatural_status': 'illegitimate', 'source_provenance': {'row_index': 1},
        'reviewed_decision_conflicts': [],
        'authorship_diagnostics': [{'target_sporely_taxon_id': 10, 'target_authorship': 'Author'}],
        'disposition': 'exception', 'rule_evidence_class': 'conservative_v2',
        'reasons': ['qualified_or_aggregate_source', 'source_nomenclatural_status_ambiguous'],
        'language': 'nb', 'vernacular_name': 'reviewed name', 'is_preferred': True,
        'existing_bound_sporely_taxon_id': 20,
    }
    document = {'pins': {'content_release_id': 'release-1', 'nortaxa_archive_sha256': 'no-hash', 'col_archive_sha256': 'col-hash'}, 'reviews': [review]}
    taxa = {10: {'canonical_source_system': 'col_xr', 'canonical_external_id': 'COL-1', 'canonical_scientific_name': 'Example species', 'taxon_rank': 'species'}}
    meta = {'content_release_id': 'release-1', 'source_release[nortaxa].archive_sha256': 'no-hash', 'source_release[col_xr].archive_sha256': 'col-hash'}
    return document, [row], taxa, collections.defaultdict(set), collections.defaultdict(set), meta


def test_review_preserves_source_identity_provenance_and_existing_preferred(inputs):
    document, rows, taxa, names, preferred, meta = inputs
    before = copy.deepcopy(rows[0])
    preferred[10].add(('nb', 'existing preferred name'))
    reviewed, unresolved, collective = subject.apply_owner_reviews(*inputs)
    assert reviewed == rows and not unresolved and not collective
    for field in ('source_status', 'source_authorship', 'source_nomenclatural_status', 'source_provenance', 'existing_bound_sporely_taxon_id'):
        assert rows[0][field] == before[field]
    assert rows[0]['automatic_exception_reasons'] == before['reasons']
    assert rows[0]['target_sporely_taxon_id'] == 10
    assert rows[0]['projected_is_preferred'] is False
    assert preferred[10] == {('nb', 'existing preferred name')}


@pytest.mark.parametrize('failure', ['wrong_col_id', 'duplicate_col_id', 'missing_target'])
def test_non_reconstructable_target_is_withheld(inputs, failure):
    document, rows, taxa, names, preferred, meta = inputs
    before = copy.deepcopy(rows)
    if failure == 'wrong_col_id':
        document['reviews'][0]['target_col_usage_id'] = 'wrong'
    elif failure == 'duplicate_col_id':
        taxa[11] = copy.deepcopy(taxa[10])
    else:
        taxa.clear()
    reviewed, unresolved, collective = subject.apply_owner_reviews(*inputs)
    assert not reviewed and len(unresolved) == 1
    assert unresolved[0]['reason'] == 'target_not_uniquely_reconstructable'
    assert rows == before


def test_contradictory_reviewed_identity_decision_cannot_be_bypassed(inputs):
    inputs[1][0]['reviewed_decision_conflicts'] = [{'decision_id': 'rejection'}]
    with pytest.raises(AssertionError, match='cannot silently override'):
        subject.apply_owner_reviews(*inputs)


def test_entoloma_collective_source_cannot_be_routed_to_strict_species(inputs):
    inputs[1][0]['source_record_identifier']['value'] = '53666'
    with pytest.raises(AssertionError, match='Collective Entoloma'):
        subject.apply_owner_reviews(*inputs)

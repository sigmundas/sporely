"""Concept-change and projection boundaries for Norwegian metadata."""
import copy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import vernacular_projection as v


def record(source, identifier, name='Example species', author='Author', **kwargs):
    return SimpleNamespace(source_code=source, taxon_id_namespace=source+'_taxon_id',
        taxon_id_value=identifier, core_row_id_namespace=source+'_dwc_id', core_row_id_value=identifier,
        scientific_name=name, authorship=author, rank=kwargs.get('rank','species'),
        taxonomic_status=kwargs.get('status','accepted'), accepted_name_usage_id=None,
        classification={'kingdom':'Fungi',**kwargs.get('classification',{})},
        kingdom=lambda:'Fungi', source_release={'version':'1'},
        raw={'nomenclatural_status':kwargs.get('nomen',''),'concept_annotation':{}})


def inputs(qualified=False):
    s=record('nortaxa','NO',author='Author sensu auct.' if qualified else 'Author')
    t=record('col_xr','COL')
    native={'sporely_taxon_id':20,'canonical_source_code':'nortaxa','canonical_source_usage':v.usage(s), 'scientific_name':s.scientific_name,'rank':s.rank,'taxonomic_status':'accepted'}
    target={'sporely_taxon_id':10,'canonical_source_code':'col_xr','canonical_source_usage':v.usage(t), 'scientific_name':t.scientific_name,'rank':t.rank,'taxonomic_status':'accepted'}
    binding={'source_code':'nortaxa','core_row_id':{'namespace':s.core_row_id_namespace,'value':'NO'},'source_usage':v.usage(s),'sporely_taxon_id':20}
    name={'source_code':'nortaxa','core_row_id':binding['core_row_id'],'sporely_taxon_id':20,'source_release':s.source_release,'language':'nb','vernacular_name':'example','is_preferred':True}
    return dict(records=[s,t],taxa=[native,target],bindings=[binding],vernaculars=[name],reviews={'reviews':[]},mappings=[],supersessions=[])


def approve(i):
    s,t=i['records']
    se,te=v.semantic_evidence(s),v.target_evidence(i['taxa'][1],t)
    review={'review_id':'review','evidence_class':v.REVIEWED,'source_evidence':se,'target_evidence':te,'vernacular_names':[['nb','example']]}
    i['reviews']={'reviews':[review]}


def test_full_universe_duplicate_blocks_automatic():
    i=inputs(); other=copy.deepcopy(i['taxa'][1]);other['sporely_taxon_id']=11
    i['taxa'].append(other)
    projected,evidence,_=v.project(**i)
    assert len(projected)==1
    assert not any(e['evidence_class']==v.AUTOMATIC for e in evidence)
    # A scoped-out homonym still makes the full universe ambiguous.
    projected,_,_=v.project(**i,eligible_target_ids={10})
    assert len(projected)==1


def test_qualified_native_is_valid_without_col_mapping():
    i=inputs(qualified=True);i['taxa']=i['taxa'][:1]
    projected,evidence,_=v.project(**i)
    assert len(projected)==1
    assert {e['evidence_class'] for e in evidence}=={v.NATIVE}
    assert evidence[0]['qualified_source_native']


def test_review_can_override_qualifier_but_does_not_mutate_identity():
    i=inputs(qualified=True);approve(i);before=copy.deepcopy((i['taxa'],i['bindings']))
    projected,evidence,validations=v.project(**i)
    assert len(projected)==2 and validations[0]['status']=='reused'
    assert any(e['evidence_class']==v.REVIEWED and e['source_evidence']['concept_annotation'] for e in evidence)
    assert (i['taxa'],i['bindings'])==before


@pytest.mark.parametrize('change',['source_qualifier','source_status','nomenclatural','replacement_target','target_name','target_rank'])
def test_changed_concept_invalidates_review_without_name_transfer(change):
    i=inputs(qualified=True);approve(i)
    if change=='source_qualifier':i['records'][0].authorship='Author aff.'
    elif change=='source_status':i['records'][0].taxonomic_status='synonym'
    elif change=='nomenclatural':i['records'][0].raw['nomenclatural_status']='illegitimate'
    elif change=='replacement_target':i['taxa'][1]['sporely_taxon_id']=11
    elif change=='target_name':i['records'][1].scientific_name='Other species'
    else:i['records'][1].rank='aggregate'
    projected,evidence,validations=v.project(**i)
    assert len(projected)==1 and validations[0]['status']=='invalidated'
    assert not any(e['evidence_class']==v.REVIEWED for e in evidence)


def test_approval_is_concept_decision_new_current_name_is_derivable():
    i=inputs(qualified=True);approve(i)
    i['vernaculars'][0]['vernacular_name']='current renamed vernacular'
    projected,_,validations=v.project(**i)
    assert validations[0]['status']=='reused'
    assert {(r['sporely_taxon_id'],r['vernacular_name']) for r in projected}=={(20,'current renamed vernacular'),(10,'current renamed vernacular')}


def test_preferred_name_survives_additional_same_language_name():
    i=inputs();i['vernaculars'].append({**i['vernaculars'][0],'sporely_taxon_id':10,'vernacular_name':'existing preferred'})
    projected,_,_=v.project(**i)
    assert any(r['sporely_taxon_id']==10 and r['vernacular_name']=='example' and not r['is_preferred'] for r in projected)


def test_previous_evidence_reports_removal_and_never_projects_old_name():
    i=inputs();_,old,_=v.project(**i)
    i['vernaculars']=[]
    projected,current,validations=v.project(**i)
    assert projected==[]
    report=v.changes(current,old,validations)
    assert len(report['removals'])==2 and report['additions']==[]
    assert set(report)>={'changed_targets','reviewed_approvals_reused','reviewed_approvals_invalidated','source_concepts_removed','target_concepts_removed','preferred_name_status_changes','movements_between_strict_and_qualified_concepts'}


def test_coll_and_aff_are_qualified_without_stripping_name():
    for name in ['Entoloma sericellum coll.','Example species aff.','Example species aggregate']:
        s=record('nortaxa','NO',name=name)
        assert v.semantic_evidence(s)['scientific_name']==name
        assert v.annotation(s)
    assert not v.annotation(record('nortaxa','NO',name='Collybia tuberosa'))


def test_collective_owner_approval_cannot_override_strict_constraint():
    i=inputs(qualified=True);approve(i)
    i['reviews']['semantic_constraints']=[{'source_taxon_identifier':{'namespace':'nortaxa_taxon_id','value':'NO'},'strict_target_sporely_taxon_id':10}]
    projected,_,validations=v.project(**i)
    assert len(projected)==1
    assert validations[0]['reasons']==['collective_to_strict_association_forbidden']


def test_review_requires_unique_canonical_usage_reconstruction():
    i=inputs(qualified=True);approve(i)
    other=copy.deepcopy(i['taxa'][1]);other['sporely_taxon_id']=11
    i['taxa'].append(other)
    projected,_,validations=v.project(**i)
    assert len(projected)==1
    assert validations[0]['reasons']==['target_not_uniquely_reconstructable']


def test_review_ledger_detects_tampered_evidence(tmp_path):
    i=inputs(qualified=True);approve(i)
    review=i['reviews']['reviews'][0]
    review.update(reviewer='Owner',rationale='review',evidence_references=['evidence'],identity_effect='none',external_identifier_emission=False)
    for key in ('source_evidence','target_evidence'):
        review[key+'_sha256']=v.fingerprint(review[key])
    path=tmp_path/'reviews.json'
    import json
    path.write_text(json.dumps({'format':'sporely-vernacular-concept-reviews-v1','reviews':[review]}))
    assert len(v.read_reviews(path)['reviews'])==1
    review['target_evidence']['sporely_taxon_id']=11
    path.write_text(json.dumps({'format':'sporely-vernacular-concept-reviews-v1','reviews':[review]}))
    with pytest.raises(v.ProjectionError,match='fingerprint'):
        v.read_reviews(path)


def test_current_col_copy_does_not_suppress_norwegian_association():
    i=inputs()
    t=i['records'][1]
    i['vernaculars'].append({**i['vernaculars'][0], 'source_code':'col_xr',
        'core_row_id':{'namespace':t.core_row_id_namespace,'value':t.core_row_id_value},
        'sporely_taxon_id':10,'is_preferred':False})
    projected,evidence,_=v.project(**i)
    assert any(e['evidence_class']==v.AUTOMATIC and e['projection_status']=='added' for e in evidence)
    assert any(r['source_code']=='nortaxa' and r['sporely_taxon_id']==10 and r['is_preferred'] for r in projected)


def _col_copy(i, name='Example', language='nb'):
    t=i['records'][1]
    i['vernaculars'].append({**i['vernaculars'][0], 'source_code':'col_xr', 'language':language,
        'vernacular_name':name, 'core_row_id':{'namespace':t.core_row_id_namespace,'value':t.core_row_id_value},
        'sporely_taxon_id':10,'is_preferred':False})


def test_col_copy_of_a_qualified_national_name_is_withheld_from_the_strict_target():
    i=inputs(qualified=True);_col_copy(i, name='  EXAMPLE ')
    projected,evidence,_=v.project(**i)
    assert not any(r['source_code']=='col_xr' for r in projected)
    assert [r['sporely_taxon_id'] for r in projected]==[20]
    [w]=[e for e in evidence if e['evidence_class']==v.COL_ECHO_WITHHELD]
    assert w['projection_status']=='withheld' and w['target_sporely_taxon_id']==10
    assert w['reasons']==[v.COL_ECHO_REASON]
    assert w['national_withheld_associations'][0]['national_reasons']==['qualified_or_aggregate_source']
    assert w['col_evidence']['source_evidence']['source_usage']['source']=='col_xr'
    assert not any(e.get('projection_status')=='existing_current_source_projection'
                   and e['source_evidence']['source_usage']['source']=='col_xr' for e in evidence)


def test_col_guard_ignores_other_names_languages_and_unqualified_sources():
    i=inputs(qualified=True);_col_copy(i, name='other name');_col_copy(i, language='sv')
    projected,_,_=v.project(**i)
    assert sum(r['source_code']=='col_xr' for r in projected)==2
    i=inputs();_col_copy(i)
    projected,evidence,_=v.project(**i)
    assert any(r['source_code']=='col_xr' for r in projected)
    assert not any(e['evidence_class']==v.COL_ECHO_WITHHELD for e in evidence)


def test_col_guard_ignores_non_qualified_withholding_reasons():
    i=inputs();i['records'][0].raw['nomenclatural_status']='invalid';_col_copy(i)
    projected,evidence,_=v.project(**i)
    assert any(e.get('reasons') and 'source_nomenclatural_status_ambiguous' in e['reasons'] for e in evidence)
    assert any(r['source_code']=='col_xr' for r in projected)


def test_owner_review_lifts_the_col_guard():
    i=inputs(qualified=True);approve(i);_col_copy(i)
    projected,evidence,_=v.project(**i)
    assert any(r['source_code']=='col_xr' and r['sporely_taxon_id']==10 for r in projected)
    assert any(e['evidence_class']==v.REVIEWED and e['target_sporely_taxon_id']==10 for e in evidence)
    assert not any(e['evidence_class']==v.COL_ECHO_WITHHELD for e in evidence)


@pytest.mark.parametrize('text', ['(Ach.) S. L. F. Meyer', 'Olariaga, Salcedo, P.P. Daniëls & Kautman',
                                  '(Pers.) L.W. Zhou & S.L. Liu'])
def test_author_initials_are_not_qualifiers(text):
    assert not v.QUALIFIED.search(text)


@pytest.mark.parametrize('text', ['(Leers) Choisy p.p., nom.confus.', 'Berk. - sensu lato', 's. auct. scand.',
                                  '(Fr. : Fr.) P. Kumm. agg.', 'Fr. s.l.', 'Fr. p.p.', 'Fr. s. l.', 'sensu Christan (2008)'])
def test_qualifier_tokens_still_match(text):
    assert v.QUALIFIED.search(text)


def test_review_approves_only_the_listed_languages_and_guard_keeps_the_rest():
    i=inputs(qualified=True);approve(i)
    i['vernaculars'].append({**i['vernaculars'][0],'language':'nn','vernacular_name':'eksempel'})
    _col_copy(i, name='eksempel', language='nn')
    projected,evidence,_=v.project(**i)
    assert {(r['language'],r['vernacular_name']) for r in projected if r['sporely_taxon_id']==10}=={('nb','example')}
    assert [e['vernacular_name'] for e in evidence if e['evidence_class']==v.COL_ECHO_WITHHELD]==['eksempel']

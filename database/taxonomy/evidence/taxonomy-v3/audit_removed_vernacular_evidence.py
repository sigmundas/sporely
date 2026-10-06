#!/usr/bin/env python3
"""Read-only per-row evidence census; no identity inference or restoration."""
import argparse
import collections
import csv
import gzip
import hashlib
import io
import json
import sqlite3
import unicodedata
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
TAX = REPO / 'database/taxonomy'
LANG = {'eng':'en','deu':'de','ger':'de','fra':'fr','fre':'fr','spa':'es','dan':'da','swe':'sv','fin':'fi','pol':'pl','por':'pt','ita':'it','nob':'nb','nno':'nn','nor':'no'}


def normalized(value):
    return ' '.join(unicodedata.normalize('NFC', str(value)).casefold().split())


def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()


def lines(path):
    with path.open() as f:
        for l in f:
            if l.strip():yield json.loads(l)


def archive_rows(path, member):
    with zipfile.ZipFile(path) as z,z.open(member) as f:
        reader=csv.DictReader(io.TextIOWrapper(f,encoding='utf-8-sig'),delimiter='\t',quoting=csv.QUOTE_NONE)
        for index,row in enumerate(reader):yield index,row


def audit(output, candidate_base=Path("/tmp/sporely-vernacular-stage1"), expected_removed=19505):
    csv.field_size_limit(10 * 1024 * 1024)
    output.mkdir(parents=True,exist_ok=True)
    base=candidate_base
    old_path=Path('/tmp/sporely-no-coverage-bundle.sqlite3')
    new_path=base/'frozenA.sqlite3'
    old=sqlite3.connect(f'file:{old_path}?mode=ro',uri=True);old.row_factory=sqlite3.Row
    new=sqlite3.connect(f'file:{new_path}?mode=ro',uri=True);new.row_factory=sqlite3.Row
    old.execute('pragma query_only=on');new.execute('pragma query_only=on')
    new_names={tuple(r) for r in new.execute('select taxon_id,language_code,vernacular_name from vernacular_min')}
    new_normalized=collections.defaultdict(list)
    for tid,lang,name in new_names:new_normalized[(tid,lang,normalized(name))].append(name)
    removed=[dict(r) for r in old.execute('select * from vernacular_min order by taxon_id,language_code,vernacular_name') if (r['taxon_id'],r['language_code'],r['vernacular_name']) not in new_names]
    assert len(removed)==expected_removed
    target_ids={r['taxon_id'] for r in removed}
    targets={r['taxon_id']:dict(r) for r in old.execute('select * from taxon_min') if r['taxon_id'] in target_ids}
    current_ids={r[0] for r in new.execute('select taxon_id from taxon_min')}
    wanted={(r['language_code'],normalized(r['vernacular_name'])) for r in removed}
    usages=list(lines(base/'frozenA/source_usages.jsonl'))
    binding={}
    source_names=collections.defaultdict(set)
    core_binding={}
    for u in usages:
        ref=u['source_usage'];key=(ref['source'],ref['namespace'],ref['identifier'])
        assert key not in binding or binding[key]==u['sporely_taxon_id']
        binding[key]=u['sporely_taxon_id']
        source_names[u['sporely_taxon_id']].add(normalized(u['scientific_name']))
        if u.get('core_row_id'):
            core=u['core_row_id'];core_binding[(u['source_code'],core['namespace'],core['value'])]=u['sporely_taxon_id']
    external=collections.defaultdict(set)
    for table in ['taxon_external_id_min','taxon_external_id_text_min']:
        for r in old.execute(f'select taxon_id,source_system,external_id from {table}'):
            if r['taxon_id'] in target_ids:external[(r['taxon_id'],r['source_system'])].add(str(r['external_id']))
    paths={
        'col_xr':TAX/'sources/col_xr/2026-07-17-XR/archive.zip',
        'nortaxa':TAX/'sources/nortaxa/1.284/archive.zip',
        'dyntaxa':TAX/'sources/dyntaxa/2026-09-30/archive.zip',
    }
    raw_matches=collections.defaultdict(list)
    evidence_source_ids=collections.defaultdict(set)
    print('Scanning pinned vernacular source records',flush=True)
    for source,member in [('col_xr','VernacularName.tsv'),('nortaxa','vernacularname.txt'),('dyntaxa','VernacularName.csv')]:
        for index,row in archive_rows(paths[source],member):
            name=row.get('col:name',row.get('vernacularName',''))
            raw_lang=row.get('col:language',row.get('language',''))
            lang=LANG.get(raw_lang,raw_lang)
            if (lang,normalized(name)) not in wanted:continue
            identifier=row.get('col:taxonID',row.get('taxonId',row.get('id','')))
            evidence_source_ids[source].add(identifier)
            ns={'col_xr':'col_usage_id','nortaxa':'nortaxa_dwc_id','dyntaxa':'dyntaxa_dwc_id'}[source]
            tid=binding.get((source,ns,identifier)) if source=='col_xr' else core_binding.get((source,ns,identifier))
            raw_matches[(lang,normalized(name))].append({'source':source,'member':member,'row_index':index,'source_usage_id':identifier,'source_namespace':ns,'vernacular_name':name,'raw_language':raw_lang,'language':lang,'bound_sporely_taxon_id':tid,'source_provider':row.get('col:sourceID',row.get('source','')),'reference_id':row.get('col:referenceID',''),'remarks':row.get('col:remarks',row.get('taxonRemarks','')),'preferred':row.get('col:preferred',row.get('isPreferredName',''))})
    # Read current source concepts for each supporting usage, independently of
    # normalization. Name equality is diagnostic and never binds a concept.
    concept_meta={}
    for source,normdir in [('nortaxa',Path('/tmp/sporely-vernacular-stage1-nortaxa-final')),('dyntaxa',Path('/tmp/sporely-6p/run1/norm/dyntaxa'))]:
        for row in lines(normdir/'taxa.jsonl'):
            if row['core_row_id']['value'] in evidence_source_ids[source]:
                concept_meta[(source,row['core_row_id']['value'])]={'scientific_name':row['scientific_name'],'authorship':row['authorship'],'rank':row['rank'],'status':row['taxonomic_status'],'accepted_usage':row.get('accepted_name_usage_id'),'classification':row.get('classification',{}),'source_taxon_id':row['taxon_id']}
    print('Scanning pinned COL usages for matching vernacular records',flush=True)
    for index,row in archive_rows(paths['col_xr'],'NameUsage.tsv'):
        identifier=row['col:ID']
        if identifier in evidence_source_ids['col_xr']:
            concept_meta[('col_xr',identifier)]={'scientific_name':row['col:scientificName'],'authorship':row['col:authorship'],'rank':row['col:rank'],'status':row['col:status'],'accepted_or_parent_usage_id':row['col:parentID'],'classification':{k:row.get('col:'+k,'') for k in ['kingdom','phylum','class','order','family','genus']},'nomenclatural_status':row['col:nameStatus'],'name_phrase':row['col:namePhrase'],'remarks':row['col:remarks'],'name_remarks':row['col:nameRemarks']}
    for records in raw_matches.values():
        for r in records:r['concept']=concept_meta.get((r['source'],r['source_usage_id']))
    # These snapshots are available locally but are NOT current recipe sources.
    # They cannot satisfy current evidence on their own.
    snapshot_paths=[REPO/'database/reference_data/generated'/name for name in ['vernacular_inat_11lang.csv','vernacular_inat.csv','artportalen_taxon_ids_by_genus.csv','artportalen_taxon_ids.csv','artportalen_taxon_ids_swedish_only_reconciled.csv']]
    snapshots=collections.defaultdict(list)
    for path in snapshot_paths:
        with path.open(encoding='utf-8-sig',newline='') as f:
            for index,row in enumerate(csv.DictReader(f)):
                if 'inaturalist' in path.name or path.name.startswith('vernacular_inat'):
                    for lang in ['en','de','fr','es','da','sv','no','fi','pl','pt','it']:
                        for name in row.get(lang,'').split(';'):
                            name=name.strip()
                            if (lang,normalized(name)) in wanted:
                                snapshots[(lang,normalized(name))].append({'path':str(path.relative_to(REPO)),'row_index':index,'provider':'inat_csv','provider_usage_id':row.get('inaturalist_taxon_id',''),'scientific_name':row.get('scientificName',''),'vernacular_name':name,'source_nortaxa_taxon_id':None,'fetched_at_utc':None})
                else:
                    name=row.get('swedish_name','')
                    if ('sv',normalized(name)) in wanted:
                        snapshots[('sv',normalized(name))].append({'path':str(path.relative_to(REPO)),'row_index':index,'provider':'artportalen','provider_usage_id':row.get('artportalen_taxon_id',''),'scientific_name':row.get('matched_scientific_name') or row.get('scientific_name',''),'vernacular_name':name,'source_nortaxa_taxon_id':row.get('adb_taxon_id') or row.get('norway_accepted_taxon_id') or row.get('norway_taxon_id'),'fetched_at_utc':row.get('fetched_at_utc',''),'match_status':row.get('match_status') or row.get('norway_match_status','')})
    legacy=list(lines(Path('/tmp/sporely-6p/run1/norm/legacy_enrichment.jsonl')))
    legacy_match=collections.defaultdict(list)
    for index,r in enumerate(legacy):
        if r['kind']!='vernacular':continue
        tid=binding.get(('nortaxa','nortaxa_taxon_id',r['nortaxa_taxon_id']))
        if tid in target_ids:
            legacy_match[(tid,r['language'],r['vernacular_name'])].append({'row_index':index,**r})
    results=[]
    for oldrow in removed:
        tid,lang,name=oldrow['taxon_id'],oldrow['language_code'],oldrow['vernacular_name']
        target=targets[tid];key=(lang,normalized(name))
        supporting=[];other=[]
        for raw in raw_matches[key]:
            match={**raw,'spelling_match':'exact' if raw['vernacular_name']==name else 'normalized_case_or_spacing'}
            if raw['bound_sporely_taxon_id']==tid and tid in current_ids:
                match['association_basis']='existing_current_source_usage_binding'
                supporting.append(match)
            else:
                match['scientific_name_matches_target']=bool(raw['concept']) and normalized(raw['concept']['scientific_name']) in source_names[tid]
                # Explicitly do not call the same name on another concept
                # current association evidence for this removed target.
                other.append(match)
        local=[]
        for snap in snapshots[key]:
            provider_id=snap['provider_usage_id']
            idmatch=provider_id in external[(tid,'inaturalist' if snap['provider']=='inat_csv' else 'artportalen')]
            no_id=snap['source_nortaxa_taxon_id']
            source_binding=binding.get(('nortaxa','nortaxa_taxon_id',no_id)) if no_id else None
            sci=normalized(snap['scientific_name']) in source_names[tid]
            if idmatch or source_binding==tid or sci:
                local.append({**snap,'existing_external_id_match':idmatch,'current_nortaxa_usage_binding_match':source_binding==tid,'scientific_name_match_diagnostic_only':sci,'recipe_pinned':False,'usable_for_current_projection':False})
        if supporting:
            category='current_evidence_exists_but_compiler_does_not_recover_it'
            reasons=sorted({'col_vernacular_member_not_normalized' if r['source']=='col_xr' else 'national_vernacular_projection_gap' for r in supporting})
            cleanup='missing_reconstructable_current_provenance'
        else:
            category='no_current_pinned_association_evidence_exists'
            cleanup='withheld_without_current_target_association_not_proven_obsolete'
            reasons=['no_current_pinned_source_to_target_association']
            if local:reasons.append('provider_snapshot_available_but_not_current_pinned_source')
            if any(x['scientific_name_matches_target'] for x in other):reasons.append('current_name_on_same_scientific_name_without_target_association')
            elif other:reasons.append('current_name_only_on_other_scientific_concepts')
            else:reasons.append('no_matching_vernacular_in_current_pinned_archives')
        historical=legacy_match[(tid,lang,name)]
        assert historical,'removed name lacks historical routing evidence'
        current_variants=sorted(new_normalized.get((tid,lang,normalized(name)),[]))
        evidence_status='current_pinned_association_evidence_exists' if supporting else 'no_current_pinned_association_evidence_exists'
        if current_variants:
            assert supporting, 'Retained current-source variant lacks raw source evidence'
            category='current_evidence_recovered_as_case_or_spacing_variant'
            cleanup='intentional_current_source_spelling_replacement'
            reasons=['historical_spelling_replaced_by_current_source_spelling']
        else:
            reasons=sorted(set('national_language_policy_exclusion' if x=='national_vernacular_projection_gap' and lang!='sv' else x for x in reasons))
        results.append({'target_sporely_taxon_id':tid,'language':lang,'vernacular_name':name,'historical_source':oldrow['source'],'historical_is_preferred':bool(oldrow['is_preferred_name']),'target_canonical_source':target['canonical_source_system'],'target_canonical_usage_id':target['canonical_external_id'],'target_scientific_name':target['canonical_scientific_name'],'target_rank':target['taxon_rank'],'current_target_exists':tid in current_ids,'evidence_status':evidence_status,'classification':category,'cleanup_assessment':cleanup,'reasons':reasons,'current_source_spelling_variants_retained':current_variants,'current_pinned_name_evidence_exists':bool(supporting or other),'current_pinned_target_association_exists':bool(supporting),'current_pinned_supporting_records':supporting,'current_pinned_other_concept_records':other,'available_unpinned_provider_snapshot_records':local,'historical_route_records':historical,'restored':False})
    def census(field):return dict(sorted(collections.Counter(r[field] for r in results).items()))
    breakdown=collections.Counter((r['historical_source'],r['language'],r['classification'],';'.join(r['reasons'])) for r in results)
    summary={'removed_rows':len(results),'by_evidence_status':census('evidence_status'),'by_classification':census('classification'),'by_cleanup_assessment':census('cleanup_assessment'),'by_historical_source':census('historical_source'),'by_language':census('language'),'source_language_reason_breakdown':[{'source':k[0],'language':k[1],'classification':k[2],'reason':k[3],'rows':n} for k,n in sorted(breakdown.items())], 'recoverable_by_current_provider':dict(collections.Counter(r['source'] for row in results for r in {x['source']:x for x in row['current_pinned_supporting_records']}.values())), 'rows_with_local_unpinned_snapshot':sum(bool(r['available_unpinned_provider_snapshot_records']) for r in results),'rows_with_other_concept_current_name':sum(bool(r['current_pinned_other_concept_records']) for r in results),'scope_of_negative_claim':'No current pinned source-to-this-concept association found in the three recipe archives/current bindings. This is not a claim of absence in live provider databases. Local unpinned snapshots remain historical candidates and require acquisition/revalidation.', 'restorations':0,'production_touched':False}
    unsupported=[r for r in results if not r['current_pinned_target_association_exists']]
    summary['no_target_association_detail']=dict(sorted(collections.Counter('same_scientific_name_without_target_association' if any(x['scientific_name_matches_target'] for x in r['current_pinned_other_concept_records']) else 'only_other_scientific_concepts' if r['current_pinned_other_concept_records'] else 'no_matching_current_name' for r in unsupported).items()))
    summary['no_target_association_snapshot_detail']=dict(sorted(collections.Counter('provider_id_or_source_binding' if any(x['existing_external_id_match'] or x['current_nortaxa_usage_binding_match'] for x in r['available_unpinned_provider_snapshot_records']) else 'scientific_name_only_diagnostic' for r in unsupported).items()))
    allpaths=[old_path,new_path,base/'frozenA/source_usages.jsonl',base/'frozenA/manifest.json',Path('/tmp/sporely-vernacular-stage1/registry-base.jsonl'),Path('/tmp/sporely-6p/run1/norm/legacy_enrichment.jsonl'),TAX/'release-recipe.json',*paths.values(),*snapshot_paths]
    summary['inputs_sha256']={str(p.relative_to(REPO)) if p.is_relative_to(REPO) else str(p):sha(p) for p in allpaths}
    outputfile=output/'removed-row-classification.jsonl.gz'
    with outputfile.open('wb') as raw,gzip.GzipFile(fileobj=raw,mode='wb',mtime=0,filename='') as f:
        for r in results:f.write((json.dumps(r,ensure_ascii=False,sort_keys=True)+'\n').encode())
    summary['classification_manifest_sha256']=sha(outputfile)
    summary['audit_script_sha256']=sha(Path(__file__))
    (output/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    for group,predicate in [('current-reconstructable',lambda r:r['classification']=='current_evidence_exists_but_compiler_does_not_recover_it'),('unpinned-snapshot-candidates',lambda r:not r['current_pinned_supporting_records'] and bool(r['available_unpinned_provider_snapshot_records']))]:
        (output/(group+'.json')).write_text(json.dumps([{'target_sporely_taxon_id':r['target_sporely_taxon_id'],'language':r['language'],'vernacular_name':r['vernacular_name'],'historical_source':r['historical_source'],'reasons':r['reasons']} for r in results if predicate(r)],ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    print(json.dumps({k:summary[k] for k in ['removed_rows','by_evidence_status','by_classification','by_historical_source','recoverable_by_current_provider','rows_with_local_unpinned_snapshot']},ensure_ascii=False,indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--candidate-base', type=Path, default=Path('/tmp/sporely-vernacular-stage1'))
    parser.add_argument('--expected-removed', type=int, default=19505)
    args=parser.parse_args()
    audit(args.output, args.candidate_base, args.expected_removed)

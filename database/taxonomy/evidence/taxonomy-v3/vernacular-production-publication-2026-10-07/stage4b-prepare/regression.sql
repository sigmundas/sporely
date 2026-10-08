BEGIN;
SET LOCAL statement_timeout='2min';
SET LOCAL plan_cache_mode=force_custom_plan;
CREATE TEMP TABLE function_before AS SELECT jsonb_build_object('owner',proowner,'acl',proacl::text,'config',proconfig,'oid',oid) value FROM pg_proc WHERE oid='public.taxonomy_v2_validate_release(text)'::regprocedure;
create function pg_temp.old_validate_release(p_release_id text)
returns jsonb
language plpgsql
stable
security definer
set search_path = public, pg_catalog
as $$
declare
  v_release public.taxonomy_v2_releases%rowtype;
  v_actual_counts jsonb;
  v_authoritative_counts jsonb;
  v_legacy_counts jsonb;
  v_dangling bigint;
  v_errors jsonb := '[]'::jsonb;
  v_search_definition text;
  v_resolver_definition text;
begin
  select * into v_release
  from public.taxonomy_v2_releases
  where release_id = p_release_id;

  if not found then
    return pg_catalog.jsonb_build_object(
      'ok', false, 'release_id', p_release_id, 'status', null,
      'expected_counts', null, 'actual_counts', null,
      'errors', pg_catalog.jsonb_build_array('release does not exist'));
  end if;

  if v_release.status not in ('ready', 'active') then
    v_errors := v_errors || pg_catalog.jsonb_build_array(
      pg_catalog.format('release status must be ready or active, got %s', v_release.status));
  end if;
  if v_release.taxonomy_schema_version <> 2 then
    v_errors := v_errors || ' ["taxonomy_schema_version must equal 2"]'::jsonb;
  end if;

  select pg_catalog.jsonb_build_object(
    'taxon.jsonl', (select count(*) from public.taxonomy_v2_taxa where release_id = p_release_id),
    'scientific_name.jsonl', (select count(*) from public.taxonomy_v2_scientific_names where release_id = p_release_id),
    'vernacular.jsonl', (select count(*) from public.taxonomy_v2_vernacular_names where release_id = p_release_id),
    'taxon_external_id.jsonl', (select count(*) from public.taxonomy_v2_external_ids where release_id = p_release_id),
    'taxon_external_id_legacy_integer.jsonl', (select count(*) from public.taxonomy_v2_legacy_external_ids where release_id = p_release_id),
    'taxon_redlist.jsonl', (select count(*) from public.taxonomy_v2_redlist where release_id = p_release_id)
  ) into v_actual_counts;

  if v_actual_counts <> v_release.row_counts then
    v_errors := v_errors || pg_catalog.jsonb_build_array('actual row counts do not match row_counts metadata');
  end if;

  select coalesce(pg_catalog.jsonb_object_agg(k, n), '{}'::jsonb)
  into v_authoritative_counts
  from (
    select source_system || '/' || namespace as k, count(*) as n
    from public.taxonomy_v2_external_ids
    where release_id = p_release_id
    group by source_system, namespace
  ) s;
  if v_authoritative_counts <> v_release.authoritative_namespace_counts then
    v_errors := v_errors || pg_catalog.jsonb_build_array('authoritative namespace counts do not match metadata');
  end if;

  select coalesce(pg_catalog.jsonb_object_agg(k, n), '{}'::jsonb)
  into v_legacy_counts
  from (
    select source_system as k, count(*) as n
    from public.taxonomy_v2_legacy_external_ids
    where release_id = p_release_id
    group by source_system
  ) s;
  if v_legacy_counts <> v_release.legacy_source_counts then
    v_errors := v_errors || pg_catalog.jsonb_build_array('legacy source counts do not match metadata');
  end if;

  select count(*) into v_dangling
  from public.taxonomy_v2_taxa t
  where t.release_id = p_release_id
    and t.parent_sporely_taxon_id is not null
    and not exists (
      select 1 from public.taxonomy_v2_taxa p
      where p.release_id = t.release_id
        and p.sporely_taxon_id = t.parent_sporely_taxon_id
    );
  if v_dangling <> v_release.dangling_parent_count then
    v_errors := v_errors || pg_catalog.jsonb_build_array('dangling parent count does not match metadata');
  end if;

  if exists (
    select 1 from public.taxonomy_v2_taxa t
    left join public.taxonomy_v2_concepts c using (sporely_taxon_id)
    where t.release_id = p_release_id and c.sporely_taxon_id is null
  ) then
    v_errors := v_errors || pg_catalog.jsonb_build_array('release-scoped taxon without stable concept');
  end if;

  if exists (
    select 1
    from (
      select release_id, sporely_taxon_id from public.taxonomy_v2_scientific_names where release_id = p_release_id
      union all
      select release_id, sporely_taxon_id from public.taxonomy_v2_vernacular_names where release_id = p_release_id
      union all
      select release_id, sporely_taxon_id from public.taxonomy_v2_external_ids where release_id = p_release_id
      union all
      select release_id, sporely_taxon_id from public.taxonomy_v2_legacy_external_ids where release_id = p_release_id
      union all
      select release_id, sporely_taxon_id from public.taxonomy_v2_redlist where release_id = p_release_id
    ) child
    left join public.taxonomy_v2_taxa t using (release_id, sporely_taxon_id)
    where t.sporely_taxon_id is null
  ) then
    v_errors := v_errors || pg_catalog.jsonb_build_array('release child row without release-scoped taxon');
  end if;

  if exists (
    select 1 from public.taxonomy_v2_external_ids
    where release_id = p_release_id
      and (source_system is null or btrim(source_system) = ''
        or namespace is null or btrim(namespace) = ''
        or external_id is null or btrim(external_id) = '')
  ) then
    v_errors := v_errors || pg_catalog.jsonb_build_array('blank authoritative identifier component');
  end if;

  if exists (
    select 1 from public.taxonomy_v2_external_ids
    where release_id = p_release_id
    group by source_system, namespace, external_id, sporely_taxon_id
    having count(*) > 1
  ) then
    v_errors := v_errors || pg_catalog.jsonb_build_array('duplicate authoritative semantic key');
  end if;

  select pg_catalog.pg_get_functiondef('public.search_taxa_v2(text,text,integer)'::regprocedure)
    into v_search_definition;
  select pg_catalog.pg_get_functiondef('public.resolve_taxon_external_id_v2(text,text,text)'::regprocedure)
    into v_resolver_definition;
  if pg_catalog.strpos(v_search_definition, 'taxonomy_v2_legacy_external_ids') > 0
     or pg_catalog.strpos(v_resolver_definition, 'taxonomy_v2_legacy_external_ids') > 0 then
    v_errors := v_errors || pg_catalog.jsonb_build_array('active RPC definition references legacy external IDs');
  end if;

  return pg_catalog.jsonb_build_object(
    'ok', pg_catalog.jsonb_array_length(v_errors) = 0,
    'release_id', v_release.release_id,
    'status', v_release.status,
    'expected_counts', v_release.row_counts,
    'actual_counts', v_actual_counts,
    'errors', v_errors
  );
exception
  when undefined_function then
    return pg_catalog.jsonb_build_object(
      'ok', false, 'release_id', v_release.release_id, 'status', v_release.status,
      'expected_counts', v_release.row_counts, 'actual_counts', v_actual_counts,
      'errors', v_errors || pg_catalog.jsonb_build_array('required taxonomy-v2 RPC is missing'));
end;
$$;
-- Forward-only correction; replace only the validator body.
-- CREATE OR REPLACE preserves owner, grants and function identity.
create or replace function public.taxonomy_v2_validate_release(p_release_id text)
returns jsonb
language plpgsql
stable
security definer
set search_path = public, pg_catalog
as $$
declare
  v_release public.taxonomy_v2_releases%rowtype;
  v_actual_counts jsonb;
  v_authoritative_counts jsonb;
  v_legacy_counts jsonb;
  v_dangling bigint;
  v_errors jsonb := '[]'::jsonb;
  v_search_definition text;
  v_resolver_definition text;
begin
  select * into v_release
  from public.taxonomy_v2_releases
  where release_id = p_release_id;

  if not found then
    return pg_catalog.jsonb_build_object(
      'ok', false, 'release_id', p_release_id, 'status', null,
      'expected_counts', null, 'actual_counts', null,
      'errors', pg_catalog.jsonb_build_array('release does not exist'));
  end if;

  if v_release.status not in ('ready', 'active') then
    v_errors := v_errors || pg_catalog.jsonb_build_array(
      pg_catalog.format('release status must be ready or active, got %s', v_release.status));
  end if;
  if v_release.taxonomy_schema_version <> 2 then
    v_errors := v_errors || ' ["taxonomy_schema_version must equal 2"]'::jsonb;
  end if;

  select pg_catalog.jsonb_build_object(
    'taxon.jsonl', (select count(*) from public.taxonomy_v2_taxa where release_id = p_release_id),
    'scientific_name.jsonl', (select count(*) from public.taxonomy_v2_scientific_names where release_id = p_release_id),
    'vernacular.jsonl', (select count(*) from public.taxonomy_v2_vernacular_names where release_id = p_release_id),
    'taxon_external_id.jsonl', (select count(*) from public.taxonomy_v2_external_ids where release_id = p_release_id),
    'taxon_external_id_legacy_integer.jsonl', (select count(*) from public.taxonomy_v2_legacy_external_ids where release_id = p_release_id),
    'taxon_redlist.jsonl', (select count(*) from public.taxonomy_v2_redlist where release_id = p_release_id)
  ) into v_actual_counts;

  if v_actual_counts <> v_release.row_counts then
    v_errors := v_errors || pg_catalog.jsonb_build_array('actual row counts do not match row_counts metadata');
  end if;

  select coalesce(pg_catalog.jsonb_object_agg(k, n), '{}'::jsonb)
  into v_authoritative_counts
  from (
    select source_system || '/' || namespace as k, count(*) as n
    from public.taxonomy_v2_external_ids
    where release_id = p_release_id
    group by source_system, namespace
  ) s;
  if v_authoritative_counts <> v_release.authoritative_namespace_counts then
    v_errors := v_errors || pg_catalog.jsonb_build_array('authoritative namespace counts do not match metadata');
  end if;

  select coalesce(pg_catalog.jsonb_object_agg(k, n), '{}'::jsonb)
  into v_legacy_counts
  from (
    select source_system as k, count(*) as n
    from public.taxonomy_v2_legacy_external_ids
    where release_id = p_release_id
    group by source_system
  ) s;
  if v_legacy_counts <> v_release.legacy_source_counts then
    v_errors := v_errors || pg_catalog.jsonb_build_array('legacy source counts do not match metadata');
  end if;

  select count(*) into v_dangling
  from public.taxonomy_v2_taxa t
  where t.release_id = p_release_id
    and t.parent_sporely_taxon_id is not null
    and not exists (
      select 1 from public.taxonomy_v2_taxa p
      where p.release_id = t.release_id
        and p.sporely_taxon_id = t.parent_sporely_taxon_id
      -- Keep this existence lookup correlated. Without the zero offset,
      -- a fresh release absent from statistics can flatten to an anti join
      -- that repeatedly scans every parent row using only release_id.
      offset 0
    );
  if v_dangling <> v_release.dangling_parent_count then
    v_errors := v_errors || pg_catalog.jsonb_build_array('dangling parent count does not match metadata');
  end if;

  if exists (
    select 1 from public.taxonomy_v2_taxa t
    left join public.taxonomy_v2_concepts c using (sporely_taxon_id)
    where t.release_id = p_release_id and c.sporely_taxon_id is null
  ) then
    v_errors := v_errors || pg_catalog.jsonb_build_array('release-scoped taxon without stable concept');
  end if;

  if exists (
    select 1
    from (
      select release_id, sporely_taxon_id from public.taxonomy_v2_scientific_names where release_id = p_release_id
      union all
      select release_id, sporely_taxon_id from public.taxonomy_v2_vernacular_names where release_id = p_release_id
      union all
      select release_id, sporely_taxon_id from public.taxonomy_v2_external_ids where release_id = p_release_id
      union all
      select release_id, sporely_taxon_id from public.taxonomy_v2_legacy_external_ids where release_id = p_release_id
      union all
      select release_id, sporely_taxon_id from public.taxonomy_v2_redlist where release_id = p_release_id
    ) child
    left join public.taxonomy_v2_taxa t using (release_id, sporely_taxon_id)
    where t.sporely_taxon_id is null
  ) then
    v_errors := v_errors || pg_catalog.jsonb_build_array('release child row without release-scoped taxon');
  end if;

  if exists (
    select 1 from public.taxonomy_v2_external_ids
    where release_id = p_release_id
      and (source_system is null or btrim(source_system) = ''
        or namespace is null or btrim(namespace) = ''
        or external_id is null or btrim(external_id) = '')
  ) then
    v_errors := v_errors || pg_catalog.jsonb_build_array('blank authoritative identifier component');
  end if;

  if exists (
    select 1 from public.taxonomy_v2_external_ids
    where release_id = p_release_id
    group by source_system, namespace, external_id, sporely_taxon_id
    having count(*) > 1
  ) then
    v_errors := v_errors || pg_catalog.jsonb_build_array('duplicate authoritative semantic key');
  end if;

  select pg_catalog.pg_get_functiondef('public.search_taxa_v2(text,text,integer)'::regprocedure)
    into v_search_definition;
  select pg_catalog.pg_get_functiondef('public.resolve_taxon_external_id_v2(text,text,text)'::regprocedure)
    into v_resolver_definition;
  if pg_catalog.strpos(v_search_definition, 'taxonomy_v2_legacy_external_ids') > 0
     or pg_catalog.strpos(v_resolver_definition, 'taxonomy_v2_legacy_external_ids') > 0 then
    v_errors := v_errors || pg_catalog.jsonb_build_array('active RPC definition references legacy external IDs');
  end if;

  return pg_catalog.jsonb_build_object(
    'ok', pg_catalog.jsonb_array_length(v_errors) = 0,
    'release_id', v_release.release_id,
    'status', v_release.status,
    'expected_counts', v_release.row_counts,
    'actual_counts', v_actual_counts,
    'errors', v_errors
  );
exception
  when undefined_function then
    return pg_catalog.jsonb_build_object(
      'ok', false, 'release_id', v_release.release_id, 'status', v_release.status,
      'expected_counts', v_release.row_counts, 'actual_counts', v_actual_counts,
      'errors', v_errors || pg_catalog.jsonb_build_array('required taxonomy-v2 RPC is missing'));
end;
$$;

DO $$ BEGIN
 IF (SELECT value FROM function_before) IS DISTINCT FROM (SELECT jsonb_build_object('owner',proowner,'acl',proacl::text,'config',proconfig,'oid',oid) FROM pg_proc WHERE oid='public.taxonomy_v2_validate_release(text)'::regprocedure) THEN RAISE EXCEPTION 'function identity/security changed'; END IF;
END $$;
CREATE FUNCTION pg_temp.fixture_release(r text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO public.taxonomy_v2_releases(release_id,taxonomy_schema_version,export_schema_version,manifest_schema_version,exporter_version,scope_predicate_id,source_gz_sha256,source_sqlite_sha256,whole_export_sha256,manifest_sha256,generated_at,status,row_counts,authoritative_namespace_counts,legacy_source_counts,dangling_parent_count,dangling_parent_report,source_manifest)
 VALUES(r,2,1,1,'fixture','fixture',repeat('a',64),repeat('b',64),md5(r)||md5(r),repeat('c',64),now(),'ready','{"taxon.jsonl":0,"scientific_name.jsonl":0,"vernacular.jsonl":0,"taxon_external_id.jsonl":0,"taxon_external_id_legacy_integer.jsonl":0,"taxon_redlist.jsonl":0}','{}','{}',0,'{}','{}');
END $$;
SELECT pg_temp.fixture_release('tax-2098.01.01-01');
SELECT pg_temp.fixture_release('tax-2098.01.02-01');
INSERT INTO public.taxonomy_v2_concepts(sporely_taxon_id,first_seen_release_id) SELECT 900000000+i,'tax-2098.01.01-01' FROM generate_series(1,52917) i;
INSERT INTO public.taxonomy_v2_taxa(release_id,sporely_taxon_id,parent_sporely_taxon_id,canonical_source_system,canonical_external_id)
SELECT 'tax-2098.01.01-01',900000000+i,CASE WHEN i=1 THEN NULL ELSE 900000001 END,'fixture',i::text FROM generate_series(1,52917) i;
-- Seed known-release statistics, then load the fresh release without ANALYZE.
ANALYZE public.taxonomy_v2_taxa;
SELECT pg_temp.fixture_release('tax-2098.01.03-01');
INSERT INTO public.taxonomy_v2_taxa SELECT 'tax-2098.01.03-01',sporely_taxon_id,parent_sporely_taxon_id,genus,specific_epithet,family,canonical_scientific_name,taxon_rank,taxonomic_status,source_system,canonical_source_system,canonical_external_id,preferred_scientific_name_no,preferred_scientific_name_no_source_system,preferred_scientific_name_no_namespace,preferred_scientific_name_no_external_id,preferred_scientific_name_sv,preferred_scientific_name_sv_source_system,preferred_scientific_name_sv_namespace,preferred_scientific_name_sv_external_id FROM public.taxonomy_v2_taxa WHERE release_id='tax-2098.01.01-01';
UPDATE public.taxonomy_v2_releases SET row_counts=jsonb_set(row_counts,'{taxon.jsonl}','52917') WHERE release_id IN ('tax-2098.01.01-01','tax-2098.01.03-01');
DO $$ DECLARE old jsonb; new jsonb; n int; BEGIN
 -- Full result equality under a safe generic plan; fixed validator also checked with fresh custom plans below.
 FOR n IN 0..3 LOOP
  IF n>0 THEN UPDATE public.taxonomy_v2_taxa SET parent_sporely_taxon_id=999999999 WHERE release_id='tax-2098.01.03-01' AND sporely_taxon_id=900000000+n; END IF;
  UPDATE public.taxonomy_v2_releases SET dangling_parent_count=n WHERE release_id='tax-2098.01.03-01';
  PERFORM set_config('plan_cache_mode','force_generic_plan',true);
  old:=pg_temp.old_validate_release('tax-2098.01.03-01');
  PERFORM set_config('plan_cache_mode','force_custom_plan',true);
  new:=public.taxonomy_v2_validate_release('tax-2098.01.03-01');
  IF old IS DISTINCT FROM new OR NOT (new->>'ok')::boolean THEN RAISE EXCEPTION 'result mismatch for % dangling: %, %',n,old,new; END IF;
  IF n>0 THEN
   UPDATE public.taxonomy_v2_releases SET dangling_parent_count=0 WHERE release_id='tax-2098.01.03-01';
   new:=public.taxonomy_v2_validate_release('tax-2098.01.03-01');
   IF (new->>'ok')::boolean OR NOT (new->'errors' ? 'dangling parent count does not match metadata') THEN RAISE EXCEPTION 'dangling parent not detected'; END IF;
  END IF;
 END LOOP;
 -- Parent exists only in another release: remove same-release parent, retain other copy.
 UPDATE public.taxonomy_v2_taxa SET parent_sporely_taxon_id=NULL WHERE release_id='tax-2098.01.03-01';
 DELETE FROM public.taxonomy_v2_taxa WHERE release_id='tax-2098.01.03-01' AND sporely_taxon_id=900000001;
 UPDATE public.taxonomy_v2_taxa SET parent_sporely_taxon_id=900000001 WHERE release_id='tax-2098.01.03-01' AND sporely_taxon_id=900000002;
 UPDATE public.taxonomy_v2_releases SET row_counts=jsonb_set(row_counts,'{taxon.jsonl}','52916'),dangling_parent_count=0 WHERE release_id='tax-2098.01.03-01';
 new:=public.taxonomy_v2_validate_release('tax-2098.01.03-01');
 IF (new->>'ok')::boolean OR NOT (new->'errors' ? 'dangling parent count does not match metadata') THEN RAISE EXCEPTION 'cross-release parent incorrectly accepted'; END IF;
 UPDATE public.taxonomy_v2_releases SET dangling_parent_count=1 WHERE release_id='tax-2098.01.03-01';
 IF NOT (public.taxonomy_v2_validate_release('tax-2098.01.03-01')->>'ok')::boolean THEN RAISE EXCEPTION 'declared boundary parent rejected'; END IF;
 IF pg_temp.old_validate_release('tax-2098.01.02-01') IS DISTINCT FROM public.taxonomy_v2_validate_release('tax-2098.01.02-01') THEN RAISE EXCEPTION 'empty release changed'; END IF;
 IF pg_temp.old_validate_release('tax-2098.01.04-01') IS DISTINCT FROM public.taxonomy_v2_validate_release('tax-2098.01.04-01') THEN RAISE EXCEPTION 'absent release changed'; END IF;
END $$;
PREPARE fixed_parent(text) AS SELECT count(*) FROM public.taxonomy_v2_taxa t WHERE t.release_id=$1 AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id OFFSET 0);
DO $$ DECLARE r text; plan json; BEGIN
 FOREACH r IN ARRAY ARRAY['tax-2098.01.01-01','tax-2098.01.03-01','tax-2098.01.04-01'] LOOP
  EXECUTE format('EXPLAIN (FORMAT JSON) EXECUTE fixed_parent(%L)',r) INTO plan;
  IF plan::text NOT LIKE '%Index Cond%sporely_taxon_id = t.parent_sporely_taxon_id%' OR plan::text NOT LIKE '%release_id = t.release_id%' THEN RAISE EXCEPTION 'both-key lookup absent: %',plan; END IF;
 END LOOP;
END $$;
DEALLOCATE fixed_parent;
SELECT 'parent-reference regression cases passed';
ROLLBACK;
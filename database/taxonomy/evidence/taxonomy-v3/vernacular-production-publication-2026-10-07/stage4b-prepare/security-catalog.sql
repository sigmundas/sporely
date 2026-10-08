begin;

do $$
declare
  v_table text;
  v_denied boolean;
  v_config text[];
begin
  foreach v_table in array array[
    'taxonomy_v2_releases', 'taxonomy_v2_concepts', 'taxonomy_v2_taxa',
    'taxonomy_v2_scientific_names', 'taxonomy_v2_vernacular_names',
    'taxonomy_v2_external_ids', 'taxonomy_v2_legacy_external_ids',
    'taxonomy_v2_redlist', 'taxonomy_v2_import_runs'
  ] loop
    if has_table_privilege('anon', 'public.'||v_table, 'select')
       or has_table_privilege('authenticated', 'public.'||v_table, 'select') then
      raise exception 'normal client has direct SELECT on %', v_table;
    end if;
  end loop;

  if not has_function_privilege('anon','public.search_taxa_v2(text,text,integer)','execute')
     or not has_function_privilege('authenticated','public.search_taxa_v2(text,text,integer)','execute')
     or not has_function_privilege('anon','public.resolve_taxon_external_id_v2(text,text,text)','execute')
     or not has_function_privilege('authenticated','public.resolve_taxon_external_id_v2(text,text,text)','execute') then
    raise exception 'normal-client read RPC grant missing';
  end if;
  if has_function_privilege('anon','public.taxonomy_v2_activate_release(text)','execute')
     or has_function_privilege('authenticated','public.taxonomy_v2_activate_release(text)','execute')
     or not has_function_privilege('service_role','public.taxonomy_v2_activate_release(text)','execute') then
    raise exception 'activation grants incorrect';
  end if;

  if has_function_privilege('public','public.search_taxa_v2(text,text,integer)','execute')
     or has_function_privilege('public','public.resolve_taxon_external_id_v2(text,text,text)','execute')
     or has_function_privilege('public','public.taxonomy_v2_activate_release(text)','execute')
     or has_function_privilege('public','public.taxonomy_v2_validate_release(text)','execute') then
    raise exception 'taxonomy-v2 function remains executable by PUBLIC';
  end if;
  if has_function_privilege('anon','public.taxonomy_v2_jsonb_nonnegative_integer_counts(jsonb)','execute')
     or has_function_privilege('authenticated','public.taxonomy_v2_jsonb_nonnegative_integer_counts(jsonb)','execute') then
    raise exception 'internal taxonomy-v2 count helper is client-executable';
  end if;

  for v_config in
    select p.proconfig
    from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public' and p.proname in (
      'search_taxa_v2','resolve_taxon_external_id_v2',
      'taxonomy_v2_validate_release','taxonomy_v2_activate_release'
    )
  loop
    if not ('search_path=public, pg_catalog' = any(v_config)) then
      raise exception 'controlled search_path missing: %', v_config;
    end if;
  end loop;

  raise notice 'catalog security checks passed';
end $$;
rollback;

"""Stage 3D F1: table-by-table content delta, fda89f37 SQLite (old) vs tax-2026.10.07-01 SQLite (new).
Usage: python f1_content_delta.py <old.sqlite3> <new.sqlite3> <out.json>. Rows compared as full tuples after relabelling the old release id to the new one;
an INTEGER surrogate PK is excluded (it renumbers)."""
import json, sqlite3, sys, collections
old, new = (sqlite3.connect(f'file:{p}?mode=ro', uri=True) for p in sys.argv[1:3])
tabs = lambda c: sorted(r[0] for r in c.execute("select name from sqlite_master where type='table'"))
out = {'tables_old': tabs(old), 'tables_new': tabs(new), 'schema_equal': list(old.execute('select type,name,sql from sqlite_master order by name')) == list(new.execute('select type,name,sql from sqlite_master order by name')), 'tables': {}}
for t in tabs(new):
    info = list(new.execute(f'pragma table_info({t})')); cols = [r[1] for r in info]
    # drop an INTEGER surrogate primary key (renumbers when a row is removed); keep text/composite keys
    sur = {r[1] for r in info if r[5] == 1 and r[2].upper() == 'INTEGER' and sum(x[5] > 0 for x in info) == 1 and r[1] in ('vernacular_id', 'scientific_name_id', 'external_id_row_id', 'redlist_row_id')}
    sel = ','.join(c for c in cols if c not in sur)
    rid = lambda r: tuple('tax-2026.10.07-01' if x == 'tax-2026.10.06-01' else x for x in r)  # release-id relabel only
    a = collections.Counter(map(rid, old.execute(f'select {sel} from {t}'))); b = collections.Counter(new.execute(f'select {sel} from {t}'))
    rem, add = a - b, b - a
    out['tables'][t] = {'old': sum(a.values()), 'new': sum(b.values()), 'removed': sorted(map(list, rem.elements()), key=str)[:50],
                        'added': sorted(map(list, add.elements()), key=str)[:50], 'removed_count': sum(rem.values()), 'added_count': sum(add.values()), 'columns': cols, 'ignored_surrogate_key': sorted(sur)}
json.dump(out, open(sys.argv[3], 'w'), ensure_ascii=False, indent=1, sort_keys=True, default=str)
for t, v in out['tables'].items():
    if v['removed_count'] or v['added_count']:
        print(t, v['old'], v['new'], '-', v['removed_count'], '+', v['added_count'])
        for r in v['removed'][:12]: print('  -', r)
        for r in v['added'][:12]: print('  +', r)
print('schema_equal', out['schema_equal'])

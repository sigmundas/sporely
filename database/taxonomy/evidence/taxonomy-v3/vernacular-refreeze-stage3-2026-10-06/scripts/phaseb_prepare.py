"""Stage 3B refreeze (2026-10-06) — Phase B preparation from the verified frozen set.

Reproduces the 2026-10-04 Stage 3 method: policies extracted from the accepted
evidence archive, W1 export (generated_at pinned), global-macrofungi scope,
desktop candidate, approved sporely-web importer payload. Read-only on finalA.
Usage (cwd = sporely-py): .venv/bin/python phaseb_prepare.py <run_dir>
"""
import gzip, hashlib, json, shutil, subprocess, sys, tarfile
from pathlib import Path

sys.path.insert(0, str(Path('database/taxonomy/scripts').resolve()))
sys.path.insert(0, str(Path('.').resolve()))
from promote_desktop_bundle import verify_frozen  # noqa: E402
from database.taxonomy.cloud_export import run_export  # noqa: E402
from database.taxonomy import macrofungi_scope as m  # noqa: E402

S = Path.home() / 'sporely-scratch/vernacular-2026-10-06-r2'
F = S / 'final'
WEB = Path('/Users/sigmundas/Documents/Code/sporely/sporely-web')
RID = 'tax-2026.10.06-01'
ACCEPTED = {
    'freeze.json': ('fda89f3744975dfb296336b73256c33df1b278efe092eca336557d067d07148e', 895),
    f'{RID}.sqlite3.gz': ('9e6690d50ddba88a092b311109f2f3ceb2bd1d5a4fb9b543c62658f0a59a58d3', 73281566),
    f'{RID}.evidence.tar.gz': ('97b4a896977ccc256f662cbf96fca195362a70ebf40cae705e27cd3d084feadd', 53373707),
    'manifest.json': ('6c9d4919b51e099cd6694ad887ab10befefc27d83a92aaadbd0aba95b970379e', 4235),
    'compatibility.json': ('747f32d27a98c7424e4cb05f52244d4d7185266b4bed482e00976442a37feccd', 3214),
}
SQLITE_SHA = '3f9037c08b918890910b16f5a7446a323d5524d08a49e674715afbb7137bb73e'


def sha(p: Path) -> str:
    h = hashlib.sha256()
    with p.open('rb') as f:
        for c in iter(lambda: f.read(1 << 20), b''):
            h.update(c)
    return h.hexdigest()


def verify_final() -> None:
    names = sorted(p.name for p in F.iterdir())
    assert names == sorted(ACCEPTED), names
    for n, (h, b) in ACCEPTED.items():
        assert sha(F / n) == h and (F / n).stat().st_size == b, n
    verify_frozen(F)


def main(run: Path) -> None:
    assert not run.exists(), run
    run.mkdir(parents=True)
    verify_final()
    # Decompressed frozen SQLite for analysis.
    with gzip.open(F / f'{RID}.sqlite3.gz', 'rb') as src, (run / 'frozen.sqlite3').open('wb') as dst:
        shutil.copyfileobj(src, dst, 1 << 20)
    assert sha(run / 'frozen.sqlite3') == SQLITE_SHA
    pol = run / 'policies'
    pol.mkdir()
    man = json.loads((F / 'manifest.json').read_text())
    with tarfile.open(F / man['compiler_evidence']['artifact']) as t:
        for name, target in [('inputs/scope_policy.json', 'global-macrofungi-scope.yml'),
                             ('inputs/mapping_policy.json', 'mapping_policy.yml'),
                             ('inputs/manual_mappings.json', 'manual_mappings.yml'),
                             ('inputs/concept_supersessions.json', 'concept_supersessions.yml')]:
            (pol / target).write_bytes(t.extractfile(name).read())
        (run / 'vernacular_evidence.jsonl').write_bytes(t.extractfile('compiler/vernacular_evidence.jsonl').read())
    r = run_export(artifact_gz=F / f'{RID}.sqlite3.gz', manifest=F / 'manifest.json', output_dir=run / 'w1',
                   policy_dir=pol, generated_at='2026-10-06T00:00:00Z')
    print('W1', {k: v.row_count for k, v in r.datasets.items()}, flush=True)
    policy = pol / 'global-macrofungi-scope.yml'
    connection, _ = m._source_connection(F / f'{RID}.sqlite3.gz', run)
    p = m.load_policy(policy)
    taxa, by_col = m.load_taxa(connection)
    rules = m.resolve_rules(p, by_col)
    results = m.evaluate(taxa, rules, p.get('source_characteristic_exclusions', []))
    m.validate_selectable_fungi(taxa, results)
    built = m.build_export(run / 'w1', run / 'scoped', taxa, results, rules,
                           {'sqlite_gz_sha256': m.sha256_file(F / f'{RID}.sqlite3.gz'),
                            'w1_manifest_sha256': m.sha256_file(run / 'w1/taxonomy_export_manifest.json')},
                           m.sha256_file(policy), RID)
    connection.close()
    v = m.validate_export(run / 'scoped', built['export_manifest'])
    (run / 'scope-validation.json').write_text(json.dumps(v, sort_keys=True, indent=2) + '\n')
    print('scoped', {x['name']: x['row_count'] for x in built['export_manifest']['files']}, flush=True)
    desk = m.build_desktop(run / 'scoped', run / f'scoped/desktop-{RID}.sqlite3', RID)
    (run / 'desktop.json').write_text(json.dumps(desk, sort_keys=True, indent=2, default=str) + '\n')
    out = subprocess.run(['node', 'scripts/taxonomy-v2/prepare-production-release-import.mjs', '--release-id', RID,
                          '--release-dir', str(run / 'scoped'), '--output', str(run / 'import.sql')],
                         cwd=WEB, check=True, capture_output=True, text=True).stdout
    (run / 'prepared.txt').write_text(out)
    verify_final()
    print('prepared', sha(run / 'import.sql'), flush=True)


if __name__ == '__main__':
    main(Path(sys.argv[1]))

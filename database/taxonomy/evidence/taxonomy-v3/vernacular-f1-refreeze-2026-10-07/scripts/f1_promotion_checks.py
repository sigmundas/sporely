"""Stage 3D F1 refreeze (2026-10-07; copied from Stage 3B 2026-10-06) — promotion mechanics against SCRATCH copies only.

cwd = sporely-py. Never touches the real repo bundle or compatibility file
(hash-checked before/after). Promotion runs as the real CLI in a subprocess
with a guard that forbids compiler/normalizer/freeze imports, subprocesses
and any gzip/tar write, and records every module loaded.
"""
import hashlib, json, os, shutil, subprocess, sys
from pathlib import Path

REPO = Path.cwd()
S = Path.home() / 'sporely-scratch/vernacular-2026-10-07-f1'
F = S / 'final'
OUT = S / 'promotion'
PY = str(REPO / '.venv/bin/python')
FREEZE = 'a86e35854fd4d01984d8b8fe121d8fc318e75a01876d243975847127462fd14e'
REAL = [REPO / 'database/reference_data/generated/taxonomy_v2', REPO / 'database/taxonomy/desktop-compatibility.json']
GUARD = r'''
import builtins, sys, json, atexit, gzip, tarfile, subprocess, os
BLOCK = {"compile_release","build_sqlite_candidate","freeze_release","normalize_col_xr","national_source",
         "vernacular_projection","export_legacy_enrichment","build_release","normalize_redlist_no","cloud_export","macrofungi_scope"}
_imp = builtins.__import__
def guarded(name, *a, **k):
    if name.split(".")[-1] in BLOCK or name.split(".")[0] in BLOCK:
        raise ImportError("GUARD: forbidden import " + name)
    return _imp(name, *a, **k)
builtins.__import__ = guarded
def deny(*a, **k): raise RuntimeError("GUARD: subprocess forbidden")
subprocess.Popen = deny; os.system = deny
_go = gzip.open
def gopen(f, mode="rb", *a, **k):
    if any(c in mode for c in "wax"): raise RuntimeError("GUARD: gzip write")
    return _go(f, mode, *a, **k)
gzip.open = gopen
_gf = gzip.GzipFile.__init__
def ginit(self, filename=None, mode=None, *a, **k):
    if mode and any(c in mode for c in "wax"): raise RuntimeError("GUARD: GzipFile write")
    return _gf(self, filename, mode, *a, **k)
gzip.GzipFile.__init__ = ginit
_to = tarfile.open
def topen(name=None, mode="r", *a, **k):
    if any(c in mode for c in "wax"): raise RuntimeError("GUARD: tar write")
    return _to(name, mode, *a, **k)
tarfile.open = topen
@atexit.register
def dump():
    p = os.environ.get("GUARD_MODULES_OUT")
    if p: open(p, "w").write(json.dumps(sorted(m for m in sys.modules if not m.startswith("_"))))
'''


def sha(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for c in iter(lambda: f.read(1 << 20), b''):
            h.update(c)
    return h.hexdigest()


def tree(p: Path):
    if p.is_file():
        return {p.name: sha(p)}
    return {str(x.relative_to(p)): sha(x) for x in sorted(p.rglob('*')) if x.is_file()}


def promote(frozen, bundle, compat, freeze_hash, tag):
    env = dict(os.environ, PYTHONPATH=str(OUT / 'guard'), GUARD_MODULES_OUT=str(OUT / f'modules-{tag}.json'))
    env.pop('DATABASE_URL', None)
    r = subprocess.run([PY, 'database/taxonomy/scripts/promote_desktop_bundle.py', '--frozen-dir', str(frozen),
                        '--expect-freeze-sha256', freeze_hash, '--bundle-dir', str(bundle), '--compatibility', str(compat)],
                       cwd=REPO, env=env, capture_output=True, text=True)
    return {'returncode': r.returncode, 'stderr_tail': r.stderr.strip().splitlines()[-1:] if r.stderr.strip() else [],
            'stdout_tail': r.stdout.strip().splitlines()[-2:]}


def fresh_bundle(name):
    b = OUT / name
    b.mkdir(parents=True)
    src = REAL[0]
    shutil.copyfile(src / 'manifest.json', b / 'manifest.json')
    old = json.loads((src / 'manifest.json').read_text())['gz_artifact']
    shutil.copyfile(src / old, b / old)
    (b / 'previous.evidence.tar.gz').write_bytes(b'previous archive marker')
    c = OUT / f'{name}-compatibility.json'
    shutil.copyfile(REAL[1], c)
    return b, c


def main():
    assert not OUT.exists()
    (OUT / 'guard').mkdir(parents=True)
    (OUT / 'guard/sitecustomize.py').write_text(GUARD)
    real_before = {str(p): tree(p) for p in REAL}
    frozen_before = tree(F)
    res = {}
    # 1. Normal promotion into scratch copy (guarded) + exact byte check.
    b, c = fresh_bundle('bundle-ok')
    before = tree(b)
    res['promote'] = promote(F, b, c, FREEZE, 'ok')
    after = tree(b)
    man = json.loads((F / 'manifest.json').read_text())
    exp = {'manifest.json': sha(F / 'manifest.json'), man['gz_artifact']: sha(F / man['gz_artifact']),
           man['compiler_evidence']['artifact']: sha(F / man['compiler_evidence']['artifact']),
           man['freeze_artifact']: sha(F / 'freeze.json')}
    res['promoted_files_equal_frozen'] = all(after.get(k) == v for k, v in exp.items())
    res['compatibility_equals_frozen'] = sha(c) == sha(F / 'compatibility.json')
    res['bundle_after'] = after
    res['previous_evidence_retained'] = (b / 'previous.evidence.tar.gz').read_bytes() == b'previous archive marker'
    res['superseded_gz_removed'] = [k for k in before if k not in after]
    res['new_files'] = sorted(k for k in after if k not in before)
    mods = json.loads((OUT / 'modules-ok.json').read_text())
    res['forbidden_modules_loaded'] = [m for m in mods if m.split('.')[-1] in {'compile_release', 'build_sqlite_candidate', 'freeze_release', 'normalize_col_xr', 'national_source', 'vernacular_projection', 'build_release', 'cloud_export', 'macrofungi_scope'}]
    res['guard'] = 'imports of compiler/normalizer/freeze/export modules, subprocess, gzip/tar writes raise inside promotion process'
    # 2. Idempotent repeat.
    res['repeat'] = promote(F, b, c, FREEZE, 'repeat')
    res['repeat_idempotent'] = tree(b) == after and sha(c) == sha(F / 'compatibility.json')
    # 3. Wrong freeze hash rejected, nothing written.
    b2, c2 = fresh_bundle('bundle-wronghash')
    t0 = (tree(b2), sha(c2))
    res['wrong_freeze_hash'] = promote(F, b2, c2, '0' * 64, 'wronghash')
    res['wrong_freeze_hash_rejected_without_writes'] = res['wrong_freeze_hash']['returncode'] != 0 and (tree(b2), sha(c2)) == t0
    # 4. Tampered byte in each frozen file rejected, nothing written.
    res['tamper'] = {}
    for name in sorted(exp.keys() - {man['freeze_artifact']}) + ['compatibility.json', 'freeze.json']:
        td = OUT / f'tampered-{name}'
        shutil.copytree(F, td)
        os.chmod(td, 0o755)
        p = td / name
        os.chmod(p, 0o644)
        data = bytearray(p.read_bytes())
        i = len(data) // 2
        data[i] ^= 0x01
        p.write_bytes(bytes(data))
        bt, ct = fresh_bundle(f'bundle-tamper-{name}')
        t0 = (tree(bt), sha(ct))
        fh = sha(td / 'freeze.json')  # for freeze.json tamper, also try with the accepted hash
        r1 = promote(td, bt, ct, FREEZE, f'tamper-{name}')
        r2 = promote(td, bt, ct, fh, f'tamper2-{name}') if name == 'freeze.json' else None
        res['tamper'][name] = {'offset': i, 'with_accepted_hash': r1, 'with_tampered_descriptor_hash': r2,
                               'rejected_without_writes': r1['returncode'] != 0 and (r2 is None or r2['returncode'] != 0) and (tree(bt), sha(ct)) == t0}
        shutil.rmtree(td)
    # 5. build_release.py --promote refusals (real CLI). Missing hash: refused before any build.
    bd = OUT / 'br-nohash'
    r = subprocess.run([PY, 'database/taxonomy/scripts/build_release.py', '--release-id', 'tax-2026.10.07-01',
                        '--build-dir', str(bd), '--promote'], cwd=REPO, capture_output=True, text=True)
    res['build_release_promote_without_hash'] = {'returncode': r.returncode, 'stderr_tail': r.stderr.strip().splitlines()[-1:],
        'build_dir_created': bd.exists()}
    # 6. build_release.py --promote with a WRONG expected freeze hash: full build, refused before any bundle write.
    bd = OUT / 'br-wronghash'
    r = subprocess.run([PY, 'database/taxonomy/scripts/build_release.py', '--release-id', 'tax-2026.10.07-01',
                        '--build-dir', str(bd), '--promote', '--expect-freeze-sha256', '0' * 64], cwd=REPO, capture_output=True, text=True)
    res['build_release_promote_wrong_hash'] = {'returncode': r.returncode, 'stderr_tail': r.stderr.strip().splitlines()[-2:],
        'stdout_tail': r.stdout.strip().splitlines()[-2:], 'build_dir_created': bd.exists(),
        'fresh_freeze_sha256': sha(bd / 'frozen/freeze.json') if (bd / 'frozen/freeze.json').exists() else None}
    res['frozen_unchanged'] = tree(F) == frozen_before
    res['real_repo_bundle_and_compatibility_unchanged'] = {str(p): tree(p) for p in REAL} == real_before
    res['real_repo_hashes'] = {'manifest.json': real_before[str(REAL[0])]['manifest.json'], 'desktop-compatibility.json': real_before[str(REAL[1])]['desktop-compatibility.json']}
    (OUT / 'promotion-checks.json').write_text(json.dumps(res, indent=2, sort_keys=True) + '\n')
    print(json.dumps({k: v for k, v in res.items() if k not in ('bundle_after', 'tamper')}, indent=1))
    print({k: v['rejected_without_writes'] for k, v in res['tamper'].items()})


if __name__ == '__main__':
    main()

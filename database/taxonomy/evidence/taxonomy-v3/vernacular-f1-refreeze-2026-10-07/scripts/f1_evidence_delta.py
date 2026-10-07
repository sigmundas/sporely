"""Stage 3D F1: per-member diff of the compiler evidence archives (old fda89f37 vs new). Usage: python f1_evidence_delta.py <old.tar.gz> <new.tar.gz> <out.json>"""
import hashlib, json, sys, tarfile
def load(p):
    with tarfile.open(p) as t:
        return {m.name: t.extractfile(m).read() for m in t.getmembers() if m.isfile()}
a, b = load(sys.argv[1]), load(sys.argv[2])
out = {'members_old': len(a), 'members_new': len(b), 'only_old': sorted(set(a) - set(b)), 'only_new': sorted(set(b) - set(a)), 'changed': {}}
for n in sorted(set(a) & set(b)):
    if a[n] != b[n]:
        x = a[n].replace(b'tax-2026.10.06-01', b'tax-2026.10.07-01')
        la, lb = x.splitlines(), b[n].splitlines()
        sa, sb = set(la), set(lb)
        out['changed'][n] = {'old_sha256': hashlib.sha256(a[n]).hexdigest(), 'new_sha256': hashlib.sha256(b[n]).hexdigest(),
                             'identical_after_release_id_relabel': x == b[n], 'lines_removed': len(sa - sb), 'lines_added': len(sb - sa),
                             'removed_sample': [l.decode('utf-8', 'replace')[:400] for l in sorted(sa - sb)[:8]],
                             'added_sample': [l.decode('utf-8', 'replace')[:400] for l in sorted(sb - sa)[:8]]}
json.dump(out, open(sys.argv[3], 'w'), ensure_ascii=False, indent=1, sort_keys=True)
for n, v in out['changed'].items():
    print(n, v['identical_after_release_id_relabel'], '-', v['lines_removed'], '+', v['lines_added'])
print('only_old', out['only_old'], 'only_new', out['only_new'])

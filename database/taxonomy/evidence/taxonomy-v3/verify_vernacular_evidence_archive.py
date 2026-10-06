"""Verify vernacular-evidence-archive.json against the stored evidence files.

Every gzipped or deduplicated original must be recoverable byte-for-byte:
sha256 of the decompressed stored (or canonical) bytes equals original_sha256.
"""
import gzip
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def payload(rel):
    data = (ROOT / rel).read_bytes()
    return gzip.decompress(data) if rel.endswith('.gz') else data


def main():
    manifest = json.loads((ROOT / 'vernacular-evidence-archive.json').read_text())
    failures = []
    for e in manifest['entries']:
        if (ROOT / e['original_path']).exists():
            failures.append(f"{e['original_path']}: original still present")
        if e['action'] == 'gzipped':
            raw = (ROOT / e['stored_path']).read_bytes()
            if hashlib.sha256(raw).hexdigest() != e['stored_sha256']:
                failures.append(f"{e['stored_path']}: stored hash mismatch")
            rel = e['stored_path']
        else:
            rel = e['canonical_stored_path']
        if hashlib.sha256(payload(rel)).hexdigest() != e['original_sha256']:
            failures.append(f"{e['original_path']}: payload at {rel} does not match original_sha256")
    for f in failures:
        print('FAIL', f)
    print(f"{len(manifest['entries'])} entries, {len(failures)} failures")
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())

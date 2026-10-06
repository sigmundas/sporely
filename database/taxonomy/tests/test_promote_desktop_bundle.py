"""promote_desktop_bundle.py: bundle manifest from authoritative build data."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "database/taxonomy/scripts"))

import promote_desktop_bundle as promote  # noqa: E402

RELEASE = "tax-2099.01.01-01"
REGISTRY_SHA = "ab" * 32
WORKBOOK_BYTES = b"synthetic red-list workbook"
WORKBOOK_SHA = hashlib.sha256(WORKBOOK_BYTES).hexdigest()


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def build(tmp_path: Path) -> dict:
    """A minimal but internally consistent compiler output + artifact."""
    release_dir = tmp_path / "release"
    release_dir.mkdir()
    compiler_manifest = {
        "content_release_id": RELEASE,
        "registry_sha256": REGISTRY_SHA,
        "redlist_no": {"source_binding": {
            "input_sha256": WORKBOOK_SHA, "source_release": "2021",
            "source_system": "artsdatabanken_redlist",
        }},
    }
    (release_dir / "manifest.json").write_text(json.dumps(compiler_manifest), encoding="utf-8")
    (release_dir / "redlist_no_diagnostics.json").write_text(json.dumps({
        "counts": {"total": 3, "resolved": 1, "unresolved": 2},
        "collisions": {"summary": {"unique": 1, "conflicting_categories": 0}},
    }), encoding="utf-8")
    (release_dir / "redlist_no.jsonl").write_text("".join(json.dumps(r) + "\n" for r in (
        {"unresolved_reason": None},
        {"unresolved_reason": "unresolved_name_id_not_found"},
        {"unresolved_reason": "unresolved_name_id_name_mismatch"},
    )), encoding="utf-8")

    outputs = {p.stem: {"name": p.name, "sha256": _sha(p), "bytes": p.stat().st_size}
               for p in release_dir.iterdir() if p.name != "manifest.json"}
    compiler_manifest["outputs"] = outputs
    (release_dir / "manifest.json").write_text(json.dumps(compiler_manifest))
    sqlite_path = tmp_path / "artifact.sqlite3"
    conn = sqlite3.connect(sqlite_path)
    conn.execute("CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.executemany("INSERT INTO taxonomy_meta VALUES (?, ?)", [
        ("taxonomy_schema_version", "2"), ("content_release_id", RELEASE),
        ("compiler_manifest_sha256", _sha(release_dir / "manifest.json")),
        ("registry_sha256", REGISTRY_SHA), ("state", "candidate"), ("publication", "none"),
    ])
    conn.execute("CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    workbook = tmp_path / "redlist.xlsx"
    workbook.write_bytes(WORKBOOK_BYTES)
    report = tmp_path / "redlist_report.json"
    report.write_text(json.dumps({
        "input_sha256": WORKBOOK_SHA, "input_filename": "redlist.xlsx", "row_count": 3,
        "source_release": "2021", "source_system": "artsdatabanken_redlist",
        "source_url": "https://example.invalid/redlist", "sheet_name": "Vurderinger",
    }), encoding="utf-8")
    registry_manifest = tmp_path / "registry-manifest.json"
    registry_manifest.write_text(json.dumps({"concatenated_sha256": REGISTRY_SHA}), encoding="utf-8")

    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "tax-old.sqlite3.gz").write_bytes(b"old")
    (bundle / "manifest.json").write_text(json.dumps({
        "content_release_id": "tax-old", "gz_artifact": "tax-old.sqlite3.gz",
        "install_target_name": "vernacular_multilanguage_v2.sqlite3",
        "redlist_no": {
            "source_system": "artsdatabanken_redlist", "source_release": "2021",
            "citation": "Curated citation", "license": {"status": "pending"},
            "workbook_sha256": WORKBOOK_SHA, "resolved_count": 999,
        },
    }), encoding="utf-8")
    compat = tmp_path / "compat.json"
    compat.write_text(json.dumps({
        "tested_taxonomy_release": "tax-old",
        "redlist_no": {"citation_evidence": "compat prose", "resolved_count": 999},
    }), encoding="utf-8")
    return dict(sqlite_path=sqlite_path, release_dir=release_dir, redlist_report_path=report,
                redlist_workbook=workbook, bundle_dir=bundle,
                registry_manifest_path=registry_manifest, compatibility_path=compat)


def _freeze(build, output):
    receipt = output.with_suffix('.receipt.json')
    receipt.write_text(json.dumps({'validated': True, 'sqlite_sha256': _sha(build['sqlite_path']),
        'compiler_manifest_sha256': _sha(build['release_dir'] / 'manifest.json')}))
    promote.freeze(**build, output_dir=output, validation_receipt_path=receipt)
    return dict(frozen_dir=output, expected_freeze_sha256=_sha(output/'freeze.json'),
        bundle_dir=build['bundle_dir'], registry_manifest_path=build['registry_manifest_path'],
        compatibility_path=build['compatibility_path'])


def test_promotion_writes_a_loadable_deterministic_bundle(build, tmp_path):
    frozen = _freeze(build, tmp_path/'frozen')
    manifest = promote.promote(**frozen)
    bundle = build["bundle_dir"]
    gz = bundle / f"{RELEASE}.sqlite3.gz"
    assert manifest["content_release_id"] == RELEASE
    assert manifest["gz_artifact"] == gz.name
    assert manifest["gz_sha256"] == _sha(gz)
    assert manifest["sqlite_sha256"] == _sha(build["sqlite_path"])
    assert manifest["registry_concatenated_sha256"] == REGISTRY_SHA
    assert manifest["compiler_manifest_sha256"] == _sha(build["release_dir"] / "manifest.json")
    assert manifest["install_target_name"] == "vernacular_multilanguage_v2.sqlite3"
    assert not (bundle / "tax-old.sqlite3.gz").exists(), "superseded bundle gzip removed"
    assert json.loads((bundle / "manifest.json").read_text(encoding="utf-8")) == manifest

    red = manifest["redlist_no"]
    assert red["resolved_count"] == 1 and red["unresolved_count"] == 2, "counts recomputed, not carried"
    assert red["unresolved_reason_counts"] == {
        "name_id_not_found_in_nortaxa": 1, "name_id_name_mismatch": 1, "name_id_ambiguous": 0}
    assert red["citation"] == "Curated citation" and red["license"] == {"status": "pending"}
    assert red["workbook_bytes"] == len(WORKBOOK_BYTES)

    compat = json.loads(build["compatibility_path"].read_text(encoding="utf-8"))
    assert compat["tested_taxonomy_release"] == RELEASE
    assert compat["bundled_gz_sha256"] == manifest["gz_sha256"]
    assert compat["redlist_no"] == {"citation_evidence": "compat prose", "resolved_count": 1}

    # The runtime loader and installer accept the result as-is.
    from utils import taxonomy_v2
    loaded = taxonomy_v2.load_manifest(bundle / "manifest.json")
    installed = taxonomy_v2.ensure_installed(app_data_dir=tmp_path / "appdata", manifest=loaded,
                                             gz_path=gz, force_verify=True)
    assert _sha(installed) == manifest["sqlite_sha256"]

    # Same inputs → same bytes.
    first = gz.read_bytes()
    promote.promote(**frozen)
    assert gz.read_bytes() == first


@pytest.mark.parametrize("breakage, message", [
    ("compiler_manifest", "no output inventory"),
    ("registry", "committed registry"),
    ("workbook", "not the workbook this build consumed"),
    ("previous_workbook", "curated"),
    ("previous_gz_is_manifest", "invalid gzip artifact suffix"),
    ("previous_gz_traversal", "invalid gzip artifact name"),
    ("release_id_traversal", "is not tax-YYYY.MM.DD-NN"),
])
def test_promotion_refuses_unproven_inputs(build, breakage, message):
    if breakage == "compiler_manifest":
        (build["release_dir"] / "manifest.json").write_text(
            json.dumps({"content_release_id": RELEASE, "edited": True}), encoding="utf-8")
    elif breakage == "registry":
        build["registry_manifest_path"].write_text(json.dumps({"concatenated_sha256": "cd" * 32}))
    elif breakage == "workbook":
        build["redlist_workbook"].write_bytes(b"a different workbook")
    elif breakage == "previous_workbook":
        path = build["bundle_dir"] / "manifest.json"
        previous = json.loads(path.read_text(encoding="utf-8"))
        previous["redlist_no"]["workbook_sha256"] = "00" * 32
        path.write_text(json.dumps(previous), encoding="utf-8")
    elif breakage in ("previous_gz_is_manifest", "previous_gz_traversal"):
        path = build["bundle_dir"] / "manifest.json"
        previous = json.loads(path.read_text(encoding="utf-8"))
        previous["gz_artifact"] = "manifest.json" if breakage == "previous_gz_is_manifest" else "..\\evil.sqlite3.gz"
        path.write_text(json.dumps(previous), encoding="utf-8")
    elif breakage == "release_id_traversal":
        conn = sqlite3.connect(build["sqlite_path"])
        conn.execute("UPDATE taxonomy_meta SET value = '../escape' WHERE key = 'content_release_id'")
        conn.commit()
        conn.close()
    before = {p.name: p.read_bytes() for p in build["bundle_dir"].iterdir()}
    with pytest.raises(promote.PromotionError, match=message):
        _freeze(build, build["bundle_dir"].parent/"frozen")
    assert {p.name: p.read_bytes() for p in build["bundle_dir"].iterdir()} == before, \
        "a refused promotion writes nothing"


def test_freeze_is_deterministic_and_promotion_only_copies(build, tmp_path, monkeypatch):
    a=_freeze(build,tmp_path/'a');b=_freeze(build,tmp_path/'b')
    assert {p.name:p.read_bytes() for p in a['frozen_dir'].iterdir()}=={p.name:p.read_bytes() for p in b['frozen_dir'].iterdir()}
    older=build['bundle_dir']/'old.evidence.tar.gz';older.write_bytes(b'old evidence')
    before={p.name:p.read_bytes() for p in a['frozen_dir'].iterdir()}
    monkeypatch.setattr(promote,'write_deterministic_gzip',lambda *a,**k:pytest.fail('promotion compressed'))
    manifest=promote.promote(**a)
    assert older.read_bytes()==b'old evidence'
    assert {p.name:p.read_bytes() for p in a['frozen_dir'].iterdir()}==before
    for name in (manifest['gz_artifact'],manifest['compiler_evidence']['artifact'],'manifest.json'):
        assert (build['bundle_dir']/name).read_bytes()==(a['frozen_dir']/name).read_bytes()
    import tarfile
    with tarfile.open(build['bundle_dir']/manifest['compiler_evidence']['artifact']) as archive:
        assert archive.extractfile('compiler/redlist_no.jsonl').read()==(build['release_dir']/'redlist_no.jsonl').read_bytes()


@pytest.mark.parametrize('change',['gzip','evidence','freeze','baseline','registry'])
def test_frozen_tampering_refused_without_writes(build,tmp_path,change):
    args=_freeze(build,tmp_path/'frozen')
    manifest=json.loads((args['frozen_dir']/'manifest.json').read_text())
    if change in {'gzip','evidence'}:
        name=manifest['gz_artifact'] if change=='gzip' else manifest['compiler_evidence']['artifact']
        (args['frozen_dir']/name).write_bytes(b'tampered')
    elif change=='freeze':
        (args['frozen_dir']/'freeze.json').write_text('{}')
    elif change=='baseline':
        (build['bundle_dir']/'manifest.json').write_text('{}')
    else:
        build['registry_manifest_path'].write_text(json.dumps({'concatenated_sha256':'00'*32}))
    before={p.name:p.read_bytes() for p in build['bundle_dir'].iterdir()}
    with pytest.raises(promote.PromotionError):promote.promote(**args)
    assert before=={p.name:p.read_bytes() for p in build['bundle_dir'].iterdir()}


def test_missing_compiler_evidence_refuses_freeze(build,tmp_path):
    manifest_path=build['release_dir']/'manifest.json'
    manifest=json.loads(manifest_path.read_text());manifest['vernacular_enrichment']={'enabled':True}
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(promote.PromotionError,match='incomplete'):_freeze(build,tmp_path/'frozen')
    assert not (tmp_path/'frozen').exists()


def test_freeze_failure_leaves_no_partial_set(build,tmp_path):
    receipt=tmp_path/'receipt.json'
    receipt.write_text(json.dumps({'validated':True,'sqlite_sha256':_sha(build['sqlite_path']),
        'compiler_manifest_sha256':_sha(build['release_dir']/'manifest.json')}))
    with pytest.raises(promote.PromotionError, match="missing evidence input"):
        promote.freeze(**build,output_dir=tmp_path/'frozen',validation_receipt_path=receipt,
                       evidence_inputs={'missing.json':tmp_path/'missing'})
    assert not (tmp_path/'frozen').exists()
    assert not list(tmp_path.glob('.freeze-*'))


def test_pinned_projection_count_mismatch_blocks_freeze(build,tmp_path):
    receipt=tmp_path/'receipt.json'
    receipt.write_text(json.dumps({'validated':True,'sqlite_sha256':_sha(build['sqlite_path']),
        'compiler_manifest_sha256':_sha(build['release_dir']/'manifest.json'),
        'expected_vernacular_enrichment':{'automatic':1619,'reviewed':17,'total':1636,'affected_concepts':766}}))
    (build['release_dir']/'vernacular_evidence.jsonl').write_text('')
    manifest=json.loads((build['release_dir']/'manifest.json').read_text())
    manifest['outputs']['evidence']={'name':'vernacular_evidence.jsonl','sha256':_sha(build['release_dir']/'vernacular_evidence.jsonl'),'bytes':0}
    (build['release_dir']/'manifest.json').write_text(json.dumps(manifest))
    r=json.loads(receipt.read_text());r['compiler_manifest_sha256']=_sha(build['release_dir']/'manifest.json');receipt.write_text(json.dumps(r))
    with pytest.raises(promote.PromotionError,match='counts disagree'):
        promote.freeze(**build,output_dir=tmp_path/'frozen',validation_receipt_path=receipt)
    assert not (tmp_path/'frozen').exists()

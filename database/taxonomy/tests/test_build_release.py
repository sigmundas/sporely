"""build_release.py: recipe, preflight, determinism gate and promotion gate."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "database/taxonomy/scripts"))

import build_release as br  # noqa: E402

RELEASE = "tax-2099.01.01-01"


def _write_recipe(path: Path, **overrides) -> Path:
    recipe = json.loads(br.DEFAULT_RECIPE.read_text(encoding="utf-8"))
    recipe.update(overrides)
    path.write_text(json.dumps(recipe), encoding="utf-8")
    return path


def _make_sqlite(path: Path, *, languages=("nb",), artportalen=0) -> None:
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY, source_system TEXT,
            taxonomic_status TEXT, norwegian_taxon_id INTEGER, swedish_taxon_id INTEGER,
            inaturalist_taxon_id INTEGER);
        CREATE TABLE vernacular_min (taxon_id INTEGER, language_code TEXT, vernacular_name TEXT);
        CREATE TABLE taxon_external_id_min (taxon_id INTEGER, source_system TEXT, external_id TEXT);
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
    """)
    conn.execute("INSERT INTO taxon_min VALUES (1, 'col_xr', 'accepted', 10, NULL, NULL)")
    conn.executemany("INSERT INTO vernacular_min VALUES (1, ?, 'x')", [(lang,) for lang in languages])
    conn.executemany("INSERT INTO taxon_external_id_min VALUES (1, 'artportalen', ?)",
                     [(str(i),) for i in range(artportalen)])
    conn.execute("INSERT INTO taxonomy_meta VALUES ('content_release_id', ?)", (RELEASE,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------------- recipe

def test_committed_recipe_loads():
    recipe = br.load_recipe(br.DEFAULT_RECIPE)
    assert [s.code for s in recipe.sources] == ["col_xr", "nortaxa"]
    assert recipe.legacy_enabled
    assert recipe.sources[0].manifest.endswith("2026-07-17-XR/manifest.json")


@pytest.mark.parametrize("overrides, message", [
    ({"format": "other"}, "format must be"),
    ({"sources": []}, "no sources"),
    ({"sources": [{"code": "dk", "normalizer": "national_source", "release_dir": "d", "profile": "p"}] * 2},
     "listed twice"),
    ({"sources": [{"code": "dk", "normalizer": "csv", "release_dir": "d"}]}, "normalizer must be"),
    ({"sources": [{"code": "col_xr", "normalizer": "col_xr", "release_dir": "d"}]}, "needs version"),
    ({"sources": [{"code": "Bad-Code", "normalizer": "col_xr", "release_dir": "d"}]}, "lowercase identifier"),
    ({"redlist_no": {"workbook": "w", "sha256": "abc"}}, "redlist_no.sha256"),
])
def test_recipe_rejects_invalid(tmp_path, overrides, message):
    with pytest.raises(br.BuildError, match=message):
        br.load_recipe(_write_recipe(tmp_path / "recipe.json", **overrides))


def test_manifest_archive_hash_shapes():
    sha = "ab" * 32
    assert br.manifest_archive_sha256({"download": {"sha256": sha}}) == sha
    assert br.manifest_archive_sha256({"download": {"archive_sha256": sha}}) == sha
    with pytest.raises(br.BuildError, match="pins no archive"):
        br.manifest_archive_sha256({"download": None})


# ------------------------------------------------------------- preflight

def test_preflight_refuses_an_archive_that_does_not_match_its_manifest(tmp_path):
    archives = tmp_path / "archives"
    for source in br.load_recipe(br.DEFAULT_RECIPE).sources:
        (archives / source.release_dir).mkdir(parents=True)
        (archives / source.archive).write_bytes(b"not the pinned archive")
    options = br.Options(release_id=RELEASE, build_dir=tmp_path / "build", archive_root=archives)
    with pytest.raises(br.BuildError, match="col_xr: archive SHA-256 .* does not match"):
        br.preflight(options, br.load_recipe(br.DEFAULT_RECIPE))
    assert not options.build_dir.exists()


@pytest.mark.parametrize("release_id, message", [
    ("tax-2026.9.1-1", "not tax-YYYY"),
    ("../escape", "not tax-YYYY"),
])
def test_preflight_refuses_bad_release_ids(tmp_path, release_id, message):
    options = br.Options(release_id=release_id, build_dir=tmp_path / "build")
    with pytest.raises(br.BuildError, match=message):
        br.preflight(options, br.load_recipe(br.DEFAULT_RECIPE))


def test_preflight_refuses_the_bundled_release_and_an_existing_build_dir(tmp_path):
    bundled = json.loads((br.DEFAULT_BUNDLE_DIR / "manifest.json").read_text())["content_release_id"]
    recipe = br.load_recipe(br.DEFAULT_RECIPE)
    with pytest.raises(br.BuildError, match="releases are immutable"):
        br.preflight(br.Options(release_id=bundled, build_dir=tmp_path / "b"), recipe)
    (tmp_path / "exists").mkdir()
    with pytest.raises(br.BuildError, match="already exists"):
        br.preflight(br.Options(release_id=RELEASE, build_dir=tmp_path / "exists"), recipe)


def test_preflight_checks_the_registry_before_any_archive(tmp_path):
    shards = tmp_path / "canonical"
    shards.mkdir()
    (shards / "part-0001.jsonl").write_bytes(b'{"a":1}\n')
    (shards / "manifest.json").write_text(json.dumps(
        {"shards": [{"name": "part-0001.jsonl"}], "concatenated_sha256": "00" * 32}))
    import os
    recipe = _write_recipe(tmp_path / "recipe.json", registry=os.path.relpath(shards, br.REPO_ROOT))
    options = br.Options(release_id=RELEASE, build_dir=tmp_path / "build",
                         archive_root=tmp_path / "no-archives")
    with pytest.raises(br.BuildError, match="registry shards concatenate to"):
        br.preflight(options, br.load_recipe(recipe))


def test_committed_registry_verifies():
    recipe = br.load_recipe(br.DEFAULT_RECIPE)
    manifest = json.loads((br.REPO_ROOT / recipe.registry / "manifest.json").read_text())
    assert br.verify_registry(br.REPO_ROOT / recipe.registry) == manifest["concatenated_sha256"]


def test_assemble_registry_verifies_the_concatenation(tmp_path):
    shards = tmp_path / "canonical"
    shards.mkdir()
    (shards / "part-0001.jsonl").write_bytes(b'{"a":1}\n')
    (shards / "manifest.json").write_text(json.dumps(
        {"shards": [{"name": "part-0001.jsonl"}], "concatenated_sha256": "00" * 32}))
    with pytest.raises(br.BuildError, match="concatenate to"):
        br.assemble_registry(shards, tmp_path / "registry.jsonl")


# ------------------------------------------------------------- pipeline

class FakeRunner:
    """Stands in for the pipeline scripts: records calls, writes plausible outputs."""

    def __init__(self, build_dir: Path, *, differ=False, allocate=False):
        self.build_dir, self.differ, self.allocate = build_dir, differ, allocate
        self.calls: list[tuple[str, list[str]]] = []

    def __call__(self, label: str, argv: list[str]) -> str:
        self.calls.append((label, argv))
        b = self.build_dir
        if label.startswith("compile"):
            name = label[-1]
            release = b / f"release{name}"
            release.mkdir()
            (release / "taxa.jsonl").write_text("B\n" if self.differ and name == "B" else "A\n")
            if self.allocate:
                with (b / f"registry-{name}.jsonl").open("a") as handle:
                    handle.write('{"new":1}\n')
        elif label.startswith("sqlite"):
            _make_sqlite(b / f"{RELEASE}-{label[-1]}.sqlite3")
            return json.dumps({"counts": {"taxon_min": 1}, "authoritative_bridge_emission": {}})
        return ""


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    monkeypatch.setattr(br, "preflight", lambda options, recipe: {"stubbed": True})

    def fake_registry(registry_dir, dest):
        dest.write_bytes(b'{"base":1}\n')
        return br.sha256_file(dest)

    monkeypatch.setattr(br, "assemble_registry", fake_registry)

    counter = iter(range(100))

    def run(*, differ=False, allocate=False, promote=False, recipe=br.DEFAULT_RECIPE, bundle_dir=None,
            expect=None):
        build_dir = tmp_path / f"build{next(counter)}"
        runner = FakeRunner(build_dir, differ=differ, allocate=allocate)
        options = br.Options(release_id=RELEASE, build_dir=build_dir, recipe_path=recipe,
                             bundle_dir=bundle_dir or tmp_path / "no-bundle", promote=promote,
                             expect_sqlite_sha256=expect)
        return br.build(options, run=runner), runner
    return run


def test_build_runs_the_pipeline_in_order_with_legacy_enrichment(pipeline):
    report, runner = pipeline()
    labels = [label for label, _ in runner.calls]
    assert labels == ["normalize_col_xr", "normalize_nortaxa", "normalize_redlist_no",
                      "export_legacy_enrichment", "compileA", "sqliteA", "compileB", "sqliteB"]
    compile_argv = dict(runner.calls)["compileA"]
    assert "--legacy-enrichment-input" in compile_argv
    manifests = [compile_argv[i + 1] for i, a in enumerate(compile_argv) if a == "--source-release-manifest"]
    assert manifests == ["col_xr=database/taxonomy/sources/col_xr/2026-07-17-XR/manifest.json",
                         "nortaxa=database/taxonomy/sources/nortaxa/1.284/manifest.json"]
    assert report["determinism"]["identical"] and report["registry"]["unchanged"]
    assert report["coverage"]["candidate"]["vernacular_by_language"] == {"nb": 1}
    assert not report["promoted"]
    assert json.loads((Path(runner.build_dir) / "build-report.json").read_text()) == report


def test_legacy_enrichment_can_be_switched_off(pipeline, tmp_path):
    recipe = _write_recipe(tmp_path / "recipe.json", legacy_enrichment={"enabled": False})
    _, runner = pipeline(recipe=recipe)
    assert "export_legacy_enrichment" not in [label for label, _ in runner.calls]
    assert "--legacy-enrichment-input" not in dict(runner.calls)["compileA"]


def test_a_nondeterministic_build_is_reported_and_never_promoted(pipeline):
    report, _ = pipeline(differ=True)
    assert report["determinism"]["mismatches"] == ["taxa.jsonl"]
    assert not report["determinism"]["identical"]
    with pytest.raises(br.BuildError, match="builds differ"):
        pipeline(differ=True, promote=True)


def test_new_registry_allocations_block_promotion(pipeline):
    with pytest.raises(br.BuildError, match="allocated 1 new IDs"):
        pipeline(allocate=True, promote=True)


def test_promotion_runs_last_for_a_clean_build(pipeline):
    report, runner = pipeline(promote=True)
    label, argv = runner.calls[-1]
    assert label == "promote" and report["promoted"]
    assert argv[argv.index("--sqlite") + 1].endswith(f"{RELEASE}-A.sqlite3")


def test_an_unreadable_bundled_release_is_a_clean_error(pipeline, tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "manifest.json").write_text(json.dumps(
        {"content_release_id": "tax-old", "gz_artifact": "tax-old.sqlite3.gz"}))
    (bundle / "tax-old.sqlite3.gz").write_bytes(b"not gzip")
    with pytest.raises(br.BuildError, match="--no-baseline"):
        pipeline(bundle_dir=bundle)


def test_the_bundled_release_is_the_comparison_baseline(pipeline, tmp_path):
    import gzip
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    _make_sqlite(tmp_path / "old.sqlite3", languages=("nb", "nn"))
    (bundle / "tax-old.sqlite3.gz").write_bytes(gzip.compress((tmp_path / "old.sqlite3").read_bytes()))
    (bundle / "manifest.json").write_text(json.dumps(
        {"content_release_id": "tax-old", "gz_artifact": "tax-old.sqlite3.gz"}))
    report, _ = pipeline(bundle_dir=bundle)
    assert report["coverage"]["baseline_release_id"] == "tax-old"
    assert report["coverage"]["delta"] == {
        "vernacular_by_language.nn": {"baseline": 1, "candidate": None}}


# -------------------------------------------------------------- coverage

def test_coverage_delta_shows_what_a_release_gains(tmp_path):
    before, after = tmp_path / "before.sqlite3", tmp_path / "after.sqlite3"
    _make_sqlite(before, languages=("nb", "nn"))
    _make_sqlite(after, languages=("nb", "nn", "sv"), artportalen=2)
    delta = br.coverage_delta(br.summarize_sqlite(before), br.summarize_sqlite(after))
    assert delta == {
        "vernacular_by_language.sv": {"baseline": None, "candidate": 1},
        "external_ids_by_source.artportalen": {"baseline": None, "candidate": 2},
    }


# ------------------------------------------------- publishing-id inputs

def test_recipe_pins_publishing_inputs_and_the_build_passes_them_on(pipeline, tmp_path):
    overlay = "database/taxonomy/overlays/artportalen-publishing.json"
    sha = br.sha256_file(br.REPO_ROOT / overlay)
    recipe = _write_recipe(tmp_path / "recipe.json",
                           publishing_overlays={"artportalen": {"path": overlay, "sha256": sha}},
                           inaturalist_refresh={"path": "some/refresh.json", "sha256": "ab" * 32})
    loaded = br.load_recipe(recipe)
    assert loaded.artportalen_overlay == {"path": overlay, "sha256": sha}
    _, runner = pipeline(recipe=recipe)
    argv = dict(runner.calls)["sqliteA"]
    assert argv[argv.index("--publishing-overlay") + 1] == overlay
    assert argv[argv.index("--inaturalist-refresh") + 1] == "some/refresh.json"


def test_a_publishing_input_must_match_its_pin():
    overlay = "database/taxonomy/overlays/artportalen-publishing.json"
    sha = br.sha256_file(br.REPO_ROOT / overlay)
    assert br.verify_pinned("artportalen_overlay", {"path": overlay, "sha256": sha}) == sha
    with pytest.raises(br.BuildError, match="does not match the recipe"):
        br.verify_pinned("artportalen_overlay", {"path": overlay, "sha256": "00" * 32})
    with pytest.raises(br.BuildError, match="missing"):
        br.verify_pinned("inaturalist_refresh", {"path": "no/such.json", "sha256": "00" * 32})


def test_promotion_requires_equality_with_an_earlier_independent_run(pipeline):
    first, _ = pipeline()
    second, runner = pipeline(promote=True, expect=first["determinism"]["sqlite_sha256"])
    assert second["matches_expected"] and second["promoted"]
    with pytest.raises(br.BuildError, match="differs from the earlier run"):
        pipeline(promote=True, expect="00" * 32)

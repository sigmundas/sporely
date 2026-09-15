"""Coverage for the reusable deterministic Qt review renderer."""
from __future__ import annotations

import json
import os
import socket
import subprocess
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QCoreApplication
from PySide6.QtWidgets import QApplication

from tools.review_ui.context import ReviewContext
from tools.review_ui.registry import ReviewScenario, ScenarioRegistry
from tools.review_ui.runner import render_scenarios
from tools.review_ui.scenarios import create_registry


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / ".venv" / "bin" / "python"
REFERENCE_IDS = {
    "reference.add-range",
    "reference.reported-statistics",
    "reference.measurement-set-form-enhanced",
    "reference.raw-points",
    "reference.existing-measurement-set",
    "reference.new-publication",
    "reference.library-manager",
    "reference.no-taxon",
    "reference.parmasto",
    "reference.attach-taxon-filter",
    "reference.nb-no",
    "reference.dark",
    "reference.community-preview",
    "reference.community-preview-dark",
    "reference.comparison-list",
    "reference.comparison-list-dark",
    "reference.comparison-list-overflow",
    "reference.comparison-list-longnames",
    "reference.add-dialog-library",
    "reference.add-dialog-library-dark",
    "reference.add-dialog-library-empty",
    "reference.add-dialog-library-empty-dark",
    "reference.add-dialog-myobs",
    "reference.add-dialog-myobs-dark",
    "reference.add-dialog-community",
    "reference.add-dialog-community-dark",
    "reference.add-dialog-community-points",
    "reference.add-dialog-community-empty",
    "reference.comparison-list-suppressed",
    "reference.comparison-list-colors",
    "reference.add-dialog-default-size",
    "reference.add-dialog-taxon-selector",
    "reference.fix2-suppressed-light",
    "reference.fix2-suppressed-dark",
    "reference.fix2-colors-light",
    "reference.fix2-colors-dark",
    "reference.fix2-natural-light",
    "reference.fix2-natural-dark",
    "reference.fix2-popup-light",
    "reference.fix2-popup-dark",
    "reference.fix2-hint-nb-no",
    "reference.fix2-selector-nb-no",
    "reference.add-dialog-manual-range",
    "reference.add-dialog-manual-range-dark",
    "reference.add-dialog-manual-points",
    "reference.add-dialog-manual-points-dark",
    "reference.add-dialog-manual-invalid",
    "reference.add-dialog-manual-invalid-dark",
    "reference.add-dialog-manual-nb-no",
    "reference.add-dialog-manual-species-mean",
    "reference.provenance-preview",
    "reference.provenance-preview-method",
    "reference.provenance-preview-nb-no",
    "reference.provenance-preview-dark",
    "reference.analysis-panel-empty",
    "reference.analysis-panel-populated",
    "reference.analysis-panel-suppressed",
    "reference.analysis-panel-longnames",
    "reference.analysis-panel-dark",
    "reference.analysis-panel-nb-no",
}
CONFLICT_IDS = {
    "conflict.local-changes",
    "conflict.field",
    "conflict.local-cloud-images",
    "conflict.geometry",
    "conflict.possible-match",
    "conflict.identity",
    "conflict.incomplete-plan",
    "conflict.progress",
    "conflict.image-order",
    "conflict.light",
    "conflict.dark",
}
OTHER_UI_IDS = {
    "measure.metadata-tags",
    "observations.image-metadata-tags",
    "raw-processing.methods",
    "portable-import.all-selected",
    "portable-import.subset",
}
ALL_IDS = REFERENCE_IDS | CONFLICT_IDS | OTHER_UI_IDS


def _run(*arguments: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["QT_QPA_PLATFORM"] = "offscreen"
    return subprocess.run(
        [str(PYTHON), "-m", "tools.render_review_screenshots", *arguments],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )


def test_registry_has_unique_semantic_ids_and_expected_groups() -> None:
    registry = create_registry()
    ids = [scenario.id for scenario in registry.all()]
    assert len(ids) == len(set(ids))
    assert set(registry.groups()) == {
        "conflict", "measure", "observations", "portable-import",
        "raw-processing", "reference-library",
    }
    assert {scenario.id for scenario in registry.all()} == ALL_IDS


def test_group_and_explicit_scenario_selection() -> None:
    registry = create_registry()
    assert {s.id for s in registry.select(groups=("conflict",))} == CONFLICT_IDS
    assert {
        s.id for s in registry.select(groups=("reference-library",))
    } == REFERENCE_IDS
    assert [
        s.id
        for s in registry.select(
            scenario_ids=("reference.add-range", "reference.dark")
        )
    ] == ["reference.add-range", "reference.dark"]


@pytest.mark.parametrize(
    ("groups", "scenario_ids", "message"),
    [
        (("missing",), (), "unknown scenario group(s): missing"),
        ((), ("missing.state",), "unknown scenario ID(s): missing.state"),
    ],
)
def test_unknown_selection_fails_clearly(groups, scenario_ids, message) -> None:
    with pytest.raises(ValueError, match=r"^" + message.replace("(", r"\(").replace(")", r"\)") + r"$"):
        create_registry().select(groups=groups, scenario_ids=scenario_ids)


def test_duplicate_and_unsafe_scenario_ids_are_rejected() -> None:
    scenario = ReviewScenario(
        id="sample.state",
        group="sample",
        title="Sample",
        description="Sample state",
        viewport=(100, 100),
        build=lambda context: context.host,
    )
    registry = ScenarioRegistry()
    registry.register(scenario)
    with pytest.raises(ValueError, match="duplicate review scenario ID"):
        registry.register(scenario)
    with pytest.raises(ValueError, match="invalid semantic scenario ID"):
        ReviewScenario(
            id="../escape",
            group="sample",
            title="Unsafe",
            description="Unsafe state",
            viewport=(100, 100),
            build=lambda context: context.host,
        )


def test_list_exposes_registered_groups_and_scenarios() -> None:
    result = _run("--list", timeout=30)
    assert result.returncode == 0, result.stderr
    assert "conflict:" in result.stdout
    assert "reference-library:" in result.stdout
    for scenario_id in ALL_IDS:
        assert scenario_id in result.stdout


def test_default_renderer_emits_central_manifest_and_all_established_states(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "screens"
    result = _run(str(output_dir))
    assert result.returncode == 0, result.stderr

    manifest_path = output_dir / "manifest.json"
    assert manifest_path.is_file()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    screens = manifest["screens"]
    assert {screen["id"] for screen in screens} == ALL_IDS
    assert len(screens) == len(ALL_IDS)

    for screen in screens:
        assert screen["title"].strip()
        assert screen["description"].strip()
        assert screen["viewport"].strip()
        relative = Path(screen["path"])
        assert relative == Path(f"{screen['id']}.png")
        assert not relative.is_absolute()
        assert ".." not in relative.parts
        image = (output_dir / relative).resolve()
        assert image.parent == output_dir.resolve()
        assert image.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")


def test_real_theme_locale_network_and_temporary_lifetime(monkeypatch) -> None:
    app = QApplication.instance() or QApplication([])
    calls: list[tuple[str, str]] = []

    import ui.styles as styles

    real_apply_palette = styles.apply_palette
    real_get_style = styles.get_style

    def recording_palette(theme: str) -> None:
        calls.append(("palette", theme))
        real_apply_palette(theme)

    def recording_style(theme: str) -> str:
        calls.append(("style", theme))
        return real_get_style(theme)

    monkeypatch.setattr(styles, "apply_palette", recording_palette)
    monkeypatch.setattr(styles, "get_style", recording_style)

    with ReviewContext(app) as context:
        temporary_root = context.temporary_root
        assert temporary_root is not None and temporary_root.is_dir()
        context.set_theme("light")
        context.set_theme("dark")
        assert app.palette().window().color().lightness() < 128
        with context.locale("nb_NO"):
            assert (
                QCoreApplication.translate(
                    "ReferenceAddDialog", "Use existing measurement set"
                )
                == "Bruk eksisterende målesett"
            )
        with pytest.raises(RuntimeError, match="network access is forbidden"):
            socket.create_connection(("127.0.0.1", 9))

    assert calls == [
        ("palette", "light"),
        ("style", "light"),
        ("palette", "dark"),
        ("style", "dark"),
    ]
    assert temporary_root is not None and not temporary_root.exists()


def test_conflict_scenario_reaches_its_meaningful_loaded_state() -> None:
    app = QApplication.instance() or QApplication([])
    scenario = create_registry().select(
        scenario_ids=("conflict.local-changes",)
    )[0]
    with ReviewContext(app) as context:
        context.set_theme(scenario.theme)
        dialog = scenario.build(context)
        try:
            assert dialog._current_detail is not None
            assert dialog._title_label.text() != "Loading comparison…"
            assert dialog._choice_specs
        finally:
            dialog.close()
            dialog.deleteLater()
            app.processEvents()


def test_failed_scenario_names_its_id_and_does_not_publish_manifest(tmp_path) -> None:
    output_dir = tmp_path / "failed-review"
    output_dir.mkdir()
    (output_dir / "manifest.json").write_text("stale", encoding="utf-8")

    def broken(_context):
        raise LookupError("fixture missing")

    scenario = ReviewScenario(
        id="sample.broken",
        group="sample",
        title="Broken sample",
        description="Exercise scenario failure attribution.",
        viewport=(100, 100),
        build=broken,
    )
    with pytest.raises(
        RuntimeError,
        match=r"sample\.broken: failed to construct widget: fixture missing",
    ):
        render_scenarios((scenario,), output_dir)
    assert not (output_dir / "manifest.json").exists()


def test_capture_natural_size_preserves_builder_initial_size(tmp_path):
    from PySide6.QtWidgets import QWidget
    from PySide6.QtGui import QImage

    def build(context):
        widget = QWidget(context.host)
        widget.resize(321, 123)
        return widget

    scenario = ReviewScenario(
        id="test.natural", group="test", title="Natural size",
        description="Initial dimensions survive capture.", build=build,
        viewport=(900, 700), natural_size=True,
    )
    manifest = render_scenarios([scenario], tmp_path)
    assert manifest["screens"][0]["viewport"] == "321x123"
    image = QImage(str(tmp_path / scenario.filename))
    assert (image.width(), image.height()) == (321, 123)


def test_capture_open_popup_uses_popup_surface(tmp_path):
    from PySide6.QtWidgets import QComboBox
    from PySide6.QtGui import QImage

    size = []

    def build(context):
        combo = QComboBox(context.host)
        combo.addItems(["Own observation", "Long vernacular æøå 82%", "Candidate 41%"])
        return combo

    def popup(combo):
        combo.showPopup()
        surface = combo.view().window()
        assert surface.isVisible()
        size.append((surface.width(), surface.height()))
        return surface

    scenario = ReviewScenario(
        id="test.popup", group="test", title="Open popup",
        description="Capture the actual separate popup window.", build=build,
        viewport=(500, 40), capture_target=popup,
    )
    manifest = render_scenarios([scenario], tmp_path)
    image = QImage(str(tmp_path / scenario.filename))
    assert (image.width(), image.height()) == size[0]
    assert manifest["screens"][0]["viewport"] == f"{size[0][0]}x{size[0][1]}"

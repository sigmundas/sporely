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
    "reference.raw-points",
    "reference.existing-measurement-set",
    "reference.new-publication",
    "reference.no-taxon",
    "reference.parmasto",
    "reference.attach-taxon-filter",
    "reference.nb-no",
    "reference.dark",
}
CONFLICT_IDS = {
    "conflict.local-changes",
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
    assert set(registry.groups()) == {"conflict", "reference-library"}
    assert {scenario.id for scenario in registry.all()} == REFERENCE_IDS | CONFLICT_IDS


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
    for scenario_id in REFERENCE_IDS | CONFLICT_IDS:
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
    assert {screen["id"] for screen in screens} == REFERENCE_IDS | CONFLICT_IDS
    assert len(screens) == len(REFERENCE_IDS | CONFLICT_IDS)

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

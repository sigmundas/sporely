from pathlib import Path

import pytest

from tools.runtime_assets import (
    REQUIRED_RUNTIME_ASSETS,
    pyinstaller_add_data,
    verify_artifact_assets,
)


def test_required_runtime_assets_cover_startup_icons_and_fonts():
    assert "assets/icons/icon_new.svg" in REQUIRED_RUNTIME_ASSETS
    assert "assets/icons/calibration.svg" in REQUIRED_RUNTIME_ASSETS
    assert "assets/icons/checkmark_white.svg" in REQUIRED_RUNTIME_ASSETS
    assert "assets/fonts/Inter_18pt-Regular.ttf" in REQUIRED_RUNTIME_ASSETS
    assert "assets/fonts/Manrope-Bold.ttf" in REQUIRED_RUNTIME_ASSETS


@pytest.mark.parametrize("separator", [":", ";"])
def test_pyinstaller_configuration_packages_the_runtime_asset_tree(separator):
    assert pyinstaller_add_data(separator) == f"assets{separator}assets"


@pytest.mark.parametrize("contents_directory", ["", "_internal"])
def test_artifact_validation_accepts_required_assets(contents_directory, tmp_path):
    artifact_root = tmp_path / "artifact"
    runtime_root = artifact_root / contents_directory if contents_directory else artifact_root
    for relative_path in REQUIRED_RUNTIME_ASSETS:
        path = runtime_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"runtime asset")

    assert verify_artifact_assets(artifact_root) == runtime_root / "assets"


@pytest.mark.parametrize(
    "missing_asset",
    ["assets/icons/icon_new.svg", "assets/fonts/Inter_18pt-Regular.ttf"],
)
def test_artifact_validation_fails_closed_for_missing_required_asset(
    missing_asset, tmp_path
):
    artifact_root = tmp_path / "artifact"
    for relative_path in REQUIRED_RUNTIME_ASSETS:
        if relative_path == missing_asset:
            continue
        path = artifact_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"runtime asset")

    with pytest.raises(FileNotFoundError, match=Path(missing_asset).name):
        verify_artifact_assets(artifact_root)


def test_all_desktop_builds_use_shared_asset_configuration_and_artifact_gate():
    root = Path(__file__).resolve().parents[1]
    expected = {
        "build.ps1": (
            'pyinstaller-add-data --separator ";"',
            "verify-artifact --artifact-root dist\\Sporely",
        ),
        "build_linux.sh": (
            "pyinstaller-add-data --separator ':'",
            "verify-artifact --artifact-root dist/Sporely",
        ),
        "build_mac.sh": (
            "pyinstaller-add-data --separator ':'",
            "--artifact-root dist/Sporely.app/Contents/Frameworks",
        ),
    }

    for script_name, required_commands in expected.items():
        script = (root / script_name).read_text(encoding="utf-8")
        for command in required_commands:
            assert command in script, f"{script_name} must run {command}"

    workflow = (root / ".github/workflows/release.yml").read_text(encoding="utf-8")
    assert "run: ./build.ps1" in workflow
    assert "./build_linux.sh" in workflow
    assert "./build_mac.sh" in workflow

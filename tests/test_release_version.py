import plistlib
from pathlib import Path

import pytest

from tools.release_version import (
    normalize_tag,
    read_app_version,
    require_matching_release,
    set_macos_bundle_version,
    verify_macos_bundle_version,
)


def test_release_tag_normalizes_to_package_version():
    assert normalize_tag("v0.9.20") == "0.9.20"


def test_release_tag_must_match_app_version(tmp_path):
    app_file = tmp_path / "main.py"
    app_file.write_text('APP_VERSION = "0.9.20"\n', encoding="utf-8")

    assert require_matching_release("v0.9.20", app_file) == "0.9.20"
    with pytest.raises(ValueError, match="does not match"):
        require_matching_release("v0.9.21", app_file)


def test_app_version_is_read_without_importing_main(tmp_path):
    app_file = tmp_path / "main.py"
    app_file.write_text(
        'raise RuntimeError("must not import")\nAPP_VERSION = "0.9.20"\n',
        encoding="utf-8",
    )

    assert read_app_version(app_file) == "0.9.20"


def test_macos_bundle_metadata_receives_expected_version(tmp_path):
    plist_path = tmp_path / "Info.plist"
    with plist_path.open("wb") as stream:
        plistlib.dump({"CFBundleName": "Sporely"}, stream)

    set_macos_bundle_version(plist_path, "0.9.20")
    verify_macos_bundle_version(plist_path, "0.9.20")

    with plist_path.open("rb") as stream:
        plist = plistlib.load(stream)
    assert plist["CFBundleShortVersionString"] == "0.9.20"
    assert plist["CFBundleVersion"] == "0.9.20"


@pytest.mark.parametrize(
    "plist, message",
    [
        ({"CFBundleVersion": "0.9.20"}, "CFBundleShortVersionString"),
        (
            {"CFBundleShortVersionString": "0.9.20", "CFBundleVersion": "0.9.19"},
            "CFBundleVersion",
        ),
    ],
)
def test_missing_or_mismatched_bundle_version_fails(tmp_path, plist, message):
    plist_path = tmp_path / "Info.plist"
    with plist_path.open("wb") as stream:
        plistlib.dump(plist, stream)

    with pytest.raises(ValueError, match=message):
        verify_macos_bundle_version(plist_path, "0.9.20")


def test_all_release_packages_use_the_validated_tag_version():
    root = Path(__file__).resolve().parents[1]
    workflow = (root / ".github/workflows/release.yml").read_text(encoding="utf-8")
    build_mac = (root / "build_mac.sh").read_text(encoding="utf-8")

    validated_version = "${{ needs.validate-version.outputs.version }}"
    assert f'$version = "{validated_version}"' in workflow
    assert f'version="{validated_version}"' in workflow
    assert f"SPORELY_BUILD_VERSION: {validated_version}" in workflow
    assert "check-macos-bundle" in workflow
    assert "set-macos-bundle" in build_mac
    assert "codesign --force --deep --sign - dist/Sporely.app" in build_mac

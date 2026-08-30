#!/usr/bin/env python3
"""Define and validate runtime assets included in packaged desktop builds."""

from __future__ import annotations

import argparse
from pathlib import Path


REQUIRED_RUNTIME_ASSETS: tuple[str, ...] = (
    "assets/icons/icon_new.svg",
    "assets/icons/calibration.svg",
    "assets/icons/checkmark_white.svg",
    "assets/fonts/Inter_18pt-Regular.ttf",
    "assets/fonts/Manrope-Bold.ttf",
)


def pyinstaller_add_data(separator: str) -> str:
    if separator not in {":", ";"}:
        raise ValueError("PyInstaller data separator must be ':' or ';'")
    return f"assets{separator}assets"


def verify_artifact_assets(artifact_root: Path) -> Path:
    roots = (artifact_root, artifact_root / "_internal")
    failures: list[tuple[Path, list[str]]] = []
    for root in roots:
        missing = [
            relative_path
            for relative_path in REQUIRED_RUNTIME_ASSETS
            if not (root / relative_path).is_file()
        ]
        if not missing:
            return root / "assets"
        failures.append((root, missing))

    details = "; ".join(
        f"{root}: {', '.join(missing)}" for root, missing in failures
    )
    raise FileNotFoundError(f"required runtime assets missing from artifact: {details}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_data = subparsers.add_parser("pyinstaller-add-data")
    add_data.add_argument("--separator", choices=(":", ";"), required=True)

    verify = subparsers.add_parser("verify-artifact")
    verify.add_argument("--artifact-root", type=Path, required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "pyinstaller-add-data":
        print(pyinstaller_add_data(args.separator))
    elif args.command == "verify-artifact":
        asset_root = verify_artifact_assets(args.artifact_root)
        print(f"Verified required runtime assets in {asset_root}")


if __name__ == "__main__":
    main()

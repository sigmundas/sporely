#!/usr/bin/env python3
"""Validate release versions and macOS bundle metadata without importing the app."""

from __future__ import annotations

import argparse
import ast
import plistlib
import re
from pathlib import Path


VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")


def normalize_tag(tag: str) -> str:
    value = tag.removeprefix("v")
    if not tag.startswith("v") or not VERSION_RE.fullmatch(value):
        raise ValueError(f"release tag must match vMAJOR.MINOR.PATCH: {tag!r}")
    return value


def read_app_version(path: Path) -> str:
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for statement in module.body:
        if not isinstance(statement, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "APP_VERSION" for target in statement.targets):
            continue
        if isinstance(statement.value, ast.Constant) and isinstance(statement.value.value, str):
            version = statement.value.value
            if VERSION_RE.fullmatch(version):
                return version
            raise ValueError(f"APP_VERSION must match MAJOR.MINOR.PATCH: {version!r}")
    raise ValueError(f"APP_VERSION string assignment not found in {path}")


def require_matching_release(tag: str, app_file: Path) -> str:
    tag_version = normalize_tag(tag)
    app_version = read_app_version(app_file)
    if tag_version != app_version:
        raise ValueError(
            f"release tag version {tag_version} does not match APP_VERSION {app_version}"
        )
    return tag_version


def set_macos_bundle_version(plist_path: Path, version: str) -> None:
    if not VERSION_RE.fullmatch(version):
        raise ValueError(f"bundle version must match MAJOR.MINOR.PATCH: {version!r}")
    with plist_path.open("rb") as stream:
        plist = plistlib.load(stream)
    plist["CFBundleShortVersionString"] = version
    plist["CFBundleVersion"] = version
    with plist_path.open("wb") as stream:
        plistlib.dump(plist, stream, sort_keys=False)


def verify_macos_bundle_version(plist_path: Path, expected: str) -> None:
    if not VERSION_RE.fullmatch(expected):
        raise ValueError(f"expected version must match MAJOR.MINOR.PATCH: {expected!r}")
    with plist_path.open("rb") as stream:
        plist = plistlib.load(stream)
    for key in ("CFBundleShortVersionString", "CFBundleVersion"):
        actual = plist.get(key)
        if actual != expected:
            raise ValueError(f"{key} is {actual!r}; expected {expected!r}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check-release")
    check.add_argument("--tag", required=True)
    check.add_argument("--app-file", type=Path, required=True)

    app_version = subparsers.add_parser("app-version")
    app_version.add_argument("--app-file", type=Path, required=True)

    set_bundle = subparsers.add_parser("set-macos-bundle")
    set_bundle.add_argument("--plist", type=Path, required=True)
    set_bundle.add_argument("--version", required=True)

    check_bundle = subparsers.add_parser("check-macos-bundle")
    check_bundle.add_argument("--plist", type=Path, required=True)
    check_bundle.add_argument("--expected", required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "check-release":
        print(require_matching_release(args.tag, args.app_file))
    elif args.command == "app-version":
        print(read_app_version(args.app_file))
    elif args.command == "set-macos-bundle":
        set_macos_bundle_version(args.plist, args.version)
    elif args.command == "check-macos-bundle":
        verify_macos_bundle_version(args.plist, args.expected)


if __name__ == "__main__":
    main()

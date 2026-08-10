"""Canonical CLI for deterministic, scenario-based Sporely UI review evidence.

Examples:

    QT_QPA_PLATFORM=offscreen ./.venv/bin/python -m tools.render_review_screenshots /tmp/review
    ./.venv/bin/python -m tools.render_review_screenshots --list
    ./.venv/bin/python -m tools.render_review_screenshots --group conflict /tmp/conflicts
    ./.venv/bin/python -m tools.render_review_screenshots \
        --scenario reference.add-range --scenario reference.dark /tmp/references
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tools.review_ui.runner import render_scenarios
from tools.review_ui.scenarios import create_registry


DEFAULT_OUTPUT_DIR = Path("/tmp/sporely-review-screens")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render deterministic production Qt widgets for UI review."
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"artifact directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--group",
        action="append",
        default=[],
        help="render a registered scenario group; may be repeated",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        help="render one semantic scenario ID; may be repeated",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="list registered groups and scenarios without rendering",
    )
    return parser


def _print_registry() -> None:
    registry = create_registry()
    for group in registry.groups():
        print(f"{group}:")
        for scenario in registry.all():
            if scenario.group == group:
                default = " [default]" if scenario.default else ""
                print(f"  {scenario.id}{default} — {scenario.title}")


def render(
    output_dir: Path,
    *,
    groups: tuple[str, ...] = (),
    scenario_ids: tuple[str, ...] = (),
) -> dict:
    registry = create_registry()
    scenarios = registry.select(groups=groups, scenario_ids=scenario_ids)
    return render_scenarios(scenarios, output_dir)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.list:
        _print_registry()
        return 0
    try:
        manifest = render(
            args.output_dir,
            groups=tuple(args.group),
            scenario_ids=tuple(args.scenario),
        )
    except (ValueError, RuntimeError) as error:
        parser.error(str(error))
    print(
        f"wrote {len(manifest['screens'])} screenshots and manifest under "
        f"{args.output_dir.resolve()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

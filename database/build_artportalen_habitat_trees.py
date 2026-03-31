"""Build partial Artportalen habitat trees from captured request/response text files.

Usage:
    python3 database/build_artportalen_habitat_trees.py \
        --biotope-source "/Users/sigmundas/Documents/Code/Artportalen/artportalen biotop.txt" \
        --substrate-source "/Users/sigmundas/Documents/Code/Artportalen/artportalen substrate.txt"
"""

from __future__ import annotations

import argparse
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_BIOTOPE_SOURCE = Path("/Users/sigmundas/Documents/Code/Artportalen/artportalen biotop.txt")
DEFAULT_SUBSTRATE_SOURCE = Path("/Users/sigmundas/Documents/Code/Artportalen/artportalen substrate.txt")
DEFAULT_BIOTOPE_OUTPUT = SCRIPT_DIR / "artportalen_biotopes_tree.json"
DEFAULT_SUBSTRATE_OUTPUT = SCRIPT_DIR / "artportalen_substrate_tree.json"

ITEM_RE = re.compile(r'<span class="itemjson">(\{.*?\})</span>')
REQUEST_RE = re.compile(
    r"GET /SimplePicker/(?P<endpoint>RenderTopLevelBiotopes|RenderTopLevelSubstrates|RenderBiotopeChildren|RenderSubstrateChildren)"
    r"(?:\?parentId=(?P<parent_id>\d+))? HTTP/1\.[01]"
)


def _extract_request_blocks(text: str) -> list[tuple[str, int | None, str]]:
    blocks: list[tuple[str, int | None, str]] = []
    matches = list(REQUEST_RE.finditer(text))
    for idx, match in enumerate(matches):
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        endpoint = match.group("endpoint")
        parent_raw = match.group("parent_id")
        parent_id = int(parent_raw) if parent_raw else None
        body = text[start:end]
        html_split = body.split("\n\n", 1)
        html = html_split[1] if len(html_split) == 2 else body
        blocks.append((endpoint, parent_id, html))
    return blocks


def _extract_nodes(html: str) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for raw_json in ITEM_RE.findall(html or ""):
        try:
            item = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        try:
            node_id = int(item.get("id"))
        except (TypeError, ValueError):
            continue
        if node_id in seen_ids:
            continue
        seen_ids.add(node_id)
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        nodes.append({"id": node_id, "name": name, "children": []})
    return nodes


def _assemble_tree(blocks: list[tuple[str, int | None, str]], top_level_endpoint: str) -> list[dict[str, Any]]:
    roots: list[dict[str, Any]] = []
    child_map: dict[int, list[dict[str, Any]]] = {}
    for endpoint, parent_id, html in blocks:
        nodes = _extract_nodes(html)
        if endpoint == top_level_endpoint:
            if not roots and nodes:
                roots = nodes
            continue
        if parent_id is not None and nodes:
            child_map[parent_id] = nodes

    def attach(node: dict[str, Any]) -> dict[str, Any]:
        attached = {"id": int(node["id"]), "name": str(node["name"]), "children": []}
        attached["children"] = [attach(child) for child in child_map.get(attached["id"], [])]
        return attached

    return [attach(root) for root in roots]


def _merge_root_and_child_capture(roots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse duplicate child nodes when a capture includes both root and child response lists."""
    merged: list[dict[str, Any]] = []
    seen_root_ids: set[int] = set()
    for root in roots:
        try:
            root_id = int(root.get("id"))
        except (TypeError, ValueError):
            continue
        if root_id in seen_root_ids:
            continue
        seen_root_ids.add(root_id)
        merged.append(deepcopy(root))
    return merged


def build_tree(source_path: Path, top_level_endpoint: str) -> list[dict[str, Any]]:
    text = source_path.read_text(encoding="utf-8", errors="ignore")
    blocks = _extract_request_blocks(text)
    roots = _assemble_tree(blocks, top_level_endpoint=top_level_endpoint)
    return _merge_root_and_child_capture(roots)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build partial Artportalen habitat JSON trees from capture files.")
    parser.add_argument("--biotope-source", type=Path, default=DEFAULT_BIOTOPE_SOURCE)
    parser.add_argument("--substrate-source", type=Path, default=DEFAULT_SUBSTRATE_SOURCE)
    parser.add_argument("--biotope-output", type=Path, default=DEFAULT_BIOTOPE_OUTPUT)
    parser.add_argument("--substrate-output", type=Path, default=DEFAULT_SUBSTRATE_OUTPUT)
    args = parser.parse_args()

    biotope_tree = build_tree(args.biotope_source, top_level_endpoint="RenderTopLevelBiotopes")
    substrate_tree = build_tree(args.substrate_source, top_level_endpoint="RenderTopLevelSubstrates")

    args.biotope_output.write_text(
        json.dumps(biotope_tree, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    args.substrate_output.write_text(
        json.dumps(substrate_tree, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(
        f"Wrote {args.biotope_output} ({len(biotope_tree)} roots) and "
        f"{args.substrate_output} ({len(substrate_tree)} roots)"
    )


if __name__ == "__main__":
    main()

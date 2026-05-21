import json
import sys
import time
import requests
from bs4 import BeautifulSoup
from typing import Dict, Any, List, Optional

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.reference_data_paths import REFERENCE_DATA_GENERATED_DIR

BASE = "https://www.artsobservasjoner.no"
REPORT_ECOLOGY = f"{BASE}/SubmitSighting/ReportEcology/"
SUBSTRATE_CHILDREN = f"{BASE}/SimplePicker/RenderSubstrateChildren"

TOP_NAMES = [
    "Mark/bunn",
    "Organismer som livsmedium",
    "Dyr",
    "Menneskeskapt livsmedium",
]

def fetch(session: requests.Session, url: str, params=None) -> str:
    r = session.get(url, params=params, headers={"X-Requested-With": "XMLHttpRequest"})
    r.raise_for_status()
    return r.text

def parse_items(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for li in soup.select("li.listitem"):
        a = li.find("a")
        if not a or not a.get("data-id"):
            continue
        node_id = int(a["data-id"])

        # Prefer embedded JSON payload if present
        itemjson = li.select_one("span.itemjson")
        if itemjson and itemjson.get_text(strip=True).startswith("{"):
            try:
                payload = json.loads(itemjson.get_text(strip=True))
                name = payload.get("name") or li.get_text(" ", strip=True)
            except Exception:
                name = li.get_text(" ", strip=True)
        else:
            name_span = li.select_one("span.itemname")
            name = name_span.get_text(strip=True) if name_span else li.get_text(" ", strip=True)

        is_leaf = li.select_one("span.node-leaf") is not None
        out.append({"id": node_id, "name": name.strip(), "is_leaf": is_leaf})
    return out

def build_tree_from_children(session: requests.Session, parent_id: int, delay_s: float = 0.05) -> List[Dict[str, Any]]:
    html = fetch(session, SUBSTRATE_CHILDREN, params={
        "parentId": str(parent_id),
        "onlyReportable": "false",
        "dontIncludeSubSpecies": "true",
    })
    items = parse_items(html)

    out = []
    for it in items:
        node = {"id": it["id"], "name": it["name"]}
        if not it["is_leaf"]:
            time.sleep(delay_s)
            node["children"] = build_tree_from_children(session, it["id"], delay_s=delay_s)
        out.append(node)
    return out

def extract_livsmedium_top_ids(report_ecology_html: str) -> Dict[str, int]:
    """
    Find IDs for the top-level livsmedium groups by scanning ReportEcology HTML.
    """
    soup = BeautifulSoup(report_ecology_html, "html.parser")

    # Grab all items in the ecology partial and match by exact label
    items = parse_items(report_ecology_html)

    # Exact-match first (best)
    by_name = {it["name"]: it["id"] for it in items}
    found: Dict[str, int] = {}
    for name in TOP_NAMES:
        if name in by_name:
            found[name] = by_name[name]

    # If exact matching fails (whitespace/variants), fall back to substring matching
    if len(found) < len(TOP_NAMES):
        for it in items:
            low = it["name"].lower()
            if "mark/bunn" in low:
                found.setdefault("Mark/bunn", it["id"])
            elif "organismer" in low and "livsmedium" in low:
                found.setdefault("Organismer som livsmedium", it["id"])
            elif low == "dyr":
                found.setdefault("Dyr", it["id"])
            elif "menneskeskapt" in low and "livsmedium" in low:
                found.setdefault("Menneskeskapt livsmedium", it["id"])

    return found

def download_livsmedium_tree(session: requests.Session, species_group_id: int) -> Dict[str, Any]:
    # 1) Load the ecology partial that contains the initial picker HTML
    html = fetch(session, REPORT_ECOLOGY, params={"speciesGroupId": str(species_group_id)})

    # 2) Extract the real top-level group IDs (the level you were missing)
    top_ids = extract_livsmedium_top_ids(html)
    if not top_ids:
        raise RuntimeError(
            "Could not find livsmedium top-level items in ReportEcology HTML. "
            "The markup may differ; inspect ReportEcology response and adjust selectors."
        )

    # 3) Recurse from each top-level group using RenderSubstrateChildren
    roots = []
    for name, node_id in top_ids.items():
        roots.append({
            "id": node_id,
            "name": name,
            "children": build_tree_from_children(session, node_id),
        })

    return {
        "speciesGroupId": species_group_id,
        "roots": roots,
    }

if __name__ == "__main__":
    COOKIE = None  # paste Cookie header if needed
    SPECIES_GROUP_ID = -1  # or whatever value your form uses for fungi

    s = requests.Session()
    if COOKIE:
        s.headers["Cookie"] = COOKIE

    tree = download_livsmedium_tree(s, species_group_id=SPECIES_GROUP_ID)
    output_path = REFERENCE_DATA_GENERATED_DIR / "livsmedium_tree.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(tree, f, ensure_ascii=False, indent=2)

    print(f"Wrote {output_path} with roots:", [r["name"] for r in tree["roots"]])

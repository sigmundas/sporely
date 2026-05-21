import json
import sys
import time
import requests
from bs4 import BeautifulSoup

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.reference_data_paths import REFERENCE_DATA_GENERATED_DIR

BASE = "https://www.artsobservasjoner.no"

TOP = f"{BASE}/SimplePicker/RenderTopLevelBiotopesNiN2"
CHILDREN = f"{BASE}/SimplePicker/RenderBiotopeNiN2Children"

def parse_items(html: str):
    """
    Returns list of dicts: {id:int, name:str, is_leaf:bool}
    Parses the HTML fragment returned by SimplePicker endpoints.
    """
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for li in soup.select("li.listitem"):
        a = li.find("a")
        if not a or not a.get("data-id"):
            continue

        node_id = int(a["data-id"])

        # Prefer the embedded JSON payload if present
        itemjson = li.select_one("span.itemjson")
        if itemjson and itemjson.get_text(strip=True).startswith("{"):
            payload = json.loads(itemjson.get_text(strip=True))
            name = payload.get("name") or li.get_text(" ", strip=True)
        else:
            name = li.select_one("span.itemname").get_text(strip=True)

        is_leaf = li.select_one("span.node-leaf") is not None
        out.append({"id": node_id, "name": name, "is_leaf": is_leaf})
    return out

def fetch(session: requests.Session, url: str, params=None) -> str:
    r = session.get(url, params=params, headers={"X-Requested-With": "XMLHttpRequest"})
    r.raise_for_status()
    return r.text

def build_tree(session: requests.Session, node_id: int, delay_s=0.05):
    """
    Fetch children for node_id and recurse.
    """
    html = fetch(session, CHILDREN, params={
        "parentId": str(node_id),
        "onlyReportable": "false",
        "dontIncludeSubSpecies": "true",
    })
    items = parse_items(html)

    children = []
    for it in items:
        entry = {"id": it["id"], "name": it["name"]}
        if not it["is_leaf"]:
            time.sleep(delay_s)  # be polite
            entry["children"] = build_tree(session, it["id"], delay_s=delay_s)
        children.append(entry)
    return children

def download_nin2_tree(cookie_header: str | None = None):
    """
    cookie_header: optional raw Cookie header copied from your browser.
    For these GETs it may work without auth, but if you see 302/login or empty results, pass cookies.
    """
    s = requests.Session()
    if cookie_header:
        s.headers["Cookie"] = cookie_header

    top_html = fetch(s, TOP)
    roots = parse_items(top_html)

    tree = []
    for r in roots:
        node = {"id": r["id"], "name": r["name"]}
        if not r["is_leaf"]:
            node["children"] = build_tree(s, r["id"])
        tree.append(node)
    return tree

if __name__ == "__main__":
    tree = download_nin2_tree(cookie_header=None)  # or paste your Cookie: ... here
    output_path = REFERENCE_DATA_GENERATED_DIR / "nin2_biotopes_tree.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(tree, f, ensure_ascii=False, indent=2)
    print(f"Wrote {output_path}")

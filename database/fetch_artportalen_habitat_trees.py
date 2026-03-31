"""Fetch full Artportalen biotope and substrate trees using a saved session.

This replaces the partial capture-based JSONs with a live crawl of the
SimplePicker endpoints used by the Artportalen observation form.

Example:
    python3 database/fetch_artportalen_habitat_trees.py --pause-seconds 0.25 --verbose
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests

from utils.artportalen_auth import ArtportalenAuth


BASE_URL = "https://www.artportalen.se"
REQUEST_TIMEOUT = 25
DEFAULT_BIOTOPE_OUTPUT = Path("database/artportalen_biotopes_tree.json")
DEFAULT_SUBSTRATE_OUTPUT = Path("database/artportalen_substrate_tree.json")

ITEM_RE = re.compile(r'<li class="listitem".*?</li>', re.DOTALL)
ID_RE = re.compile(r'data-id="(\d+)"')
ITEMJSON_RE = re.compile(r'<span class="itemjson">(\{.*?\})</span>', re.DOTALL)
ITEMNAME_RE = re.compile(r'<span class="itemname">(.*?)</span>', re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")


def _strip_tags(text: str) -> str:
    return TAG_RE.sub("", text or "").replace("&nbsp;", " ").strip()


def parse_items(html: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for block in ITEM_RE.findall(html or ""):
        id_match = ID_RE.search(block)
        if not id_match:
            continue
        try:
            node_id = int(id_match.group(1))
        except (TypeError, ValueError):
            continue

        payload_match = ITEMJSON_RE.search(block)
        name = ""
        if payload_match:
            try:
                payload = json.loads(payload_match.group(1))
                name = str(payload.get("name") or "").strip()
            except Exception:
                name = ""
        if not name:
            name_match = ITEMNAME_RE.search(block)
            if name_match:
                name = _strip_tags(name_match.group(1))
        if not name:
            continue

        is_leaf = 'class="node-leaf"' in block
        items.append({"id": node_id, "name": name, "is_leaf": is_leaf})
    return items


class ArtportalenHabitatFetcher:
    def __init__(
        self,
        session: requests.Session,
        pause_seconds: float = 0.2,
        pause_jitter: float = 0.1,
        retries: int = 4,
        timeout: int = REQUEST_TIMEOUT,
        verbose: bool = False,
    ) -> None:
        self.session = session
        self.pause_seconds = max(0.0, float(pause_seconds))
        self.pause_jitter = max(0.0, float(pause_jitter))
        self.retries = max(1, int(retries))
        self.timeout = int(timeout)
        self.verbose = bool(verbose)
        self._visited_biotope: set[int] = set()
        self._visited_substrate: set[int] = set()

    def _sleep(self) -> None:
        delay = self.pause_seconds
        if self.pause_jitter > 0:
            delay += random.uniform(0.0, self.pause_jitter)
        if delay > 0:
            time.sleep(delay)

    def _fetch(self, url: str, params: dict[str, Any] | None = None) -> str:
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{BASE_URL}/SubmitSighting/Report",
            "Accept": "text/html, */*; q=0.01",
        }
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=True,
                )
                if response.status_code >= 400:
                    raise RuntimeError(f"HTTP {response.status_code} for {response.url}")
                final_url = str(response.url or "").lower()
                if "useradmin-auth.slu.se" in final_url or "/account/login" in final_url:
                    raise RuntimeError(f"Redirected to login for {url}")
                return response.text or ""
            except Exception as exc:
                last_error = exc
                if attempt >= self.retries:
                    break
                backoff = min(5.0, 0.5 * attempt)
                if self.verbose:
                    print(f"[retry {attempt}/{self.retries}] {url} -> {exc}", file=sys.stderr)
                time.sleep(backoff)
        raise RuntimeError(f"Request failed for {url}: {last_error}")

    def _build_tree(
        self,
        *,
        top_level_url: str,
        children_url: str,
        visited: set[int],
        label: str,
    ) -> list[dict[str, Any]]:
        top_html = self._fetch(top_level_url)
        roots = parse_items(top_html)
        if not roots:
            raise RuntimeError(f"No top-level {label} items returned from {top_level_url}")

        def descend(node_id: int, depth: int) -> list[dict[str, Any]]:
            if node_id in visited:
                return []
            visited.add(node_id)
            self._sleep()
            html = self._fetch(children_url, params={"parentId": str(node_id)})
            items = parse_items(html)
            children: list[dict[str, Any]] = []
            for item in items:
                entry = {"id": item["id"], "name": item["name"]}
                print(f"{label} {'  ' * depth}{item['id']} {item['name']}")
                if not item["is_leaf"]:
                    entry["children"] = descend(int(item["id"]), depth + 1)
                children.append(entry)
            return children

        tree: list[dict[str, Any]] = []
        for root in roots:
            root_entry = {"id": root["id"], "name": root["name"]}
            print(f"{label} {root['id']} {root['name']}")
            if not root["is_leaf"]:
                root_entry["children"] = descend(int(root["id"]), 1)
            tree.append(root_entry)
        return tree

    def fetch_biotopes(self) -> list[dict[str, Any]]:
        return self._build_tree(
            top_level_url=f"{BASE_URL}/SimplePicker/RenderTopLevelBiotopes",
            children_url=f"{BASE_URL}/SimplePicker/RenderBiotopeChildren",
            visited=self._visited_biotope,
            label="biotope",
        )

    def fetch_substrates(self) -> list[dict[str, Any]]:
        return self._build_tree(
            top_level_url=f"{BASE_URL}/SimplePicker/RenderTopLevelSubstrates",
            children_url=f"{BASE_URL}/SimplePicker/RenderSubstrateChildren",
            visited=self._visited_substrate,
            label="substrate",
        )


def _save_json(path: Path, data: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _session_from_cookies(cookies: dict[str, str] | None) -> requests.Session:
    session = requests.Session()
    for name, value in (cookies or {}).items():
        if not str(name).strip() or value is None:
            continue
        session.cookies.set(str(name), str(value), domain=".artportalen.se")
    return session


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Artportalen biotope/substrate trees.")
    parser.add_argument("--pause-seconds", type=float, default=0.2)
    parser.add_argument("--pause-jitter", type=float, default=0.1)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=REQUEST_TIMEOUT)
    parser.add_argument("--biotope-output", type=Path, default=DEFAULT_BIOTOPE_OUTPUT)
    parser.add_argument("--substrate-output", type=Path, default=DEFAULT_SUBSTRATE_OUTPUT)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--anonymous", action="store_true", help="Do not use saved Artportalen cookies.")
    args = parser.parse_args()

    cookies: dict[str, str] | None = None
    if not args.anonymous:
        cookies = ArtportalenAuth().ensure_valid_cookies()
        if not cookies:
            raise SystemExit(
                "Could not load a valid Artportalen session. Log in in Sporely first, or rerun with --anonymous."
            )

    session = _session_from_cookies(cookies)
    fetcher = ArtportalenHabitatFetcher(
        session=session,
        pause_seconds=args.pause_seconds,
        pause_jitter=args.pause_jitter,
        retries=args.retries,
        timeout=args.timeout,
        verbose=args.verbose,
    )

    biotopes = fetcher.fetch_biotopes()
    substrates = fetcher.fetch_substrates()
    _save_json(args.biotope_output, biotopes)
    _save_json(args.substrate_output, substrates)
    print(
        f"Wrote {args.biotope_output} ({len(biotopes)} roots) and "
        f"{args.substrate_output} ({len(substrates)} roots)"
    )


if __name__ == "__main__":
    main()

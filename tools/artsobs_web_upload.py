"""CLI helper for two-step Artsobservasjoner web upload (save + images)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app_identity import app_data_dir
from utils.artsobservasjoner_submit import ArtsObservasjonerWebClient


def _default_cookies_file() -> Path:
    return app_data_dir() / "artsobservasjoner_cookies.json"


def _load_cookies(path: Path) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"Cookies file not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Cookies file must contain a JSON object.")
    cookies: dict[str, str] = {}
    for key, value in payload.items():
        if key and value is not None:
            cookies[str(key)] = str(value)
    if not cookies:
        raise RuntimeError("No cookies found in cookies file.")
    return cookies


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload Artsobservasjoner web observation and images in one step."
    )
    parser.add_argument("--taxon-id", type=int, required=True, help="Artsdatabanken taxon id.")
    parser.add_argument(
        "--observed-datetime",
        required=True,
        help="Observation datetime (e.g. '2026-02-10 04:00' or ISO datetime).",
    )
    parser.add_argument("--site-id", type=int, default=None, help="Existing site id (optional).")
    parser.add_argument("--site-name", default=None, help="Site name fallback (optional).")
    parser.add_argument("--latitude", type=float, default=None, help="Latitude in decimal degrees.")
    parser.add_argument("--longitude", type=float, default=None, help="Longitude in decimal degrees.")
    parser.add_argument("--accuracy-meters", type=int, default=25, help="GPS accuracy in meters.")
    parser.add_argument("--count", type=int, default=1, help="Quantity. Default: 1")
    parser.add_argument("--habitat", default=None, help="Habitat text.")
    parser.add_argument("--notes", default=None, help="Public notes.")
    parser.add_argument(
        "--image",
        dest="images",
        action="append",
        default=[],
        help="Image path. Repeat for multiple images.",
    )
    parser.add_argument(
        "--cookies-file",
        default=str(_default_cookies_file()),
        help="Path to artsobservasjoner_cookies.json",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    cookies_file = Path(args.cookies_file).expanduser().resolve()

    try:
        if args.site_id is None and (args.latitude is None or args.longitude is None):
            raise RuntimeError(
                "Provide --latitude and --longitude when --site-id is not supplied."
            )
        cookies = _load_cookies(cookies_file)
        client = ArtsObservasjonerWebClient()
        client.set_cookies_from_browser(cookies)

        def progress_cb(text: str, current: int, total: int) -> None:
            print(f"[{current}/{total}] {text}")

        result = client.submit_observation_web(
            taxon_id=args.taxon_id,
            observed_datetime=args.observed_datetime,
            site_id=args.site_id,
            site_name=args.site_name,
            latitude=args.latitude,
            longitude=args.longitude,
            accuracy_meters=args.accuracy_meters,
            count=args.count,
            habitat=args.habitat,
            notes=args.notes,
            image_paths=args.images,
            progress_cb=progress_cb,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, ensure_ascii=False))
    sighting_id = result.get("sighting_id")
    if sighting_id:
        print(f"\nSighting URL: https://www.artsobservasjoner.no/Sighting/{sighting_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

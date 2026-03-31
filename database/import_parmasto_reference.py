"""Import Parmasto reference rows into the live reference database."""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.models import ReferenceDB
from database.schema import get_reference_database_path, init_reference_database


DEFAULT_SOURCE = "Parmasto, 1987"

TABLE35_DEFAULT = Path(__file__).resolve().with_name("parmasto_table35.csv")
TABLE36_CANDIDATES = (
    Path(__file__).resolve().with_name("parmasto_table36.csv"),
    Path.home() / "Documents" / "table36.csv",
)


def _canonical_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _read_rows(path: Path) -> list[dict[str, str]]:
    text = path.read_text(encoding="utf-8-sig")
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 2:
        return []
    delimiter = ";" if lines[0].count(";") >= lines[0].count(",") else ","
    reader = csv.DictReader(lines, delimiter=delimiter)
    return [{_canonical_header(key): (value or "").strip() for key, value in row.items()} for row in reader]


def _pick(row: dict[str, str], *names: str) -> str:
    for name in names:
        value = row.get(_canonical_header(name), "")
        if value:
            return value
    return ""


def _parse_float(value: str) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _species_key(name: str) -> str:
    return re.sub(r"\s+", " ", str(name or "").strip().lower())


def _split_species(full_name: str) -> tuple[str, str]:
    parts = str(full_name or "").strip().split(None, 1)
    if len(parts) != 2:
        raise ValueError(f"Could not split species name: {full_name!r}")
    return parts[0], parts[1]


def _load_table35(path: Path) -> list[dict]:
    rows = []
    for row in _read_rows(path):
        species = _pick(row, "species", "scientific_name", "scientificname")
        if not species:
            continue
        rows.append(
            {
                "species_name": species,
                "species_key": _species_key(species),
                "table36_id": _pick(row, "table36_id", "id", "no"),
                "length_min": _parse_float(_pick(row, "l_min", "length_min")),
                "length_max": _parse_float(_pick(row, "l_max", "length_max")),
                "width_min": _parse_float(_pick(row, "w_min", "width_min")),
                "width_max": _parse_float(_pick(row, "w_max", "width_max")),
                "q_min": _parse_float(_pick(row, "q_min")),
                "q_max": _parse_float(_pick(row, "q_max")),
            }
        )
    return rows


def _load_table36(path: Path) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    by_species: dict[str, dict] = {}
    by_id: dict[str, list[dict]] = {}
    for row in _read_rows(path):
        species = _pick(row, "species", "scientific_name", "scientificname")
        row_id = _pick(row, "id", "table36_id", "no")
        if not species:
            continue
        parsed = {
            "species_name": species,
            "species_key": _species_key(species),
            "table36_id": row_id,
            "parmasto_length_mean": _parse_float(_pick(row, "l_mean", "length_mean", "lbar", "l_bar", "lm")),
            "parmasto_width_mean": _parse_float(_pick(row, "w_mean", "width_mean", "wbar", "w_bar", "wm")),
            "parmasto_q_mean": _parse_float(_pick(row, "q_mean", "quotient_mean", "qbar", "q_bar", "qm")),
            "parmasto_v_sp_length": _parse_float(_pick(row, "vspl", "v_spl", "v_sp_l", "v_sp_length")),
            "parmasto_v_sp_width": _parse_float(_pick(row, "vspw", "v_spw", "v_sp_w", "v_sp_width")),
            "parmasto_v_sp_q": _parse_float(_pick(row, "vspq", "v_spq", "v_sp_q", "v_sp_quotient")),
            "parmasto_v_ind_length": _parse_float(_pick(row, "vindl", "meanvindl", "vindlmean", "v_indl", "v_ind_l")),
            "parmasto_v_ind_width": _parse_float(_pick(row, "vindw", "meanvindw", "vindwmean", "v_indw", "v_ind_w")),
            "parmasto_v_ind_q": _parse_float(_pick(row, "vinde", "meanvinde", "vindemean", "vindq", "meanvindq", "vindqmean", "v_inde", "v_inde", "v_indq", "v_ind_q")),
        }
        by_species[parsed["species_key"]] = parsed
        if row_id:
            by_id.setdefault(str(row_id).strip(), []).append(parsed)
    return by_species, by_id


def _find_table36(path_arg: str | None) -> Path:
    if path_arg:
        return Path(path_arg).expanduser().resolve()
    for candidate in TABLE36_CANDIDATES:
        if candidate.exists():
            return candidate
    return TABLE36_CANDIDATES[0]


def import_parmasto(table35_path: Path, table36_path: Path, source: str) -> int:
    table35 = _load_table35(table35_path)
    if not table35:
        raise ValueError(f"No usable rows found in {table35_path}")

    table36_by_species, table36_by_id = _load_table36(table36_path)
    if not table36_by_species:
        raise ValueError(
            f"No usable rows found in {table36_path}. "
            "The current file appears to be empty or missing the mean/Vind columns."
        )

    init_reference_database()

    imported = 0
    for row35 in table35:
        row36 = table36_by_species.get(row35["species_key"])
        if row36 is None and row35["table36_id"]:
            candidates = table36_by_id.get(str(row35["table36_id"]).strip(), [])
            if len(candidates) == 1:
                row36 = candidates[0]
        if row36 is None:
            continue

        genus, species = _split_species(row35["species_name"])
        ReferenceDB.set_reference(
            {
                "genus": genus,
                "species": species,
                "source": source,
                "mount_medium": None,
                "stain": None,
                "plot_color": None,
                "length_min": row35["length_min"],
                "length_max": row35["length_max"],
                "width_min": row35["width_min"],
                "width_max": row35["width_max"],
                "q_min": row35["q_min"],
                "q_max": row35["q_max"],
                "parmasto_length_mean": row36["parmasto_length_mean"],
                "parmasto_width_mean": row36["parmasto_width_mean"],
                "parmasto_q_mean": row36["parmasto_q_mean"],
                "parmasto_v_sp_length": row36["parmasto_v_sp_length"],
                "parmasto_v_sp_width": row36["parmasto_v_sp_width"],
                "parmasto_v_sp_q": row36["parmasto_v_sp_q"],
                "parmasto_v_ind_length": row36["parmasto_v_ind_length"],
                "parmasto_v_ind_width": row36["parmasto_v_ind_width"],
                "parmasto_v_ind_q": row36["parmasto_v_ind_q"],
            }
        )
        imported += 1
    return imported


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table35", default=str(TABLE35_DEFAULT), help="Path to the Parmasto table 35 CSV")
    parser.add_argument("--table36", default=None, help="Path to the Parmasto table 36 CSV")
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="Reference source label")
    args = parser.parse_args()

    table35_path = Path(args.table35).expanduser().resolve()
    table36_path = _find_table36(args.table36)

    imported = import_parmasto(table35_path, table36_path, args.source)

    print(f"Imported {imported} Parmasto reference rows into {get_reference_database_path()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Merge Parmasto CSV tables and rebuild the bundled reference database."""

from __future__ import annotations

import argparse
import csv
import shutil
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.import_parmasto_reference import (  # noqa: E402
    DEFAULT_SOURCE,
    TABLE35_DEFAULT,
    _find_table36,
    _load_table35,
    _load_table36,
    _split_species,
)
from database.schema import (  # noqa: E402
    get_bundled_reference_database_path,
    get_reference_database_path,
    init_reference_database,
)


MERGED_CSV_DEFAULT = Path(__file__).resolve().with_name("parmasto_reference_merged.csv")

REFERENCE_INSERT_COLUMNS = [
    "genus",
    "species",
    "source",
    "mount_medium",
    "stain",
    "plot_color",
    "parmasto_length_mean",
    "parmasto_width_mean",
    "parmasto_q_mean",
    "parmasto_v_sp_length",
    "parmasto_v_sp_width",
    "parmasto_v_sp_q",
    "parmasto_v_ind_length",
    "parmasto_v_ind_width",
    "parmasto_v_ind_q",
    "length_min",
    "length_p05",
    "length_p50",
    "length_p95",
    "length_max",
    "length_avg",
    "width_min",
    "width_p05",
    "width_p50",
    "width_p95",
    "width_max",
    "width_avg",
    "q_min",
    "q_p50",
    "q_max",
    "q_avg",
]

MERGED_CSV_COLUMNS = [
    "table36_id",
    "species_name",
    "genus",
    "species",
    "source",
    "length_min",
    "length_max",
    "width_min",
    "width_max",
    "q_min",
    "q_max",
    "parmasto_length_mean",
    "parmasto_width_mean",
    "parmasto_q_mean",
    "parmasto_v_sp_length",
    "parmasto_v_sp_width",
    "parmasto_v_sp_q",
    "parmasto_v_ind_length",
    "parmasto_v_ind_width",
    "parmasto_v_ind_q",
]


def build_parmasto_reference_rows(table35_path: Path, table36_path: Path, source: str) -> list[dict]:
    table35 = _load_table35(table35_path)
    if not table35:
        raise ValueError(f"No usable rows found in {table35_path}")

    table36_by_species, table36_by_id = _load_table36(table36_path)
    if not table36_by_species:
        raise ValueError(
            f"No usable rows found in {table36_path}. "
            "The current file appears to be empty or missing the mean/Vind columns."
        )

    merged_rows: list[dict] = []
    unmatched: list[str] = []
    for row35 in table35:
        row36 = table36_by_species.get(row35["species_key"])
        if row36 is None and row35["table36_id"]:
            candidates = table36_by_id.get(str(row35["table36_id"]).strip(), [])
            if len(candidates) == 1:
                row36 = candidates[0]
        if row36 is None:
            unmatched.append(row35["species_name"])
            continue

        genus, species = _split_species(row35["species_name"])
        merged_rows.append(
            {
                "table36_id": row35["table36_id"],
                "species_name": row35["species_name"],
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
                "length_p05": None,
                "length_p50": None,
                "length_p95": None,
                "length_avg": None,
                "width_p05": None,
                "width_p50": None,
                "width_p95": None,
                "width_avg": None,
                "q_p50": None,
                "q_avg": None,
            }
        )

    if unmatched:
        preview = ", ".join(unmatched[:5])
        more = "" if len(unmatched) <= 5 else f" (+{len(unmatched) - 5} more)"
        raise ValueError(f"Could not match {len(unmatched)} Parmasto rows: {preview}{more}")

    merged_rows.sort(key=lambda row: (str(row["genus"]).lower(), str(row["species"]).lower()))
    return merged_rows


def write_merged_csv(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MERGED_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    column: "" if row.get(column) is None else row.get(column)
                    for column in MERGED_CSV_COLUMNS
                }
            )


def _select_base_database(base_db_arg: str | None, output_db: Path) -> Path | None:
    if base_db_arg is not None:
        base_text = str(base_db_arg).strip()
        if not base_text:
            return None
        base_path = Path(base_text).expanduser().resolve()
        return base_path if base_path.exists() else None

    candidates = []
    for candidate in (output_db, get_reference_database_path()):
        resolved = Path(candidate).expanduser().resolve()
        if resolved not in candidates:
            candidates.append(resolved)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def rebuild_reference_database(rows: list[dict], output_db: Path, base_db: Path | None, source: str) -> int:
    output_db.parent.mkdir(parents=True, exist_ok=True)
    temp_db = output_db.with_suffix(output_db.suffix + ".tmp")
    if temp_db.exists():
        temp_db.unlink()

    if base_db is not None:
        shutil.copy2(base_db, temp_db)

    init_reference_database(temp_db, seed_from_bundle=False, migrate_legacy=False)

    conn = sqlite3.connect(temp_db)
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM reference_values WHERE source = ?", (source,))
        placeholders = ", ".join("?" for _ in REFERENCE_INSERT_COLUMNS)
        cursor.executemany(
            f"""
            INSERT INTO reference_values ({', '.join(REFERENCE_INSERT_COLUMNS)})
            VALUES ({placeholders})
            """,
            [tuple(row.get(column) for column in REFERENCE_INSERT_COLUMNS) for row in rows],
        )
        conn.commit()
        final_count = int(cursor.execute("SELECT COUNT(*) FROM reference_values").fetchone()[0] or 0)
    finally:
        conn.close()

    temp_db.replace(output_db)
    return final_count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table35", default=str(TABLE35_DEFAULT), help="Path to Parmasto table 35 CSV")
    parser.add_argument("--table36", default=None, help="Path to Parmasto table 36 CSV")
    parser.add_argument("--merged-csv", default=str(MERGED_CSV_DEFAULT), help="Output path for the merged Parmasto CSV")
    parser.add_argument(
        "--output-db",
        default=str(get_bundled_reference_database_path()),
        help="Output path for the bundled reference_values.db",
    )
    parser.add_argument(
        "--base-db",
        default=None,
        help="Optional existing reference_values.db to copy before updating Parmasto rows; pass '' for an empty base",
    )
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="Reference source label")
    args = parser.parse_args()

    table35_path = Path(args.table35).expanduser().resolve()
    table36_path = _find_table36(args.table36)
    merged_csv_path = Path(args.merged_csv).expanduser().resolve()
    output_db = Path(args.output_db).expanduser().resolve()
    base_db = _select_base_database(args.base_db, output_db)

    rows = build_parmasto_reference_rows(table35_path, table36_path, args.source)
    write_merged_csv(rows, merged_csv_path)
    final_count = rebuild_reference_database(rows, output_db, base_db, args.source)

    base_label = str(base_db) if base_db is not None else "<empty>"
    print(f"Merged {len(rows)} Parmasto rows into {merged_csv_path}")
    print(f"Rebuilt {output_db} from base {base_label}")
    print(f"reference_values rows in output DB: {final_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

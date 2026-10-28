"""Try the taxon picker by hand (development aid; writes nothing).

    python -m tools.review_ui.taxon_picker_demo            # real sample
    python -m tools.review_ui.taxon_picker_demo --db PATH  # an installed release

Picks are printed to the terminal. The first-time note is reset each run.
"""
from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

from PySide6.QtWidgets import QApplication, QCheckBox, QHBoxLayout, QLabel, QLineEdit, QVBoxLayout, QWidget


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, help="taxonomy-v2 release database (opened read-only)")
    parser.add_argument("--language", default="no", help="vernacular language code (default: no)")
    args = parser.parse_args()
    app = QApplication(sys.argv[:1])

    from database.taxon_lookup import TaxonLookupService
    from database.vernacular_db import VernacularDB
    from tools.review_ui.taxon_sample import sample_lookup
    from ui.styles import apply_palette, get_style
    from ui.taxon_picker import AiSuggestion, TaxonPicker

    apply_palette("auto")
    app.setStyleSheet(get_style("auto"))
    if args.db:
        db = VernacularDB(args.db, language_code=args.language)
        lookup = TaxonLookupService(db, language_code=args.language, include_reference_data=False)
    else:
        lookup = sample_lookup(Path(tempfile.mkdtemp()) / "taxonomy.sqlite3", args.language)

    settings: dict[str, str] = {}
    window = QWidget()
    window.setWindowTitle("Taxon picker demo")
    layout = QVBoxLayout(window)
    row = QHBoxLayout()
    label = QLabel("Art:")
    label.setFixedWidth(96)
    row.addWidget(label)
    picker = TaxonPicker(lookup, setting_get=lambda k, d: settings.get(k, d), setting_set=settings.__setitem__)
    row.addWidget(picker, 1)
    layout.addLayout(row)
    uncertain = QCheckBox("Usikker")
    uncertain.toggled.connect(picker.set_uncertain)
    layout.addWidget(uncertain)
    layout.addWidget(QLineEdit(placeholderText="Another field (focus target)"))
    layout.addStretch(1)
    picker.set_ai_suggestions([
        AiSuggestion("vintersopp", "Flammulina velutipes", 0.82, 625621),
        AiSuggestion("langsporet vintersopp", "Flammulina elastica", 0.09, 98272),
        AiSuggestion("Gymnopus", "Gymnopus", 0.03, None),
    ])
    picker.concept_picked.connect(lambda r, c: print(f"picked {r.name!r} -> {c.sporely_taxon_id} ({c.source_name})"))
    picker.free_text_committed.connect(lambda t: print(f"free name {t!r}"))
    picker.cleared.connect(lambda: print("cleared"))
    window.resize(680, 620)
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())

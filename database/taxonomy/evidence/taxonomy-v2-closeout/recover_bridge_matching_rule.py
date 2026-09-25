#!/usr/bin/env python3
"""Recover the per-association matching rule for NorTaxa -> Sporely bridges.

Stage 1 audit helper for ``docs/plans/active/2026-09-15-taxonomy-v2-closeout.md``.
Read-only: it opens pinned artifacts and writes a per-association JSON plus a
counts summary. It does not build, activate or mutate anything.

Why this exists
---------------
``compile_release.py:755-756`` records every automatic cross-source alias as the
single ``alias_reason`` value ``cross_source_automatic_exact``. That label does
not say *which* rule admitted the alias, because
``cross_source_mapping.py:232-259`` emits ``PROPOSAL_AUTOMATIC_EXACT`` for both:

* the strict conservative rule (``conservative_exact_rule_satisfied``), and
* the missing-authorship fallback
  (``missing_authorship_classification_rule_satisfied``).

The distinction matters: the fallback admits a bridge *despite* absent
authorship, which is materially weaker evidence for concept identity.

Recovery is possible without regenerating ``mappings.jsonl`` because the
fallback fires **only** when the strict rule failed *exclusively* on missing
authorship, and **never** on mismatch (``cross_source_mapping.py:240-247``).
For an alias that was actually applied, the rule is therefore decidable from
authorship presence alone:

* both sides present  -> ``strict``   (a mismatch would have produced no alias,
  so "present and applied" implies the authorships agreed)
* either side absent  -> ``missing_authorship_fallback``

Inputs (all pinned; hashes verified in the ledger's lineage section)
-------------------------------------------------------------------
* ``tax-2026.07.30-02.sqlite3.gz`` -- the compiled candidate whose
  ``taxon_external_id_min.note`` carries ``alias_reason``.
* ``sources/nortaxa/1.284/archive.zip`` -- ``taxon.txt`` provides
  ``scientificNameAuthorship`` per ``taxonID``.
* ``sources/col_xr/2026-07-17-XR/archive.zip`` -- ``NameUsage.tsv`` provides
  ``col:authorship`` per ``col:ID``. Streamed; never extracted to disk.
* ``global_macrofungi_tax-2026.08.01-01/vernacular.jsonl`` -- the 2,041
  vernacular-joined taxa of the active release.

Usage
-----
    python recover_bridge_matching_rule.py [--generated DIR] [--sources DIR]
                                           [--out-dir DIR]

Defaults point at the primary checkout, because generated artifacts and
acquired source archives are gitignored and therefore exist in exactly one
checkout.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import shutil
import sqlite3
import sys
import tempfile
import zipfile
from collections import Counter
from pathlib import Path

PRIMARY = Path("/Users/sigmundas/Documents/Code/sporely/sporely-py")
DEFAULT_GENERATED = PRIMARY / "database/reference_data/generated/taxonomy_v2"
DEFAULT_SOURCES = PRIMARY / "database/taxonomy/sources"

CANDIDATE_GZ = "tax-2026.07.30-02.sqlite3.gz"
ACTIVE_RELEASE = "global_macrofungi_tax-2026.08.01-01"
COL_RELEASE = "col_xr/2026-07-17-XR/archive.zip"
NORTAXA_RELEASE = "nortaxa/1.284/archive.zip"

ALIAS_REASON = "cross_source_automatic_exact"
RULE_STRICT = "strict"
RULE_FALLBACK = "missing_authorship_fallback"


def _bridges(generated: Path) -> list[tuple[str, int, str]]:
    """Return (nortaxa_taxon_id, sporely_taxon_id, col_usage_id) triples."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "candidate.sqlite3"
        with gzip.open(generated / CANDIDATE_GZ, "rb") as src, db.open("wb") as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            rows = conn.execute(
                "SELECT e.external_id, e.taxon_id, t.canonical_external_id "
                "FROM taxon_external_id_min e "
                "JOIN taxon_min t ON t.taxon_id = e.taxon_id "
                "WHERE e.note = ? ORDER BY e.taxon_id, e.external_id",
                (ALIAS_REASON,),
            ).fetchall()
        finally:
            conn.close()
    return [(str(a), int(b), str(c)) for a, b, c in rows]


def _nortaxa_authorship(sources: Path) -> dict[str, str]:
    csv.field_size_limit(sys.maxsize)
    out: dict[str, str] = {}
    with zipfile.ZipFile(sources / NORTAXA_RELEASE) as z, z.open("taxon.txt") as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
        for row in csv.DictReader(text, delimiter="\t", quoting=csv.QUOTE_NONE):
            taxon_id = (row.get("taxonID") or "").strip()
            if taxon_id:
                out[taxon_id] = (row.get("scientificNameAuthorship") or "").strip()
    return out


def _col_authorship(sources: Path, wanted: set[str]) -> dict[str, str]:
    """Stream NameUsage.tsv, keeping ``col:authorship`` for wanted ``col:ID``."""
    found: dict[str, str] = {}
    with zipfile.ZipFile(sources / COL_RELEASE) as z, z.open("NameUsage.tsv") as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
        header = text.readline().rstrip("\n").split("\t")
        id_col, auth_col = header.index("col:ID"), header.index("col:authorship")
        need = max(id_col, auth_col) + 1
        for line in text:
            parts = line.split("\t", need)
            if len(parts) <= auth_col:
                continue
            usage_id = parts[id_col]
            if usage_id in wanted:
                found[usage_id] = parts[auth_col].strip()
                if len(found) == len(wanted):
                    break
    return found


def _scoped_taxa(generated: Path) -> set[int]:
    out: set[int] = set()
    path = generated / ACTIVE_RELEASE / "vernacular.jsonl"
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                out.add(int(json.loads(line)["taxon_id"]))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--generated", type=Path, default=DEFAULT_GENERATED)
    ap.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    ap.add_argument("--out-dir", type=Path, default=Path(__file__).parent)
    args = ap.parse_args()

    bridges = _bridges(args.generated)
    nortaxa = _nortaxa_authorship(args.sources)
    col = _col_authorship(args.sources, {c for _, _, c in bridges})
    scoped = _scoped_taxa(args.generated)

    counts: Counter[str] = Counter()
    per_taxon: dict[int, set[str]] = {}
    rows = []
    for ext_id, sporely_id, col_id in bridges:
        nt_auth = nortaxa.get(ext_id, "")
        col_auth = col.get(col_id, "")
        rule = RULE_STRICT if (nt_auth and col_auth) else RULE_FALLBACK
        counts[rule] += 1
        if rule == RULE_FALLBACK:
            if not nt_auth and not col_auth:
                counts["fallback_both_absent"] += 1
            elif not nt_auth:
                counts["fallback_nortaxa_absent"] += 1
            else:
                counts["fallback_col_absent"] += 1
        counts["unresolved_nortaxa"] += 0 if ext_id in nortaxa else 1
        counts["unresolved_col"] += 0 if col_id in col else 1
        per_taxon.setdefault(sporely_id, set()).add(rule)
        rows.append([
            sporely_id, ext_id, col_id, rule, int(sporely_id in scoped),
        ])

    # Emitted as JSON rather than CSV: the repository ignores ``*.csv``
    # (.gitignore:77) and the surrounding evidence directory is JSON/Markdown.
    # Array-of-arrays keeps 19,808 rows compact and diff-reviewable.
    out_rows = args.out_dir / "nortaxa-bridge-association-audit.json"
    out_rows.write_text(
        json.dumps(
            {
                "columns": [
                    "sporely_taxon_id", "nortaxa_taxon_id", "col_usage_id",
                    "matching_rule", "in_active_release_vernacular_2041",
                ],
                "alias_reason_filter": ALIAS_REASON,
                "row_count": len(rows),
                "rows": rows,
            },
            separators=(",", ":"),
        ) + "\n",
        encoding="utf-8",
    )

    scoped_all_strict = sum(
        1 for t in scoped if per_taxon.get(t) == {RULE_STRICT}
    )
    scoped_any_fallback = sum(
        1 for t in scoped if RULE_FALLBACK in per_taxon.get(t, set())
    )
    scoped_unbound = sum(1 for t in scoped if t not in per_taxon)

    summary = {
        "alias_reason_filter": ALIAS_REASON,
        "bridge_bindings_total": len(rows),
        "bindings_strict": counts[RULE_STRICT],
        "bindings_missing_authorship_fallback": counts[RULE_FALLBACK],
        "fallback_nortaxa_authorship_absent_only": counts["fallback_nortaxa_absent"],
        "fallback_col_authorship_absent_only": counts["fallback_col_absent"],
        "fallback_both_absent": counts["fallback_both_absent"],
        "unresolved_nortaxa_ids": counts["unresolved_nortaxa"],
        "unresolved_col_usage_ids": counts["unresolved_col"],
        "distinct_sporely_concepts_bound": len(per_taxon),
        "active_release_vernacular_taxa": len(scoped),
        "scoped_all_bindings_strict": scoped_all_strict,
        "scoped_with_any_fallback_binding": scoped_any_fallback,
        "scoped_without_any_bridge_binding": scoped_unbound,
        "scoped_taxa_with_fallback_binding": sorted(
            t for t in scoped if RULE_FALLBACK in per_taxon.get(t, set())
        ),
    }
    out_json = args.out_dir / "nortaxa-bridge-association-audit.summary.json"
    out_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8")

    print(json.dumps({k: v for k, v in summary.items()
                      if k != "scoped_taxa_with_fallback_binding"}, indent=2,
                     sort_keys=True))
    print(f"wrote {out_rows}")
    print(f"wrote {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

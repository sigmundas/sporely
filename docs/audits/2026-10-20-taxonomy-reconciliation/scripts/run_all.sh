#!/bin/sh
# Reproduce every count in the 2026-10-20 taxonomy reconciliation audit.
# Read-only over the repository, the installed release and the source
# archives; all intermediate output goes to $SCRATCH. Run from the worktree root.
#
#   SCRATCH=/path/to/scratch PRIMARY=/path/to/sporely-py sh docs/audits/2026-10-20-taxonomy-reconciliation/scripts/run_all.sh
#
# PRIMARY is the checkout that holds the gitignored archives
# (source archives and the release evidence archive).
set -eu
: "${SCRATCH:?set SCRATCH}"
: "${PRIMARY:?set PRIMARY}"
PY="$PRIMARY/.venv/bin/python -B"
HERE=docs/audits/2026-10-20-taxonomy-reconciliation
APP="$HOME/Library/Application Support/Sporely"
mkdir -p "$SCRATCH/evidence" "$SCRATCH/out"

# 1. Byte copies (installed release must hash to e349e8b1...; never opened in place).
#    Desktop databases are copied with the backup API from file:...?mode=ro&immutable=1 URIs.
cp "$APP/taxonomy_v2/vernacular_multilanguage_v2.sqlite3" "$SCRATCH/release.sqlite3"
shasum -a 256 "$SCRATCH/release.sqlite3"
$PY -c "import sqlite3,sys; s=sqlite3.connect('file:'+sys.argv[1].replace(' ','%20')+'?mode=ro&immutable=1',uri=True); d=sqlite3.connect(sys.argv[2]); s.backup(d)" \
    "$APP/mushrooms.db" "$SCRATCH/mushrooms.db"
$PY -c "import sqlite3,sys; s=sqlite3.connect('file:'+sys.argv[1].replace(' ','%20')+'?mode=ro&immutable=1',uri=True); d=sqlite3.connect(sys.argv[2]); s.backup(d)" \
    "$APP/reference_values.db" "$SCRATCH/reference_values.db"
tar xzf "$PRIMARY/database/reference_data/generated/taxonomy_v2/tax-2026.10.07-01.evidence.tar.gz" -C "$SCRATCH/evidence"

# 2. Work DB, cloud scope, (optional) production read-only aggregates.
$PY $HERE/scripts/build_workdb.py --release "$SCRATCH/release.sqlite3" --evidence-dir "$SCRATCH/evidence" \
    --nortaxa "$PRIMARY/database/taxonomy/sources/nortaxa/1.284/archive.zip" \
    --col "$PRIMARY/database/taxonomy/sources/col_xr/2026-07-17-XR/archive.zip" \
    --observations "$SCRATCH/mushrooms.db" --out "$SCRATCH/workdb.sqlite3"
$PY $HERE/scripts/compute_cloud_scope.py --repo . --release "$SCRATCH/release.sqlite3" --workdb "$SCRATCH/workdb.sqlite3"
# Cloud: reuse the saved 2026-10-20 snapshot (cloud_usage.jsonl). The audit does
# not re-query production; cloud_usage_readonly.py --out documents the query used.
if [ -f "$SCRATCH/cloud_usage.jsonl" ]; then
  $PY $HERE/scripts/cloud_usage_readonly.py --load "$SCRATCH/cloud_usage.jsonl" --workdb "$SCRATCH/workdb.sqlite3"
fi

# 3. Section 2 counts, classification, groups, manifests, checks (revision 3).
#    Movement tables need earlier exports: $SCRATCH/out/pairs_v1.jsonl.gz (first
#    delivery) and pairs_v2.jsonl.gz / index_v2.json (revision 2); optional.
$PY $HERE/scripts/authorship.py
$PY $HERE/scripts/reproduce_counts.py --workdb "$SCRATCH/workdb.sqlite3" --release "$SCRATCH/release.sqlite3" --repo . --out "$SCRATCH/out/section2_counts.json"
$PY $HERE/scripts/classify.py --repo . --workdb "$SCRATCH/workdb.sqlite3" --release "$SCRATCH/release.sqlite3" --out-dir "$SCRATCH/out"
$PY $HERE/scripts/existing_bindings.py --workdb "$SCRATCH/workdb.sqlite3" --release "$SCRATCH/release.sqlite3" --evidence-dir "$SCRATCH/evidence" --out-dir "$SCRATCH/out"
$PY $HERE/scripts/coverage.py --workdb "$SCRATCH/workdb.sqlite3" --release "$SCRATCH/release.sqlite3" --pairs "$SCRATCH/out/cross_source_pairs.jsonl.gz" --out "$SCRATCH/out/coverage.json"
[ -f "$SCRATCH/out/pairs_v1.jsonl.gz" ] && $PY $HERE/scripts/transitions.py --old "$SCRATCH/out/pairs_v1.jsonl.gz" --new "$SCRATCH/out/pairs_v2.jsonl.gz" --out "$SCRATCH/out/transitions.json"
[ -f "$SCRATCH/out/pairs_v3.jsonl.gz" ] && $PY $HERE/scripts/transitions.py --old "$SCRATCH/out/pairs_v3.jsonl.gz" --new "$SCRATCH/out/cross_source_pairs.jsonl.gz" --out "$SCRATCH/out/transitions_v3_v4.json"
DEST="${DEST:-$SCRATCH/export}"
$PY $HERE/scripts/export_manifests.py --in-dir "$SCRATCH/out" --dest "$DEST"
$PY $HERE/scripts/groups.py --repo . --workdb "$SCRATCH/workdb.sqlite3" --release "$SCRATCH/release.sqlite3" \
    --evidence-dir "$SCRATCH/evidence" --desktop "$SCRATCH/mushrooms.db" --reference-db "$SCRATCH/reference_values.db" \
    --pairs "$SCRATCH/out/cross_source_pairs.jsonl.gz" --col-internal "$SCRATCH/out/col_internal_groups.jsonl" \
    --pairs-v2 "$SCRATCH/out/pairs_v3.jsonl.gz" --index-v2 "$SCRATCH/out/index_v3.json" --out-dir "$SCRATCH/out" --dest "$DEST"
$PY $HERE/scripts/references.py --manifests "$DEST/manifests" --release "$SCRATCH/release.sqlite3" --workdb "$SCRATCH/workdb.sqlite3" \
    --desktop "$SCRATCH/mushrooms.db" --reference-db "$SCRATCH/reference_values.db" \
    --out "$SCRATCH/out/references.json" --sql-out "$DEST/scripts/cloud_reference_check.sql"
$PY $HERE/scripts/artsobs_lookup.py --workdb "$SCRATCH/workdb.sqlite3" --release "$SCRATCH/release.sqlite3" \
    --manifests "$DEST/manifests" --desktop "$SCRATCH/mushrooms.db" --out "$SCRATCH/out/artsobs_lookup_coverage.json"
$PY -c "import sqlite3,sys; s=sqlite3.connect('file:'+sys.argv[1].replace(' ','%20')+'?mode=ro&immutable=1',uri=True); d=sqlite3.connect(sys.argv[2]); s.backup(d)" \
    "$APP/reference_values.db" "$SCRATCH/reference_values.db"
$PY $HERE/scripts/desktop_migration.py --manifests "$DEST/manifests" --release "$SCRATCH/release.sqlite3" \
    --desktop "$SCRATCH/mushrooms.db" --reference-db "$SCRATCH/reference_values.db" --work-dir "$SCRATCH/dryrun" --out-dir "$SCRATCH/out"
$PY $HERE/scripts/export_manifests.py --in-dir "$SCRATCH/out" --dest "$DEST"   # copies the final data files
PYTHONDONTWRITEBYTECODE=1 $PY -m pytest -q -p no:cacheprovider "$HERE/scripts/test_manifests_validator.py"

# 4. Revision 5 (S1 scope). Independent re-derivation and freeze, synonym
#    inheritance proof (real builder on a sample), schema-driven desktop
#    inventory + gates + dry run, and the S1-only section of the cloud SQL.
AUD=$HERE
$PY $HERE/scripts/s1_derive.py --workdb "$SCRATCH/workdb.sqlite3" --release "$SCRATCH/release.sqlite3" \
    --desktop "$SCRATCH/mushrooms.db" --reference-db "$SCRATCH/reference_values.db" --audit "$AUD" --out "$AUD/data/s1-candidate.json"
$PY $HERE/scripts/s1_synonyms.py --repo . --workdb "$SCRATCH/workdb.sqlite3" --release "$SCRATCH/release.sqlite3" \
    --evidence-dir "$SCRATCH/evidence" --s1 "$AUD/data/s1-candidate.json" --work-dir "$SCRATCH/syn" --out "$AUD/data/s1-synonyms.json"
$PY $HERE/scripts/s1_desktop_gates.py --s1 "$AUD/data/s1-candidate.json" --workdb "$SCRATCH/workdb.sqlite3" \
    --release "$SCRATCH/release.sqlite3" --desktop "$SCRATCH/mushrooms.db" --reference-db "$SCRATCH/reference_values.db" \
    --app-dir "$APP" --work-dir "$SCRATCH/dryrun-s1" --out "$AUD/data/s1-desktop-gates.json"
$PY $HERE/scripts/references.py --manifests "$AUD/manifests" --release "$SCRATCH/release.sqlite3" --workdb "$SCRATCH/workdb.sqlite3" \
    --desktop "$SCRATCH/mushrooms.db" --reference-db "$SCRATCH/reference_values.db" \
    --out "$SCRATCH/out/references.json" --sql-out "$AUD/scripts/cloud_reference_check.sql" --s1 "$AUD/data/s1-candidate.json"

#!/bin/sh
# Stage 3 rerun (2026-10-06) Phase A — hash-gated re-materialization of the accepted
# Stage 1B/Stage 2 frozen set (owner-authorized exception). cwd = sporely-py; no code edits.
set -eu
S=$HOME/sporely-scratch/vernacular-2026-10-06
# A1 (attempt): recipe pipeline with current working-tree code. Deterministic, but NOT the
#    accepted bytes: compiler vernacular.jsonl/vernacular_evidence.jsonl differ (see rematerialization.json).
python3 database/taxonomy/scripts/build_release.py --release-id tax-2026.10.04-01 --build-dir $S/buildA
# A2: the pipeline that actually produced the accepted set (Codex session 2026-10-04T19:39Z/19:53Z):
#  - COL + NorTaxa normalized by current code (buildA/norm; COL hashes equal Stage 1B record),
#  - Dyntaxa normalized by the COMMITTED HEAD national_source.py (accepted run reused /tmp/sporely-6p/run1/norm/dyntaxa),
#  - compile + SQLite with current code, registry-base from buildA (1cc2003a...).
D=$S/diag-headdyntaxa; mkdir -p $D/code
git show HEAD:database/taxonomy/scripts/national_source.py > $D/code/national_source.py
PYTHONPATH=database/taxonomy/scripts .venv/bin/python $D/code/national_source.py normalize \
  --profile database/taxonomy/national_sources/dyntaxa/2026-09-30/source.json \
  --archive database/taxonomy/sources/dyntaxa/2026-09-30/archive.zip --output $D/dyntaxa
B=$S/buildA; cp $B/registry-base.jsonl $D/registry.jsonl
.venv/bin/python database/taxonomy/scripts/compile_release.py --source $B/norm/col_xr --source $B/norm/nortaxa \
  --source $D/dyntaxa --redlist $B/norm/redlist_no --manual-mappings database/taxonomy/policies/manual_mappings.yml \
  --concept-supersessions database/taxonomy/policies/concept_supersessions.yml \
  --mapping-policy database/taxonomy/policies/mapping_policy.yml --registry $D/registry.jsonl --output $D/release \
  --release-id tax-2026.10.04-01 --legacy-enrichment-input $B/norm/legacy_enrichment.jsonl \
  --vernacular-reviews database/taxonomy/policies/vernacular_associations.json \
  --vernacular-scope-policy database/taxonomy/policies/global-macrofungi-scope.yml
.venv/bin/python database/taxonomy/scripts/build_sqlite_candidate.py --release-dir $D/release --registry $D/registry.jsonl \
  --output $D/frozenA.sqlite3 --concept-supersessions database/taxonomy/policies/concept_supersessions.yml \
  --publishing-overlay database/taxonomy/overlays/artportalen-publishing.json \
  --inaturalist-refresh database/taxonomy/sources/inaturalist_refresh/2026-09-26/inaturalist-refresh.json
# A3: Stage 2 freeze exactly as run on 2026-10-04 (freeze-final.py reconstruction), twice (finalA/finalB).
.venv/bin/python $S/stage2/freeze-final.py
chmod -R a-w $S/stage2/finalA

#!/usr/bin/env bash
# Local Supabase sync harness: rebuild the LOCAL stack from sporely-web
# origin/main minus the production-deferred migrations, then run the
# desktop's real sync code against it (pytest marker ``local_supabase``).
#
# Safety:
#   * Never touches production: the stack is the local Docker one
#     (http://127.0.0.1:54321); the exported tree has no supabase/.temp, so the
#     CLI cannot be linked to the production project.
#   * Never edits the sporely-web repository: the migrations are exported with
#     ``git archive`` into a temp dir.
#   * Resets the local database only when no other session uses it: any client
#     backend other than PostgREST's ``authenticator`` and the platform service
#     roles aborts the run (pass --skip-reset to reuse the current database).
#   * Each simulated device runs in its own process with its own temporary
#     SPORELY_APP_DATA_DIR; the developer's real profile is never used.
#
# Usage: tools/run_local_sync_harness.sh [--skip-reset] [pytest args...]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WEB="${SPORELY_WEB_REPO:-$ROOT/../sporely-web}"
WEB_REF="${SPORELY_WEB_REF:-origin/main}"
PY="${SPORELY_PYTHON:-/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python}"
DB_CONTAINER="${SPORELY_LOCAL_DB_CONTAINER:-supabase_db_zkpjklzfwzefhjluvhfw}"
API_URL="http://127.0.0.1:54321"
SKIP_RESET=0
if [[ "${1:-}" == "--skip-reset" ]]; then SKIP_RESET=1; shift; fi

start=$(date +%s)
TREE="$(mktemp -d -t sporely-local-web-XXXXXX)"
trap 'rm -rf "$TREE"' EXIT

echo "== exporting $WEB_REF:supabase (read-only)"
git -C "$WEB" fetch -q origin || echo "   (fetch failed; using local $WEB_REF)"
git -C "$WEB" archive "$WEB_REF" supabase | tar -x -C "$TREE"
WEB_SHA="$(git -C "$WEB" rev-parse --short "$WEB_REF")"
deferred="$("$PY" - "$TREE/supabase/deploy-exceptions.json" <<'PYEOF'
import json, sys
try:
    data = json.load(open(sys.argv[1]))
except FileNotFoundError:
    data = {}
print(" ".join(item["file"] for item in data.get("deferredMigrations", [])))
PYEOF
)"
for file in $deferred; do
  rm -f "$TREE/supabase/migrations/$file"
  echo "   omitted deferred migration $file"
done
rm -rf "$TREE/supabase/.temp"

curl -sf -o /dev/null "$API_URL/rest/v1/" -H "apikey: x" || {
  code=$(curl -s -o /dev/null -w '%{http_code}' "$API_URL/rest/v1/" || true)
  [[ "$code" =~ ^(200|401)$ ]] || { echo "local stack not reachable at $API_URL (start it with 'supabase start' in sporely-web)"; exit 2; }
}

if [[ $SKIP_RESET -eq 0 ]]; then
  others="$(docker exec "$DB_CONTAINER" psql -U postgres -tAc "
    SELECT count(*) FROM pg_stat_activity
     WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()
       AND usename NOT IN ('authenticator','supabase_admin','supabase_auth_admin',
                           'supabase_storage_admin','supabase_realtime_admin')")"
  if [[ "${others// /}" != "0" ]]; then
    echo "refusing to reset: $others other database session(s) connected"; exit 3
  fi
  echo "== resetting LOCAL database ($WEB_SHA without deferred migrations)"
  supabase db reset --local --no-seed --workdir "$TREE" --yes >"$TREE/reset.log" 2>&1 || { tail -20 "$TREE/reset.log"; exit 5; }
fi

eval "$(supabase status -o env --workdir "$TREE" 2>/dev/null | grep -E '^(ANON_KEY|SERVICE_ROLE_KEY|API_URL)=')"
[[ "${API_URL:-}" == http://127.0.0.1:* || "${API_URL:-}" == http://localhost:* ]] || { echo "unexpected API_URL"; exit 4; }

echo "== running local_supabase scenarios"
cd "$ROOT"
set +e
SPORELY_LOCAL_SUPABASE=1 SPORELY_LOCAL_SUPABASE_URL="$API_URL" \
SPORELY_LOCAL_SUPABASE_ANON_KEY="$ANON_KEY" SPORELY_LOCAL_SUPABASE_SERVICE_KEY="$SERVICE_ROLE_KEY" \
SPORELY_LOCAL_SUPABASE_DB_CONTAINER="$DB_CONTAINER" QT_QPA_PLATFORM=offscreen \
  timeout 600 "$PY" -m pytest -p no:cacheprovider -m local_supabase -rA -q \
  tests/local_supabase "$@" 2>&1 | grep -E "^(PASSED|FAILED|XFAIL|XPASS|SKIPPED|ERROR)|passed|failed|error"
status=${PIPESTATUS[0]}
set -e
echo "== done in $(( $(date +%s) - start ))s (sporely-web $WEB_SHA)"
exit "$status"

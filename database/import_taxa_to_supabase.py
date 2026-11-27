#!/usr/bin/env python3
"""
One-time import of taxa from vernacular_multilanguage.sqlite3
into the Supabase taxa / taxa_vernacular tables.

Usage:
    python database/import_taxa_to_supabase.py            # dry run: prints counts, sends nothing
    python database/import_taxa_to_supabase.py --apply    # writes to the Supabase project below

Writing requires the SUPABASE_SERVICE_ROLE_KEY environment variable:
    export SUPABASE_SERVICE_ROLE_KEY=eyJ...   (from Supabase → Settings → API → service_role)
Or put the secret in sporely-admin.env for local admin/developer use.

Run supabase_taxa_schema.sql in the Supabase SQL editor first.
"""

import argparse
import os
import sys
import sqlite3
import requests
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.r2_storage import load_admin_env_file  # noqa: E402

SUPABASE_URL = 'https://zkpjklzfwzefhjluvhfw.supabase.co'
load_admin_env_file()
SERVICE_KEY  = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
DB_PATH      = Path(__file__).resolve().with_name('reference_data') / 'generated' / 'vernacular_multilanguage.sqlite3'
BATCH        = 500

def _headers() -> dict:
    return {
        'apikey':        SERVICE_KEY,
        'Authorization': f'Bearer {SERVICE_KEY}',
        'Content-Type':  'application/json',
        'Prefer':        'resolution=merge-duplicates',
    }

def post_batch(table: str, rows: list[dict], session=None) -> None:
    url = f'{SUPABASE_URL}/rest/v1/{table}'
    poster = session if session is not None else requests
    r = poster.post(url, headers=_headers(), json=rows, timeout=60)
    if r.status_code not in (200, 201):
        print(f'  ERROR {r.status_code}: {r.text[:300]}')

def run(apply: bool = False, session=None) -> None:
    """Dry run by default; only ``apply=True`` sends anything to Supabase."""
    if not DB_PATH.exists():
        sys.exit(f'Database not found: {DB_PATH}')

    conn = sqlite3.connect(DB_PATH)
    taxa_count = conn.execute('SELECT COUNT(*) FROM taxon_min').fetchone()[0]
    vernacular_count = conn.execute('SELECT COUNT(*) FROM vernacular_min').fetchone()[0]
    if not apply:
        conn.close()
        print(f'Dry run: {taxa_count} taxa and {vernacular_count} vernacular names '
              f'would be upserted into {SUPABASE_URL}.')
        print('Nothing was sent. Re-run with --apply to write.')
        return
    if not SERVICE_KEY:
        conn.close()
        sys.exit('Set SUPABASE_SERVICE_ROLE_KEY environment variable first.\n'
                 'Get it from Supabase → Settings → API → service_role (secret).')
    print(f'APPLY: writing {taxa_count} taxa and {vernacular_count} vernacular names to {SUPABASE_URL}')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # ── 1. taxa ───────────────────────────────────────────────────────────────
    print('Importing taxa…')
    cur.execute('''
        SELECT
            t.taxon_id,
            t.genus,
            t.specific_epithet,
            t.canonical_scientific_name,
            t.family,
            t.taxon_rank,
            t.norwegian_taxon_id,
            t.swedish_taxon_id,
            t.inaturalist_taxon_id,
            (SELECT e.external_id FROM taxon_external_id_min e
             WHERE e.taxon_id = t.taxon_id
               AND e.source_system = 'artportalen'
               AND e.is_preferred = 1
             LIMIT 1) AS artportalen_taxon_id
        FROM taxon_min t
    ''')
    batch, total = [], 0
    for row in cur:
        batch.append({
            'taxon_id':             row['taxon_id'],
            'genus':                row['genus'],
            'specific_epithet':     row['specific_epithet'],
            'canonical_scientific_name': row['canonical_scientific_name'],
            'family':               row['family'],
            'taxon_rank':           row['taxon_rank'],
            'norwegian_taxon_id':   row['norwegian_taxon_id'],
            'swedish_taxon_id':     row['swedish_taxon_id'],
            'inaturalist_taxon_id': row['inaturalist_taxon_id'],
            'artportalen_taxon_id': row['artportalen_taxon_id'],
        })
        if len(batch) >= BATCH:
            post_batch('taxa', batch, session)
            total += len(batch)
            print(f'  {total} rows', end='\r')
            batch = []
    if batch:
        post_batch('taxa', batch, session)
        total += len(batch)
    print(f'  Done — {total} taxa')

    # ── 2. taxa_vernacular ────────────────────────────────────────────────────
    print('Importing vernacular names…')
    cur.execute('''
        SELECT taxon_id, language_code, vernacular_name, is_preferred_name
        FROM vernacular_min
    ''')
    batch, total = [], 0
    for row in cur:
        batch.append({
            'taxon_id':       row['taxon_id'],
            'language_code':  row['language_code'],
            'vernacular_name': row['vernacular_name'],
            'is_preferred':   bool(row['is_preferred_name']),
        })
        if len(batch) >= BATCH:
            post_batch('taxa_vernacular', batch, session)
            total += len(batch)
            print(f'  {total} rows', end='\r')
            batch = []
    if batch:
        post_batch('taxa_vernacular', batch, session)
        total += len(batch)
    print(f'  Done — {total} vernacular names')

    conn.close()
    print('Import complete.')

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(allow_abbrev=False, description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--apply', action='store_true',
                        help='Write to the Supabase project. Without it the script only prints counts.')
    args = parser.parse_args(argv)
    run(apply=args.apply)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())

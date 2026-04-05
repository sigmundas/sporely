#!/usr/bin/env python3
"""
One-time import of taxa from vernacular_multilanguage_unified.sqlite3
into the Supabase taxa / taxa_vernacular tables.

Usage:
    python database/import_taxa_to_supabase.py

Requires the SUPABASE_SERVICE_ROLE_KEY environment variable:
    export SUPABASE_SERVICE_ROLE_KEY=eyJ...   (from Supabase → Settings → API → service_role)

Run supabase_taxa_schema.sql in the Supabase SQL editor first.
"""

import os
import sys
import sqlite3
import requests
from pathlib import Path

SUPABASE_URL = 'https://zkpjklzfwzefhjluvhfw.supabase.co'
SERVICE_KEY  = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
DB_PATH      = Path(__file__).parent / 'vernacular_multilanguage_unified.sqlite3'
BATCH        = 500

if not SERVICE_KEY:
    sys.exit('Set SUPABASE_SERVICE_ROLE_KEY environment variable first.\n'
             'Get it from Supabase → Settings → API → service_role (secret).')

headers = {
    'apikey':        SERVICE_KEY,
    'Authorization': f'Bearer {SERVICE_KEY}',
    'Content-Type':  'application/json',
    'Prefer':        'resolution=merge-duplicates',
}

def post_batch(table: str, rows: list[dict]) -> None:
    url = f'{SUPABASE_URL}/rest/v1/{table}'
    r = requests.post(url, headers=headers, json=rows, timeout=60)
    if r.status_code not in (200, 201):
        print(f'  ERROR {r.status_code}: {r.text[:300]}')

def run() -> None:
    if not DB_PATH.exists():
        sys.exit(f'Database not found: {DB_PATH}')

    conn = sqlite3.connect(DB_PATH)
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
            post_batch('taxa', batch)
            total += len(batch)
            print(f'  {total} rows', end='\r')
            batch = []
    if batch:
        post_batch('taxa', batch)
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
            post_batch('taxa_vernacular', batch)
            total += len(batch)
            print(f'  {total} rows', end='\r')
            batch = []
    if batch:
        post_batch('taxa_vernacular', batch)
        total += len(batch)
    print(f'  Done — {total} vernacular names')

    conn.close()
    print('Import complete.')

if __name__ == '__main__':
    run()

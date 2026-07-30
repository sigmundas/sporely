# Stage 3B runtime integration notes

These are **recorded, not implemented**. Every item below is a runtime-side
change that will be needed before the Stage 3B / 3B.1 SQLite candidate can
replace the currently bundled `vernacular_multilanguage.sqlite3`. No code
under `ui/`, `utils/`, `database/vernacular_db.py`, `database/taxon_lookup.py`,
`database/models.py`, or the Supabase publication path is touched by this
stage.

## 1. `language_code='no'` → `nb` + `nn` fallback

Stage 3B stores every Norwegian vernacular under its distinct code (`nb`
Bokmål or `nn` Nynorsk); the umbrella code `no` is not emitted. Consumers
that today query `WHERE language_code = 'no'` must fan out. Concrete
touchpoints:

- `database/vernacular_db.py:72-88` (`suggest_vernacular`) — accept `no`
  as an alias that expands to `('no', 'nb', 'nn')` at query time.
- `database/vernacular_db.py:106-121` (`suggest_vernacular_entries`) —
  same expansion.
- `database/vernacular_db.py:317-330` (`taxon_from_vernacular`) — same.
- `database/vernacular_db.py:343-360` (`vernacular_from_taxon`) — return
  `nb`/`nn` rows when the caller asks for `no`.
- `database/taxon_lookup.py:389-455` (`_local_common_name_rows`) — same.

Fallback shape (parameter-list, not string interpolation):

```python
if language == "no":
    languages = ("no", "nb", "nn")
else:
    languages = (language,)
```

An observation-side migration must NOT rewrite existing `no` snapshots on
observations — they stay as attributed source-truth text.

## 2. COL text identifiers use `taxon_external_id_text_min`

`database/models.py:687-703, 739-836` currently only consult
`taxon_external_id_min` (INTEGER `external_id`). To bridge a raw COL usage
ID like `9Z2GC`, teach `_lookup_external_taxon_id_from_db` to fall through:

```python
row = conn.execute(
    "SELECT taxon_id FROM taxon_external_id_min "
    "WHERE source_system=? AND external_id=? AND id_role='accepted' "
    "ORDER BY is_preferred DESC LIMIT 1", (source_system, external_id)).fetchone()
if row is None:
    row = conn.execute(
        "SELECT taxon_id FROM taxon_external_id_text_min "
        "WHERE source_system=? AND external_id=? AND id_role='accepted' "
        "ORDER BY is_preferred DESC LIMIT 1", (source_system, str(external_id))).fetchone()
```

`taxon_external_id_text_min` is populated for `source_system='col_xr'` today;
future non-integer identifier sources (MycoBank name IDs, Index Fungorum name
IDs) will land there too under an explicit `namespace`.

## 3. Observation back-fill precedence

The observation store persists three levels of taxonomic attribution:
`norwegian_taxon_id INTEGER`, an `ai_selected_taxon_id`/`taxonId` text
field (often carrying `NBIC:xxxx`), and a scientific-name snapshot. When we
add an explicit `sporely_taxon_id` column, the back-fill order is:

1. **Explicit NorTaxa/NBIC identifier first.** If the observation has a
   non-null `norwegian_taxon_id`, resolve via
   `taxon_external_id_min WHERE source_system='artsdatabanken' AND external_id=?`.
   This wins whenever it hits, because it is the least ambiguous binding.
   The Stage 3B.1 legacy import guarantees that every taxon carried by the
   pre-Stage-3A database is still reachable through this path.
2. **NBIC-prefixed string identifiers next.** If the observation carries
   `NBIC:xxxxx` (stripped from a mixed `taxonId` / `ai_selected_taxon_id`
   text field), pass the numeric suffix through the same
   `taxon_external_id_min` lookup — do NOT interpret the raw integer as a
   Sporely id.
3. **Unique scientific-name alias fallback.** Look up the persisted
   `ai_selected_scientific_name` via
   `taxon_min.canonical_scientific_name` + `scientific_name_min.scientific_name`
   (UNION with `language_code IN ('sci')`). Accept the fill ONLY when the
   union returns exactly one distinct `sporely_taxon_id` — Stage 3A
   preserves multiple canonical rows for the same scientific name when
   authorship disagrees, and back-filling into one of two homonyms would
   silently corrupt observation identity.
4. **Ambiguous or unresolved → leave the new column NULL.** Never guess.

This order is not code — it is the specification for the future migration.
Persisted scientific-name and NBIC snapshots on observations remain
untouched regardless.

## 4. Observer / maintainer UI surface

Recommendation only. See [`ui-menu-recommendation.md`](ui-menu-recommendation.md)
for the scoped `Help → Taxonomy` submenu, the items deliberately kept CLI-only
behind approval envelopes, and the non-goals for any GUI over
`database/taxonomy/`. No `ui/` change is part of Stage 3B.

## 5. Compatibility manifests still owed

Each consumer must publish `supported_taxonomy_schema_min/max`,
`tested_taxonomy_release`, and (desktop) `bundled_sqlite_sha256`. See
`database/taxonomy/docs/compatibility-contract.md`. Values for the current
Stage 3B.1 candidate (not yet activated):

- `taxonomy_schema_version = 2`
- `content_release_id = tax-2026.07.30-02`
- candidate `sqlite_sha256 = 993d1608df1cb0ae93aab7b35a889b29858f0add720b933f3c58ed1e8485b94f`
- candidate `gz_sha256 = fb7660c613d0909c22591abe90768a9ae3c0ea88a8b8d5b2ee2bdf6c69cb8938`
- `registry_concatenated_sha256 = 21b5d39d257799fdd4ea758d857fec4d29758b83dd7ea91bf3ecd24e3d1d3077`
- `compiler_manifest_sha256 = 5be1cd0971934a13ae12bf12c59392fe3eb55c72df8e99666f93c3232d6a9094`

## 6. Norwegian Red List 2021 overlay (Stage 3B.4)

The candidate now carries a `taxon_redlist_min` assessment overlay. This is
**data**, not identity — the overlay never allocates a Sporely id, never
adds a column to `taxon_min`, and never binds via name equality. See
[`redlist-overlay.md`](redlist-overlay.md) for the full contract; the
notes relevant to consumers are:

- Runtime lookup goes through
  `TaxonLookupService.get_redlist_lookup(taxon_id, area="Norge",
  source_release="2021")`, which returns an explicit
  `RedlistLookupResult` with one of four statuses: `none`, `unique`,
  `multiple_same_category`, `conflict`. The legacy
  `get_redlist_assessment(...)` remains and returns the deterministic
  representative for `unique` and `multiple_same_category`; it returns
  `None` for `conflict` — **it never auto-picks a category from a
  conflicting group**.
- Norway and Svalbard are separate rows; the API requires `area=`
  explicitly and never defaults across areas.
- Downgraded categories (`°` suffix) preserve both raw and normalized forms
  in `category_raw` / `category_code` / `category_is_downgraded`.
- Observation identity-invalidation rules from Stage 3B.3 are unchanged.
  The red-list runtime API is read-only; no write path exists into
  observations yet.
- Adding the overlay does **not** bump `TAXONOMY_SCHEMA_VERSION` — it
  produces a new content release (`tax-2026.07.30-02`).

## 7. Artsdatabanken identifier semantics — externally confirmed facts

Recorded facts from external verification (2026-07-30). These describe
existing runtime adapters that are **not modified in Stage 3B.4**; the
changes needed to align them will happen in a later stage.

- `https://artsdatabanken.no/Api/Taxon/ScientificName/{id}` — the URL
  path takes an Artsnavnebase **name-id**; the JSON response carries an
  Artsdatabanken **taxon-concept** identity. Read consumer at
  `ui/observations_tab.py:1187-97` (`_taxon_id_from_nbic`) is consistent
  with that contract.
- `https://artsdatabanken.no/arter/takson/{id}` — display page; the URL
  path requires an Artsdatabanken **taxon-concept id**.
- Current direct URL builders that embed `taxon_min.norwegian_taxon_id`
  into `/arter/takson/{id}` (`ui/image_import_dialog.py:2613`,
  `ui/observations_tab.py:13525-35`, `utils/cloud_sync.py:6800-09`) pass
  a name-id where a concept-id is required. **This is a known runtime
  bug**, tracked for a separate stage. Stage 3B.4 does not touch any
  runtime adapter.
- Artsobservasjoner `Sighting.Taxon` (`utils/artsobservasjoner_submit.py:654-673`,
  `931-32`) remains **unverified** as to whether it expects a name-id or
  a concept-id. The live submission path is currently fed with
  `taxon_min.norwegian_taxon_id` (a name-id). Behavior is unchanged
  pending explicit external-API-doc confirmation.
- All other classifications from the 2026-07-30 audit —
  Artsorakel prediction reads, `taxon_min.norwegian_taxon_id` storage,
  Supabase `norwegian_taxon_id`, `observations.ai_selected_taxon_id`,
  Artportalen — stand as documented there.

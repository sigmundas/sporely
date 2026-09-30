# Dyntaxa 2026-09-30 — pinned national-source profile

This directory pins the source profile for the Dyntaxa export published
`2026-09-30` (its `eml.xml` `pubDate`). Dyntaxa, SLU Artdatabanken's Swedish
taxonomic database, is Sweden's national source (taxonomy-v3 Stage 4P).

- `source.json` — profile bound to the identifier namespaces in
  `docs/identity-contract.md`: `dyntaxa_dwc_id`, `dyntaxa_taxon_id`,
  `dyntaxa_accepted_name_usage_id`, `dyntaxa_parent_name_usage_id`. Ids stay
  full LSIDs (`urn:lsid:dyntaxa.se:Taxon:<n>` for concepts,
  `urn:lsid:dyntaxa.se:TaxonName:<n>` for synonym names). The core lives at
  `Taxon.csv` and the vernaculars at `VernacularName.csv`. The Distribution
  extension (`SpeciesDistribution.csv`) is validated only; the Reference
  extension (`Reference.csv`) is declared in `ignored_extensions` and never
  read.

The offline test archive is generated in
`database/taxonomy/tests/test_taxonomy_v3_stage4p.py` with the same shape
(LSID ids, BOMs, a `UTF8`-labelled extension, a Reference extension).

## Acquisition

`sources/dyntaxa/2026-09-30/request.json` records the route and
`manifest.json` the pin:

- Route: the one `DWC_ARCHIVE` endpoint of the GBIF-registered checklist
  (dataset `de8934f4-a136-481c-a87a-b0b202b80a31`, DOI `10.15468/j43wfc`).
  That is SLU's standard Taxon Service Darwin Core download, the file GBIF
  crawls, not a custom export. The endpoint embeds SLU's subscription key; it
  is public in the GBIF registry record but is not copied here.
- Archive: 15,756,509 bytes, SHA-256
  `7947b7a7681f3654d5478694f1fe37fdd9e72dc3aab8f9b6311525d85cc4879c`,
  fetched 2026-09-30 in one GET. The bytes are Git-ignored at
  `sources/dyntaxa/2026-09-30/archive.zip`; the manifest is the source of
  truth and also pins every member.
- Licence: CC0 1.0 — stated in the archive's `eml.xml` `intellectualRights`
  and in the GBIF registry; the two agree.
- Citation: Backlund M (2026). Dyntaxa. Svensk taxonomisk databas. SLU
  Artdatabanken. Checklist dataset https://doi.org/10.15468/j43wfc accessed
  via GBIF.org on 2026-09-30. The archive's own EML citation (2021, version
  1.2) is stale and recorded verbatim only.

Dyntaxa has no version numbers; each export is identified by its `pubDate` and
its SHA-256. A later export is a new directory and a new pin, never an update
of this one.

## Validation

`national_source.py validate` passes: 196,304 Taxon rows (48,489 with kingdom
`Fungi`), 50,979 VernacularName rows (44,482 Swedish), 110,776 Distribution
rows; no orphan parent or accepted references.

Taxonomic statuses: `accepted`, `synonym`, `homotypicSynonym`,
`heterotypicSynonym`, `misapplied`, `proParteSynonym`. The compiler binds the
three synonym statuses to their accepted usage; misapplied and pro parte
usages are never bound (`compile_release._SYNONYM_STATUSES`).

## Identity

Dyntaxa is reviewed-identity-only: nothing in this archive gains a Sporely
identity until an owner-approved relationship binds it (see the taxonomy
README, "Adding a national source", and
`evidence/taxonomy-v3/stage4p/`).

```bash
./.venv/bin/python database/taxonomy/scripts/national_source.py validate \
  --profile database/taxonomy/national_sources/dyntaxa/2026-09-30/source.json \
  --archive database/taxonomy/sources/dyntaxa/2026-09-30/archive.zip
```

# Vernacular projection and evidence

Vernacular association never establishes identity. `compile_release.py` calls
`vernacular_projection.project` after canonical taxa, source usages and mapping
outputs have been fixed. No registry allocation, alias, supersession, scientific
name or authoritative bridge is introduced by this projection.

The additional association classes are `automatic_vernacular_enrichment` and
`owner_reviewed_vernacular_association`. A qualified concept kept at its existing
national source identity is `source_native_concept_no_col_mapping`; it is valid
source-native metadata, not failed COL enrichment. Existing source projections
and withheld association diagnostics remain explicit evidence as well.

Automatic enrichment compares exact normalized scientific name and rank against
the full accepted COL canonical universe before applying publication scope.
Accepted/current fungal usage, no qualifier/aggregate, no nomenclatural warning,
no reviewed contradiction and no explicit split/merge annotation are required.
Species classification differences below kingdom and authorship differences are
diagnostic. Scientific names are never reduced by stripping concept qualifiers.
Bibliographic publication titles are not concept qualifiers.

`policies/vernacular_associations.json` holds the reviewed concept snapshots.
They bind source namespace/usage ID, scientific name, rank, accepted-usage
reference, status, nomenclatural status, kingdom, qualified fields and raw concept
annotations, plus target stable ID and canonical source usage with the same
semantic fields. Both snapshots are fingerprinted. Current source and target
snapshots must match. Duplicate canonical usage reconstruction invalidates the
approval. Missing or replaced targets cannot be recovered by scientific-name
string, registry replacement or a supersession. A still-valid concept approval
applies to its current source vernaculars; previously approved strings are
historical review evidence, not an independent source of future names.

Entoloma's collective source constraint prohibits its metadata from reaching the
strict species. Its source-native identity is preserved; the compiler does not
invent a canonical aggregate.

Every release is rebuilt from current normalized inputs. Historical bundle
vernaculars are logged in `legacy_enrichment_skips.jsonl` and do not supply names.
Their external-ID compatibility branch remains independent. Existing current
source preferred names take precedence over new enrichment. No enrichment makes
an existing same-language preferred name nonpreferred.

Compiler artifacts include `vernacular.jsonl`, `vernacular_evidence.jsonl`,
`vernacular_reviews.json` (snapshots and current validation), and
`vernacular_changes.json`. The compiler manifest hashes each output and binds
review-ledger, previous-evidence and scope-policy hashes. Every projected row
carries its association evidence hash before compact SQLite export.

`--previous-vernacular-evidence` is comparison input only. It produces additions,
removals, changed targets, reused/invalidated approvals, source/target concept
removals, preferred-name flag changes and strict/qualified movements. Concept
removal checks use complete current source and target inventories, not whether a
concept still has a vernacular. The release recipe can supply `previous_evidence`
in its `vernacular_enrichment` block. The first evidence-enabled release also
needs a full SQLite baseline comparison because the previous release lacks this
compiler artifact.

The pinned 2026-10-04 acceptance is 1,619 automatic additions, 17 reviewed
additions, 1,636 new target/language/name rows on 766 concepts, and zero identity
or authoritative external-ID changes. The five open species remain withheld
from additional association. Qualified source-native names remain on their own
concepts.

Publication lifecycle is a separate stage. Before promotion, freeze and validate
the complete compiler/SQLite/evidence set and produce its archive and fingerprint.
Promotion must only publish the already frozen set, with no evidence generation
or mutation. Publication must retain both evidence and previous-release lineage.

## Current COL source-native names (Stage 1B)

`normalize_col_xr.py` emits the pinned `VernacularName.tsv` as
`vernacular.jsonl`, restricted to its normalized usage scope. It retains source
spelling and preferred metadata, maps ISO 639-2 to two-letter codes where
available, and stores raw fields, release, member and row provenance. Missing
language is explicitly `und`. Dangling archive usage references are recorded in
`vernacular_rejections.jsonl`; they never create target associations.

Compiler projection resolves only the existing canonical COL usage ID. No
scientific-name association is involved. Every normalized COL row attached to a
current canonical usage must appear in compiled evidence and the exact-key
SQLite projection (possibly deduplicated against another current source).
Independent Norwegian automatic/reviewed association evidence remains derivable
when COL carries the same name. Current national-source copies retain priority
for their existing preferred flags and compact-row provenance. Detailed COL
provenance remains in compiler/evidence artifacts even when SQLite deduplicates
its copy. Historical spellings supply neither new names nor preferred flags.

## Frozen evidence publication (Stage 2)

Before promotion, `freeze_release.py` validates the complete compiler manifest
inventory and a receipt bound to the accepted SQLite/compiler hashes. The pinned
recipe asserts 1,619 automatic and 18 reviewed additions on 767 concepts. The
freeze prepares the SQLite gzip, deterministic compiler/evidence tar archive,
bundle manifest, compatibility bytes and a fingerprinted `freeze.json`. Archive
members retain complete vernacular evidence/provenance, reviews, change report,
source acquisition records, normalization reports, policies and validation.

`promote_desktop_bundle.py` consumes only that frozen set and its expected hash.
The single reviewed-publication command is:

```bash
.venv/bin/python database/taxonomy/scripts/promote_desktop_bundle.py \
  --frozen-dir <durable reviewed frozen dir> --expect-freeze-sha256 <reviewed freeze.json sha256>
```

`build_release.py --promote` requires `--expect-freeze-sha256` and refuses
(nothing written to the bundle) unless its fresh freeze equals the reviewed
hash; no path rebuilds and promotes an unreviewed set.

## COL echo guard (Stage 3B, 2026-10-06)

Aggregated COL evidence must not override an explicit national qualifier that
it may echo. A `col_xr` vernacular row is withheld from a target when the same
(target, language, normalized name) is a NorTaxa association withheld for
`qualified_or_aggregate_source` (other withholding classes are unaffected),
recorded as `withheld_col_vernacular_association` evidence. An owner review
lifts the guard for the languages it lists. Reviews are concept decisions
limited to the languages in their `vernacular_names`. The qualifier pattern
treats `s.l.`/`p.p.` followed by a capital letter as author initials
(e.g. "S. L. F. Meyer", "P.P. Daniëls"), not qualifiers.
It verifies all file/member hashes, decompressed SQLite hash and publication
baseline before copying exact bytes. It publishes the archive descriptor in the
bundle manifest and retains prior archives and per-release freeze descriptors.
Future comparisons use a verified archived compiler evidence file explicitly;
previous evidence supplies lineage/change reporting only. No compact-database
schema extension is required for detailed vernacular provenance.

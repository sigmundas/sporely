# NorTaxa concepts have no parent in the desktop SQLite

Date: 2026-10-20. Status: diagnosed by reading the code and confirmed against
`tax-2026.10.07-01` data. Nothing is fixed here. This defect is separate from
the duplicate reconciliation work.

## Symptom

All 12,566 NorTaxa-canonical concepts in `taxon_min` have
`parent_taxon_id IS NULL`. Of the 620,976 COL concepts, only the root has a
NULL parent.

## Root cause

`database/taxonomy/scripts/build_sqlite_candidate.py:354–366`:

```python
def _resolve_parent_sporely_id(canonical_row, source_usage_index):
    parent_ref = canonical_row.get("parent_name_usage_id")
    resolution = canonical_row.get("parent_reference_resolution", "absent")
    if not parent_ref or resolution != "resolved":
        return None
    key = (
        canonical_row["canonical_source_code"],
        parent_ref["namespace"],          # 'nortaxa_parent_name_usage_id'
        parent_ref["value"],
    )
    return source_usage_index.get(key)    # index is keyed on 'nortaxa_taxon_id'
```

The compiler writes a NorTaxa parent reference under the reference namespace
`nortaxa_parent_name_usage_id` (identity contract: "parent-usage reference").
`_iter_source_usage_source_index` keys every usage by its own namespace, which
for NorTaxa is always `nortaxa_taxon_id`, so the lookup never hits. COL is not
affected: its parent references use `col_usage_id`, the same namespace as its
usages. Dyntaxa would hit the same mismatch (`dyntaxa_parent_name_usage_id`),
but Dyntaxa never anchors concepts.

## Evidence

The compiler marks every NorTaxa parent reference `resolved` in `taxa.jsonl`.
Resolved against `source_usages.jsonl` by `nortaxa_taxon_id`
(`scripts/reproduce_counts.py`, query `nortaxa_parent_reference_resolution`):

| Parent reference | Concepts |
|---|---|
| Parent is a NorTaxa-canonical concept | 7,012 |
| Parent is a NorTaxa usage bound into a COL concept (automatic or reviewed alias) | 5,551 |
| Parent reference not bound: out of scope (622049 *Sabella spallanzanii* → 139145; 623476 *Mitthyridium repens* → 189689) | 2 |
| No parent reference (the NorTaxa kingdom *Fungi*, 634865, the root) | 1 |
| **Total** | **12,566** |

`test_build_sqlite_candidate::test_hierarchy_only_when_resolved` checks only
that no parent dangles. A NULL parent passes it.

## Impact

- **Desktop.** No hierarchy for national-only concepts. Anything that walks
  `parent_taxon_id` sees 12,566 roots: genus → species browsing, ancestry for
  display, and any scope or rank reasoning on desktop.
- **Batch reconciliation.** None today, because nothing points at a NorTaxa
  concept as a parent. Once the defect is fixed, children of a retired NorTaxa
  genus resolve to its survivor: retired usages are re-keyed in
  `source_usages.jsonl`, so the index maps the parent usage to the survivor.
  Retirements then become hierarchy changes, and the release delta must
  account for them.
- **Cloud.** Not affected. The cloud export and the macrofungi scope read only
  COL-canonical concepts, through `macrofungi_scope.load_taxa`, which filters
  on `source_system='col_xr'`.

## Proposed test

Not added to the suite.

```python
# database/taxonomy/tests/test_build_sqlite_candidate.py
def test_national_concept_parent_resolves(tmp_path: Path) -> None:
    """A NorTaxa-canonical species whose parentNameUsageID names a NorTaxa
    genus gets that genus's Sporely id as parent_taxon_id."""
    release_dir = _compile(tmp_path, extra_nortaxa_rows=[
        # national-only genus and species with no COL twin
        {"taxonID": "900001", "scientificName": "Nationalia", "taxonRank": "genus",
         "taxonomicStatus": "valid", "parentNameUsageID": ""},
        {"taxonID": "900002", "scientificName": "Nationalia testis", "taxonRank": "species",
         "taxonomicStatus": "valid", "parentNameUsageID": "900001"},
    ])
    output = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release_dir, registry_path=tmp_path / "registry.jsonl",
                    output_db=output)
    conn = sqlite3.connect(f"file:{output}?mode=ro", uri=True)
    genus_id, = conn.execute("SELECT taxon_id FROM taxon_min WHERE canonical_scientific_name='Nationalia'").fetchone()
    parent, = conn.execute("SELECT parent_taxon_id FROM taxon_min WHERE canonical_scientific_name='Nationalia testis'").fetchone()
    assert parent == genus_id
    # and every non-root concept whose source parent reference is 'resolved'
    # has a parent, per source
    nulls = conn.execute("SELECT canonical_source_system, count(*) FROM taxon_min "
                         "WHERE parent_taxon_id IS NULL GROUP BY 1").fetchall()
    assert dict(nulls).get("nortaxa", 0) <= 1  # only the fixture's root genus
```

The `_compile` helper may need an `extra_nortaxa_rows` parameter, depending on
how the fixture is built. The assertion is the point.

## Proposed fix

Not applied.

```diff
--- a/database/taxonomy/scripts/build_sqlite_candidate.py
+++ b/database/taxonomy/scripts/build_sqlite_candidate.py
@@
+#: A parent reference is written under the source's *reference* namespace;
+#: the referenced row is a usage identified in the source's taxon namespace
+#: (identity-contract.md: parentNameUsageID is a parent-usage reference, its
+#: value a taxonID).
+_PARENT_REFERENCE_TARGET_NAMESPACE = {
+    "nortaxa_parent_name_usage_id": "nortaxa_taxon_id",
+    "dyntaxa_parent_name_usage_id": "dyntaxa_taxon_id",
+}
+
 def _resolve_parent_sporely_id(
     canonical_row: dict,
     source_usage_index: dict[tuple[str, str, str], int],
 ) -> int | None:
     parent_ref = canonical_row.get("parent_name_usage_id")
     resolution = canonical_row.get("parent_reference_resolution", "absent")
     if not parent_ref or resolution != "resolved":
         return None
+    namespace = _PARENT_REFERENCE_TARGET_NAMESPACE.get(
+        parent_ref["namespace"], parent_ref["namespace"])
     key = (
         canonical_row["canonical_source_code"],
-        parent_ref["namespace"],
+        namespace,
         parent_ref["value"],
     )
-    return source_usage_index.get(key)
+    parent = source_usage_index.get(key)
+    if parent == int(canonical_row["sporely_taxon_id"]):
+        return None  # a usage bound to its own concept is not a parent
+    return parent
```

This is a behaviour change to the shipped SQLite. It produces a new content
release and needs the product decision below first.

## Isolated repair (revision 3)

`scripts/groups.py` does the repair in the audit only. No repository code is
edited. It imports the real `_iter_source_usage_source_index` and
`_resolve_parent_sporely_id` read-only and runs them on the release's
`source_usages.jsonl`.

The shipped function resolves **0** of the 12,566 NorTaxa parents. With the
corrected key it resolves 12,563:

| Parent | Concepts |
|---|---|
| A NorTaxa-canonical concept | 7,012 |
| A COL concept | 5,551 |
| Unbound | 2 |
| Root | 1 |

The figures are in `data/groups_summary.json` under `parent_repair`.

**How the compiler re-parents.** The compiler already re-keys every usage of a
superseded concept onto its survivor. For example, NorTaxa 52369 now has
`sporely_taxon_id` 83668 in `source_usages.jsonl`, with `superseded_from`
624680. A child's parent reference therefore resolves to the survivor once the
key is fixed. No explicit re-parenting step exists or is needed. 74 NorTaxa
concepts would get a Stage 2 survivor (a COL concept) as parent this way.
Retiring a parent is therefore a hierarchy change. The release delta must
account for it, and decision D6 governs whether it is acceptable.

**Re-parenting operations, revision 4.** These are computed per release stage
(Codex review-3 finding 2), against what is retired by the end of each stage.
They are listed in `manifests/reparent-ops.jsonl` with fields `stage`,
`child_sporely_taxon_id`, `old_parent_retiree` and `new_parent_survivor`.

| Stage | Operations | Notes |
|---|---|---|
| S1 (A0 species + infraspecific) | **0** | 33 species with later-retiring children moved to A2 |
| S2 (A0 genus + higher) | 0 | 284 moved to A2 |
| S3 (A1) | 0 | 41 moved to A2 |
| S4 (A2) | 1,768 | |
| S5 (B-placement) | 8 | |

All children are NorTaxa-canonical. The revision-4 total is 1,776 (1,768 + 8).
Revision 3 counted 1,784 operations
against the union of every retirement, which hid links between stages: Codex
found 145, 51 of them crossing groups, for example Didymellaceae 623821 →
Stagonosporopsis 623822. The per-stage calculation removes that defect.

**Reconciliation with review-2 finding 3.** Counted at source level, over valid
NorTaxa usages, the retirees in the revision-2 partitions are parents as
follows:

| Revision-2 partition | Retirees that are parents |
|---|---|
| A-species | 61 |
| A-genus | 508 |
| A-higher | 196 |

This is reproduced in `review2_check_…`. The concept-level counts are
smaller for two reasons:
- children bound into COL concepts take their parent from COL, so they need
  no operation;
- children that retire themselves need no operation.

The old R9/H9 guard ("retirees have no children") checked only the broken
`parent_taxon_id`. It is replaced by R9 in the policy proposal and by H9 in the
build tests: the guard now checks the repaired source parents and the explicit
operation list.

## Product decision: what hierarchy may a NorTaxa concept have?

With the fix, 5,551 NorTaxa-canonical concepts get a COL-canonical parent,
and 7,012 get a NorTaxa parent. Desktop trees would mix the NorTaxa and COL
classifications: for example, a NorTaxa species under a COL genus whose family
differs from NorTaxa's.

| Option | Effect | Trade-off |
|---|---|---|
| (a) Accept mixed hierarchy | Every parent link applied | Simplest; matches what the source says |
| (b) Only NorTaxa → NorTaxa links | 7,012 linked; the 5,551 stay roots | Keeps the national tree internally consistent, but leaves gaps |
| (c) Keep NULL by design | No change | The policy is then documented, and the build test asserts it explicitly |

Either way, the parent links interact with batch A: retiring NorTaxa genera
moves their national children under COL genera. So decide this before, or
together with, decision D2, not only with D4 (review-2 finding 3). Until it is
decided, every candidate with re-parenting operations stays in A2.

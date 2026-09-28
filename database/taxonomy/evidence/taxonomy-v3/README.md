# Taxonomy v3 evidence

Plan: `docs/plans/active/2026-09-27-taxonomy-v3.md`.

## Stage 0 — coverage audit (`stage0/`)

Reproduce from the repository root:

```sh
.venv/bin/python database/taxonomy/evidence/taxonomy-v3/audit_stage0_coverage.py
```

Inputs:

- the tracked release `database/reference_data/generated/taxonomy_v2/tax-2026.09.26-02.sqlite3.gz`,
  checked against `manifest.json`;
- the gitignored source archives `database/taxonomy/sources/nortaxa/1.284/archive.zip`
  and `database/taxonomy/sources/col_xr/2026-07-17-XR/archive.zip`. The script
  refuses to run unless each matches the `archive_sha256` the release records
  in `taxonomy_meta`;
- `database/taxonomy/policies/global-macrofungi-scope.yml` for the cloud scope.

Outputs are byte-deterministic:

- `coverage-report.json` — Group A and Group B, full release and cloud scope,
  by evidence class, with shared-synonym-count distributions, the
  authoritative NorTaxa emission census and the regression-species placements;
- `group-a-<evidence-class>.manifest.json` — one immutable candidate manifest
  per evidence class. `members_sha256` is SHA-256 over the canonical JSON of
  `{"columns", "members"}`; `pins` carries the release and archive
  fingerprints. Every member is `needs_review`.
- `group-a-shared-synonymy--<review-class>.manifest.json` — the
  `shared_synonymy` manifest partitioned for review, so strong evidence can be
  approved by fingerprint without approving weak cases with it. The parent
  stays whole for accounting; the audit fails unless the classes partition it
  exactly.

The split is two fixed tables in the script, each pairing a rule's published
text with the predicate that executes it:

1. `SYNONYM_KIND_TESTS` gives every shared synonym one kind: the first test
   it meets, most certain evidence first — the sources' structured statuses
   (NorTaxa `nomenclaturalStatus`, COL `col:status` / `col:nameStatus`), then
   textual annotations (authorship, then a COL usage's `col:remarks` /
   `col:nameRemarks`), then exact name comparison, then a spelling heuristic,
   then provenance — else `ordinary`. Structured status always wins over a
   string heuristic.
2. `SHARED_REVIEW_CLASS_TESTS` gives the association the first class whose
   rule it meets, stated over those kinds. `ordinary` states its full
   conditions and holds exactly when no earlier rule does.

Each review manifest carries its `membership_rule`, both precedence lists and
the kind rules, the COL source identified as NorTaxa, the parent's
`members_sha256` and its cloud-scope count. Its members are the parent's
columns plus `shared_synonym_kind_counts` (exact) and
`shared_synonym_evidence`: for each listed shared synonym, its kind, the
evidence that decided it, both sources' structured statuses on it and the
source and `clb:merged` flag of each COL usage behind it.

An association is `ordinary` only when both accepted usages are species, the
accepted name keys agree, and at least one shared synonym is of kind
`ordinary`, unless that is its only `ordinary` shared synonym while each source
publishes at least five. A shared synonym is `ordinary` when:

- both sources publish it with an authorship;
- neither source's structured status marks it orthographic, manuscript, not
  validly published or misapplied, and COL does not publish it as an
  ambiguous (pro parte) synonym;
- its authorship carries no `ined.`, `nom. herb.`, `nom. nud.`, `nom. inval.`,
  `nom. illeg.`, sensu-style or `p.p.` qualifier, and no COL usage behind it
  has a remark or name remark carrying `ined.`, `nom. herb.`, `nom. nud.`,
  `nom. inval.`, `nom. illeg.`, "later homonym" or "published without a valid
  description";
- it is neither accepted name nor almost spelled like one; and
- at least one COL usage behind it comes from a COL source other than NorTaxa.

A weak synonym beside an `ordinary` one does not demote the association.

### What the pinned evidence establishes

Structured status and textual annotation are separate signals, and each is
used only as far as the pinned archives support it.

- **NorTaxa structured statuses.** `meta.xml` maps `nomenclaturalStatus` to
  the Darwin Core term, and `eml.xml` defines no values. `illegitimate` is set
  on 6,868 of the 8,965 Group-A shared synonyms, including usages that other
  sources treat as ordinary synonyms or basionyms (Lichen parietinus L.,
  NorTaxa 89847, is the basionym of Xanthoria parietina in Species Fungorum
  and COL). Its formal meaning is not established here. It is too broad to
  reject automatically, so it is recorded but not used. `orthographic`,
  `notvalidlypublished` and `misapplied` are specific and are used.
- **COL `col:nameStatus`.** This is a name status separate from
  `col:status = synonym`. `unacceptable` is not used: some of its values are
  propagated from NorTaxa, while some independently sourced ones mark real
  problems such as documented later homonyms. Those problems are caught by
  the textual warnings instead. `not established` and `manuscript` are used.
- **Textual annotations.** An explicit invalidity, illegitimacy or homonym
  warning in an authorship or in a COL remark makes that evidence item weak.
  Only the narrow phrases above are read. Citations, spelling or
  author-citation queries and taxonomic doubts are not evidence either way.
  Four otherwise ordinary items are weak only because of a remark: COL RQ6NJ
  (Nom. illeg.), M6LB4 (later homonym), R3ZLN and MD6D7 (nom. nud.). Two
  associations change class because of them: 56896 / 3F9D5 becomes
  `only_illegitimate`, and 56216 / QMQN, whose only independent synonym is the
  later homonym, becomes `only_mixed_weak`. 74124 and 204742 keep other
  independent `ordinary` synonyms.
- **Provenance.** COL XR merges programmatically integrated sources into the
  expert-curated base release. A COL usage whose `col:sourceID` is NorTaxa's
  COL source republishes NorTaxa's own assertion, so it is not independent
  corroboration of it. That source is identified at run time as the one COL
  source whose `source/<id>.yaml` title equals the NorTaxa archive's `eml.xml`
  dataset title, `Nortaxa (Artsnavnebasen)`, which is COL source 2030. The
  audit fails unless exactly one matches. Other merged sources (Dyntaxa, UKSI,
  FinBIF and others) are independent sources and still count. Of the 8,966
  COL usages behind the 8,965 Group-A shared synonyms, 3,316 come from source
  2030.

`members_sha256` covers only the columns and members, so an empty manifest has
the same value in every class. `file_sha256` (in `coverage-report.json`) also
covers the pins, evidence class and review class: approve by `file_sha256`.

Headline counts against `tax-2026.09.26-02`:

| | Full release | Cloud scope |
|---|---:|---:|
| Group A associations | 19,807 | 7,099 |
| — `reciprocal_accepted_synonymy` | 0 | 0 |
| — `one_directional_accepted_synonymy` | 0 | 0 |
| — `shared_synonymy` | 4,861 | 1,888 |
| — `no_published_cross_reference` | 14,946 | 5,211 |
| Group B pairs (NorTaxa concepts) | 7,423 (7,338) | 2,132 (2,113) |
| — `one_directional_accepted_synonymy` | 62 | 15 |
| — `shared_synonymy` | 2,032 | 574 |
| — `no_published_cross_reference` | 5,329 | 1,543 |

Group B's cloud column counts pairs whose COL side is in scope; no NorTaxa side
is. The cloud scope is 52,917 concepts, all COL-canonical.

`shared_synonymy` review classes, in precedence order:

| Review class | Full release | Cloud scope |
|---|---:|---:|
| `non_species_rank` | 12 | 5 |
| `rank_mismatch` | 0 | 0 |
| `infraspecific_rank` | 35 | 6 |
| `accepted_authorship_disagrees` | 6 | 0 |
| `only_orthographic_variant` | 35 | 29 |
| `only_unpublished` | 20 | 5 |
| `only_not_validly_published` | 11 | 6 |
| `only_illegitimate` | 1 | 1 |
| `only_interpretation_qualified` | 20 | 20 |
| `only_pro_parte` | 39 | 25 |
| `only_accepted_name_reauthored` | 1 | 0 |
| `only_unauthored` | 71 | 9 |
| `only_name_variant` | 74 | 24 |
| `only_nortaxa_derived` | 971 | 122 |
| `only_mixed_weak` | 70 | 16 |
| `single_shared_synonym_low_overlap` | 112 | 33 |
| `ordinary` | 3,383 | 1,587 |

The one former `one_directional_accepted_synonymy` member, NorTaxa 227128 /
COL 35YQ2 (Sporely 3841), was a grading artifact: NorTaxa publishes a synonym
usage (226877) spelled exactly like its own accepted name, which
`cross_reference_evidence.grade` used to count as listing COL's identical
accepted name. It now grades `no_published_cross_reference`. No Group-B pair
changed class.

Authoritative NorTaxa emission on the cloud scope is exactly 53482 → 7821 and
52369, 58722 → 83668, computed as `cloud_export.emit_taxon_external_id_authoritative`
does. Every raw NorTaxa row on those two concepts is reconciled: 58722 is
published because the reviewed supersession re-keyed it, while the seven
synonym rows on 7821 are `intra_source_synonym`, which
`mapping_policy.yml.authoritative_bridge_emission` does not admit.

Group A has no reciprocal candidate: the only one in the v2 closeout, 52369,
is now a reviewed supersession. Conocybe vexans / Pholiotina vexans is not in
Group B, because the canonical names differ; its pair (NorTaxa 58766, COL XQZ6)
is graded explicitly and is `reciprocal_accepted_synonymy`.

NBIC:56449, which sporely-web's `taxonomy-v2.test.js` uses as an id that does
not resolve, is a real NorTaxa taxon: Gloeophyllum odoratum, NorTaxa-canonical
Sporely 626327, in Group B with COL 3GBK2 (Sporely 11307, `shared_synonymy`).
It is a temporary fixture. If Stage 2 reconciles that pair, the test needs a
synthetic id.

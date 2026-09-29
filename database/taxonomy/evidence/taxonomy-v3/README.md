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

## Stage 1A — reviewed mappings for the approved manifest

The owner approved one manifest, `stage0/group-a-shared-synonymy--ordinary.manifest.json`
(`file_sha256` `1eda453a…cda4d`, 3,383 members). `generate_stage1a_mappings.py`
turns that decision into `policies/manual_mappings.yml`:

- `approved_manifests` records the approval once: path, `file_sha256`, pins,
  approver, date and decision reference;
- `mappings` gains one approved `exact` record per member, NorTaxa taxonID →
  COL usage, one line each, each carrying `approved_manifest` (the
  `file_sha256` and the member it came from).

The generator stops, writing nothing, unless the manifest hashes to the
approved `file_sha256`, its pins equal the approved pins, and the registry
already binds every member's NorTaxa usage (alias) and COL usage (anchor) to
the member's `sporely_taxon_id`. It is idempotent; `--check` verifies the
committed ledger is current.

    .venv/bin/python database/taxonomy/evidence/taxonomy-v3/generate_stage1a_mappings.py --check

The compiler and `validate_policies.py` both run
`bridge_emission.verify_manifest_approvals`: a record that claims the approval
is refused unless the manifest file still hashes to the cited digest and the
record is exactly one of its members. Emission is unchanged: only
`manual_approved_exact` bindings are published, so a sibling review class,
`no_published_cross_reference`, and taxa new in a later NorTaxa release stay
unemitted. NorTaxa 56227 (Craterellus tubaeformis) stays unresolved.
Compiled `mappings.jsonl` records carry `approved_manifest_file_sha256`, so an
emitted bridge traces to its record and the approval inside the release.

## Stage 2 — Group-B duplicate candidates graded for review (`stage2/`)

Reproduce from the repository root, with the same inputs as Stage 0:

```sh
.venv/bin/python database/taxonomy/evidence/taxonomy-v3/audit_stage2_group_b.py
```

Group B is found by name equality: a NorTaxa-canonical concept and a
COL-canonical concept with the same `canonical_scientific_name`. That query
only finds candidates. Each pair is graded by `cross_reference_evidence.grade`
and, for `shared_synonymy`, by Stage 0's synonym kinds and review classes, so
a class name means the same in both groups. The pins equal Stage 0's.

Nearly every Group-B `shared_synonymy` pair (2,002 of 2,032) is in Stage 0's
`accepted_authorship_disagrees` class. The compiler matches automatically on
the accepted name key, so Group B is mostly pairs whose authorships differ.
That class is split again (`AUTHORSHIP_DIFFERENCE_TESTS`, first rule met) by
how the accepted authorships differ:

- `same`, `accepted_name_differs`;
- `typography_only`: equal after removing whitespace, diacritics and case;
- `sanctioning_citation`: equal after also removing a sanctioning citation
  (`(Pers. : Fr.) Boud.` against `(Pers.) Boud.`);
- `different_authorship`: everything else, including abbreviations.

It is also split by the review class its shared synonyms would get if the
accepted keys agreed. The resulting sub-class, for example
`sanctioning_citation--ordinary`, is a citation-form reading only. It proves
no identity, and it keeps strong and weak synonym evidence out of the same
batch.

Outputs are byte-deterministic:

- `group-b-<evidence-class>.manifest.json`, `group-b-shared-synonymy--<review-class>.manifest.json`
  and `group-b-shared-synonymy--accepted-authorship-disagrees--<sub-class>.manifest.json`.
  Every member is `needs_review`. Members are in cloud-impact order: COL side
  in the cloud scope, then NorTaxa side with vernaculars, then identifier. Each
  manifest's `approval_mode` says how decision 2 lets it be decided:
  `batch_by_file_sha256` (a leaf class), `individual` (one-directional),
  `decided_through_its_partition` (a parent kept for accounting) or
  `not_approvable` (`no_published_cross_reference`).
- `group-b-report.json`: counts, `file_sha256` per manifest, the
  `review_queue` in cloud-impact order, `not_one_to_one` pairs, and the
  recorded regression outcomes.

| | Full release | Cloud scope |
|---|---:|---:|
| Group-B pairs | 7,423 | 2,132 |
| — NorTaxa side has vernaculars | 2,287 | 1,323 |
| — `one_directional_accepted_synonymy` | 62 | 15 |
| — `shared_synonymy` | 2,032 | 574 |
| — `no_published_cross_reference` | 5,329 | 1,543 |

The head of the review queue:

| Sub-class manifest | Members | Cloud | Cloud with vernaculars |
|---|---:|---:|---:|
| `sanctioning_citation--ordinary` | 336 | 241 | 236 |
| `different_authorship--ordinary` | 839 | 116 | 78 |
| `sanctioning_citation--only_nortaxa_derived` | 63 | 45 | 45 |
| `sanctioning_citation--single_shared_synonym_low_overlap` | 46 | 41 | 41 |
| `typography_only--ordinary` | 179 | 46 | 38 |

Not one-to-one: 82 NorTaxa concepts share a name with more than one COL
concept (167 pairs), and 25 COL concepts with more than one NorTaxa concept
(54 pairs). A supersession names one current concept, so each such pair needs
its own choice even inside an approved batch.

**Decision status.** No Group-B manifest or pair has an owner decision.
Nothing is approved, and no supersession or mapping was generated.
`concept_supersessions.yml` still holds only 52369's record.

Regression outcomes, which the audit re-derives and refuses if they change:

- Cantharellus cibarius (NorTaxa 56210 / COL QMKY):
  `sanctioning_citation--ordinary`, left open. COL 168873 has no vernaculars
  and does not gain "kantarell". That name stays on NorTaxa concept 626243.
- Conocybe vexans / Pholiotina vexans (NorTaxa 58766 / COL XQZ6): outside
  Group B, `reciprocal_accepted_synonymy`, left open. The evidence has the
  same shape as the approved 52369 supersession, but superseding 627000 by
  617026 needs the owner's own decision. NBIC:58766 stays unresolved.
- Gloeophyllum odoratum (NorTaxa 56449 / COL 3GBK2):
  `sanctioning_citation--ordinary`, not reconciled. NBIC:56449 did not become
  resolvable, so sporely-web's temporary fixture stays valid.

Before any Group-B supersession ships, the read-only check that no cloud
observation references a retiring concept must run (deferred to its external
gate). Desktop observations are checked with
`database/audit_observation_identity.py`.

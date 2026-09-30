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
  Also `group-b-reciprocal-accepted-synonymy--one-to-one.manifest.json` and
  `group-b-not-one-to-one.manifest.json`. Every member is `needs_review`.
  Members are in cloud-impact order: COL side in the cloud scope, then
  NorTaxa side with vernaculars, then identifier. Each manifest's
  `approval_mode` says how decision 2 lets it be decided:
  - `batch_by_file_sha256`: a one-to-one reciprocal or `shared_synonymy` leaf;
  - `individual`: one-directional, and not one-to-one;
  - `decided_through_its_partition`: a parent kept for accounting;
  - `not_approvable`: `no_published_cross_reference`.

  Every pair is in exactly one decision leaf (a manifest that is not
  `decided_through_its_partition`).
- `group-b-report.json` contains:
  - counts and the `file_sha256` of each manifest;
  - `pair_review_queue`: all 2,094 decidable pairs in one global cloud-impact
    order, each naming its decision manifest and that manifest's
    `file_sha256`;
  - `not_one_to_one`: every such pair and the manifest that decides it;
  - `decision_manifests`: an index of the decision leaves, which is not a
    review order;
  - the recorded regression outcomes.

| | Full release | Cloud scope |
|---|---:|---:|
| Group-B pairs | 7,423 | 2,132 |
| — NorTaxa side has vernaculars | 2,287 | 1,323 |
| — `one_directional_accepted_synonymy` | 62 | 15 |
| — `shared_synonymy` | 2,032 | 574 |
| — `no_published_cross_reference` | 5,329 | 1,543 |

The pair queue orders pairs, not manifests: 518 cloud pairs whose NorTaxa
side has vernaculars come first, then 71 other cloud pairs, then 614 and 891
non-cloud pairs. The largest decision leaves by cloud pairs with NorTaxa
vernaculars:

| `accepted_authorship_disagrees` sub-class | Members | Cloud | Cloud with vernaculars |
|---|---:|---:|---:|
| `sanctioning_citation--ordinary` | 336 | 241 | 236 |
| `different_authorship--ordinary` | 836 | 115 | 77 |
| `sanctioning_citation--only_nortaxa_derived` | 63 | 45 | 45 |
| `sanctioning_citation--single_shared_synonym_low_overlap` | 46 | 41 | 41 |
| `typography_only--ordinary` | 179 | 46 | 38 |

Not one-to-one: 217 pairs. On 82 NorTaxa concepts, one concept shares its
name with more than one COL concept; on 25 COL concepts, one shares its name
with more than one NorTaxa concept. A supersession names one current concept,
so no batch decision covers such a pair. All 217 are listed in the report:

- 211 are `no_published_cross_reference`, so they are not approvable;
- none is one-directional;
- 6 `shared_synonymy` pairs are routed out of their review classes into
  `group-b-not-one-to-one.manifest.json`, which is `individual`.

### Owner decisions and supersessions

On 2026-09-29 the owner recorded two decisions (manual verification, gate
`fbeea497a1cb45c8a94baae48be43acc`):

- **`group-b-authorship-partition`:** the owner accepted the
  `typography_only` / `sanctioning_citation` / `different_authorship` split.
- **`group-b-owner-decisions`:** the owner approved three decision manifests
  by `file_sha256`, and three pairs individually.

Approved manifests:

| Sub-class manifest | `file_sha256` | Members |
|---|---|---:|
| `typography_only--ordinary` | `dd7a7bd0dcb6b57e0f147b5a7f27f91eb1e513f640bdcf126dc72c5485808e42` | 179 |
| `sanctioning_citation--ordinary` | `b49338db18c097689fb0239bf68adc5640409604ad6816cad74967c20c9fdcba` | 336 |
| `different_authorship--ordinary` | `7edfb0f253bd278af6c4d5e3f7462cc69c7fa6f9aac1c95b71840a4271bd8653` | 836 |

Individually approved pairs:

- Cantharellus cibarius (NorTaxa 56210 ↔ COL QMKY);
- Gloeophyllum odoratum (NorTaxa 56449 ↔ COL 3GBK2);
- Conocybe vexans / Pholiotina vexans (NorTaxa 58766 ↔ COL XQZ6).

The first two are also members of the sanctioning-citation manifest. Every
other candidate stays open, including:

- not-one-to-one pairs;
- the weak-evidence classes;
- one-directional pairs;
- pairs with no published cross-reference.

In a Group-B pair both concepts already hold an allocated `sporely_taxon_id`.
An approved relationship is therefore a concept supersession, as 52369 → 83668
was. `generate_stage2_supersessions.py` writes the approvals into
`policies/concept_supersessions.yml`:

- each manifest approval is recorded once in `approved_manifests`, with its
  path, `file_sha256`, pins, approver, date and decision reference;
- each member gets its own approved `exact` supersession, carrying
  `approved_manifest` (the `file_sha256` and the member);
- Conocybe vexans gets an individually reviewed record.

Together with 52369's record, the ledger now holds 1,353 supersessions: 1,351
manifest members, Conocybe vexans and 52369.

    .venv/bin/python database/taxonomy/evidence/taxonomy-v3/generate_stage2_supersessions.py --check

The generator stops, writing nothing, unless:

- every manifest hashes to its approved digest, carries the approved pins,
  and is a `batch_by_file_sha256` leaf of one-to-one pairs;
- the registry anchors both usages of every pair at the pair's two concepts;
- the retiring concepts are distinct and none of them is a survivor.

The compiler and `validate_policies.py` re-check every manifest-bound record
with `bridge_emission.verify_supersession_manifest_approvals`. It refuses a
tampered manifest, different pins, a non-batch or not-one-to-one manifest, a
record for a non-member, a duplicate, and an approved manifest with any
member left without a record. The registry is not written, so
every retired concept keeps its anchor and its allocation history. The
compiler re-keys the retired concept's usages, vernaculars and external ids
onto the survivor and emits no row for the retired concept.

Checks against `tax-2026.09.26-02`:

- no retiring concept has child concepts;
- no survivor already carries a NorTaxa id;
- no retiring NorTaxa concept is in the cloud scope.

The manifests stay immutable `needs_review` inputs. Re-running the audit
after the decision changed only `group-b-report.json`, which records the
decision and the outcomes below.

Regression outcomes, which the audit re-derives and refuses if they change:

- Cantharellus cibarius (NorTaxa 56210 / COL QMKY):
  `sanctioning_citation--ordinary`, **approved**. NorTaxa concept 626243 is
  superseded by 168873, so "kantarell" reaches 168873 only through that
  approved relationship.
- Conocybe vexans / Pholiotina vexans (NorTaxa 58766 / COL XQZ6): outside
  Group B, `reciprocal_accepted_synonymy`, **approved** individually. 627000
  is superseded by 617026, and NBIC:58766 resolves to 617026.
- Gloeophyllum odoratum (NorTaxa 56449 / COL 3GBK2):
  `sanctioning_citation--ordinary`, **reconciled**. 626327 is superseded by
  11307, so NBIC:56449 becomes resolvable. sporely-web's temporary
  unresolved fixture then needs a synthetic id, which Stage 5 owns.

These take effect in a release compiled with the updated ledger. No release
is built or published by this stage.

Before any of these supersessions ships:

- **Cloud:** the read-only check that no cloud observation references a
  retiring concept must run. It is deferred to its external gate and repeated
  before production activation.
- **Desktop:** run `database/audit_observation_identity.py --taxonomy
  <candidate.sqlite3> --observations <desktop database>` against the
  candidate compiled with this ledger. The retiring set is every
  `superseded_sporely_taxon_id` in the ledger.

## Stage 3R — release-build inputs follow concept supersessions (`stage3r/`)

`superseded-references.json` records the recount of pinned publishing-id
entries keyed to a concept a reviewed supersession retired, taken from the
committed inputs by the `build_release.py` preflight
(`check_superseded_references`): 1 Artportalen overlay entry and 20
iNaturalist refresh entries, none colliding, none unresolvable. It also
records the scratch `build_release.py` run over the committed recipe
(deterministic, no new allocations, not promoted) and a control build that
shows the only output difference is the re-keying of those entries. The pinned
inputs are unchanged; the re-keying happens at build time. Reproduce the
recount with:

```sh
.venv/bin/python -c "import sys; sys.path.insert(0, 'database/taxonomy/scripts'); import build_release as br, json; print(json.dumps(br.check_superseded_references(br.load_recipe(br.DEFAULT_RECIPE)), indent=2))"
```

## Stage 4P — Dyntaxa as a national source (`stage4p/`)

Dyntaxa (SLU Artdatabanken) is pinned at `sources/dyntaxa/2026-09-30/`
(archive SHA-256 `7947b7a7681f3654d5478694f1fe37fdd9e72dc3aab8f9b6311525d85cc4879c`,
CC0 1.0, GBIF DOI `10.15468/j43wfc`) and compiled as a
reviewed-identity-only source: no Dyntaxa usage gains a Sporely identity
until an owner-approved relationship binds it (decision 2).

Reproduce the candidate evidence with:

```sh
.venv/bin/python database/taxonomy/evidence/taxonomy-v3/audit_stage4p_dyntaxa.py
```

`dyntaxa-report.json` is pinned to release `tax-2026.09.26-02`, the COL XR
archive and the Dyntaxa archive. Its population is the 18,383 accepted
`Fungi` concepts of the Dyntaxa export; 947 misapplied and pro parte usages
are kept out of their concepts' synonymy. Partition:

| Class | Concepts | Meaning |
|---|---:|---|
| matched | 16,214 | exactly one COL-canonical candidate, by the Dyntaxa accepted name (15,392) or, failing that, by a Dyntaxa synonym name (822), and no other Dyntaxa concept claims it |
| ambiguous | 173 | more than one candidate for one basis |
| not one-to-one | 20 | one candidate, claimed by more than one Dyntaxa concept |
| unmatched | 1,976 | no candidate: Dyntaxa-only; reported, never allocated |

Matched pairs graded by `cross_reference_evidence.py` (cloud scope in
brackets): `reciprocal_accepted_synonymy` 203 (103),
`one_directional_accepted_synonymy` 510 (192), `shared_synonymy` 5,343
(1,575), `no_published_cross_reference` 10,158 (4,081). `shared_synonymy` is
split into Stage 0's review classes with Dyntaxa's status vocabulary
(`orthographia`, `invalidum`, `nudum`, `illegitimum`, `rejiciendum`); its
provenance class is `dyntaxa_derived`, because COL source 2041 is Dyntaxa
itself and a synonym COL has only from it is not independent corroboration.
`ordinary` has 2,158 members (719 in the cloud scope).

Every manifest is a review input. **None is approved.** Each is identified by
the `file_sha256` recorded under `manifests` in `dyntaxa-report.json`; an
owner approval must name that value, as for Stage 1A.

`generate_stage4p_mappings.py` turns a recorded owner approval into one
`manual_mappings.yml` record per member. Its `OWNER_APPROVALS` is empty, so a
run today changes nothing (`--check` passes on the committed ledger). It
refuses a file whose SHA-256 differs from the approval, a manifest whose pins
differ from the approval's or whose Dyntaxa pin is not the acquired archive,
the parent `shared_synonymy` manifest, and the classes decision 2 keeps out
of batch approval (`one_directional_accepted_synonymy`,
`no_published_cross_reference`, ambiguous, not one-to-one, unmatched). The
compiler's verifier then requires every Dyntaxa record to be a member of the
approved file, with the approval restating the manifest's pins and the
record's `source_release_range` equal to the pinned Dyntaxa release.

Regression species (by Sporely id): 83668 (canonical Conocybe rugosa) pairs
with Dyntaxa `Taxon:3423` Pholiotina rugosa by synonym name and grades
`reciprocal_accepted_synonymy`; 7821 Entoloma conferendum with `Taxon:3957`
(`shared_synonymy`, `ordinary`); 617026 Conocybe vexans with `Taxon:236654`
(`one_directional_accepted_synonymy`); 620306 Craterellus tubaeformis and
168873 Cantharellus cibarius grade `no_published_cross_reference`.

`artportalen_relation` measures how the release's Artportalen ids relate to
Dyntaxa: 8,964 of 9,000 are the number of a Dyntaxa accepted Fungi concept
with the same name, 25 the number of a renamed or sensu-lato concept, 11 a
number with no accepted Fungi concept. Artportalen numbers Dyntaxa concepts;
it is still its own namespace, and its rows bind nothing.

`build-comparison.json` records two scratch `build_release.py` runs of
`tax-2026.09.30-01` (neither promoted), with and without Dyntaxa in the
recipe: both deterministic, registry unchanged, no new allocations, and every
compile artifact and SQLite table identical except the Dyntaxa pin in
`taxonomy_meta`, the compiler manifest that records it, and additive Dyntaxa
diagnostics. With no approved mapping, Dyntaxa contributes nothing to a
release yet.

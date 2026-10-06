# Owner-reviewed Norwegian vernacular simulation — 2026-10-04

Applied the eight requested owner approvals to the evidence simulation, alongside the earlier Helvella crispa and Henningsomyces puber approvals. The automatic rule is unchanged; reviewed selections are a separate metadata-only evidence class. Changes remain uncommitted.

| Result | Count |
|---|---:|
| Automatic source vernacular rows | 1,621 |
| Automatic unique projected name/language/target rows (unchanged) | 1,619 |
| Automatic canonical concepts (unchanged) | 756 |
| Latest reviewed additions | 13 rows / 8 species |
| All reviewed additions | 17 rows / 10 species |
| Total proposed new rows / affected concepts | 1,636 / 766 |
| Remaining exceptions | 104 source name rows / 52 usages |
| Remaining species / genera | 6 / 46 |
| Species needing further owner review / collective hold | 5 / 1 |
| Reviewed target reconstruction failures | 0 |

The automatic manifest is byte-identical to the preceding revised-policy manifest. Each automatic row still asserts `len(full_bundle_target_ids) == 1`. Owner selection does not relax that guard; Helvella crispa and Henningsomyces puber retain their original two-target ambiguity in provenance.

## Reviewed target selections

| Source species | Norwegian approved name | Sporely target | COL usage |
|---|---|---:|---|
| Cortinarius milvinicolor | oliven rådyrslørsopp | 618912 | YLJ4 |
| Helvella crispa | lys høstmorkel | 14541 | 3KJX6 |
| Henningsomyces puber | dunpipe | 14834 | 3KV7Z |
| Hypsizygus tessulatus | dråpeknippesopp | 103130 | 6N4RM |
| Lentaria micheneri | slank vedkorallsopp | 23749 | 3T5CV |
| Mycena pearsoniana | sumpreddikhette | 34794 | 44TLW |
| Pseudoboletus parasiticus | snylterørsopp | 52936 | 4NQ37 |
| Pseudoinonotus dryadeus | tårekjuke | 54615 | 4NYKZ |
| Ramaria conjunctipes | norsk flammekorallsopp | 58693 | 4RBNJ |
| Rhizopogon luteolus | gul ekornnøtt | 60559 | 4SD8Z |

Approved names include the corresponding existing NorTaxa Nynorsk associations where present. The eight latest source usages contribute 13 rows. Hypsizygus source/target spelling is preserved as pinned, with no scientific alias creation. The selected stable ID, COL usage, canonical scientific name and available authorship are checked against the full pinned bundle. Source release, source scientific name/rank, original status, authorship/qualifier and nomenclatural metadata remain unchanged in [reviewed-associations.json](reviewed-associations.json). Classification disagreements and original conservative reasons remain diagnostic evidence. Existing preferred vernaculars are preserved.

## Remaining species exceptions

| Species | Bokmål names | Disposition |
|---|---|---|
| Entoloma sericellum | silkerødskivesopp, silkerødspore-gruppen | Collective concept preserved; no strict projection |
| Inocybe praetervisa | knolltrevlesopp, vanlig knolltrevlesopp | Qualified usage; needs owner review |
| Ramaria aurea | falsk lindekorallsopp | Qualified usage; needs owner review |
| Ramaria rubrievanescens | taigarødtuppsopp | Qualified usage; needs owner review |
| Ramaria rubripermanens | sørlig rødtuppsopp | Qualified usage; needs owner review |
| Ramaria strasseri | blek rødtuppsopp | Qualified usage; needs owner review |

Entoloma is deliberately distinct: strict **E. sericellum**, Sporely **96276 / COL 6FDM9**, already has **silkerødspore / silkeraudspore**. NorTaxa record **53666**, with raw authorship `(Fr. : Fr.) P. Kumm. agg.`, carries **silkerødspore-gruppen / silkeraudspore-gruppa** on existing source-bound concept **625268**. Those names are absent from the strict target. The owner semantic display `Entoloma sericellum coll.` describes this collective evidence without rewriting the source or allocating a canonical concept. The hold applies to every vernacular of that collective source usage. See [collective-concept-preservation.json](collective-concept-preservation.json).

## Exact remaining exception reasons

Counts below are source usages, with overlapping reasons; they are not additive.

| Reason | Source usages | Source name rows |
|---|---:|---:|
| collective_concept_must_remain_distinct | 1 | 4 |
| higher_classification_disagreement | 42 | 83 |
| kingdom_disagreement | 1 | 2 |
| multiple_canonical_targets_in_full_release | 10 | 19 |
| multiple_canonical_targets_in_scope | 7 | 13 |
| qualified_or_aggregate_source | 6 | 13 |
| source_not_fungi | 1 | 2 |

See [exception-source-usages.json](exception-source-usages.json) for all 52 remaining usages and [species-review-queue.json](species-review-queue.json) for the original 16 species and their current review statuses.

## Verification and boundary

Six focused offline safety tests passed: provenance/identity linkage and preferred-name preservation; missing, mismatched and duplicate target withholding; reviewed identity contradiction blocking; collective-to-strict projection blocking. Full simulation assertions passed for grønnkremle, grønn navlesopp, Gerronema, lys høstmorkel, rognerust and gråfiolett køllesopp.

A repeat execution against the same pinned inputs produced byte-identical generated evidence files, including the compressed all-source ledger. Syntax and diff checks passed. [verification.json](verification.json) records input/output fingerprints, the repeat comparison and unchanged identity/bridge hashes.

No compiler, source-usage binding, alias, supersession, registry allocation, manual identity mapping or authoritative bridge file was edited. The current bundle and pinned compiler evidence remain unchanged. This pass writes audit evidence only; there is no compiler enrichment implementation, release build, commit or production change. Publication-time evidence archiving remains a later bounded implementation stage.

# Review of relationship pass 2 by Codex (gpt-6.1-sol, high effort), 2026-10-21

This read-only review ran through `codex exec` in a read-only sandbox. The text below is its output, unedited.

**VERIFIED: sound with corrections. PRAGMATIC: unsound under its “no known contradiction” definition.**
Paths below are relative to `relationship-pass/v2/`.

1. **CORRECTNESS — documented links do not resolve contradictory basionym citations.**
   `relate_v2.py:286–313` admits accepted-author `candidate` results without checking consistency against the documented basionym.
   **Sebacina grisea 57952/4W79Z**: NorTaxa accepted `(Pers.) Bres.`, but verifying synonym **62018/6HKXR** derives from **Bres.**, with `6HKXR→4W79Z`.
   **Cryptodiaporthe populea 129389/ZX7T**: both accepted citations name **Sacc.**; documented basionym **169573/37GG3** names **Sacc. & Briard**.
   Source rows confirm both contradictions. Hold these for review; neither proves different taxa.

2. **CORRECTNESS — author-role conflicts enter the 1,207 pragmatic candidates.**
   `authors_v2.py:part_cmp/compare` compares roles within each parenthetical/combining part but misses roles moving between parts.
   Executed probes classify `(Pers.: Fr.) Fr.` / `Pers. ex Fr.` and `(Bull.: Fr.) Lam.` / `Bull. ex Lam.` as `candidate`.
   Accordingly **Gyromitra esculenta 79004/3HZQ4** and **Amanita verna 52179/5TYR8** enter pragmatic despite sanctioning/basionym/validating-role changes.
   Census found **10 pragmatic pairs** moving the same author from basionym to pre-`ex` role, including these two. They need unresolved review.

3. **CORRECTNESS — particle removal still deletes initials.**
   `authors_v2.py:52` removes `D.` because folded `d` is a particle.
   Probes: `D. Hawksw.` / `Hawksw.` → `identical`; `D. Hawksw.` / `J. Hawksw.` → `candidate`.
   Preserving `D.` changes **12 pragmatic “identical” pairs to candidates**, including **Mycomicrothelia inaequalis 79691/44WWF**.
   No verified eligible-witness comparison changed in this bounded probe. Blanket particle deletion also makes `de Vries` / `van Vries` identical.

4. **RISK — pragmatic includes missing evidence and unsupported attribution explanations.**
   **234 candidates lack accepted authorship:** 202 both sides, 32 one side; **Lecidea lanuginosa 70713/DSLY3** is blank/blank.
   These are name/rank candidates, not author-abbreviation evidence.
   `f.` is parsed as a surname; `in` as initials. **Hypogymnia rugosa 71787/PLQNZ**, `Pike in Hale` / `Pike`, is therefore mislabeled “combining author attribution differs.”
   Differing combining authors alone, including **Cladonia pocillum 69234/VPKC**, do not establish different taxa; the inference remains unverified.

5. **RISK — verified’s author exception is broader than documented.**
   `relate_v2.py:183–185` permits any combining-author `candidate` with matching basionym authors, including initials and omitted pre-`ex` authors.
   **Caloplaca cerina 81483/PZSY**, witness **97742/4JDVH**, admits `Hepp` / `Nägeli ex Hepp`, not merely an abbreviation.
   Its link is real, but the README should describe this weaker author assurance. Different archive provenance also does not establish independent research.

6. **CLEANUP — provenance counts hold; some descriptions need precision.**
   Verified vernacular source labels: **2041 Dyntaxa, 2011 WoRMS, 53131 IUCN**. The 15 independently attributed pairs reproduce.
   The **642 copied vernaculars are evidence occurrences across 481 pairs**; 27 blank-source occurrences remain unverified.
   **Dasyscyphella nivea 67749/349ZZ** differs in family in the inspected rows; both orders are **Helotiales**.

**Sample / false-positive estimate:** seed **20261021**; 36 stratified verified pairs, including 18 placement-differing, plus two targeted additions; 30 pragmatic pairs, six per subgroup; 12 unresolved pairs.
All requested examples were checked. Links genuinely reach the selected COL concept/basionym or its COL-linked Dyntaxa concept.
Unsupported high-confidence classifications: **1/36 (2.8%)** in the primary sample, **2/38 (5.3%)** after targeted additions, as in finding 1. These descriptive proportions are not population estimates. **Source-proven distinct-taxon false matches: 0 observed.**

**Pass-1 findings:** copied-source promotions, author-only homotypy, invalid COL witnesses, “explicit cross-reference” terminology, blank vernacular provenance, and placement-as-identity-conflict are fixed. Author comparison is only partly fixed. Field-inventory/compiler review was outside scope. Outcomes are mechanically disjoint; semantic soundness still needs the corrections above.

**Ran:** `python -B -m pytest -s -q -p no:cacheprovider --noconftest …/test_authors_v2.py` — **25 passed**; adversarial probes; complete `main()` rerun with writes redirected to memory — all **2,805 JSONL records and summary matched**; CSV/JSON and 566-transition censuses passed (**158/329/65/14**).
Immutable SQLite checks validated documented endpoints and guards; source archives exactly matched **285 COL, 224 NorTaxa and 16 Dyntaxa selected rows**. `2030`-only evidence never verified; identifier cross-references remained **0**. No writes, commits or network. Taxonomic identity beyond these source records remains inferred.
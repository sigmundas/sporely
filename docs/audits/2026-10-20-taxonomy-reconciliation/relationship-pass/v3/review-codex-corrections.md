# Focused review of the pass-3 correction pass by Codex (gpt-6.1-sol, high effort), 2026-10-21

This read-only review ran through `codex exec` in a read-only sandbox. The text below is its output, unedited.

**Verdict: accept with notes.** All four earlier findings are fixed; no regression found within this focused scope.

1. **Role detection fixed.** Both cited examples return `role_mismatch`, including through `basionym_cmp`. `Pike ex Hale` / `Pike in Hale` also returns `role_mismatch`; ordinary host omission remains compatible.

2. **Unicode and parser safeguards fixed.** `Łind.` / `Ind.` and `Þor` / `Or` return `different`; Unicode initials retain their documented folding. Misplaced/repeated `et al.` returns `unparsed`. Halıcı remains `candidate:et_al`; Łubek remains `different`. Multiword person comparisons follow the stated exact-key/shared-word rules. **Note:** “never candidate/compatible” applies to the person comparison; whole citations can still qualify through list or role omissions, e.g. `San Martín et al.` / `San Martín`.

3. **ChecklistBank labels fixed.** Database checks reproduce **240 accepted + 11 provisional** exact-copy pairs and **309 + 20** population copies. All 11 provisional pairs remain unresolved. Six cache entries support the stated 200/404/500 responses, search total **12,102**, and **TZ76P → 336906909 → NorTaxa 295832**. Classification generators do not consume these new responses.

4. **Forecast withdrawn.** No active projection or confidence-interval claim remains. Old numbers occur only in explicit withdrawal/historical review. Retained counts reproduce **6/31**, including V2MTQ/PPY6B re-sourcing; forward **4/31 vs 2/210**; and **3/3** retained source-record keys. The anchoring recommendation remains.

5. **Correction impact confirmed against the saved baseline.** Exactly **27 subclass changes**, all remaining in or moving into review. Verified membership remains **147**, with unchanged `members_sha256` **cbabfb78…**. None of **209 eligible witnesses** depends on a newly-unparsed comparison. Imleria badia’s changed evidence was already excluded. All 12 S1 file pins match, including **62790e59… / 6706b6a6…**; actual A0 overlap is zero.

**Ran:** 111 comparator + 13 v3 validator + 18 S1 tests: **142 passed**, using virtual temporary files and the unmodified validator. Two in-memory regeneration runs reproduced all **24 generated files byte-identically**. README/docstring guarantees are appropriately bounded.

**Inferred:** historical request execution and full-search paging behavior from cached evidence. No writes or network requests performed.
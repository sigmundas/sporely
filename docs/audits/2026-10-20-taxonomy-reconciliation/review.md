# Review of the audit (2026-10-20)

This review was run by a separate reviewer agent. Nothing in the audit has
been corrected yet.

**Verdict: sound with corrections.** The counts, the defect claims and the
supersession mechanics hold. Batch A is **not ready to approve** as generated.

## Findings

1. **CORRECTNESS: a misapplied name is in A.**
   - Russula neglecta: NorTaxa 626854 `Singer ss. Blum (non Singer)` vs COL
     122656 `Singer` was classed `abbreviation`.
   - `scripts/authorship.py` does not recognise `ss.` or `non`, and `_person`
     takes the last token, so both sides reduce to "Singer".
   - Fix: make `ss.`, `sensu`, `non` and `auct.` non-batchable, and add a
     negative self-test case.
2. **CORRECTNESS: the comparator equates different people.**
   - `_author_compatible` accepts any surname prefix of at least two letters.
   - It therefore equates Fr. with Fresen., Bon with Bonord., Lind with
     Lindau, Rick with Ricken, Lange with Langer, and Kühner with Kühn.
   - In A this affects Mycosphaerella caulicola (Lind/Lindau) and Teloschistes
     exilis (Mich./Michx.).
   - Fix: add negative tests or a standard author-abbreviation list before
     decision D2.
3. **RISK: different validating authors after "ex".**
   - These pairs agree only on the author before "ex":
     - Cortinarius vitellinopes (Secr. ex Gillet / Secr. ex J. Schröt.);
     - Schismatomma glaucescens (Nyl. ex Willey / Nyl. ex Fink);
     - Phellinidium rufitinctum (ex Cooke / ex A.L. Sm.).
   - Fix: send these to B.
4. **RISK: `ined.` names on the NorTaxa side only.**
   - Affected: Cortinarius bovinaster, Menegazzia chrysogaster, two Solenopsora
     pairs and Placynthium pulvinatum.
   - Biatorella flavella `(?)` also passes the rules.
   - These need an explicit owner decision.
5. **CORRECTNESS: rollout claim §8.4 is wrong.**
   - When the desktop pulls a Sporely id that is missing from its release, it
     keeps it as `external_unresolved`. It does not drop it
     (`docs/supabase-sync-contract.md:1105`,
     `utils/cloud_sync_impl/identity_state.py:38–43`).
   - The desktop pulls only `selected_sporely_taxon_id`, never
     `resolved_sporely_taxon_id`.
   - Repairing the cloud first therefore protects cloud resolution links and
     the web app, not desktops.
6. **CORRECTNESS: §1.3 framing.**
   - `mapping_policy.yml:57` and the docstring of `cross_source_mapping.py`
     say name-based binding must not become identity.
   - The compiler binds by name anyway. Policy and compiler disagree, and the
     amendment should settle that explicitly.
7. **CORRECTNESS: §9 overstates Artsobservasjoner coverage.**
   - The desktop looks up by genus and epithet, and returns None when the
     match is ambiguous (`database/models.py:1003–1013`).
   - `Hygrocybe conica` returns None because var. conica (625373) carries
     NorTaxa 53968.
   - 77 of 9,367 species are ambiguous in this way, and batch A does not
     change that.
   - Observed ids that are missing from the release (634896–634916) drop out
     of the denominator without notice.
8. **RISK: the plan is missing these checks.**
   - 844 `taxon_redlist_min` rows sit on NorTaxa concepts that would retire.
   - `observation_reference_uses` and `shared_reference_contributions` rows
     that refer to retiring ids, which the 09.30 repair refuses.
   - Search and vernacular caches in sporely-web and Android.
9. **CLEANUP.**
   - NorTaxa parents: 7,012 + 5,551 + 2 unbound + 1 absent (the root), not
     "3 unbound".
   - `stale_sporely_identity` applies only to proven Sporely identities
     (`database/audit_observation_identity.py:777–787`).
   - The existing validator (`bridge_emission.py:373–386`) already accepts the
     columns of the cross-source A manifests. A new format is needed only for
     the within-source cases.
   - H4 already passes on the current release.

## Verified by running

- All the counts in §3.
- The 5,956 + 6,610 split, with 81 ambiguous.
- 7,423 − 1,351 = 6,072, and every live pair is in a Stage 2 manifest.
- 16,424 = 14,291 + 2,133 bindings.
- The Hygrocybe conica rows and COL's `inat:51872` / `Taxon:4379`.
- Stereum durum, Inocybe lucifuga and Amanita are not in A.
- A has no duplicate survivors or retirees, and no chains.
- The parent namespace mismatch (`build_sqlite_candidate.py:354–366`).
- The Sporely-id fallback for Artsobservasjoner (`models.py:1047–1064`):
  Cryptosphaerella annonae returns 1. It is called from
  `ui/observations_tab.py:11086`.

## False positives in A

- Random sample of 25 `abbreviation` species: 1 clearly wrong, about 4%.
- Targeted scan of all 2,775 cross-source A members: 1 definite and about 11
  questionable, about 0.4%.

## Not checked

- Cloud counts and desktop observation counts.

## Still needs a mycologist

- The questionable pairs above.
- A stratified sample of the 906 `abbreviation` members.

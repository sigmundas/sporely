# Decisions for the owner (revision 5, S1 scope)

No batch is approved. Reviews 1–4 are accepted as feedback. The S1 decisions
and their evidence are in [s1-approval-sheet.md](s1-approval-sheet.md).

- **S1-1. Scope.** Approve exactly the 988 pairs pinned by the
  `A0-species` manifest (`62790e59…`) and the `A0-infraspecific` manifest
  (`6706b6a6…`). The count is provisional until independently verified.
- **S1-2. Policy.** Adopt the batch-manifest clauses P1, P4, P5 and P7, and
  P10 (inherited usages are names, not bridges), for new ledger records. The
  1,353 shipped supersessions and their 4,779 synonym bridges stay unchanged;
  a separate decision covers them.
- **S1-3. Implementation.** Authorise the stage that implements:
  - the P10 compiler change;
  - the 988 ledger records and the two approved-manifest entries;
  - tests H14 and H16.

  This authorises no release.
- **S1-4. Cloud check.** Authorise an operator to run the S1 section of
  `scripts/cloud_reference_check.sql` read-only.
- **S1-5. Release.** After S1-1 to S1-4 and every gate pass, authorise the
  build, cloud import and activation, and the desktop install, following the
  S1 runbook in `staged-release-plan.md`.
- **S1-6. Unresolved tuples.** Accept that S1 rewrites no
  `external_unresolved` cloud row, including the known one on NorTaxa id
  55146. Resolving such rows later is a separate decision.

## Deferred (not part of S1)

Revision-4 decisions, kept for later stages. D2 is replaced by S1-1 to S1-6.

- **D1. Policy.** Adopt the policy amendment P1–P9, together with the compiler
  changes: no name binding outside manifests, P9 limits on synonym
  inheritance, and pin verification.
- **D2. S1.** Approve S1: A0 species 943 and infraspecific 45. Its dependency
  closure is verified, with 0 operations and 0 desktop rows to migrate.
- **D3. B-placement.** 508 pairs differ only in order or family placement.
  2,757 failing legacy bindings depend on the same question.
- **D4. S2.** A0 genus (301) and higher (66), after D6.
- **D5. A2-col-internal.** 4 pairs. They need a new manifest kind and the
  web/Android remap.
- **D6. NorTaxa parent links.** Option (a), (b) or (c). S4 holds 1,768
  operations that depend on it.
- **D7. Placeholders.** Exclude the 70 `Incertae sedis` placeholders from
  search.
- **D8. Maintenance window.** Approve the runbook in
  `staged-release-plan.md` R3, including the order sync-pause → desktop
  migration → cloud repair → consistency check → resume.
- **D9. Cloud.** Authorise an operator to run `cloud_reference_check.sql`
  read-only first. Repairs follow only per authorisation.
- **D10. Publishing.**
  - Switch to a concept-verified Artsobservasjoner lookup.
  - Validate COL `inat:` ids and the Dyntaxa → Artportalen mapping.
- **D11. Ratify legacy bindings.** 10,309 A0-equivalent legacy bindings could
  be ratified, with bridges emitted. The 1,993 that rely on missing
  authorship go through the expert gate.
- **D12. Freeze failing legacy bindings.** 4,122 legacy bindings would be
  frozen as legacy_unratified, with no split mechanism. Accept that, or
  commission a split design.
- **D13. Provisional names.** Should provisional names, or names with a
  blocked nomenclatural status, ever be batchable? Current answer: no, on
  either side.

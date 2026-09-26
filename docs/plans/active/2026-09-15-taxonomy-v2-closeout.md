# Sporely Taxonomy v2 Closeout — Execution Plan

**Plan date:** 2026-09-15  
**Last amended:** 2026-09-26  
**Status:** Implementation complete — awaiting confirmation of the cloud activation (see status note below)  
**Primary repository:** `sigmundas/sporely`  
**Primary local path:** `/Users/sigmundas/Documents/Code/sporely/sporely-py-taxonomy-v2-identity-reconciliation`  
**Expected branch:** `feature/taxonomy-v2-identity-reconciliation`  
**Companion repository (required):** `sigmundas/sporely-web` — required for Stage 2 Part B.  
**Supersedes for execution:** `docs/plans/completed/2026-07-23-taxonomy-v2-integration.md`  
**Historical architecture source:** the superseded plan and `database/taxonomy/docs/**` remain authoritative for accepted architecture decisions unless this plan explicitly changes them.

### Status note 2026-09-26

- Desktop `0.9.23` (`8cc688f`, tag `v0.9.23`) ships `tax-2026.09.23-01`
  with taxonomy v2 ON by default and runtime resolution of provider
  identities. `sporely-web` `89754d6` carries the importer `--release-id`
  and the release-transition runbook section.
- The cloud activation of `tax-2026.09.23-01` is a human operator step
  (sporely-web `supabase/taxonomy-v2-production-import-runbook.md`). A
  read-only check on 2026-09-25 found production still on
  `tax-2026.08.01-01`. Record the activation and its post-checks here, then
  move this plan to `completed/`.
- The superseded July plan has been moved to `completed/`.
- Out of scope, carried as follow-ups: `tax-2026.09.23-01` has no
  Swedish/iNaturalist-language names and no Artportalen/iNaturalist IDs
  (compiled without `--legacy-enrichment-input`; see
  `database/taxonomy/README.md`), and legacy retirement (July plan,
  section 18).

> **Use the existing worktree and branch.** All stages of this plan execute in
> the worktree and branch named above. Do not create a separate
> `feature/taxonomy-v2-closeout` branch or worktree.

### Amendment 2026-09-19 — Artsorakel identity and save-boundary integrity

Triggered by a reported incident: `Entoloma conferendum`, identified via
NorTaxa/Artsorakel, saved as unidentified in `sporely-web`.

Investigation found **two independent defects**, not one:

1. The NorTaxa → Sporely identity bridge is dropped from the active release,
   which is the class of defect Stage 3 already targets. The new evidence makes
   Stage 3 both more important and more general than the original
   `Pholiotina rugosa` framing suggested.
2. A separate provider-response/save defect in `sporely-web` that can write null
   `genus`/`species`/`common_name` over a usable identification. The original
   plan did not require fixing this, and failing taxonomy resolution does not by
   itself produce it.

Changes made by this amendment:

- added the quantified bridge-loss evidence to the deployed baseline;
- added NorTaxa `53482` / `Entoloma conferendum` as a second mandatory regression alongside `52369`;
- renamed Stage 2 from "Close desktop taxonomy identity boundary" to "Close client taxonomy identity boundaries" and added Part B for the `sporely-web` provider/save boundary;
- added a NorTaxa coverage **audit** requirement to Stage 1/3 so neither named regression can be satisfied by special-casing, without inferring authoritative concept identity from the mere existence of a vernacular join;
- added Stage 4 Part D and a name-loss audit class;
- recorded the expected `src/taxonomy-v2.test.js` expectation flip as a required deliverable.

The amendment deliberately folds defect 2 into this plan rather than spawning a
separate branch of taxonomy-adjacent cleanup: it shares the Artsorakel path,
the observation save path, and the regression case with Stage 3.

One correction to the review that prompted this amendment: the affected client
is `sporely-web`, not an Android-specific client. The defect is in shared web
code and is not platform-specific.

## Agent handoff

- Work on **one stage at a time**.
- Before coding, inspect the current repository state, current branch, relevant tests, migrations/RPCs, and the deployed-state evidence recorded below.
- Do not trust stale stage labels in the superseded July plan.
- Do not infer taxonomic identity from scientific-name equality.
- Do not reinterpret an external integer ID as a Sporely-owned ID.
- Preserve source system, namespace, external ID, release/provenance, and selected-name snapshots wherever identity crosses a client/cloud boundary.
- Do not activate a new taxonomy release or modify production observations unless the current stage explicitly authorizes it.
- Prefer additive changes until the closeout validation is complete.
- Every stage must end with:
  1. tests/evidence,
  2. an explicit stage verdict,
  3. a commit,
  4. a pushed branch,
  5. an updated handoff in this file.
- If evidence contradicts this plan, stop implementation and update the plan/evidence first. Do not silently reinterpret the contract.
- Agent-sparring should challenge the implementation, not merely confirm it.

## Why this plan exists

The original Taxonomy v2 integration plan is no longer a reliable execution ledger. Later W3 work was implemented and deployed after its main status table was written.

The remaining problem is not “implement Taxonomy v2 from scratch.” It is to close specific identity/source-boundary gaps and verify that the deployed system is internally consistent.

Three concrete failures define this closeout:

1. **Client identity provenance leak**
   - A taxonomy picker can currently surface an integer `taxon_id` and commit it as `sporely_taxon_id`.
   - That is safe only when the local taxonomy database is proven to use Sporely-owned v2 IDs.
   - Legacy/source-specific IDs must retain `(source_system, namespace, external_id)` and be explicitly resolved before assigning a Sporely ID.
   - This is not desktop-specific. `sporely-web` exhibits the mirror-image defect: it holds a namespaced external identifier (`NBIC:53482`), never attempts resolution, and silently discards the identity instead of leaking it.

2. **National-source bridge loss**
   - The global macrofungi builder starts from COL concepts and filters exported enrichment/mappings by retained COL taxon IDs.
   - A distinct NorTaxa concept can therefore disappear from the active release unless its reviewed relationship to the retained Sporely concept is explicitly represented.
   - Regression cases: NorTaxa `52369` (`Pholiotina rugosa` vs the COL-backed `Conocybe rugosa`) and NorTaxa `53482` (`Entoloma conferendum`).
   - The `53482` case shows the loss is not confined to divergent accepted-name treatments. There both sources agree on the name, the bridge was demonstrably computed during the build, and it was still dropped from the emitted identity mappings.

3. **Provider-response integrity loss at the save boundary**
   - A malformed, deprecated, or unexpectedly-shaped Artsorakel candidate can reach the observation save path with a name that no longer parses as a binomial.
   - The client then writes `genus = null`, `species = null`, `common_name = null`, and the observation renders as unidentified even though the provider returned a usable scientific name.
   - This is independent of taxonomy resolution: failing to bind `selected_sporely_taxon_id` does not, by itself, clear those columns.

These are contract problems. Do not fix them with numeric replacement, name matching, or a one-species special case.

## Accepted architecture that remains binding

The following decisions from the earlier taxonomy work remain in force:

- `sporely_taxon_id` is an immutable positive **Sporely-owned** identifier.
- External identifiers are authoritative only as the tuple:
  `(source_system, namespace, external_id)`.
- Namespace-lost integers are legacy/audit evidence only.
- COL XR is the global canonical backbone for the reviewed global macrofungi scope.
- NorTaxa is a national authority/enrichment/bridge source and may preserve a different accepted-name treatment.
- Scientific-name equality is not sufficient identity evidence.
- Distinct concepts must not be merged because strings resemble each other.
- Synonym/accepted-name relationships must be explicit and provenance-backed.
- Existing observations are historical records and must not be rewritten merely because current taxonomy changes.
- Observation identification snapshots and current selected/resolved identity are separate concerns.
- Missing identity and explicit clearing/null are not interchangeable.
- Cloud changes remain additive until closeout validation succeeds.

## Verified deployed baseline

As of 2026-09-15, the production Supabase project has:

- active taxonomy release: `tax-2026.08.01-01`
- scope: `global_macrofungi_policy_v1`
- active-release taxonomy rows:
  - `taxon.jsonl`: 52,917
  - `scientific_name.jsonl`: 57,769
  - `vernacular.jsonl`: 3,923
  - `taxon_external_id.jsonl`: 52,881
  - `taxon_redlist.jsonl`: 2,262
- authoritative external namespace counts:
  - `col_xr / col_usage_id`: 52,881
  - no active NorTaxa authoritative external mappings in this release

### Quantified bridge loss in the active release

Measured directly against the generated release artifacts for `tax-2026.08.01-01`
(`database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01/`):

- `taxon.jsonl`: 52,917 rows, **all** `source_system = col_xr`, and **zero** rows with `norwegian_taxon_id` set.
- `taxon_external_id.jsonl`: 52,881 rows, **all** `namespace = col_usage_id`. Zero `nortaxa_taxon_id`.
- `vernacular.jsonl`: 3,923 rows, **all** `source = nortaxa`, spanning 2,041 distinct taxa.

The third line is the decisive evidence. The builder resolved NorTaxa concepts to
retained COL-backed Sporely concepts accurately enough to attach Norwegian
vernacular names, then discarded that same mapping instead of emitting it as
`norwegian_taxon_id` or a `nortaxa / nortaxa_taxon_id` external identifier.

The bridge is therefore not missing from the build — it is **computed and then
half-materialized**. Any fix that only widens the NorTaxa preload would be
treating a symptom. Stage 3 must retain the mapping the builder already derives.

Consequence: `resolve_taxon_external_id_v2('nortaxa', 'nortaxa_taxon_id', <any>)`
cannot match anything in the active release. No Artsorakel/NBIC result can bind
to a Sporely concept today, regardless of species.

For contrast, the earlier `cloud_export_tax-2026.07.30-02` release failed the
opposite way: NorTaxa rows were appended as an un-deduplicated id block
(625xxx–626xxx) with `parent_taxon_id = null`, producing duplicate concepts
(for example `Cantharellus cibarius` as both col_xr `168873` and nortaxa
`626243`). Neither release represents the reviewed bridge correctly. Stage 3
must not reintroduce the duplicate-block behavior while fixing the drop.

Production also contains deployed W3 reconciliation state:

- `taxonomy_v3.identification_snapshot`: 369 rows
- `taxonomy_v3.resolution_link`: 369 rows
- resolution states:
  - `resolved_exact` via trusted secondary provider mapping: 233
  - `resolved_exact` via operator manual review: 78
  - `unresolved_external_identifier`: 21
  - `manual_unresolved`: 7
  - `no_identity_evidence`: 30

This matches the later W3-B reconciliation evidence and proves that the July plan's W3 status table is stale.

Relevant deployed interfaces include:

- `search_taxa_v2`
- `resolve_taxon_external_id_v2`
- `set_observation_selected_taxon_v2`
- `public.observations.selected_sporely_taxon_id`
- `public.observations.resolved_sporely_taxon_id`
- `taxonomy_v3.identification_snapshot`
- `taxonomy_v3.resolution_link`

### Regression case: NorTaxa 52369

Verified current production behavior:

- `search_taxa_v2('Conocybe rugosa')` resolves to Sporely taxon `83668`
- that concept is backed by COL usage ID `5ZT3G`
- `search_taxa_v2('Pholiotina rugosa')` currently returns no result
- `resolve_taxon_external_id_v2('nortaxa', 'nortaxa_taxon_id', '52369')` currently returns no result

The closeout must establish a reviewed cross-source relationship without collapsing concepts by name.

### Regression case: NorTaxa 53482 / `Entoloma conferendum`

Second mandatory regression, of equal standing to `52369`. It exercises the
Artsorakel → taxonomy → observation-save boundary end to end.

Source evidence (`database/reference_data/sources/taxon.txt`):

- NorTaxa `taxonID` `53482`, `Entoloma conferendum`, rank species, status valid
- children `53483` (var. *conferendum*), `53484` (var. *incrustatum*), `53485` (var. *pusillum*)
- Norwegian vernacular names include `stjernesporet rødspore` (nb) and `stjernespora raudspore` (nn)

Active-release state (`tax-2026.08.01-01`):

- Sporely taxon `7821`, `Entoloma conferendum`, `canonical_source_system = col_xr`, COL usage ID `39ZCL`
- `norwegian_taxon_id`: null
- no `nortaxa / nortaxa_taxon_id` external identifier
- **but** taxon `7821` does carry the NorTaxa vernacular names above, with `source = nortaxa`

Verified current behavior:

- `resolve_taxon_external_id_v2('nortaxa', 'nortaxa_taxon_id', '53482')` returns no result
- Artsorakel returns `scientific_name_id: "NBIC:53482"` for this taxon
- `sporely-web` drops that identifier without attempting resolution
  (`src/taxonomy-v2.js`, `taxonomySelectionForTaxon`), asserted by
  `src/taxonomy-v2.test.js` as current expected behavior
- the observation subsequently renders as unidentified in `sporely-web`

Note that this case differs from `52369` in an important way: COL and NorTaxa
**agree** that the accepted name is `Entoloma conferendum`. There is no
name-treatment divergence to explain the loss. A Stage 3 solution that only
handles divergent-name bridges will not fix this case.

Unresolved at plan-amendment time: the exact mechanism that produced null
`genus`/`species`/`common_name` for this observation has not been confirmed
against the live row, because the Supabase MCP server was not authorized during
investigation. The leading hypothesis is recorded under Stage 2 Part B and must
be confirmed or refuted by evidence before it is implemented against.

### Regression case: observation 917

Verified current cloud state:

- observation ID: `917`
- image/media reference exists
- `genus`: null
- `species`: null
- `selected_sporely_taxon_id`: null
- `resolved_sporely_taxon_id`: null
- W3 identification snapshot state: `no_identity_evidence`
- historical W3 snapshot has no source namespace/external ID
- AI-identification history exists independently and must not be mistaken for an accepted observation identity

Observation 917 is the end-to-end regression case for this plan.

## Scope boundaries

This plan DOES cover:

- reconciling the stale taxonomy programme ledger against repository + deployed state;
- desktop **and `sporely-web`** taxonomy identity provenance;
- integrity of provider (Artsorakel/iNaturalist) candidates across the observation save boundary;
- namespaced external-ID resolution, including prefixed provider identifiers such as `NBIC:<id>`;
- reviewed COL ↔ NorTaxa bridging needed by active product behavior;
- searchability of reviewed accepted/synonym presentations;
- dry-run auditing of suspect historical/legacy identity writes;
- repair of only proven mappings;
- end-to-end verification using observation 917;
- end-to-end verification of `NBIC:53482` / `Entoloma conferendum`;
- cloud/desktop/web taxonomy-selection consistency.

This plan DOES NOT automatically authorize:

- importing all NorTaxa concepts into the macrofungi preload;
- replacing COL as the global backbone;
- global name-based concept unification;
- rewriting immutable historical identification snapshots;
- deleting legacy taxonomy tables/RPCs;
- activating an unpublished taxonomy release;
- bulk production repair before a reviewed dry-run;
- unrelated macrofungi clade-scope expansion;
- redesigning the identification UI beyond the minimum affordance needed to show an unresolved-identity state;
- changing Artsorakel request/model behavior, image preparation, or provider selection.

## Stage 1 — Reconcile deployed taxonomy state

### Objective

Replace the stale execution ledger with an evidence-backed description of what is actually implemented, deployed, partially deployed, or still outstanding.

No taxonomy behavior change belongs in this stage unless a tiny test/audit helper is required to prove state.

### Required investigation

Inspect:

- this plan;
- `docs/plans/completed/2026-07-23-taxonomy-v2-integration.md`;
- `database/taxonomy/docs/**`;
- accepted W3 evidence and commits, including:
  - `20859a2`
  - `bffd142`
  - `fe1d035`
- current `main`;
- current taxonomy-related tests;
- current cloud-sync taxonomy write paths;
- current Supabase schema/RPC/release state;
- current `sporely-web` taxonomy use.

Classify the old programme stages and significant sub-stages as one of:

- implemented and deployed;
- implemented but not deployed;
- deployed but not reflected in the old plan;
- superseded/obsolete;
- genuinely outstanding;
- unresolved/needs evidence.

### Required identity-path audit

Trace all current uses/writes of:

- `sporely_taxon_id`
- `selected_sporely_taxon_id`
- `resolved_sporely_taxon_id`
- `taxon_id`
- `artsdata_id`
- `ai_selected_taxon_id`
- source system
- namespace
- external ID
- scientific-name snapshots

At minimum inspect:

- desktop picker/autocomplete;
- observation save/load;
- desktop cloud sync;
- web observation write paths, including `sporely-web` `src/screens/find_detail.js` save/patch construction;
- Artsorakel/NorTaxa paths, including provider-response flattening and normalization in `sporely-web` `src/artsorakel.js` and `src/identify.js`;
- taxonomy release search and external resolver RPCs;
- W3 immutable snapshot/resolution-link semantics.

### Observation 917 baseline

Freeze a before-state regression record for observation 917.

The baseline must distinguish:

- immutable W3 historical snapshot;
- current observation selected identity;
- current resolved identity;
- AI-identification history;
- media/images;
- measurements;
- mosaic data.

Do not "correct" the immutable W3 snapshot merely because a current identity can later be selected.

### Deliverables

- update this plan's handoff/status with the reconciled ledger;
- add committed evidence or an audit note under an appropriate taxonomy evidence/docs path;
- identify the exact desktop identity leak path;
- identify the exact release/compiler point where NorTaxa mappings are dropped, covering both 52369 and 53482, and explain why the vernacular join survives the same point;
- characterize the evidence behind the vernacular join: for the 2,041 taxa carrying NorTaxa-sourced vernacular names, state what join key and matching rule produced each NorTaxa → Sporely association, and whether that evidence is strong enough to constitute authoritative concept identity or only name-level enrichment. This is the input to Stage 3's coverage audit; Stage 3 cannot classify associations that Stage 1 has not characterized;
- confirm or refute the Stage 2 Part B hypothesis with raw Artsorakel JSON and the live observation row, and record the result in this plan;
- list any additional identity-boundary defects discovered;
- propose the smallest Stage 2 and Stage 3 implementation surfaces.

Reading the live observation row requires an authorized Supabase MCP session.
If that authorization is unavailable, say so explicitly in the stage report and
mark the Part B mechanism unconfirmed rather than assuming the hypothesis.

### Sparring challenge

The sparrer must actively challenge:

- any claim that W3, W4, or W5 is “done” merely because code exists;
- any assumption that a field called `sporely_taxon_id` actually contains a proven Sporely ID;
- any conflation of immutable historical snapshot with current selected identity;
- any reliance on the stale July plan over deployed evidence;
- any claim that a vernacular join proves concept equivalence, absent a stated join key and matching rule.

### Acceptance gate

Stage 1 is accepted only when:

- the deployed and repository taxonomy state agree with the new ledger;
- observation 917 has a reproducible before-state;
- the desktop leak is traced to a concrete write path;
- the NorTaxa bridge loss is traced to a concrete compile/export/runtime path;
- the evidence behind the NorTaxa vernacular join is characterized well enough for Stage 3 to classify associations against the authoritative bridge standard;
- the `Entoloma conferendum` display failure is traced to a concrete write path, or explicitly recorded as unconfirmed with the blocking reason;
- there are no unexplained taxonomy production objects relevant to this closeout.

Required verdict:

`Stage 1 accepted — proceed to client identity-boundary correction`

## Stage 2 — Close client taxonomy identity boundaries

Applies to the desktop client **and `sporely-web`**. The incident that added
Part B showed the boundary is not desktop-specific: the same rule must hold
everywhere. An external identifier remains `(source_system, namespace,
external_id)` until explicitly resolved, and **failure to resolve must not
destroy the source prediction**.

### Part A — desktop identity boundary

### Objective

Make it impossible for the desktop client to assign `sporely_taxon_id` from an unqualified or legacy integer.

A Sporely-owned ID may be assigned only when:

1. it originates from a taxonomy artifact whose identity contract proves the integer is a Sporely ID; or
2. a namespaced external identifier has been explicitly resolved through an authoritative mapping.

### Required behavior

A taxonomy selection must preserve enough provenance to distinguish:

- a native Sporely taxonomy concept;
- a COL external ID;
- a NorTaxa external ID;
- another supported external source;
- unresolved/manual text.

For external-source selections, preserve:

- `source_system`
- `namespace`
- `external_id`
- selected scientific-name snapshot
- selected rank
- relevant release/response provenance if available

Do not assign a Sporely ID until resolution succeeds.

If resolution is unavailable or ambiguous:

- retain the source-specific identity evidence;
- expose a clear unresolved state;
- do not copy the external integer into `sporely_taxon_id`.

### Implementation constraints

- Keep identity binding explicit.
- Do not rebind from name text after the user edits a committed selection.
- Do not infer from genus/species equality alone.
- Preserve existing v2 picker behavior for genuine Sporely taxonomy artifacts.
- Prefer an explicit typed/provenance-bearing selection object over parallel loosely-related fields.
- Audit load/save paths so provenance survives app restart.
- Audit cloud sync so only proven Sporely identity reaches `set_observation_selected_taxon_v2`.

### Required regressions (Part A)

Add tests for at least:

1. genuine v2 Sporely selection remains stable;
2. a legacy external integer numerically equal to an existing Sporely ID does **not** bind by collision;
3. NorTaxa `52369` remains a NorTaxa external identity until explicitly resolved;
4. unresolved external taxonomy remains unresolved;
5. manual scientific text never creates identity implicitly;
6. edited committed text invalidates identity without deleting unrelated observation content;
7. cloud sync refuses/skips an unproven external integer;
8. save/reload preserves source + namespace + external ID.

### Part B — web (`sporely-web`) identity and provider-response boundary

### Objective

Ensure a provider candidate that cannot be resolved to a Sporely concept still
produces a truthful, usable observation identification, and that the provider's
own identifier survives instead of being silently discarded.

This part covers the Artsorakel → taxonomy → observation-save boundary in
`sporely-web`. It must be completed even if Stage 3 slips: the two defects are
independent and Part B alone is what stops an observation with a valid
scientific name from rendering as unidentified.

### Required behavior

1. **Preserve the source identifier.** An Artsorakel candidate carries
   `scientific_name_id` in prefixed form (for example `NBIC:53482`). The client
   must normalize that to `(source_system = nortaxa, namespace =
   nortaxa_taxon_id, external_id = 53482)` and persist it with the observation,
   whether or not resolution succeeds. Today the prefix is never parsed and the
   identifier is dropped.
2. **Attempt resolution explicitly.** Once Stage 3 lands, a preserved external
   identifier must be offered to `resolve_taxon_external_id_v2` before the
   client concludes there is no Sporely identity. Failure to resolve is a state,
   not an absence.
3. **Never destroy a usable name.** A candidate that fails to parse as a
   binomial, or that is rejected as deprecated, must not overwrite existing
   `genus` / `species` / `common_name` with nulls. If a provider returned a
   usable scientific name, that name is what gets stored.
4. **Degrade to "identified text + unresolved identity."** An observation with a
   valid provider scientific name but no bound Sporely concept must not render
   as unidentified. Unidentified is reserved for the genuine no-name case.
5. **Reject deprecated candidates consistently across response shapes.**
   Artsorakel marks superseded records with the sentinel vernacular string
   `*** Utdatert versjon ***`.

### Leading hypothesis for the `53482` display failure

**This is a hypothesis, not an accepted finding.** Confirm or refute it with
evidence before implementing against it.

`src/artsorakel.js` filters the deprecation sentinel at one nesting level only:

```js
.filter(p => p?.taxon?.vernacularName !== '*** Utdatert versjon ***')
```

Candidates flattened out of `predictions[].taxa.items[]` are pushed as bare
objects, so their vernacular field sits at `p.vernacularName`, not
`p.taxon.vernacularName`. For those candidates the filter cannot match and a
deprecated record survives normalization. If such a record also lacks a parseable
`scientific_name`, `splitScientificName` returns `[null, null]`, and the detail
save path writes null `genus`, null `species`, and null `common_name` — which is
exactly the unidentified condition.

Required evidence to confirm or refute:

- the raw Artsorakel JSON for an identification of this taxon, captured through
  the existing debug dashboard;
- the live `observations` row: `genus`, `species`, `common_name`,
  `ai_selected_scientific_name`, `ai_selected_taxon_id`,
  `selected_sporely_taxon_id`.

The Supabase MCP server must be authorized in an interactive session before the
second item can be read. If the evidence refutes the hypothesis, record the
actual mechanism here and keep the Part B required-behavior list, which stands
on its own regardless of which defect produced this particular incident.

### Required regressions (Part B)

Add tests for at least:

1. a flattened `taxa.items` candidate carrying the deprecation sentinel at
   `p.vernacularName` is rejected, exactly as one carrying it at
   `p.taxon.vernacularName` is;
2. a candidate whose `scientific_name` is absent or unparseable never clears an
   existing identification;
3. `NBIC:53482` normalizes to `(nortaxa, nortaxa_taxon_id, 53482)` and that
   tuple survives save and reload;
4. an unresolved external identity renders as identified-with-unresolved-identity,
   not unidentified;
5. a genuinely name-less observation still renders as unidentified;
6. `Entoloma conferendum` selected from an Artsorakel result saves non-null
   `genus` and `species`.

### Expected test-expectation flip

`src/taxonomy-v2.test.js` currently asserts that an Artsorakel `NBIC:` taxon ID
yields **no** taxonomy selection. That assertion encodes today's broken contract.

- Part B changes the client so the identifier is parsed and preserved rather than dropped.
- Stage 3 changes the release so the identifier resolves.

After both land, that expectation must flip to assert successful resolution to
the reviewed Sporely concept. Treat the flip as a required deliverable, not an
incidental test edit: it is the cleanest available signal that the release
contract genuinely changed rather than the UI having been patched over. If the
test still passes unchanged at the end of Stage 3, the fix is in the wrong place.

### Sparring challenge

The sparrer must try to find:

- hidden integer-copy paths;
- load-time coercion;
- sync-time coercion;
- migration helpers that assume one integer namespace;
- tests that pass only because fixture IDs do not collide;
- UI code that displays source provenance but drops it on save;
- other provider-response nesting levels where the deprecation sentinel or a
  name field can hide, including the iNaturalist path, which has its own
  normalizer with the same flatten-then-read shape;
- any remaining path that can write null `genus`/`species`/`common_name` over a
  previously identified observation;
- a Part B fix that repairs the display while still discarding the provider's
  external identifier.

A solution that merely adds validation in one UI handler is insufficient if another save/sync path can still leak identity.

### Acceptance gate

Stage 2 is accepted only when:

- no unqualified external/legacy integer can become `sporely_taxon_id`;
- existing v2-native identity still works;
- provenance round-trips through desktop persistence;
- cloud sync emits a Sporely ID only after explicit proof/resolution;
- numeric-collision tests pass;
- `NBIC:53482` is normalized, persisted, and round-trips through `sporely-web` save/reload;
- no provider candidate can null out an existing identification;
- an unresolved external identity is displayed as unresolved rather than unidentified;
- the mechanism behind the `Entoloma conferendum` display failure is confirmed by evidence and fixed, or refuted and replaced with the actual mechanism in this plan;
- existing taxonomy and observation tests pass in both repositories.

Required verdict:

`Stage 2 accepted — proceed to reviewed cross-source bridge correction`

## Stage 3 — Restore reviewed cross-source taxonomy bridges

### Objective

Allow nationally authoritative/source-specific taxonomy relationships to survive the global macrofungi release without turning NorTaxa into the global preload or merging concepts by spelling.

There are two mandatory regression cases, of equal standing:

- **NorTaxa `52369`** / `Pholiotina rugosa` against the retained COL-backed concept presented as `Conocybe rugosa` — the **divergent accepted-name** case.
- **NorTaxa `53482`** / `Entoloma conferendum` against retained Sporely concept `7821` — the **agreeing-name** case, where the builder already derives the mapping (it attaches NorTaxa vernacular names to `7821`) and then fails to emit it.

Both must pass. A mechanism that handles only name divergence does not satisfy
this stage, because `53482` has no divergence to key on. Conversely, a mechanism
that simply re-emits every derived vernacular link as an identity mapping does
not satisfy `52369`, which needs an explicit reviewed relationship between
differently-named concepts.

### Design requirement

Do not solve this by broadening `load_taxa()` to preload all NorTaxa concepts.

Do not solve it by reintroducing the `cloud_export_tax-2026.07.30-02`
duplicate-block approach, which emitted parallel NorTaxa concepts with
`parent_taxon_id = null` alongside the COL concepts for the same taxon.

Instead, introduce or complete a **reviewed cross-source bridge mechanism** that can attach source-specific identity, accepted-name treatment, aliases/synonyms, vernacular names, and provenance to a retained Sporely concept.

The relationship must be explicit evidence.

It may be represented through an existing reconciliation/bridge artifact if one already fits the accepted architecture. Do not invent a parallel relationship model until existing mechanisms have been audited.

### Required source semantics

For NorTaxa `52369`, preserve at minimum:

- source: `nortaxa`
- namespace: `nortaxa_taxon_id`
- external ID: `52369`
- NorTaxa accepted scientific name: `Pholiotina rugosa`
- the reviewed relationship to the retained Sporely/COL concept
- Norwegian common names available from the reviewed source data
- relationship/source provenance

For NorTaxa `53482`, preserve at minimum:

- source: `nortaxa`
- namespace: `nortaxa_taxon_id`
- external ID: `53482`
- the reviewed relationship to retained Sporely concept `7821`
- the NorTaxa vernacular names already attached to `7821`
  (`stjernesporet rødspore`, `stjernespora raudspore` and their non-preferred variants)
- relationship/source provenance

Search/display must be able to expose differing source treatments without pretending the sources use the same accepted name.

Where the builder already derives a NorTaxa → Sporely relationship in order to
attach vernacular names, that relationship must be emitted as an identity
mapping rather than consumed and discarded. Explain in the stage report why the
existing derivation is or is not sufficient evidence for a reviewed bridge; if
it is not, say what additional review it requires.

### Required product behavior

After the corrected release/projection:

- resolving `nortaxa / nortaxa_taxon_id / 52369` returns the reviewed Sporely concept;
- resolving `nortaxa / nortaxa_taxon_id / 53482` returns Sporely concept `7821`;
- searching `Pholiotina rugosa` finds that concept through the reviewed NorTaxa treatment;
- searching `Conocybe rugosa` continues to find the COL-backed concept;
- searching `Entoloma conferendum` continues to find `7821`, and its Norwegian vernacular names remain attached;
- the response contains enough metadata to explain which name/source matched;
- Norwegian vernacular data is preserved where source data supports it;
- no unrelated same-name concept is merged;
- no duplicate concept is emitted for a taxon that both sources describe;
- existing ambiguity behavior remains intact.

### Coverage audit requirement

The purpose of this requirement is to prevent a two-species special case. It is
**not** a numeric target, and no mapping may be emitted merely to reach one.

In `tax-2026.08.01-01`, 2,041 distinct Sporely taxa carry NorTaxa-sourced
vernacular names while zero `nortaxa_taxon_id` external identifiers are emitted.
That asymmetry proves the compiler computed some NorTaxa → Sporely association
for each of those 2,041 taxa and then discarded it. It does **not** prove that
each such association is authoritative concept identity: a vernacular join may
have been made on weaker evidence than a reviewed bridge requires.

Stage 1 and Stage 3 must therefore:

1. Enumerate all 2,041 taxa and, for each, determine **what evidence produced
   the NorTaxa → Sporely association** in the compiler path — the join key, the
   matching rule, and whether a human-reviewed decision backs it.
2. Classify each association against the accepted authoritative bridge standard
   defined by this plan's reviewed-bridge mechanism.
3. Emit every relationship that **meets** that standard as
   `nortaxa / nortaxa_taxon_id`.
4. Count, classify, and explain every relationship that does **not** meet it.
   Those must be left unemitted, with the reason recorded — not promoted.

Report both counts. A candidate release that emits only the two named
regressions, with no audit explaining why the remaining associations were
rejected, has special-cased them and must be rejected. A candidate that emits
all 2,041 without the audit is equally unacceptable: it infers authoritative
concept identity from the existence of a vernacular join, which the compiler
path has not yet been shown to justify.

Until that audit exists, do not treat vernacular-join presence as evidence of
concept equivalence anywhere in this plan.

### Generality requirement

The implementation must be a reusable reviewed-bridge mechanism.

The test fixture must include:

- the 52369 regression (divergent accepted names);
- the 53482 regression (agreeing accepted names);
- at least one control where two similar/equal-looking names must remain distinct;
- at least one existing bridge/synonym case to prove no regression.

Do not special-case `52369` or `53482` in runtime code.

### Release safety

- Build a new candidate release/projection with a new release identity.
- Validate determinism and hashes according to the accepted taxonomy release contract.
- Do not mutate the active release in place.
- Do not activate the candidate in production during implementation/sparring.
- Measure row-count and namespace changes.
- Explain whether the bridge adds sparse mappings only or changes desktop search-pack content.

### Sparring challenge

The sparrer must reject:

- name-only matching;
- fuzzy matching as identity evidence;
- importing all NorTaxa concepts merely to fix one mapping;
- source-name loss;
- overwriting the COL presentation with NorTaxa's preferred name globally;
- a runtime hard-coded exception;
- a bridge that works in desktop SQLite but disappears from cloud export;
- a cloud-only bridge that cannot survive desktop sync;
- a bridge that only covers divergent accepted names and silently skips the agreeing-name case;
- a fix that satisfies both named regressions without the coverage audit explaining what happened to the remaining NorTaxa associations;
- emitting mappings to reach a coverage number, without evidence that each meets the authoritative bridge standard;
- a fix that restores mappings by reintroducing duplicate NorTaxa concepts.

### Acceptance gate

Stage 3 is accepted only when a non-active candidate proves:

- authoritative NorTaxa `52369` resolution;
- authoritative NorTaxa `53482` resolution to Sporely `7821`;
- the coverage audit above is complete: every one of the 2,041 vernacular-joined associations is either emitted as an authoritative bridge or counted and explained as rejected;
- both scientific-name treatments are searchable;
- provenance is preserved;
- vernacular enrichment survives;
- ambiguity controls pass;
- release validation/determinism pass;
- no active production release was modified.

Required verdict:

`Stage 3 accepted — proceed to audit, migration, and observation 917 verification`

## Stage 4 — Audit, migrate, and verify observations 917 and the `53482` case

### Objective

Find observations affected by the client identity leak **and by the provider-response
save defect**, repair only identities supported by authoritative evidence, and
prove the corrected system end-to-end using observation 917 and the
`Entoloma conferendum` / `NBIC:53482` case.

### Part A — dry-run audit

Build/read an audit of observations that may contain an external or legacy integer in a field now interpreted as Sporely-owned identity.

The audit must classify rows using evidence, not number shape.

At minimum classify:

- proven Sporely identity;
- proven external identity with unique authoritative bridge;
- unresolved external identity;
- ambiguous external identity;
- manual/no identity evidence;
- suspicious numeric collision;
- **name loss from a provider candidate**: null `genus`/`species`/`common_name`
  alongside a non-null `ai_selected_scientific_name`, which indicates the Part B
  defect destroyed a usable identification;
- no change required.

The name-loss class is a distinct population from the identity-leak class and
must be counted separately. It is the one class where the repair is restoring a
name the system already recorded elsewhere on the same row, not resolving an
identity. That makes it lower-risk than identity repair, but it is still a
production write and still subject to the Part B gate below.

The dry run must output:

- observation ID;
- current stored identity fields;
- preserved source/namespace/external ID where known;
- candidate resolved Sporely ID where proven;
- evidence/mapping used;
- proposed action;
- reason for refusal when not repairable.

### Repair rule

Repair only when the mapping is proven by an authoritative namespaced relationship or an already-reviewed frozen reconciliation decision.

Do not repair because:

- integer values happen to match;
- current scientific names are equal;
- a fuzzy name search returns one result;
- the expected species “looks obvious.”

Unresolved and ambiguous rows stay unresolved and are reported.

### Part B — production migration gate

Production mutation is a separate reviewed operation.

Before any write:

- dry-run artifact must be reviewed;
- counts must reconcile;
- candidate release must be validated;
- rollback procedure must exist;
- observation/media/measurement integrity checks must be defined.

If those conditions are not met, stop with `NEEDS_YOU`.

### Part C — observation 917 regression

Use observation 917 to verify the complete corrected path.

Verify before and after:

- observation identity fields;
- immutable W3 snapshot remains historically truthful;
- current selected identity can be set through the corrected namespaced bridge;
- desktop save/reload;
- desktop ↔ cloud sync;
- microscope image/media references;
- image metadata;
- measurements;
- calibration references if present;
- mosaic/spore atlas state if present;
- public/private visibility behavior;
- no accidental deletion/replacement of unrelated observation content.

The test should prove that correcting taxonomy identity does not damage microscopy/media data.

### Expected 917 semantics

A later correct selection does **not** retroactively rewrite the original W3 snapshot from `no_identity_evidence`.

Instead:

- historical snapshot remains immutable;
- current selected identity records the newly proven choice;
- current resolved identity follows the accepted product contract;
- selected source/provenance is retained.

If current schema cannot represent this cleanly, stop and surface the contract gap rather than overwriting history.

### Part D — `Entoloma conferendum` / `NBIC:53482` regression

Verify the full corrected path for the incident that motivated this amendment:

1. an Artsorakel identification returning `NBIC:53482` is normalized to
   `(nortaxa, nortaxa_taxon_id, 53482)`;
2. that tuple resolves to Sporely concept `7821` through the Stage 3 bridge;
3. the saved observation has non-null `genus` = `Entoloma` and `species` = `conferendum`;
4. the Norwegian vernacular name is available for display;
5. the observation does not render as unidentified in `sporely-web`;
6. the same observation round-trips through reload and, where applicable, desktop sync;
7. a deliberately deprecated or malformed candidate for the same taxon is
   rejected without clearing an existing identification.

Step 7 must be exercised even if Stage 1 refuted the deprecation-sentinel
hypothesis. The required behavior does not depend on which defect caused this
particular incident.

### Cross-client verification

Where feasible, verify:

1. desktop opens 917;
2. corrected taxonomy search/selection works;
3. selection saves locally;
4. sync sends only proven Sporely identity plus preserved provenance;
5. cloud reflects the intended selected identity;
6. cloud reload/pull preserves 917;
7. web/cloud display uses the correct current identity;
8. microscopy images, measurements and mosaic remain unchanged.

### Sparring challenge

The sparrer must actively look for:

- data loss hidden by taxonomy-focused tests;
- immutable snapshot mutation;
- selected/resolved field confusion;
- one-way sync success that fails on pull/reload;
- media/mosaic associations keyed through fields accidentally touched by migration;
- repair scripts that are not idempotent;
- a second run changing already-correct rows.

### Acceptance gate

Stage 4 is accepted only when:

- audit classification is deterministic;
- only proven rows are repairable;
- unresolved/ambiguous rows are preserved and reported;
- repair is idempotent;
- rollback is documented/tested as appropriate;
- observation 917 passes end-to-end verification;
- the `NBIC:53482` / `Entoloma conferendum` regression passes end-to-end verification;
- the name-loss population is counted, classified, and either repaired from evidence already on the row or explicitly reported as unrepairable;
- taxonomy correction does not alter its unrelated microscopy/media content;
- old and new identity semantics are documented;
- all relevant desktop and cloud tests pass.

Required verdict:

`Stage 4 accepted — taxonomy v2 closeout complete`

## Closeout criteria

Taxonomy v2 closeout is complete only when all of the following are true:

- the active execution ledger matches deployed reality;
- no client — desktop or web — can reinterpret an external integer as a Sporely ID;
- no client discards a provider's external identifier when resolution fails;
- no provider candidate can null out an existing identification;
- an unresolved external identity displays as unresolved, never as unidentified, when a valid scientific name exists;
- namespace/source provenance survives selection, persistence, and sync;
- reviewed NorTaxa bridges survive release construction;
- `nortaxa:nortaxa_taxon_id:52369` resolves through a general bridge mechanism;
- `nortaxa:nortaxa_taxon_id:53482` resolves to Sporely `7821` through the same mechanism;
- the NorTaxa coverage audit is complete, with emitted and rejected associations both counted and explained;
- both `Pholiotina rugosa` and `Conocybe rugosa` are discoverable with source-aware semantics;
- `src/taxonomy-v2.test.js`'s NBIC expectation has flipped from "does not resolve" to "resolves to the reviewed concept";
- historical observations are audited without name-based mass rewriting;
- observation 917 passes desktop/cloud/media/measurement/mosaic regression;
- the `Entoloma conferendum` / `NBIC:53482` case passes end-to-end regression;
- immutable historical snapshots remain truthful;
- any new release is activated only after explicit final review;
- the superseded July execution plan is moved out of active plans or clearly marked superseded.

## Recommended repository workflow

Use the existing taxonomy worktree and branch. Do not create a new worktree or
a `feature/taxonomy-v2-closeout` branch:

```bash
cd /Users/sigmundas/Documents/Code/sporely/sporely-py-taxonomy-v2-identity-reconciliation
git status --short --branch   # expect: feature/taxonomy-v2-identity-reconciliation
```

This plan lives at:

```text
docs/plans/active/2026-09-15-taxonomy-v2-closeout.md
```

Recommended first run:

```bash
sparring run-plan \
  docs/plans/active/2026-09-15-taxonomy-v2-closeout.md \
  --repo-root . \
  --expected-branch feature/taxonomy-v2-identity-reconciliation
```

Do not start Stage 2 until Stage 1 has produced the reconciled ledger and an accepted sparring verdict.

## Superseded-plan handling

After this file is committed:

1. remove `2026-07-23-taxonomy-v2-integration.md` from the active execution set;
2. preserve it under the repository's existing completed/archive convention if one exists;
3. if no archive convention exists, leave the file in place temporarily but add a prominent top-level marker:

```text
SUPERSEDED FOR EXECUTION by:
docs/plans/active/2026-09-15-taxonomy-v2-closeout.md

This document remains historical architecture/evidence and is not the active agent-runner plan.
```

Do not delete the old plan until its historical evidence links and architecture decisions have a stable retained location.

# Taxonomy v2 closeout — Stage 1 reconciled ledger and identity-path audit

**Stage:** 1 — Reconcile deployed taxonomy state
**Plan:** `docs/plans/active/2026-09-15-taxonomy-v2-closeout.md`
**Branch:** `feature/taxonomy-v2-identity-reconciliation`
**Date:** 2026-09-22

This document is the committed Stage 1 evidence artifact. It replaces the stale
status table in `docs/plans/active/2026-07-23-taxonomy-v2-integration.md` as the
execution ledger for the taxonomy v2 programme.

---

## 0. Verification environment and what could NOT be verified

Stage 1 is an evidence stage, so the limits of the evidence are part of the
deliverable. Three required inputs were unavailable in this session.

### 0.1 Supabase MCP is not authorized — deployed state is unverified

The Supabase MCP server is present but unauthenticated, and this session is
non-interactive, so no OAuth flow can run here. Consequently **every deployed
fact in this ledger is quoted from the closeout plan's recorded baseline, not
independently re-measured.** Specifically, the following were NOT verified:

- the active release identity `tax-2026.08.01-01` and its row counts;
- the production `taxonomy_v3.identification_snapshot` / `resolution_link` counts;
- the behavior of `search_taxa_v2`, `resolve_taxon_external_id_v2`,
  `set_observation_selected_taxon_v2`;
- the live `observations` row for observation 917;
- the live `observations` row behind the `Entoloma conferendum` incident.

**Consequence for the acceptance gate:** the gate requires that "observation 917
has a reproducible before-state" and that deployed and repository state "agree
with the new ledger". Neither can be discharged from this session. The
observation 917 baseline is **not frozen** and the Part B mechanism is
**unconfirmed** — see §5 and §6.

### 0.2 The active release artifacts are absent from the repository

The plan's quantified bridge-loss evidence (§"Quantified bridge loss in the
active release") is attributed to
`database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01/`.
That directory **does not exist in this worktree** and is gitignored
(`.gitignore:70` — `database/reference_data/generated/taxonomy_v2/*`). Only
`tax-2026.07.30-02.sqlite3.gz` and a `manifest.json` are present.

The counts 52,917 / 57,769 / 3,923 / 52,881 / 2,041 are therefore **not
independently reproducible here**. They are treated in this ledger as recorded
prior measurements, not as re-verified fact. This is consistent with the July
plan's accepted decision that the generated export is reproducible and not
committed to Git — but it means the release cannot be audited from a clean
checkout without first rebuilding it.

### 0.3 The NorTaxa source data cited by the plan is absent

The plan cites `database/reference_data/sources/taxon.txt` for the NorTaxa
`53482` source evidence. **That path does not exist**; neither does
`database/reference_data/sources/`. The only NorTaxa payload in the repository is
`database/taxonomy/national_sources/nortaxa/1.284/synthetic-fixture.zip` (1.4 KB,
a synthetic test fixture). The `53482` source-side claims (children `53483`–`53485`,
the `stjernesporet rødspore` vernaculars) could not be checked against real data.

### 0.4 Taxonomy test suite baseline

Command (from the worktree root):

```
.venv/bin/pytest database/taxonomy/tests tests/taxonomy -q
→ 13 failed, 771 passed, 2 skipped, 19 errors in 52.08s
```

**All 32 failures/errors share one root cause** — the missing generated release
directory from §0.2:

```
ReleaseValidationError: release dir not found: .../global_macrofungi_tax-2026.08.01-01
SupplementLineageError: base release missing taxonomy_export_manifest.json: .../global_macrofungi_tax-2026.08.01-01
```

Affected files: `tests/taxonomy/test_w2d_reconciliation.py` (1 failure + 19
errors), `tests/taxonomy/test_supplement_loader.py` (12 failures). These are
pre-existing on this branch's HEAD; Stage 1 changed no code.

**Finding (defect D5, §7):** the W2D/supplement test suites are not
self-contained. They depend on a gitignored, locally-absent build output, so the
W3 reconciliation contract cannot be re-verified in a clean checkout. Any claim
that "W3 is done because tests pass" is currently unfalsifiable.

---

## 1. Reconciled programme ledger

Classification of the July plan's §2 status table. "Deployed" claims are
inherited from the closeout plan's baseline per §0.1, not re-measured.

| July stage | July marker | Reconciled classification | Evidence |
|---|---|---|---|
| W0 — Cross-repo audit | DONE | **Implemented** — unchanged | — |
| W1 — Model-neutral cloud exporter | DONE | **Implemented and deployed** | `database/taxonomy/cloud_export.py`, seven-file emit order at `cloud_export.py:66-72` |
| W1 repository integration | CLOSEOUT | **Superseded/obsolete** — the generated export is deliberately not committed | `.gitignore:70` |
| W2A — Additive Supabase schema | DONE | **Implemented and deployed** | plan baseline only (§0.1) |
| W2B — Importer / full load | DONE (scope rejected) | **Superseded/obsolete** — complete-Fungi scope rejected | July plan §2 |
| W2C — Macrofungi scope + sparse registry | **GATE** | **STALE → deployed but not reflected in the old plan.** The scope build is implemented and a release was produced | `database/taxonomy/macrofungi_scope.py` (732 lines, `global_macrofungi_policy_v1` at `:441`) |
| W2D — Cloud implementation | **BLOCKED** | **STALE → implemented.** Full reconciliation package exists | `database/taxonomy/reconciliation/` (resolver, namespace_rules, snapshot, manifest), `docs/w2d-reconciliation-contract.md` |
| Publication and provenance | BLOCKED for production activation | **Unresolved / needs evidence.** See §1.1 | — |
| W3A — Observation identity schema | **PLANNED** | **STALE → implemented and deployed** | commits `20859a2` (W3-A local rehearsal), `bffd142` (W3-A2 pseudonym bridge); both merged |
| W3B — Sync, migration, backfill | **PLANNED** | **STALE → implemented and deployed** | commit `fe1d035` (W3-B final reconciliation freeze), merged |
| W4A — Web taxonomy service and picker | PLANNED | **Partially implemented** | desktop: `ui/taxon_input_controller.py`, `utils/taxonomy_v2.py`; web: `src/taxonomy-v2.js` |
| W4B — Artsorakel resolution and ambiguity UX | PLANNED | **Genuinely outstanding** — confirmed absent, see §4 | `src/taxonomy-v2.js:129-134` requires `sporelyTaxonId`; no `NBIC:` parsing exists |
| W5 — Shadow validation, cutover, rollback | PLANNED | **Unresolved / needs evidence.** See §1.1 | — |
| Legacy retirement | PLANNED | **Genuinely outstanding** | — |

All three cited W3 commits exist on this branch's history and are merged into
multiple branches (`git log -1` + `git branch --contains` for `20859a2`,
`bffd142`, `fe1d035`, all dated 2026-08-02).

### 1.1 Contradiction requiring resolution

The July plan records **"Publication and provenance — BLOCKED for production
activation"** and **"W5 — PLANNED"**. The closeout plan records an **active
production release `tax-2026.08.01-01`**. A release cannot be simultaneously
active in production and blocked from activation pending licence/publication
metadata.

Exactly one of these is true, and Stage 1 cannot tell which without Supabase
access:

1. the publication/licence gate was satisfied and the old plan is simply stale; or
2. a release was activated in production without satisfying the accepted
   publication gate.

This is the one open item under the acceptance gate's "no unexplained taxonomy
production objects" clause. **It must be resolved with an authorized Supabase
session before Stage 3 builds a successor release**, because Stage 3's release
safety rules assume a known, reviewed activation state.

---

## 2. Where the NorTaxa bridge is actually lost

**The plan's stated mechanism is wrong, and the correction matters.**

The closeout plan (lines 83-85) says the loss happens because "the global
macrofungi builder ... filters exported enrichment/mappings by retained COL
taxon IDs". That is not the cause. In `macrofungi_scope.py:473-479`,
`vernacular.jsonl` and `taxon_external_id.jsonl` are passed through the **same
function with the same argument**:

```python
for filename in ("scientific_name.jsonl", "vernacular.jsonl", "taxon_external_id.jsonl", "taxon_redlist.jsonl"):
    ...
    rows = _iter_jsonl(w1_dir / filename, included)
```

An identical filter cannot produce an asymmetric result. The vernaculars
survive and the external IDs do not **because the two datasets already differ
before the scope filter runs.** The loss is upstream, at two distinct gates.

### Gate 1 — namespace routing sends NorTaxa IDs to the legacy-integer table

`database/taxonomy/scripts/build_sqlite_candidate.py:56-70`:

```python
SOURCE_SYSTEM_MAP = {"col_xr": "col_xr", "nortaxa": "artsdatabanken"}
INTEGER_NAMESPACES = frozenset({"nortaxa_dwc_id", "nortaxa_taxon_id",
                                "nortaxa_accepted_name_usage_id",
                                "nortaxa_parent_name_usage_id"})
TEXT_NAMESPACES = frozenset({"col_usage_id"})
```

At `:474-490`, a usage whose namespace is in `TEXT_NAMESPACES` goes to
`external_text_rows`; one in `INTEGER_NAMESPACES` with a parseable integer goes
to `external_int_rows`. Since `col_usage_id` is the **only** text namespace,
`taxon_external_id_text_min` contains **only** COL rows — which is exactly the
observed "all 52,881 rows are `col_usage_id`".

`cloud_export.py:687-707` emits `taxon_external_id.jsonl` from
`taxon_external_id_text_min` (plus the derived branch in Gate 2). The NorTaxa
integer rows land instead in `taxon_external_id_legacy_integer.jsonl` — which
`macrofungi_scope.py:480` writes **unconditionally empty**:

```python
count, size, digest = _write_jsonl(output_dir / "taxon_external_id_legacy_integer.jsonl", [])
```

### Gate 2 — the derived NorTaxa row requires an *anchor* binding

`cloud_export.py:696-704` has a second, derived source of authoritative NorTaxa
rows, gated on `taxon_min.norwegian_taxon_id IS NOT NULL`. That column is
populated at `build_sqlite_candidate.py:603-608`, fed only by `:492-495`:

```python
if source_system == "artsdatabanken" and ns == "nortaxa_taxon_id" and is_preferred == 1:
    taxon_ids_with_norwegian.setdefault(sporely_id, []).append(numeric)
```

and `is_preferred` is defined at `:458`:

```python
is_preferred = 1 if u["identity_binding"] == "anchor" else 0
```

For a COL-backed concept the **COL usage is the anchor** and the NorTaxa usage
is an **alias** (`identity_registry.py:48-49`, `ENTRY_KIND_ANCHOR` /
`ENTRY_KIND_ALIAS`). So `is_preferred == 0`, `norwegian_taxon_id` stays NULL,
and the derived branch emits nothing — matching the observed "zero rows with
`norwegian_taxon_id` set".

### Why the vernacular join survives both gates

`cloud_export.py:639-644` selects vernaculars from `vernacular_min` keyed on
`v.taxon_id` alone. `vernacular_min` is populated in Pass 4
(`build_sqlite_candidate.py:611-620`) straight from `v["sporely_taxon_id"]`.
That path **never consults `identity_binding`, `is_preferred`, namespace
routing, or `norwegian_taxon_id`.** It only needs the vernacular row to have
been bound to a Sporely ID at compile time — which it was.

**Summary:** the association is computed once, then read by two consumers with
different gates. The vernacular consumer accepts any binding; the external-ID
consumers accept only an anchor binding (Gate 2) or a text namespace (Gate 1).

### The two regression cases fail at *different* points

This is the most consequential correction in this ledger. The plan treats
`52369` and `53482` as two instances of one loss. They are not.

- **`53482` / `Entoloma conferendum` (agreeing names)** — COL and NorTaxa agree
  on the name, so the classifier matched and the NorTaxa usage was applied as an
  **alias** on Sporely `7821`. Vernaculars attached (via the ungated path); the
  external ID was dropped at **Gate 2** (alias, not anchor).
- **`52369` / `Pholiotina rugosa` (divergent names)** — the names differ, so the
  classifier finds no backbone candidate at all and returns
  `PROPOSAL_NATIONAL_ONLY` / `review_status="unreviewed"` /
  `reason="no_backbone_match"` (`cross_source_mapping.py:213-219`). No binding is
  ever created, so this case fails **before** Gate 1 or Gate 2 — and it should
  carry **no** vernacular join either.

**Testable prediction for Stage 3:** taxon `7821` appears among the 2,041
vernacular-joined taxa; the `52369` concept does not appear at all. A Stage 3
mechanism that only relaxes Gate 2 fixes `53482` and does **not** fix `52369`.

---

## 3. Characterizing the vernacular join (Stage 3 coverage-audit input)

This is the deliverable that gates Stage 3's audit.

### The join key is a source-usage identity, not a name match

`compile_release.py:878-899` builds the vernacular linkage:

```python
core_row_id_to_sporely: dict[tuple[str, str, str], int] = {}
for usage in source_usages:
    key = (usage["source_code"], usage["core_row_id"]["namespace"], usage["core_row_id"]["value"])
    ...
    core_row_id_to_sporely[key] = usage["sporely_taxon_id"]
```

So the join key is **`(source_code, core_row_id.namespace, core_row_id.value)`**
resolved through the compiled `source_usages`, with a hard collision check
(`CompilerError: core_row_id collision`) and fail-closed handling of unresolved
linkage. A vernacular is attached because its NorTaxa source row belongs to a
NorTaxa **source usage that was bound to that Sporely concept** — not because
the names matched at vernacular time.

### But the *binding itself* is name-derived for the auto subset

The evidence grade therefore reduces to: **why was that NorTaxa usage bound to
that Sporely ID?** For manually reviewed mappings the answer is human review.
For the automatic path, `cross_source_mapping.py` classifies by
**canonical scientific name + rank exact match** against a backbone index
(`BackboneIndex.by_name_rank`, `:117-146`), then applies guards. Outcomes
(`:212-250`):

| Proposal class | `review_status` | Evidence rule |
|---|---|---|
| `PROPOSAL_AUTOMATIC_EXACT` | `policy_auto_approved` | conservative exact rule: name + rank + authorship + kingdom + status agreement, no competing homonym |
| missing-authorship fallback | (fallback status) | strict rule failed **exclusively** because an authorship was absent — never on mismatch |
| `PROPOSAL_AMBIGUOUS` | `needs_review` | name+rank matched multiple backbone concepts |
| `PROPOSAL_NATIONAL_ONLY` | `unreviewed` | no backbone match |
| `PROPOSAL_REJECTED` | `rejected` | manual-mappings rejection |

### Verdict on evidence strength

**The vernacular join does NOT constitute authoritative concept identity for the
auto-approved subset.** It is materially stronger than bare name equality — it
requires rank agreement, authorship agreement, kingdom and status agreement, and
an explicit homonym guard — but it is still a *machine-generated proposal keyed
on a name*, and the accepted architecture states plainly that "scientific-name
equality is not sufficient identity evidence".

The honest characterization is: **name-level enrichment backed by a conservative
automatic classifier, not a human-reviewed concept bridge.** The status string
`policy_auto_approved` means *approved by policy*, not *reviewed by a person* —
Stage 3 must not read it as the latter.

### Concrete audit scheme handed to Stage 3

Stage 3 can classify all 2,041 associations deterministically without new
analysis, because the compiler already records the grade. For each taxon
carrying a NorTaxa vernacular, join the vernacular's `core_row_id` back to its
`source_usages` row, then to the `mappings.jsonl` record for that
`source_usage`, and read:

1. `identity_binding` (`anchor` | `alias`) — from `source_usages.jsonl`;
2. `kind` (`manual` | `cross_source_proposal`) — manual entries are reviewed;
3. `review_status` — per the table above;
4. `evidence.reason` — `conservative_exact_rule_satisfied` vs the
   missing-authorship fallback vs `no_backbone_match`;
5. `identity_applied` — whether the alias binding was actually applied
   (`auto_alias_applied`, built at `compile_release.py:655-679`, consumed at
   `:756` and `:860`).

The compiler already emits `auto_alias_applied_count` into its diagnostics
(`compile_release.py:1121`). That number is the expected upper bound on the
2,041 figure and should be reconciled against it as the audit's first check: if
they disagree, some vernacular joins came from a path other than an applied
auto alias, and that path must be identified before any mapping is emitted.

Emit as authoritative only those meeting the reviewed-bridge standard Stage 3
defines; count and explain the rest. Note that (4) is the discriminator that
separates "full agreement" from "agreed because authorship was missing on one
side" — the latter is the weakest class and should be expected to fail an
authoritative-bridge standard.

**Caveat:** this scheme is derived from compiler source, not executed, because
the release artifacts are absent (§0.2). Stage 3 must rebuild a candidate
release and confirm the fields are actually populated as read here.

---

## 4. Identity-path audit

### 4.1 Desktop — the picker displays provenance and then drops it

The v2 picker commit path is `ui/taxon_input_controller.py:1004-1041`. The
suggestion carries `sporely_taxon_id` from the taxonomy-v2 search pack — which
*is* a compiler-owned artifact keyed on Sporely IDs, so in this path the integer
is genuinely proven by artifact.

The defect is what the commit **discards**. At `:50-53` the picker reads
`canonical_source_system` purely to render a disambiguation label:

```python
source = str(suggestion.get("canonical_source_system") or "").strip()
if len(parts) == 1 and source and source != "col_xr":
    parts.append(f"({source})")
```

but the committed snapshot at `:1028-1036` stores only `genus`, `species`,
`scientific_name`, `taxon_rank_snapshot`, `sporely_taxon_id`, `link_kind`,
`canonical_scientific_name`, `canonical_rank`. There is **no `source_system`,
no `namespace`, no `external_id`.**

This is precisely the Stage 2 sparring target "UI code that displays source
provenance but drops it on save". Once committed, the snapshot is
indistinguishable from any other integer — the provenance needed to *prove* the
ID is Sporely-owned exists at render time and is destroyed at commit time.

### 4.2 Desktop — the concrete leak write path

Three hops, no validation at any of them:

1. `ui/observations_tab.py:17934-17942` — `snapshot['sporely_taxon_id']` is
   coerced with a bare `int()` into the local column. The only guard is a
   `TypeError/ValueError` catch; there is no check that the value is a Sporely ID.
2. `utils/cloud_sync.py:16124` — `taxon_id = _normalize_observation_int_value(obs.get('sporely_taxon_id'))`, guarded only by `taxon_id is None or taxon_id <= 0`.
3. `utils/cloud_sync.py:16132` → `:16135-16142` —
   `set_observation_selected_taxon(cloud_id, taxon_id)` passes it to
   `set_observation_selected_taxon_v2` as `'p_sporely_taxon_id': int(sporely_taxon_id)`.

**The leak boundary is `utils/cloud_sync.py:16124`.** Sign and non-nullity are
the *entire* proof standard applied before an integer is asserted to the cloud
as an owner-selected Sporely identity. Because §4.1 destroyed the provenance
upstream, cloud sync has nothing better available to check — the two defects
compose. Fixing sync alone is insufficient; the snapshot must carry provenance.

Credit where due: `_sync_observation_selected_taxon` is careful in two respects
the plan cares about — it does not infer identity from genus/species text, and a
missing local value does not erase cloud identity (`:16118-16123`).

### 4.3 Desktop — a name-based resolution path in migration

`database/migrate_observations_sporely_id.py` resolves legacy identity in four
steps (`:132-190`). Steps 1-3 are sound: an existing `sporely_taxon_id` is
validated against `valid_ids` and **cleared if invalid** (`:148-153`); NBIC-style
`ai_selected_taxon_id` and `artsdata_id` are resolved through
`_resolve_via_nortaxa`, an authoritative namespaced lookup.

**Step 4 (`:177-190`) resolves by unique scientific-name / synonym alias.** That
is name-based identity inference, which the accepted architecture forbids.

This compounds badly with §2: because the active release contains **zero**
NorTaxa mappings, `_resolve_via_nortaxa` cannot match anything, so steps 2 and 3
are dead code in production and **step 4 is the de-facto primary resolution
path**. The bridge loss silently promoted name matching to the main mechanism.
Stage 4's audit must treat rows resolved this way as unproven.

### 4.4 Web — `NBIC:` identifiers are dropped unparsed

`src/taxonomy-v2.js:129-134`: `taxonomySelectionForTaxon()` requires
`taxon.sporelyTaxonId`; there is no parsing of the `NBIC:` prefix anywhere. The
current expectation is pinned in `src/taxonomy-v2.test.js:89`, which asserts
that `taxonomySelectionForTaxon({ taxonId: 'NBIC:56449', scientificName:
'Amanita muscaria' })` returns `null`. This confirms W4B is genuinely
outstanding and confirms the plan's "expected test-expectation flip" as a valid
Stage 2/3 signal.

### 4.5 Web — the null-write path

`src/screens/find_detail.js:1059-1072` builds the AI-selection patch:
`genus`/`species` from `splitScientificName(selectedPrediction?.scientificName || '')`,
`common_name` from `selectedPrediction?.vernacularName || null`. The patch is
applied unconditionally at `:1087`.

`src/artsorakel.js:290-308` confirms the null return is reachable:

```js
const value = String(text || '').trim()
if (!value) return [null, null]
const parts = value.split(/\s+/).filter(Boolean)
if (parts.length < 2) return [null, null]
```

So a candidate whose `scientificName` is absent or is a single token yields
`[null, null]`, and an unconditional update then writes null `genus`, null
`species` and null `common_name` over whatever was there.

---

## 5. Stage 2 Part B hypothesis — structurally supported, NOT confirmed

The hypothesis has two links. One is confirmed in code; the other is not
confirmable here.

**Confirmed (code):** the deprecation filter is applied at exactly one nesting
level, `src/artsorakel.js:732`:

```js
.filter(p => p?.taxon?.vernacularName !== '*** Utdatert versjon ***')
```

while candidates flattened out of `predictions[].taxa.items[]` are pushed as
bare spreads at `:482-493` (`flattened.push({ ...item })`). A flattened
candidate therefore carries its vernacular at `p.vernacularName`, and
`p?.taxon?.vernacularName` evaluates to `undefined` — which never equals the
sentinel, so the filter cannot reject it. **A deprecated record can survive
normalization via the flattened path.** Combined with §4.5, the null-write
outcome is reachable.

**NOT confirmed (evidence):** whether *this* is what happened to the
`Entoloma conferendum` observation. Both required evidence items are
unavailable:

- the raw Artsorakel JSON for that identification (debug dashboard not
  accessible from this session);
- the live `observations` row (`genus`, `species`, `common_name`,
  `ai_selected_scientific_name`, `ai_selected_taxon_id`,
  `selected_sporely_taxon_id`) — blocked by §0.1.

**Recorded status: the mechanism behind the `Entoloma conferendum` display
failure is UNCONFIRMED, blocked on Supabase MCP authorization and on capturing
the raw provider response.** Per the plan, it is recorded as unconfirmed rather
than assumed.

This does not weaken Part B's required-behavior list, which stands on its own:
the flatten/filter defect is real and independently worth fixing whether or not
it caused this particular incident. Note also that a competing explanation
exists and has not been excluded — a candidate with a *present but
single-token* scientific name reaches `[null, null]` through §4.5 **without**
any deprecation involvement. Stage 2 should fix both; Stage 1 cannot say which
fired.

---

## 6. Observation 917 baseline — NOT frozen

The plan requires a before-state distinguishing the immutable W3 snapshot,
current selected identity, current resolved identity, AI-identification history,
media/images, measurements and mosaic data.

**This baseline could not be captured.** Reading it requires the live row, and
the Supabase MCP server is unauthorized (§0.1). The plan's recorded values
(`genus` null, `species` null, both taxon IDs null, W3 state
`no_identity_evidence`, historical snapshot without source namespace/external
ID, independent AI-identification history) are carried forward **unverified**.

Stage 1 deliberately does not restate those as a frozen baseline: a regression
baseline that was never read is not a baseline. The media/measurement/mosaic
dimensions were not recorded in the plan at all and remain entirely unknown.

**Required to unblock:** an authorized interactive Supabase session, then a
single read of observation 917 across the seven dimensions above, committed as
an evidence artifact alongside this file.

---

## 7. Additional identity-boundary defects discovered

| ID | Defect | Location | Severity |
|---|---|---|---|
| D1 | Picker renders `canonical_source_system` but the committed snapshot stores no source/namespace/external ID | `ui/taxon_input_controller.py:50-53` vs `:1028-1036` | High — the root enabler of the desktop leak |
| D2 | Cloud sync's entire proof standard for a Sporely identity is `> 0` | `utils/cloud_sync.py:16124-16132` | High |
| D3 | Migration step 4 resolves identity by unique scientific-name match | `database/migrate_observations_sporely_id.py:177-190` | High — and currently the *only live* path, since D4 kills steps 2-3 |
| D4 | Zero NorTaxa mappings in the active release make `_resolve_via_nortaxa` unmatchable, silently promoting D3 | §2 + `migrate_observations_sporely_id.py:156-175` | High (compounding) |
| D5 | W2D/supplement tests depend on a gitignored absent build output; 32 tests cannot run in a clean checkout | `tests/taxonomy/test_w2d_reconciliation.py`, `tests/taxonomy/test_supplement_loader.py` | Medium — makes "W3 is done" unfalsifiable |
| D6 | July plan's publication gate contradicts the recorded active production release | §1.1 | Medium — unresolved, blocks Stage 3 release safety |
| D7 | `taxon_external_id_legacy_integer.jsonl` is written unconditionally empty, silently discarding every integer-namespace external ID | `macrofungi_scope.py:480` | Medium — data loss with no diagnostic |

D7 deserves emphasis: the file is emitted with a correct row count and hash, so
every determinism and manifest check passes while the content is empty by
construction. Release validation cannot detect this class of loss.

---

## 8. Proposed smallest Stage 2 and Stage 3 surfaces

### Stage 2 Part A (desktop) — 3 files

1. `ui/taxon_input_controller.py` — add `source_system` / `namespace` /
   `external_id` to the committed snapshot (§4.1). The values are already in the
   suggestion; this is a widening of the dict literal at `:1028-1036` plus
   `load_committed_snapshot` at `:822-828` for round-trip.
2. `ui/observations_tab.py` + persistence — carry the three fields through
   save/load so provenance survives restart.
3. `utils/cloud_sync.py:16124` — require proven Sporely provenance before
   calling the RPC; retain and skip otherwise rather than erasing.

Do **not** change the v2 search-pack path itself; its IDs are artifact-proven.

### Stage 2 Part B (web) — 3 files

1. `src/artsorakel.js` — normalize the deprecation check to run on a candidate
   *after* flattening (check both nesting levels), and apply it to the
   iNaturalist normalizer too.
2. `src/screens/find_detail.js:1059-1087` — never write null `genus`/`species`/
   `common_name` over existing non-null values; treat `[null, null]` as
   "no update" rather than "clear".
3. `src/taxonomy-v2.js` — parse `NBIC:<id>` into
   `(nortaxa, nortaxa_taxon_id, <id>)` and preserve it regardless of resolution.

### Stage 3 — 2 files, plus the audit

1. `database/taxonomy/scripts/build_sqlite_candidate.py` — the bridge must be
   emitted from the **alias** binding, not only the anchor. The minimal change is
   to stop routing reviewed NorTaxa bridges through the integer/legacy path
   (Gate 1) and to stop requiring `is_preferred == 1` (Gate 2). Preferably emit
   `nortaxa / nortaxa_taxon_id` rows into `taxon_external_id_text_min` directly,
   with `is_preferred` reflecting anchor status rather than gating emission.
2. `database/taxonomy/macrofungi_scope.py:480` — stop unconditionally emptying
   the legacy-integer file, or make the emptying explicit and validated (D7).

`52369` needs more than this: it has no binding at all
(`PROPOSAL_NATIONAL_ONLY`). It requires a reviewed manual-mapping entry creating
the alias in the first place — i.e. the manual-mappings input, not the emitter.
**That is why the two regressions need two different mechanisms**, and why a
Stage 3 candidate should be rejected if it fixes only one.

---

## 9. Stage 1 verdict

**The required verdict `Stage 1 accepted — proceed to client identity-boundary
correction` is NOT claimed.** Three acceptance-gate criteria are unmet:

| Gate criterion | Status |
|---|---|
| Deployed and repository state agree with the ledger | **Unmet** — deployed side unverifiable (§0.1) |
| Observation 917 has a reproducible before-state | **Unmet** — not readable (§6) |
| Desktop leak traced to a concrete write path | **Met** — §4.2 |
| NorTaxa bridge loss traced to a concrete compile/export path | **Met** — §2, two gates, both located |
| Vernacular join characterized for Stage 3 classification | **Met** — §3, with the caveat in §3 |
| `Entoloma conferendum` failure traced, or recorded unconfirmed with reason | **Met** (recorded unconfirmed, §5) |
| No unexplained taxonomy production objects | **Unmet** — §1.1 activation/publication contradiction |

The repository-side investigation is complete. The three unmet criteria all
reduce to one external blocker — **an authorized Supabase MCP session** — plus
capturing one raw Artsorakel response.

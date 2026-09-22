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

### 0.1 Live SQL is blocked — deployed state is unverified

Two different blockers were observed, and the distinction matters for how it
gets unblocked:

- **In the implementation session:** the Supabase MCP server was present but
  **unauthenticated**, and the session was non-interactive, so no OAuth flow
  could run. No Supabase tool was callable at all.
- **In the review session:** Supabase **project discovery succeeded**, so the
  connection itself is usable. The blocker there was that the read-only SQL
  query was **rejected by automatic approval review, because the approval policy
  is set to `never`.**

So the deployed state is not blocked merely by missing credentials. **Even with a
working connection, no SQL will execute until the approval policy is changed**
for this project. Both must be resolved: an authorized session *and* an approval
policy that permits read-only SQL.

Consequently **every deployed fact in this ledger is quoted from the closeout
plan's recorded baseline, not independently re-measured.** Specifically, the
following were NOT verified:

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

**The `07.30-02` artifact *was* inspected** — see §2.1. It is the
`build_sqlite_candidate.py` output for the full unscoped build and it carries
the compiler's actual bindings, which is enough to verify §2's binding claims
and §3's join key from data. What it does **not** carry is the compiler's
`mappings.jsonl` provenance (no mapping table exists in the schema), nor the
scoped active release. So §3's evidence grading and all `08.01-01`-specific
counts remain blocked on the missing artifacts.

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

This worktree has no local `.venv`; `AGENTS.md` requires the absolute
interpreter path from the primary checkout. The command actually run, from this
worktree root:

```
/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest \
    database/taxonomy/tests tests/taxonomy -q
→ 13 failed, 771 passed, 2 skipped, 1 warning, 19 errors in 52.08s
```

Root causes were isolated with two follow-up runs scoped to
`tests/taxonomy/test_w2d_reconciliation.py` and
`tests/taxonomy/test_supplement_loader.py` using the same interpreter.

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

Classification of the July plan's §2 status table.

**Every "deployed" classification below is PROVISIONAL.** No deployed object was
queried (§0.1); "deployed" is inherited from the closeout plan's recorded
baseline. What this ledger verifies directly is the *repository* side: that the
code and commits exist, which is a claim about implementation, not deployment.
Per the stage's own sparring challenge, the presence of code is not evidence
that a stage is done — so W3A/W3B are recorded as implemented-and-merged with
their deployment status carried over unverified, and W4/W5 remain open.

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

**The plan states one mechanism; there are three, and the plan's account is
correct for only one of the two named regressions.**

The closeout plan (lines 83-85) says the loss happens because "the global
macrofungi builder ... filters exported enrichment/mappings by retained COL
taxon IDs". For `52369` that is essentially right (see "the two regression
cases" below). For `53482` — the case where the mapping demonstrably *was*
computed — it is not the cause. In `macrofungi_scope.py:473-479`,
`vernacular.jsonl` and `taxon_external_id.jsonl` are passed through the **same
function with the same argument**:

```python
for filename in ("scientific_name.jsonl", "vernacular.jsonl", "taxon_external_id.jsonl", "taxon_redlist.jsonl"):
    ...
    rows = _iter_jsonl(w1_dir / filename, included)
```

An identical filter cannot produce an asymmetric result. For a taxon that *is*
in scope — such as `7821` — the vernaculars survive and the external IDs do not
**because the two datasets already differ before the scope filter runs.** That
loss is upstream, at the two gates below. A third mechanism, the COL-only scope
universe, removes `52369`'s concept wholesale and is described afterwards.

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
  classifier finds no backbone candidate and returns `PROPOSAL_NATIONAL_ONLY` /
  `review_status="unreviewed"` / `reason="no_backbone_match"`
  (`cross_source_mapping.py:213-219`). It is therefore **not** alias-bound in
  Phase 2c. But it is **not unbound** either: Phase 2d
  (`compile_release.py:681-692`) allocates a registry **anchor** for every
  accepted bridge record that has no registry entry yet:

  ```python
  # ----- Phase 2d: allocate remaining accepted bridge anchors ------------
  for record in sorted(accepted_bridge_records, ...):
      key = (record.source_code, record.taxon_id_namespace, record.taxon_id_value)
      if registry.lookup(*key) is not None:
          continue
      registry.allocate(source=key[0], namespace=key[1], identifier=key[2], ...)
  ```

  So `52369` receives **its own distinct Sporely concept**, anchored on
  `(nortaxa, nortaxa_taxon_id, 52369)`. Being an anchor, it would even satisfy
  Gate 2's `is_preferred == 1` test.

  It disappears at a **third, separate point**: the macrofungi scope universe is
  COL-only. `macrofungi_scope.py:105-108`:

  ```sql
  select taxon_id, canonical_external_id, ... from taxon_min
   where source_system='col_xr'
  ```

  A NorTaxa-anchored concept never enters `taxa`, so it can never be `included`,
  so `_iter_jsonl(..., included)` drops it **and everything keyed to it** —
  including its vernaculars.

**Correction to this ledger's earlier draft:** an earlier version asserted that
`52369` receives no binding at all and fails before Gates 1 and 2. That was
wrong; Phase 2d binds it as an anchor. For `52369` specifically, the closeout
plan's original framing — loss by COL-retention filtering — is **correct**, and
my Gate 1 / Gate 2 analysis does not apply to it.

**Three distinct loss mechanisms, not one — now verified against compiled
records.** The `tax-2026.07.30-02` SQLite artifact *is* present locally and has
been inspected (§2.1). It is the `build_sqlite_candidate.py` output for the full
unscoped build, so it contains the compiler's actual bindings for both
regression cases.

| Case | Binding (**measured**) | Lost at |
|---|---|---|
| `53482` (agreeing names) | **alias** onto COL concept `7821` — `is_preferred=0` | Gate 1 (namespace routing) and Gate 2 (anchor-only derivation) |
| `52369` (divergent names) | **own anchor concept `624680`** (Phase 2d) — `is_preferred=1`, `parent_taxon_id` NULL | the COL-only scope universe, `macrofungi_scope.py:105-108` |
| — | — | Gate 1 also silently empties the legacy-integer file (D7) |

### 2.1 Evidence from `tax-2026.07.30-02`

Reproduce with (65 MB gzip → 305 MB database; written to `/tmp`, not committed):

```
gunzip -c database/reference_data/generated/taxonomy_v2/tax-2026.07.30-02.sqlite3.gz > /tmp/tax0730.sqlite3
sqlite3 /tmp/tax0730.sqlite3
```

**Release composition** — `taxon_min` holds two disjoint id ranges:

| `source_system` | rows | id range |
|---|---:|---|
| `col_xr` | 620,976 | 1 – 620,976 |
| `nortaxa` | 13,919 | 620,977 – 634,895 |

All 13,919 NorTaxa concepts have `norwegian_taxon_id` set — consistent with
their being Phase 2d **anchors** (`is_preferred=1`), which is exactly Gate 2's
condition. The count matches `cloud_export.py:454`
(`external_authoritative_nortaxa_rows: 13919`) and `macrofungi_scope.py:362`
(`nortaxa_additional_concepts: 13919`).

**The duplicate block is confirmed, not hypothesised.** For
`taxon_id BETWEEN 625000 AND 626999`: 2,000 rows, **all** `source_system =
nortaxa`, **all** with `parent_taxon_id IS NULL`. The plan's description of the
625xxx–626xxx block matches the Phase 2d anchor set exactly. (This confirms the
block's composition in that range; it does not exhaustively prove Phase 2d is
its only contributor across the whole NorTaxa range.)

**Gate 1 confirmed.** `taxon_external_id_text_min` — the authoritative namespaced
table — contains **only** `col_xr` / `col_usage_id` (620,976 rows). Not one
NorTaxa row. Every NorTaxa identifier (61,583 rows, all `artsdatabanken`) sits in
the namespace-free `taxon_external_id_min`, matching
`cloud-export-contract.md:416` verbatim.

**The two regression cases:**

```
sqlite> SELECT taxon_id, source_system, external_id, id_role, is_preferred, external_name
        FROM taxon_external_id_min WHERE external_id IN (52369,53482);
624680|artsdatabanken|52369|accepted|1|Pholiotina rugosa
  7821|artsdatabanken|53482|accepted|0|Entoloma conferendum
```

- **`52369` → taxon `624680`**, `source_system = nortaxa`,
  `canonical_scientific_name = 'Pholiotina rugosa'`, `parent_taxon_id` NULL,
  `norwegian_taxon_id = 52369`, `is_preferred = 1`. **Independently anchored**,
  as predicted.
- **`53482` → taxon `7821`**, `is_preferred = 0`, alongside seven further
  `synonym`-role NorTaxa aliases (`59746`, `59754`, `59796`, `59818`, `59926`,
  `59945`, `59946`). **Alias-bound to the COL concept**, as predicted — and
  because `is_preferred = 0`, it does not populate `norwegian_taxon_id`
  (confirmed: querying `norwegian_taxon_id IN (52369,53482)` returns only
  `624680`, never `7821`).

**Independent corroboration of the plan's baseline.** The plan's recorded
identities check out against this artifact: `Conocybe rugosa` is Sporely
`83668` (col_xr) and `Entoloma conferendum` is Sporely `7821` (col_xr). Taxon
`7821` carries exactly the NorTaxa vernaculars the plan names —
`stjernesporet rødspore` (nb, preferred), `stjernespora raudspore` (nn,
preferred), plus the two `…rødskivesopp` / `…raudskivesopp` non-preferred
variants — all with `source = nortaxa`.

**The `52369` prediction holds, and the consequence is worse than the plan
states.** `83668` (`Conocybe rugosa`) carries **zero** NorTaxa external IDs and
**zero** vernaculars. The Norwegian name for that concept
(`slank ringkjeglesopp`, nb and nn) sits on the *separate* anchor `624680`,
which the COL-only scope drops wholesale. So for the divergent-name case the
release loses not just the identity bridge but the **Norwegian vernacular name
as well** — a name-loss class the plan does not currently enumerate.

**Scope caveat.** This is the `07.30-02` build, not the active `08.01-01`. The
bindings above are compiler-determined and the compiler is unchanged between
them, so they carry over; the `08.01-01`-specific step is the macrofungi scope
filter, which is precisely the mechanism that removes `624680`. Confirming the
active release still requires its artifacts (§0.2).

**Testable predictions for Stage 3** (neither executed here — see §0.2):

1. taxon `7821` appears among the 2,041 vernacular-joined taxa;
2. the `52369` anchor concept exists in the compiler registry and in
   `tax-2026.07.30-02`, but is absent from `tax-2026.08.01-01` entirely;
3. `52369` carries **no** vernacular join in the active release, because its
   vernaculars are keyed to the dropped anchor concept, not to a COL concept.

A Stage 3 mechanism that only relaxes Gate 2 fixes `53482` and does **not** fix
`52369`; one that only admits Phase 2d anchors into scope fixes `52369` by
reintroducing exactly the duplicate-concept behavior the plan forbids.

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

### 3.1 Population structure — measured

`tax-2026.07.30-02` (§2.1) lets the *shape* of the association population be
measured, though not its evidence grade. Counts are for the full unscoped build:

| Population | rows | distinct taxa |
|---|---:|---:|
| NorTaxa external IDs, **anchor** (`is_preferred=1`) | 13,919 | 13,919 |
| NorTaxa external IDs, **alias** (`is_preferred=0`) | 47,664 | — |
| …of which alias rows land on a **`col_xr`** concept | 32,988 | **19,808** |
| …of which alias rows land on a **`nortaxa`** concept | 14,676 | 5,551 |
| NorTaxa vernaculars on a **`col_xr`** concept | 5,413 | **3,070** |
| NorTaxa vernaculars on a **`nortaxa`** concept | 4,881 | 2,633 |

**The decisive structural fact: 3,070 ⊂ 19,808.** Every COL concept carrying a
NorTaxa vernacular is a strict subset of those carrying a NorTaxa *alias
binding*. This confirms §3's join-key account directly from data — a vernacular
reaches a COL concept **only** where a NorTaxa source usage was alias-bound to
it. The vernacular is not joined by name at vernacular time; it rides an
existing identity binding.

It also bounds the Stage 3 question usefully: the association population is
**19,808 alias bindings**, of which only 3,070 happen to carry vernaculars. The
plan's 2,041 figure is the scoped-release remnant of that 3,070. **Vernacular
presence is therefore an arbitrary sampling of the bridge population, not a
criterion** — Stage 3 should classify the alias bindings, not the vernacular
joins, or it will silently ignore ~16,700 bindings that differ only in whether
Artsdatabanken happened to publish a Norwegian name.

### This criterion is still INCOMPLETE

What remains unmeasured is the **evidence grade**: for each alias binding, was
it `kind=manual` (reviewed) or `kind=cross_source_proposal` (automatic), and
under the strict exact rule or the missing-authorship fallback? That lives in
the compiler's `mappings.jsonl`, which is **not** carried into the SQLite
artifact — it has no mapping/provenance table. So the grading still requires the
pinned compiler outputs (§0.2).

What §3 now delivers is a *classification scheme*, a *verdict on the strongest
available rule*, and the *population structure* (§3.1). What it does not deliver
is the grade distribution — how many bindings are `policy_auto_approved` under
the strict rule, how many rest on the missing-authorship fallback, and how many
came from reviewed manual mappings.

The acceptance-gate criterion "the evidence behind the NorTaxa vernacular join
is characterized well enough for Stage 3 to classify associations" is therefore
**not yet met**, though §3.1 closes a material part of it: the join key is now
confirmed from data rather than inferred from source.

**This remains unfinished Stage 1 work and must not be handed to Stage 3.** An
earlier draft assigned the measurement to Stage 3; that was wrong. The stage
brief is explicit that this is Stage 1's deliverable and that "Stage 3 cannot
classify associations that Stage 1 has not characterized". Deferring it would
invert the dependency the plan was written to enforce — and would leave Stage 3
free to emit mappings against a standard nobody had measured, which is precisely
the special-casing risk the coverage requirement exists to prevent.

**To complete this criterion, Stage 1 needs** the pinned artifacts from §0.2 —
a rebuilt or recovered `global_macrofungi_tax-2026.08.01-01/` plus the
compiler's `mappings.jsonl` and `source_usages.jsonl` — and must then report,
per §3.1, the count by `review_status`, the split between `kind=manual` and
`kind=cross_source_proposal`, and the split between the strict exact rule and
the missing-authorship fallback.

Report these **over the alias-binding population, not the vernacular-joined
subset**, for the reason given in §3.1. The 2,041 figure should appear only as
the scoped intersection, so that the count of bindings deliberately left
unemitted is visible rather than hidden behind a vernacular filter.

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

This matches the Stage 2 sparring target "UI code that displays source
provenance but drops it on save".

**Two qualifications, both material.** First, `canonical_source_system` records
which source backs the *concept* (COL vs NorTaxa canonical presentation). That is
a **different axis** from the *identifier namespace* that Stage 2 Part A is about.
Preserving it would not, by itself, tell you whether an integer is a Sporely ID —
it tells you which source authored the name. Conflating the two would lead
Stage 2 to add the wrong field.

Second, and more importantly: **dropping this field does not demonstrate that any
external integer ever becomes `sporely_taxon_id` here.** It is a
provenance-completeness gap, not proof of contamination. See §4.2 for what the
evidence actually supports.

### 4.2 Desktop — the plan's premise is NOT supported for the live UI

The closeout plan asserts (failure 1) that "a taxonomy picker can currently
surface an integer `taxon_id` and commit it as `sporely_taxon_id`". **Stage 1
could not substantiate that for either live producer.**

There are exactly two paths that write `sporely_taxon_id` into a committed
snapshot, and both read an artifact-proven ID:

1. **Picker** — `ui/taxon_input_controller.py:1013`, `sporely_id =
   suggestion.get("sporely_taxon_id")`. The suggestion comes from the taxonomy-v2
   search pack, whose `taxon_id` **is** the Sporely ID by the compiler's identity
   contract (`build_sqlite_candidate.py:11` — "`taxon_min.taxon_id` =
   `sporely_taxon_id`").
2. **Manual editing-finished resolve** — `ui/observations_tab.py:19502-19547`
   calls `resolve_manual_scientific(genus, species)`
   (`database/taxon_lookup.py:710`), which resolves typed text against the same
   taxonomy DB and returns `None` for empty, unknown or ambiguous pairs.

Neither converts an external integer. This is a **negative finding that Stage 2
must account for**: if the desktop leak is to be fixed, the fix cannot be aimed
at the v2 picker, because no evidence here shows the v2 picker leaking.

Two caveats that keep this from being a clean bill of health:

- `resolve_manual_scientific` has a documented **source-system preference
  fallback** (`taxon_lookup.py:726-733`): when strict resolution returns `None`
  because a `col_xr` and a `nortaxa` canonical share an exact scientific name, it
  prefers the COL row. That is a name-collision tie-break establishing identity.
  It yields an artifact ID, so it is not namespace contamination — but it *is*
  identity selection by name, which the accepted architecture restricts.
- Legacy observation rows predating taxonomy v2 were not inspected; their
  `sporely_taxon_id` values may have been written by the migration in §4.3.

### 4.3 Desktop — the actual contaminating producer is the migration

`database/migrate_observations_sporely_id.py` is the only code found that
converts an **external** identifier into `sporely_taxon_id`. Its resolver is
`:73-80`:

```python
def _resolve_via_nortaxa(conn, value: int) -> int | None:
    row = conn.execute(
        "SELECT taxon_id FROM taxon_external_id_min "
        "WHERE source_system='artsdatabanken' AND external_id=? LIMIT 1",
        (int(value),),
    ).fetchone()
```

This is **not** an authoritative namespaced lookup, and an earlier draft of this
ledger was wrong to call it one. Two defects:

**(a) The namespace does not exist to filter on.** `taxon_external_id_min`
(`build_sqlite_candidate.py:158-168`) has columns `taxon_id`, `source_system`,
`external_id`, `id_role`, `is_preferred`, `external_name`, `note` — **no
namespace column**. The namespace is discarded at write time
(`:488-490`), where `ns` is simply absent from the inserted tuple:

```python
external_int_rows.append((
    sporely_id, source_system, numeric, id_role,
    is_preferred, external_name, note,
))
```

All four `INTEGER_NAMESPACES` — `nortaxa_dwc_id`, `nortaxa_taxon_id`,
`nortaxa_accepted_name_usage_id`, `nortaxa_parent_name_usage_id` — collapse into
a single undifferentiated `source_system='artsdatabanken'` integer space. A
NorTaxa `taxonID` can therefore silently match a row that was actually a
`nortaxa_dwc_id` or a `parent_name_usage_id`.

These are exactly the "namespace-lost integers" that the accepted architecture
designates **legacy/audit evidence only**. The migration uses them as resolution
evidence.

**(b) `LIMIT 1` swallows ambiguity.** Where the collapsed space produces multiple
matches, the query silently returns whichever row SQLite yields first, with no
ambiguity signal — contradicting the accepted rule that resolvers "return
ambiguity; they never collapse".

The module docstring at `build_sqlite_candidate.py:12-14` claims every external
identifier "is stored under an explicit `(source_system, id_role)` namespace".
`id_role` holds `accepted`/`synonym`, which is a **taxonomic status, not a
namespace**. The stated identity contract is not upheld by the schema it
describes.

**Step 4 (`:177-190`) additionally resolves by unique scientific-name / synonym
alias** — direct name-based identity inference.

**Correction to D3/D4:** an earlier draft claimed the NorTaxa steps were "dead
code in production" because the active *release* contains zero NorTaxa mappings,
making name matching "the only live path". That inference was unsound. The
migration reads the **desktop SQLite** (`taxon_min` / `taxon_external_id_min`,
the `build_sqlite_candidate.py` schema), which is a separately supplied artifact
and is **not** the cloud export whose namespace counts the plan measured. Absence
of NorTaxa mappings in the cloud export does not prove their absence in the
desktop DB. Stage 1 did not inspect a desktop DB (none is present — §0.2), so the
population of `taxon_external_id_min` is **unknown**. Steps 2-3 should be
presumed **live and namespace-unsafe**, which is worse than dead.

### 4.4 Desktop — cloud sync is a weak gate, not a producer

Three hops carry the value to the cloud:

1. `ui/observations_tab.py:17934-17942` — `snapshot['sporely_taxon_id']` coerced
   with a bare `int()` into the local column.
2. `utils/cloud_sync.py:16124` — `_normalize_observation_int_value(...)`, guarded
   only by `taxon_id is None or taxon_id <= 0`.
3. `utils/cloud_sync.py:16132` → `:16135-16142` — forwarded to
   `set_observation_selected_taxon_v2` as `'p_sporely_taxon_id'`.

Sign and non-nullity are the entire proof standard applied before an integer is
asserted to the cloud as an owner-selected Sporely identity. **This is a gate
weakness, not a leak source**: sync does not create a contaminated value, it
fails to detect one. Given §4.3, a value contaminated by the migration would pass
this gate unchallenged and reach production.

Credit where due: `_sync_observation_selected_taxon` is careful in two respects
the plan cares about — it does not infer identity from genus/species text, and a
missing local value does not erase cloud identity (`:16118-16123`).

### 4.5 Web — `NBIC:` identifiers are dropped unparsed

`src/taxonomy-v2.js:129-134`: `taxonomySelectionForTaxon()` requires
`taxon.sporelyTaxonId`; there is no parsing of the `NBIC:` prefix anywhere. The
current expectation is pinned in `src/taxonomy-v2.test.js:89`, which asserts
that `taxonomySelectionForTaxon({ taxonId: 'NBIC:56449', scientificName:
'Amanita muscaria' })` returns `null`. This confirms W4B is genuinely
outstanding and confirms the plan's "expected test-expectation flip" as a valid
Stage 2/3 signal.

**Contract disagreement to resolve before Stage 2 Part B implements this.** The
plan requires the client to normalize `NBIC:53482` to
`(source_system = nortaxa, namespace = nortaxa_taxon_id, external_id = 53482)`.
The accepted identity contract assigns a different home to that value
(`identity-contract.md:31-49`): an Artsorakel `NBIC:` ID is
`source = artsorakel`, `namespace = nbic_scientific_name_id`, and the contract
states that "`NBIC:54995` must be retained as the raw external value; any match
on its numeric component is valid only under an explicit, evidenced namespace
bridge."

The plan's target tuple is **reachable but not direct**. `identity-contract.md:71-79`
declares that NorTaxa's `dwc:taxonID` values *are* Artsnavnebase
scientific-name IDs, so `nortaxa_taxon_id` may be bridged to
`artsnavnebase_scientific_name_id` — the same registry Artsorakel's `NBIC:`
prefix returns. The plan therefore skips a declared namespace hop rather than
contradicting the contract outright.

Two obligations follow that the plan's wording omits: the raw `NBIC:53482`
value must be **retained**, not replaced by the stripped integer; and the hop to
`nortaxa_taxon_id` must be recorded as an evidenced namespace bridge. The
contract also warns that `artsdatabanken_taxon_concept_id` is a *different*
registry whose numeric equality with a name ID is coincidence — so a Part B
implementation that simply strips `NBIC:` and stores an integer would be one
registry-confusion away from the exact defect this closeout is about.

### 4.6 Web — the null-write path

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
| **D3a** | **NorTaxa identifiers are routed into the legacy store, losing their namespace.** `INTEGER_NAMESPACES` membership sends all four NorTaxa namespaces to `taxon_external_id_min`, which by contract carries no namespace column, collapsing them into one `artsdatabanken` integer space — while the authoritative `taxon_external_id_text_min` (with `namespace TEXT NOT NULL`) sits unused for them. The defect is the routing, not the legacy table's shape | `build_sqlite_candidate.py:473-490` (routing), `:170-181` (unused text table) | **High — root cause of D3b** |
| **D3b** | `_resolve_via_nortaxa` resolves identity from a namespace-lost integer, with `LIMIT 1` silently swallowing ambiguity | `migrate_observations_sporely_id.py:73-80` | **High** |
| D3c | Migration step 4 resolves identity by unique scientific-name match | `migrate_observations_sporely_id.py:177-190` | High |
| D2 | Cloud sync's entire proof standard for a Sporely identity is `> 0`; cannot detect a value contaminated by D3b | `utils/cloud_sync.py:16124-16132` | Medium — a gate weakness, not a leak source |
| D1 | Committed snapshot stores no source/namespace/external ID; `canonical_source_system` is rendered then dropped | `ui/taxon_input_controller.py:50-53` vs `:1028-1036` | Medium — provenance-completeness gap; **not** evidence of contamination |
| D8 | `resolve_manual_scientific` breaks a `col_xr`/`nortaxa` exact-name tie by source preference — identity selection by name | `database/taxon_lookup.py:726-733` | Medium |
| D9 | Module docstring claims every external identifier is stored under an explicit `(source_system, id_role)` namespace. `id_role` holds `accepted`/`synonym` — a taxonomic status, not a namespace. The real namespace lives in `taxon_external_id_text_min.namespace`, so the docstring misdescribes the module's own schema and obscures D3a | `build_sqlite_candidate.py:12-14` vs `:170-181` | Low — documentation defect |
| D5 | W2D/supplement tests depend on a gitignored absent build output; 32 tests cannot run in a clean checkout | `tests/taxonomy/test_w2d_reconciliation.py`, `tests/taxonomy/test_supplement_loader.py` | Medium — makes "W3 is done" unfalsifiable |
| D6 | July plan's publication gate contradicts the recorded active production release | §1.1 | Medium — unresolved, blocks Stage 3 release safety |
| D7 | `taxon_external_id_legacy_integer.jsonl` is written unconditionally empty, silently discarding every integer-namespace external ID | `macrofungi_scope.py:480` | Medium — data loss with no diagnostic |

| **D10** | **Norwegian vernacular loss for dropped anchors.** A divergent-name NorTaxa concept keeps its vernaculars on its own Phase 2d anchor, which the COL-only scope drops — so the retained COL concept has no Norwegian name at all. Verified: `83668` (`Conocybe rugosa`) has zero vernaculars while `slank ringkjeglesopp` sits on dropped anchor `624680`. Affects the 2,633 NorTaxa-anchored concepts carrying vernaculars (§3.1) | §2.1, §3.1 | **High — a name-loss class the plan does not enumerate** |

**Withdrawn:** the earlier D4 ("zero NorTaxa mappings make `_resolve_via_nortaxa`
unmatchable, so name matching is the only live path") is retracted. It inferred
the contents of the desktop SQLite from cloud-export measurements; those are
separate artifacts. See §4.3.

D7 deserves emphasis: the file is emitted with a correct row count and hash, so
every determinism and manifest check passes while the content is empty by
construction. Release validation cannot detect this class of loss.

---

## 8. Proposed smallest Stage 2 and Stage 3 surfaces

### Stage 2 Part A (desktop) — re-aimed by the §4.2 negative finding

Stage 1 found **no live UI path** converting an external integer into
`sporely_taxon_id`. The single demonstrated converter is the migration. The
smallest surface that fixes a *demonstrated* defect is therefore:

1. `database/taxonomy/scripts/build_sqlite_candidate.py` — route reviewed
   NorTaxa bridges to the **existing authoritative text path** (D3a).
2. `database/migrate_observations_sporely_id.py:73-80` — resolve against
   `taxon_external_id_text_min` with an explicit `namespace` predicate, and
   return ambiguity instead of `LIMIT 1` (D3b); demote step 4's name matching to
   a reported, non-repairing classification (D3c).
3. `utils/cloud_sync.py:16124` — require proven provenance before the RPC;
   retain and skip otherwise rather than erasing (D2).

**Correction to an earlier draft of this section.** It proposed adding a
`namespace` column to `taxon_external_id_min` and claimed "nothing downstream
can be made namespace-safe until this exists". Both parts were wrong, and the
proposal conflicted with the accepted contract:

- `taxon_external_id_text_min` **already exists** with exactly the needed shape
  (`build_sqlite_candidate.py:170-181`): `source_system TEXT NOT NULL`,
  `namespace TEXT NOT NULL`, `external_id TEXT NOT NULL`. The authoritative
  namespaced path is present; nothing new needs inventing.
- `identity-contract.md:27` requires that "every external identifier is stored
  as text with both `source` and `namespace`" — so namespaced text *is* the
  contract, and the text table is where an authoritative identifier belongs.
- `cloud-export-contract.md` reserves the integer table for legacy evidence and
  records its namespace loss as a known property, not a defect to repair:
  `taxon_external_id_legacy_integer.jsonl` is documented at `:416` as
  "61,583 (all `artsdatabanken` source; namespace lost)".

Adding a namespace column to the integer table would therefore have promoted a
deliberately legacy, audit-only store into a **second identity authority**
alongside the text table — creating exactly the competing-source-of-truth
problem this closeout exists to remove. The contract is not the obstacle here;
it already describes the right destination.

So the correct minimal change is at the **routing** decision
(`build_sqlite_candidate.py:473-490`): a reviewed NorTaxa bridge should be
emitted to `external_text_rows` with its namespace intact, rather than being
funnelled into `external_int_rows` by membership in `INTEGER_NAMESPACES`. That
single change addresses Gate 1 directly, and it makes Gate 2's derived
`norwegian_taxon_id` branch (`cloud_export.py:696-704`) redundant rather than
requiring a second fix — the derived branch exists only to reconstruct an
identifier that routing had discarded.

Two consequences worth stating: the change must be driven by *reviewed bridge
evidence*, not by namespace membership alone (otherwise every NorTaxa DwC row
becomes authoritative, which §3 shows the evidence does not support); and it
does **not** require touching `taxon_external_id_min`, whose legacy contents and
contract stay as they are.

**Deliberately deferred, with reason:** adding `source_system` / `namespace` /
`external_id` to the committed picker snapshot (D1). It is likely still correct
for Part B interop — the web client will need to persist a preserved external
identifier — but Stage 1 produced **no evidence** that the desktop picker leaks,
so it should not be justified as a leak fix. Decide it on Part B's requirements.
Do **not** change the v2 search-pack resolution path; its IDs are artifact-proven.

### Stage 2 Part B (web) — 3 files

1. `src/artsorakel.js` — normalize the deprecation check to run on a candidate
   *after* flattening (check both nesting levels), and apply it to the
   iNaturalist normalizer too.
2. `src/screens/find_detail.js:1059-1087` — never write null `genus`/`species`/
   `common_name` over existing non-null values; treat `[null, null]` as
   "no update" rather than "clear".
3. `src/taxonomy-v2.js` — parse `NBIC:<id>` into
   `(nortaxa, nortaxa_taxon_id, <id>)` and preserve it regardless of resolution.

### Stage 3 — one mechanism, two entry points

The bindings this rests on are measured (§2.1). What remains conditional is the
**evidence grade** of the alias population (§3), so the proposal below is stated
as: *this is the shape of the fix; which bindings are eligible to flow through it
is decided by the §3 audit, which Stage 1 must still complete.*

**Single change, at the routing decision** —
`build_sqlite_candidate.py:473-490`. A NorTaxa identifier that the §3 audit
grades as a reviewed bridge is emitted to `external_text_rows`
(`taxon_external_id_text_min`) with `source_system`, `namespace` and its
identifier as text. `is_preferred` then records anchor-vs-alias status rather
than gating emission.

This is one change, not two. It addresses Gate 1 directly, and it makes Gate 2's
derived `norwegian_taxon_id` branch (`cloud_export.py:696-704`) **redundant
rather than something to also modify** — that branch exists only to reconstruct
an identifier routing had discarded. An earlier draft of this section prescribed
changing both gates, which contradicted §8's own desktop subsection; the
single-route version is the consistent one. Gate 2 should be *retired* once the
text route carries NorTaxa, not relaxed to accept aliases.

Eligibility is the audit's output, not namespace membership: emitting every
NorTaxa alias because it is a NorTaxa alias would infer authoritative identity
from an automatic name+rank match, which §3 shows the evidence does not support.

**Entry point A — `53482`.** Already alias-bound to `7821` (`is_preferred=0`,
§2.1). If the §3 audit grades that binding as a reviewed bridge, the routing
change alone emits it. No new relationship is created.

**Entry point B — `52369`.** Anchored as its own concept `624680` (§2.1) and
excluded by the COL-only scope universe (`macrofungi_scope.py:105-108`), so the
routing change does **not** reach it. It needs an **approved manual bridge entry**
so it is alias-bound onto `83668` in Phase 2b/2c instead of anchored in 2d —
touching the manual-mappings input, not the emitter. That is a human decision no
automatic rule can supply, which is the correct outcome given that the sources
genuinely disagree on the accepted name.

Rejected alternative: admitting Phase 2d anchors into scope. §2.1 confirms that
is what produced the `tax-2026.07.30-02` duplicate block (2,000 null-parent
NorTaxa rows in 625xxx–626xxx alone).

Once B is aliased, it flows through the *same* routing change as A — so this is
one mechanism with two entry points, not two mechanisms. A Stage 3 candidate
should still be rejected if it satisfies only one regression, because A alone
leaves divergent-name bridges unrepresented and B alone is a special case.

**Also in scope for Stage 3:**

- `macrofungi_scope.py:480` — stop unconditionally emptying the legacy-integer
  file, or make the emptying explicit and validated (D7).
- **Vernacular loss for dropped anchors.** §2.1 shows `83668` has no Norwegian
  name because `slank ringkjeglesopp` sits on the dropped anchor `624680`. Any
  fix for B must carry the vernaculars across with the identity, or the concept
  stays unnamed in Norwegian. This affects the 2,633 NorTaxa-anchored concepts
  carrying their own vernaculars (§3.1), not just `52369`.

**Constraint inherited from the identity contract** (§4.5): where a client-side
external identifier enters this path, the raw provider value must be retained
and any namespace hop — for example `nbic_scientific_name_id` →
`artsnavnebase_scientific_name_id` → `nortaxa_taxon_id` — recorded as an
evidenced bridge rather than performed silently by stripping a prefix.

---

## 9. Stage 1 verdict

**The required verdict `Stage 1 accepted — proceed to client identity-boundary
correction` is NOT claimed.** Three acceptance-gate criteria are unmet:

| Gate criterion | Status |
|---|---|
| Deployed and repository state agree with the ledger | **Unmet** — deployed side unverifiable (§0.1) |
| Observation 917 has a reproducible before-state | **Unmet** — not readable (§6) |
| Desktop leak traced to a concrete write path | **Partially met** — the only demonstrated converter is the migration (§4.3, D3a/D3b). The plan's premise that the *picker* leaks is **unsupported** (§4.2) |
| NorTaxa bridge loss traced to a concrete compile/export path | **Met for the compiler bindings** — §2.1 verifies both cases against compiled records (`52369` → anchor `624680`; `53482` → alias on `7821`, `is_preferred=0`), plus the duplicate block. Confirmation against the *active* release remains outstanding (§0.2) |
| Vernacular join characterized for Stage 3 classification | **Partially met** — the join key is now confirmed from data (§3.1: vernacular-joined taxa are a strict subset of alias-bound taxa), but the evidence **grading** is unmeasured. **Unfinished Stage 1 work**, not deferrable to Stage 3 (§3) |
| `Entoloma conferendum` failure traced, or recorded unconfirmed with reason | **Met** (recorded unconfirmed, §5) |
| No unexplained taxonomy production objects | **Unmet** — §1.1 activation/publication contradiction |

The **code-reading** investigation is complete to the limit of the artifacts
present; the **measurement** work is not. Stage 1 has outstanding work of its
own, blocked on two external dependencies:

1. **an authorized Supabase session *and* an approval policy permitting
   read-only SQL** (§0.1) — for observation 917's seven-part baseline, the
   deployed reconciliation, and the §1.1 activation contradiction. Note that
   authentication alone does not resolve this: the review session had a working
   connection and was still refused under approval policy `never`;
2. **the pinned release and compiler artifacts** (§0.2, §0.3) — for the evidence
   **grading** of the alias population (§3, a Stage 1 deliverable) and for
   confirming §2.1's bindings against the *active* release. The compiled-record
   bindings and the duplicate-block question are **no longer blocked**: both were
   resolved in §2.1 from the locally present `tax-2026.07.30-02` artifact;

plus capturing one raw Artsorakel response for §5, which remains unavailable, so
the Part B mechanism stays unconfirmed.

Test results in §0.4 were produced in the implementation session and have not
been independently rerun by a reviewer.

### Corrections applied after third review

The third review found that §8 still asserted conclusions §2 had just retracted,
prescribed changing both export gates while §8's own desktop subsection said the
text route makes the derived gate redundant, and left the compiled-record
inspection assigned to Stage 3.

Rather than only softening the wording, this revision **obtained the evidence**:
the `tax-2026.07.30-02` SQLite artifact was present locally and had not been
opened. Inspecting it (§2.1) converts §2's conditional bindings into measured
facts, confirms the duplicate-block hypothesis, and confirms §3's join key from
data. §8 is rewritten as one consistent mechanism with two entry points, with
Gate 2 retired rather than also modified, and the contract constraints from §4.5
carried into the proposal. The record inspection stays in Stage 1.

New finding from that evidence: D10, Norwegian vernacular loss for dropped
anchors — a name-loss class the plan does not enumerate.

### Corrections applied after second review

1. **§2 stated the two cases' bindings as fact.** They are conditional code-path
   explanations; no compiled record was inspected. Marked as inferred, with
   explicit falsifiers, and the bridge-loss gate criterion changed from Met to
   partially verified. The `625xxx–626xxx` attribution is downgraded from an
   identification to a hypothesis.
2. **§8 proposed adding a `namespace` column to the legacy integer table.**
   Contract-violating and unnecessary: `taxon_external_id_text_min` already has
   that shape, `identity-contract.md:27` requires namespaced text, and
   `cloud-export-contract.md:416` reserves the integer table as legacy with
   namespace loss as a known property. The proposal would have created a second
   identity authority. Re-aimed at the routing decision instead.
3. **§3/§9 deferred the 2,041 characterization to Stage 3.** It is Stage 1's
   deliverable by the brief's own wording; deferring inverts the dependency the
   plan enforces. Reclassified as unmet, unfinished Stage 1 work, with the
   required counts specified.

Also added: a contract disagreement affecting Stage 2 Part B's `NBIC:`
normalization (§4.5), found while checking the identity contract for item 2.

### Corrections applied after first review

This revision retracts four claims from the first candidate. They are recorded
rather than silently edited, because each changed a Stage 2 or Stage 3
conclusion:

1. **`52369` "has no binding at all"** — wrong. Phase 2d anchors it
   (`compile_release.py:681-692`); it is lost to the COL-only scope filter. §2.
2. **`_resolve_via_nortaxa` is "an authoritative namespaced lookup"** — wrong.
   It has no namespace filter because the namespace column does not exist, and
   `LIMIT 1` hides ambiguity. §4.3, D3a/D3b.
3. **"Name matching is the only live path" (old D4)** — withdrawn. It inferred
   desktop SQLite contents from cloud-export measurements; separate artifacts. §7.
4. **"The desktop leak boundary is `cloud_sync.py:16124`"** — downgraded. That is
   a weak *gate*, not a producer; dropping `canonical_source_system` does not
   demonstrate identifier contamination, and concept provenance is a different
   axis from identifier namespace. §4.1, §4.2, §4.4.

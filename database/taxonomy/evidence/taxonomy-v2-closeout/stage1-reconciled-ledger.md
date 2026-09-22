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

Stage 1 is an evidence stage, so the provenance and limits of the evidence are
part of the deliverable. Three required inputs were initially unavailable; two
were supplied and the third was worked around. This section records what each
conclusion rests on, and what remains outside the evidence.

### 0.1 Live SQL — RESOLVED 2026-09-22; deployed state verified

**Read-only production SQL now works in the stage session.** `select
current_database()` returns `postgres`, and the observation 917 and taxonomy
reconciliation queries executed successfully. Results are in §1.0 and §6; the
deployed state is no longer inherited from the plan.

One scope limit observed: the read-only role cannot *execute*
`resolve_taxon_external_id_v2` (`ERROR 42501: permission denied for function`).
Resolver behavior was therefore verified against `taxonomy_v2_external_ids`
directly, which is the table the function reads (§6.10).

The blocker history below is retained because it took three distinct fixes, and
a future stage hitting one of them should be able to tell them apart.

### 0.1.1 Blocker history (resolved)

**Three distinct blockers have now been observed in sequence.** Each was
resolved and revealed the next, so the distinction matters:

1. **MCP unauthenticated** (first implementation session) — the Supabase server
   was present but not authorized, and the session was non-interactive, so no
   OAuth flow could run. No Supabase tool was callable. **Resolved.**
2. **Approval policy `never`** (review session) — project discovery succeeded,
   so the connection was usable, but the read-only SQL query was rejected by
   automatic approval review. **Reported as resolved by the operator.**
3. **Harness permission grant not available** (current session, 2026-09-22) —
   `mcp__supabase__execute_sql` and `mcp__supabase__list_tables` are now
   *loaded and callable*, and the server is authenticated, but every invocation
   returns:

   ```
   Claude requested permissions to use mcp__supabase__execute_sql,
   but you haven't granted it yet.
   ```

   This is neither an authentication failure nor the Supabase approval policy;
   it is the agent harness's own tool-permission gate, which a non-interactive
   session cannot satisfy. Two calls were attempted — a trivial
   `select current_database()` and the observation 917 read — both refused
   identically.

All three are now resolved; the permission was granted for this session.

Consequently the deployed facts in this ledger are **measured**, not inherited:

| Item | Status | Where |
|---|---|---|
| active release identity, status, hashes, row counts | **verified** | §1.0 |
| `taxonomy_v3` snapshot / resolution-link counts and states | **verified** | §1.0 |
| deployed functions exist with expected signatures | **verified** | §1.0 |
| `set_observation_selected_taxon_v2` body and guard trigger | **verified** | §7, D2 |
| live `observations` row 917 and all seven baseline dimensions | **verified** | §6 |
| `resolve_taxon_external_id_v2` *executed* | **not permitted** for the read-only role; verified against the underlying table instead | §6.10 |
| the live row behind the `Entoloma conferendum` incident | **not identified** — the incident observation was never pinned to an ID | §5 |

### 0.2 The active release artifacts are absent from the repository

The plan's quantified bridge-loss evidence (§"Quantified bridge loss in the
active release") is attributed to
`database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01/`.
That directory **does not exist in this worktree** and is gitignored
(`.gitignore:70` — `database/reference_data/generated/taxonomy_v2/*`). Only
`tax-2026.07.30-02.sqlite3.gz` and a `manifest.json` are present.

**Update 2026-09-22 — the artifacts were supplied and have been measured.**
They are in the **primary checkout**, not this worktree:

```
/Users/sigmundas/Documents/Code/sporely/sporely-py/database/reference_data/
  generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01/   ← active release
  generated/taxonomy_v2/cloud_export_tax-2026.07.30-02/        ← its W1 input
```

Generated artifacts are gitignored, so they exist in exactly one checkout; this
ledger reads them there deliberately and records the path for reproducibility.
The `07.30-02` SQLite was likewise inspected (§2.1). Results: §2.2 and §3.1.

**Lineage verified by full hash comparison.** `shasum -a 256` was recomputed over
every dataset file on disk and compared, in full, against both the local
`taxonomy_export_manifest.json` and production's
`taxonomy_v2_releases.source_manifest` (§1.0). **All seven match exactly in both
directions:**

| File | Rows | SHA-256 (disk = local manifest = production) |
|---|---:|---|
| `taxonomy_release.jsonl` | 1 | `a9315694222bb76413e2e4eb2d4b88305ab6b03b8cdf966c3aa9bdfa03f58fb5` |
| `taxon.jsonl` | 52,917 | `d292cda4cc9bdd7855311a5b26f6ee5f58bda4fd4e55f1901464d0920e842233` |
| `scientific_name.jsonl` | 57,769 | `c692a08dc3baafbadb82cc507ba5b6043b8f8d6f2de8e8977457ba35caf711a1` |
| `vernacular.jsonl` | 3,923 | `f9432e349ecb83a3f279c947bfc9d8b09cd948d93f47c2365746da1ee7a57c0c` |
| `taxon_external_id.jsonl` | 52,881 | `7d364e5d0ce17504d75ce1388fd49fe819d491be6cf1b399847415bfa7c34101` |
| `taxon_external_id_legacy_integer.jsonl` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `taxon_redlist.jsonl` | 2,262 | `71ef145c818cd760a5d2729e3b60f0ab0b548e3f0bc33b64cba78257b8538981` |

(The legacy-integer hash is the SHA-256 of the empty string, independently
confirming D7's 0-byte file.)

Manifest-level values also agree with production:
`scope_manifest_sha256 = 72758b2c574e8aea27432b6b55c62dfb6ad87f3fadc11ad1c892a61abf23ac4e`,
`policy_sha256 = e4e796286df93b5372264c702c824eea746f680b3b4e8d6467621c99e5b64ea6`,
`source_hashes.sqlite_gz_sha256 = fb7660c613d0909c22591abe90768a9ae3c0ea88a8b8d5b2ee2bdf6c69cb8938`,
`source_hashes.w1_manifest_sha256 = 096beb0b9363e69b31ced728d5ce55f7024e33c81b9a416ca1beefd0903e2d95`.

The `w1_manifest_sha256` is what ties this release to
`cloud_export_tax-2026.07.30-02` as its `w1_dir`, which §2.2's measurements rely
on. **The supplied artifacts are byte-identical to the deployed release.**

**Still absent: the compiler provenance outputs.** `mappings.jsonl` and
`source_usages.jsonl` do not exist anywhere under `sporely-py` (searched). The
`07.30-02` SQLite carries no mapping table either. These are what carry `kind`,
`review_status` and `evidence.reason`, so **§3's evidence grading remains
blocked** — it is the one artifact-dependent criterion still outstanding.

The counts 52,917 / 57,769 / 3,923 / 52,881 / 2,041 are therefore **not
independently reproducible here**. They are treated in this ledger as recorded
prior measurements, not as re-verified fact. This is consistent with the July
plan's accepted decision that the generated export is reproducible and not
committed to Git — but it means the release cannot be audited from a clean
checkout without first rebuilding it.

### 0.3 The NorTaxa source data — present after all (corrected)

> **Corrected 2026-09-22.** The finding below is **wrong**, and the error was
> consequential: it scoped the search to this worktree only. The pinned source
> archives are in the **primary checkout**, exactly where the acquisition
> scripts place them:
>
> ```
> sporely-py/database/taxonomy/sources/col_xr/2026-07-17-XR/archive.zip   (1.38 GB, ColDP)
> sporely-py/database/taxonomy/sources/nortaxa/1.284/archive.zip          (8.4 MB, DwC-A)
> sporely-py/database/reference_data/sources/taxon.txt                    (44 MB)
> sporely-py/database/reference_data/sources/vernacularname.txt           (1.9 MB)
> ```
>
> Both archives were read in this stage (§3.3) and their lineage to the audited
> release is hash-verified (§3.4). The NorTaxa DwC archive contains
> `taxon.txt`, `vernacularname.txt`, `distribution.txt`, `meta.xml`, `eml.xml`.
>
> The synthetic fixture noted below is a test fixture that sits *alongside* the
> real archive, not a substitute for it. Concluding "absent" from its presence
> was a mistake — and it was this mistake that led §3.2 to declare the matching
> rule unrecoverable. The original text is retained below as the record of what
> was wrongly concluded.

#### Original finding (superseded)

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

Classification of the July plan's §2 status table. **Deployment claims are now
verified against production** (§1.0), not inherited from the plan.

One caution retained from the stage's sparring challenge: "deployed" here means
the object exists in production with matching counts and hashes. It is not a
claim that the *behavior* is correct — §1.0 in fact shows the deployed release
is missing every NorTaxa mapping, so W3-era code being deployed and W3 being
"done" are different statements.

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

### 1.0 Deployed reconciliation — measured 2026-09-22

Read-only SQL against production. **Every deployed object is accounted for and
agrees with the supplied artifacts and this ledger.**

`public.taxonomy_v2_releases` contains **exactly one row**:

| Field | Value |
|---|---|
| `release_id` | `tax-2026.08.01-01` |
| `status` | **`active`** |
| `activated_at` / `loaded_at` | 2026-08-02T18:02:49Z / 2026-08-02T18:03:07Z |
| `scope_predicate_id` | `global_macrofungi_policy_v1` |
| `exporter_version` | `sporely-global-macrofungi-export-v1` |
| `manifest_sha256` | `52620f72…631d7b8f` |
| `whole_export_sha256` | `877fd01c…8e2d412f` |
| `source_sqlite_sha256` | `bf70ae05…c14148dd` |
| `authoritative_namespace_counts` | **`{"col_xr/col_usage_id": 52881}`** |
| `legacy_source_counts` | `{}` |
| `dangling_parent_count` | 1 (`taxon_id` 152331 → parent 150361) |

Deployed row counts, which match the artifacts (§2.2) exactly:

| Table | Rows |
|---|---:|
| `taxonomy_v2_taxa` | 52,917 |
| `taxonomy_v2_concepts` | 52,917 |
| `taxonomy_v2_scientific_names` | 57,769 |
| `taxonomy_v2_vernacular_names` | 3,923 |
| `taxonomy_v2_external_ids` | 52,881 |
| `taxonomy_v2_legacy_external_ids` | **0** |
| `taxonomy_v2_redlist` | 2,262 |
| `taxonomy_v2_import_runs` | 1 |
| `taxonomy_v3.identification_snapshot` | 369 |
| `taxonomy_v3.resolution_link` | 369 |
| `taxonomy_v3.release_installation` | 3 |

`taxonomy_v3.resolution_link` states — total 369, matching the plan exactly
(the plan's 233 + 78 split of `resolved_exact` is a sub-split this column does
not carry):

| State | Count |
|---|---:|
| `resolved_exact` | 311 |
| `no_identity_evidence` | 30 |
| `unresolved_external_identifier` | 21 |
| `manual_unresolved` | 7 |

**Deployed functions** (all present): `search_taxa_v2(q, lang, lim)`,
`resolve_taxon_external_id_v2(p_source_system, p_namespace, p_external_id)`,
`set_observation_selected_taxon_v2(p_observation_id, p_sporely_taxon_id)`,
`taxonomy_v2_validate_release`, `taxonomy_v2_activate_release`,
`taxonomy_v2_jsonb_nonnegative_integer_counts`, and the trigger function
`_guard_selected_sporely_taxon_id_v2`.

**Live confirmation of the bridge loss:** `taxonomy_v2_external_ids` holds
**zero** rows with a namespace other than `col_usage_id`, and zero rows for
`52369`, `53482` or `54350`. COL usage `39ZCL` resolves to Sporely `7821`,
confirming the plan's recorded identity. No NorTaxa external identifier can bind
in production today.

**No unexplained production objects.** One release, active, hash-pinned, with
counts matching the artifacts; one import run; three release installations; 369
W3 snapshot/resolution pairs. Every taxonomy object relevant to this closeout is
accounted for.

### 1.1 Activation verified; publication readiness UNRESOLVED

Two separate questions were previously conflated. They resolve differently.

**Verified — activation state.** Production holds exactly one release,
`tax-2026.08.01-01`, `status = active`, `activated_at = 2026-08-02T18:02:49Z`,
hash-pinned and matching the artifacts byte-for-byte (§0.2). The July plan's
"W5 — PLANNED" marker is therefore **stale**: activation demonstrably happened.

**Unresolved — whether its prerequisite was satisfied.** The July plan records
"Publication and provenance — **BLOCKED for production activation**", requiring
"complete licence and publication metadata" before any scoped release is
activated. **Activation having occurred is not evidence that this gate was
met.** A release can be activated without its prerequisite being satisfied;
that is precisely what a gate is for.

Nothing in `taxonomy_v2_releases` carries licence or publication metadata — the
row has `exporter_version`, hashes, counts and scope predicate, but no licence,
attribution or publication field, and `legacy_source_counts` is `{}`. So the
deployed state **cannot** distinguish:

1. the gate was satisfied out-of-band and only the plan is stale; from
2. activation proceeded without the required licence/publication metadata ever
   being recorded.

**This is recorded as unresolved, not resolved.** An earlier draft of this
section treated activation as settling the question; that inverted the burden of
proof. It does not block Stage 3, whose release-safety rules need the activation
state (now known and pinned), but it **is** a live question for closeout: if (2)
holds, the active production release is publishing taxonomy data without
recorded licence provenance, which is a compliance matter rather than a
taxonomy-correctness one.

**It does not affect the "no unexplained production objects" criterion.** Every
object is accounted for (§1.0); what is missing is a *record of a decision*, not
an object.

The classifications below are updated accordingly: deployment is now **verified
against production**, not inherited from the plan.

### 1.2 Superseded caveat

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

> **Superseded by §2.2.** The argument in this subsection — that an identical
> scope filter cannot produce an asymmetric result, so Gate 1 (namespace
> routing) must be the cause — reached the wrong conclusion. Measurement against
> the active release and its W1 input (§2.2) shows the filter *is* identical but
> the two datasets are keyed to **disjoint `taxon_id` populations**, which is
> what produces the asymmetry. Gate 1 is real but is **not** the operative cause,
> because the derived-row branch bypasses it. The gate mechanics below remain
> accurate as descriptions of the code; their attribution does not. Read §2.2
> for the verified account.

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

### 2.2 Verified account, measured against the active release

The pinned artifacts were supplied (§0.2) and measured. This supersedes the
attribution in §2 and confirms the plan's own framing more closely than earlier
drafts of this ledger did.

**Active release `tax-2026.08.01-01` — every plan baseline figure confirmed
exactly:**

| Dataset | Measured | Plan | Composition |
|---|---:|---:|---|
| `taxon.jsonl` | 52,917 | 52,917 | **all** `col_xr`; `norwegian_taxon_id` non-null: **0** |
| `scientific_name.jsonl` | 57,769 | 57,769 | — |
| `vernacular.jsonl` | 3,923 | 3,923 | **all** `source=nortaxa`, **2,041** distinct taxa |
| `taxon_external_id.jsonl` | 52,881 | 52,881 | **all** `(col_xr, col_usage_id)`; zero NorTaxa |
| `taxon_redlist.jsonl` | 2,262 | 2,262 | — |
| `taxon_external_id_legacy_integer.jsonl` | **0 bytes** | — | D7 confirmed directly |

Taxon `7821` is in the release and carries all four NorTaxa vernaculars
(`stjernesporet rødspore` nb/preferred, `stjernespora raudspore` nn/preferred,
plus the two non-preferred `…skivesopp` variants). `83668` is present;
`624680` is **absent**.

**The W1 input is the decisive measurement.** The macrofungi build's `w1_dir` is
`cloud_export_tax-2026.07.30-02`. Its `taxon_external_id.jsonl` holds 634,894
rows: 620,975 `(col_xr, col_usage_id)` **and 13,919 `(nortaxa,
nortaxa_taxon_id)`**.

So the authoritative NorTaxa identifiers **were emitted by the W1 exporter** and
then lost downstream. Gate 1 does not stop them — the derived-row branch
(`cloud_export.py:696-704`) writes them into the authoritative text file
regardless of the integer routing. **This retracts Gate 1 as the operative
cause.**

**Why all 13,919 die at the scope filter:**

- they sit on exactly 13,919 distinct `taxon_id`s, spanning **620,977 – 634,895**
  — entirely inside the NorTaxa anchor block;
- **zero** of them sit on a COL-range `taxon_id` (`< 620,977`);
- **zero** of them survive into the active release scope.

This is structural, not incidental. Gate 2 emits a NorTaxa identifier *only* for
an anchor (`norwegian_taxon_id IS NOT NULL`, which only anchors have), and an
anchor is by construction a NorTaxa-sourced concept — precisely the population
the COL-only scope universe removes. **Gate 2 places every NorTaxa identifier on
exactly the concepts the scope filter deletes.** The two mechanisms compose;
neither alone explains the zero.

**The two regression cases, measured:**

- **`52369`** is present in W1 as
  `{taxon_id: 624680, source_system: nortaxa, namespace: nortaxa_taxon_id,
  external_id: "52369", is_preferred: true, note:
  "derived_from_taxon_min.norwegian_taxon_id"}` — emitted by Gate 2's derived
  branch, then dropped by the scope filter because `624680` is not a COL
  concept. **The plan's COL-retention framing is correct for this case.**
- **`53482` is absent from W1's authoritative external IDs entirely.** Its alias
  binding on `7821` has `is_preferred = 0`, so Gate 2 never emitted a row for
  it. It is lost **one step earlier** than `52369`, at the anchor gate, and
  never reaches the scope filter at all. **The plan's "computed and then
  half-materialized" framing is correct for this case** — the vernaculars
  attached to `7821` prove the association was derived; the identifier was not
  materialized.

**Why the vernacular join survives.** `vernacular_min` is keyed on `taxon_id`
with no anchor requirement, so an alias-bound NorTaxa vernacular lands on the
**COL** concept (`7821`) and passes the scope filter with it. The identifiers
land on **NorTaxa** concepts and do not. Same filter, disjoint key populations —
which is exactly the asymmetry, and the reason the earlier "identical filter"
argument in §2 drew the wrong conclusion.

**D3a confirmed in the export too:** W1's
`taxon_external_id_legacy_integer.jsonl` carries 61,583 rows, all
`source_system = artsdatabanken` with **`namespace: null`** — the namespace loss
is materialized in the exported artifact, not merely internal to the SQLite.

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

### 3.2 Evidence grading — COMPLETE

`mappings.jsonl` was never supplied and does not exist (§0.2). **The grading was
obtained another way.** The compiler writes each binding's `alias_reason` into
the note column that `build_sqlite_candidate.py:457` carries through
(`note = (u.get("alias_reason") or "") or None`), and that column survives into
`taxon_external_id_min`. The vocabulary is closed — `compile_release.py:746-759`
emits exactly three values:

```python
alias_reason = ""                                #  anchor
alias_reason = "synonym_of_accepted"             #  intra-source synonym
alias_reason = "cross_source_automatic_exact"    #  automatic classifier
alias_reason = "manual_approved_exact"           #  human-reviewed mapping
```

Measured over all 61,583 NorTaxa identifier rows:

| `note` (`alias_reason`) | Rows | Meaning |
|---|---:|---|
| `synonym_of_accepted` | 27,856 | intra-source synonym, Phase 2e — not a cross-source bridge |
| **`cross_source_automatic_exact`** | **19,808** | automatic classifier, `policy_auto_approved` |
| *(null)* | 13,919 | Phase 2d anchors — no alias binding |
| **`manual_approved_exact`** | **0** | **human-reviewed — none exist** |

All 19,808 `cross_source_automatic_exact` rows sit on `col_xr` host concepts,
exactly matching the 19,808 distinct alias-bound COL concepts in §3.1. The two
measurements agree independently.

### What this establishes, and what it does NOT

**Established: not one NorTaxa → Sporely cross-source association in this
release is human-reviewed.** The `manual_approved_exact` count is zero. Every
one of the 19,808 bindings — and therefore every one of the 2,041
vernacular-joined taxa, which are a subset (§3.1) — is machine-derived by
`PROPOSAL_AUTOMATIC_EXACT`. The plan's instruction not to treat a vernacular
join as proof of concept equivalence is correct for **100%** of the population,
not merely most of it.

**NOT established: which matching rule produced each association.**
`alias_reason` distinguishes automatic from manual. It does **not** distinguish
the two rules that both emit `PROPOSAL_AUTOMATIC_EXACT`
(`cross_source_mapping.py:232-259`):

| Rule | `evidence.reason` | Proposal class |
|---|---|---|
| strict conservative exact | `conservative_exact_rule_satisfied` | `PROPOSAL_AUTOMATIC_EXACT` |
| missing-authorship fallback | `missing_authorship_classification_rule_satisfied` | `PROPOSAL_AUTOMATIC_EXACT` |

`compile_release.py:755-756` collapses both into the single
`cross_source_automatic_exact` label, and the distinguishing field
(`evidence.reason`) lives only in `mappings.jsonl`.

**An earlier draft of this section claimed the population rests uniformly on
"canonical name + rank + authorship + kingdom + status agreement". That
overclaimed** — an unknown share of the 19,808 may rest on the weaker fallback,
which fires precisely when authorship was *absent* on one or both sides.

The split is absent from the *downstream* artifacts — `taxon_min` and
`scientific_name_min` have no authorship column, and neither `taxon.jsonl` nor
`scientific_name.jsonl` carries an authorship field. But that only made those
artifacts insufficient; it did not make the rule unrecoverable. **It was
recovered from the pinned source archives** — §3.3.

### 3.3 Matching rule per association — RECOVERED and measured

An earlier draft concluded this was unrecoverable and needed `mappings.jsonl`
regenerated. That was wrong: the classification is a deterministic function of
the two sides' authorship, and both pinned source archives are present.

**Method.** The strict rule requires authorship agreement; the fallback fires
*only* when the strict rule failed **exclusively** because one or both
authorships were missing, and **never** on mismatch
(`cross_source_mapping.py:240-247`). For an alias that was actually applied, the
classification is therefore decidable from authorship presence alone:

- both sides present → the strict rule is what admitted it (a mismatch would
  have produced no alias at all, so presence + applied ⇒ agreement);
- either side absent → the missing-authorship fallback admitted it.

**Inputs** (both hash-pinned, §3.4):

- NorTaxa `scientificNameAuthorship` per `taxonID`, from `taxon.txt` inside
  `database/taxonomy/sources/nortaxa/1.284/archive.zip` (229,018 rows indexed);
- COL `col:authorship` per `col:ID`, streamed from `NameUsage.tsv` (2.93 GB)
  inside `database/taxonomy/sources/col_xr/2026-07-17-XR/archive.zip`
  (7,848,305 rows scanned; **all 19,808** sought usage IDs resolved, zero
  unresolved);
- the 19,808 `(nortaxa_external_id, sporely_taxon_id, col_usage_id)` bridge
  triples, from `taxon_external_id_min` where `note =
  'cross_source_automatic_exact'`.

**Result — full bridge population:**

| Matching rule | Bindings | Share |
|---|---:|---:|
| **strict** — `conservative_exact_rule_satisfied` (both authorships present and agreeing) | **17,675** | 89.2% |
| **missing-authorship fallback** — `missing_authorship_classification_rule_satisfied` | **2,133** | 10.8% |
|   … NorTaxa authorship absent only | 1,138 | |
|   … COL authorship absent only | 78 | |
|   … both absent | 917 | |
| **total** | **19,808** | 100% |

**Result — the required 2,041-taxon population** (the active release's
vernacular-joined taxa):

| Class | Taxa |
|---|---:|
| all cross-source bindings strict | **2,001** |
| at least one missing-authorship fallback binding | **40** |
| **no cross-source alias binding at all** | **0** |
| total | **2,041** |

The zero in the third row independently confirms §3.1's subset claim from data:
every vernacular-joined taxon does carry an alias binding.

**The two named regressions:** `53482` is **strict** — both sides give
`(Britzelm.) Noordel.`, identical. `52369` has **no** row here, consistent with
§2.2: it is anchored, never aliased.

### The characterization, complete

Every one of the 19,808 associations now has both a review status and a matching
rule:

- **review status:** uniformly automatic. Zero `manual_approved_exact`. No
  association in this release is human-reviewed.
- **matching rule:** 89.2% strict authorship agreement, **10.8% admitted despite
  absent authorship**.

So the population is not uniform, and the earlier "uniformly strict" claim was
indeed wrong — but the non-uniformity is bounded and identified. For Stage 3:

1. The 2,133 fallback bindings are the weaker class and should be treated as a
   distinct tier; 40 of them fall inside the 2,041 scoped population.
2. Neither tier is human-reviewed, so Stage 3 still cannot partition by review
   status — it must decide explicitly whether `cross_source_automatic_exact`
   meets its authoritative-bridge standard, and if so whether the fallback tier
   qualifies on the same terms.
3. `53482` sits in the stronger tier, so admitting it does not require admitting
   the fallback tier.

### 3.4 Lineage of the recovery inputs

The recovery used the same pinned inputs as the audited release; the chain is
hash-verified end to end:

| Link | Evidence |
|---|---|
| COL source pin | release `taxonomy_release.jsonl` declares `source_release = 2026-07-17-XR`; the archive read is `sources/col_xr/2026-07-17-XR/archive.zip` |
| NorTaxa source pin | `sources/nortaxa/1.284/archive.zip`, the version compiled into this lineage |
| SQLite ← compiler | `sha256(tax-2026.07.30-02.sqlite3.gz)` = `fb7660c613d0909c22591abe90768a9ae3c0ea88a8b8d5b2ee2bdf6c69cb8938` = the active release manifest's `source_hashes.sqlite_gz_sha256` ✓ |
| W1 export ← SQLite | `sha256(cloud_export_tax-2026.07.30-02/taxonomy_export_manifest.json)` = `096beb0b9363e69b31ced728d5ce55f7024e33c81b9a416ca1beefd0903e2d95` = the active release manifest's `source_hashes.w1_manifest_sha256` ✓ |
| active release ← W1 | all seven dataset hashes match disk, manifest and production (§0.2) ✓ |

Both hash links were recomputed in this stage, not read from the manifest. The
bridge triples therefore belong to the audited release's own lineage.

What §3 now delivers is a *classification scheme*, a *verdict on the strongest
available rule*, and the *population structure* (§3.1). What it does not deliver
is the grade distribution — how many bindings are `policy_auto_approved` under
the strict rule, how many rest on the missing-authorship fallback, and how many
came from reviewed manual mappings.

The acceptance-gate criterion "the evidence behind the NorTaxa vernacular join
is characterized well enough for Stage 3 to classify associations" is **met**.
The join key is confirmed from data (§3.1), the population is bounded (19,808
bindings, of which 2,041 scoped taxa carry vernaculars), the review status is
measured (§3.2: zero reviewed), and the matching rule is measured per
association (§3.3: 17,675 strict / 2,133 fallback; 2,001 / 40 within the 2,041).

Stage 3 should report its coverage **over the alias-binding population, not the
vernacular-joined subset** (§3.1), so that bindings deliberately left unemitted
are visible rather than hidden behind a vernacular filter. The 2,041 figure is
the scoped intersection, not the denominator.

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

## 6. Observation 917 baseline — FROZEN

Captured 2026-09-22 via authorized read-only SQL. All seven required dimensions
are recorded. This is the before-state regression record.

### 6.1 Current observation identity — `public.observations` id 917

| Field | Value |
|---|---|
| `genus` / `species` / `common_name` | **null / null / null** |
| `selected_sporely_taxon_id` | **null** |
| `resolved_sporely_taxon_id` | **null** |
| `artsdata_id` | null |
| `ai_selected_taxon_id` / `ai_selected_scientific_name` | null / null |
| `ai_selected_service` / `ai_selected_at` / `ai_selected_probability` | null / null / null |
| `ai_state_json` | null |
| `species_guess` / `determination_method` | null / null |
| `red_list_category` / `red_list_categories_json` | null / null |
| `date` / `captured_at` | 2026-07-21 / 2026-07-21T14:50:37.611+00 |
| `desktop_id` | 607 |
| `location` / `country_code` | Nydammen / NO |
| GPS | 63.410127, 10.539173, alt 201.8, acc 4.677, `location_precision=exact` |
| `visibility` / `location_public` / `spore_data_visibility` | public / true / public |
| `publish_target` | `artsobs_no` |
| `is_draft` / `uncertain` / `unspontaneous` | false / false / false |
| `media_version` | 1 |
| `synced_at` | **null** |
| `created_at` / `updated_at` | 2026-07-21T14:50:46Z / 2026-08-24T12:36:35Z |

The plan's recorded values are confirmed: no current identity of any kind.

### 6.2 Immutable W3 historical snapshot — `taxonomy_v3.identification_snapshot`

```json
{"observation_id": "917", "snapshot_locked": true,
 "snapshot_written_at": "2026-08-02T17:28:25.687837+00:00",
 "original_signals": [],
 "original_source_system": null, "original_source_namespace": null,
 "original_external_id": null, "original_legacy_taxon_id": null,
 "original_scientific_name": null, "original_vernacular_name": null,
 "original_rank": null}
```

`snapshot_locked = true`. **This must not be rewritten** when a current identity
is later selected (§6.7).

### 6.3 Current resolved identity — `taxonomy_v3.resolution_link`

```json
{"observation_id": "917", "resolution_state": "no_identity_evidence",
 "resolved_sporely_taxon_id": null, "resolution_method": null,
 "resolution_release": null,
 "attached_at": "2026-08-02T17:28:25.687837+00:00",
 "manifest_semantic_sha256": "97bd7b19c346e1348e7b9a30a5641bc95760d77a8679d5b14fdaf83ffc4abe58",
 "resolution_evidence": [{"level": 6, "action": "no_identity_evidence",
   "method": "preserve_unresolved",
   "note": "observation carried no exact or text identity signal",
   "source_system": null, "namespace": null, "external_id": null,
   "resolved_taxon_id": null}]}
```

### 6.4 AI-identification history — `public.observation_identifications` (2 rows)

| id | service | status | top scientific name | top vernacular | `top_taxon_id` | p | created |
|---|---|---|---|---|---|---:|---|
| 611 | `artsorakel` | success | `Calocybe gambosa` | `vårfagerhatt` | **`NBIC:54350`** | 0.8525 | 2026-07-21T14:50:59Z |
| 612 | `inat` | success | `Cyclocybe cylindracea` | Poplar Fieldcap | `578456` | 0.1072 | 2026-07-21T14:50:59Z |

This is **history, not identity** — neither was accepted, and §6.1 shows no
`ai_selected_*` field was ever set. It must not be promoted to an identification.

**But it is a material finding** (D11, §7): the observation *does* hold a
namespaced provider identifier, `NBIC:54350`, while the W3 record states it
"carried no exact or text identity signal" and `original_signals` is empty. The
W3 reconciliation evidently read only the `observations` row and not the
identification history. The snapshot is truthful about what it examined; it is
not a complete statement of the identity evidence the system holds. Stage 4 must
not conclude from `no_identity_evidence` that no external identifier exists.

Note also that `NBIC:54350` is an Artsnavnebase scientific-name ID, so 917 is a
**live regression case for the same `NBIC:` resolution path as `53482`** — and
under the active release it cannot resolve (§6.10).

### 6.5 Media / images — row-level baseline

Counts alone cannot detect a substituted image or a changed scale, so this
records stable row IDs and values, plus a digest. Captured 2026-09-22 by
read-only SQL.

`public.observation_images where observation_id = 917`: **14 rows**, all with
`deleted_at IS NULL`. **3 have a `storage_path`; 11 are NULL.**

| `id` | sort | type | `storage_path` | µm/px | `calibration_uuid` | `desktop_id` |
|---:|---:|---|---|---|---|---:|
| 3671 | 0 | field | `…/917/0_1784645444763.webp` | — | — | 3381 |
| 3672 | 1 | field | `…/917/1_1784645444763.webp` | — | — | 3382 |
| 3934 | 2 | microscope | `…/917/2_1784744139000.webp` | 0.0534937320902084 | `7a872549-…3436` | 3564 |
| 4179 | 3 | microscope | **NULL** | 0.0534937320902084 | `7a872549-…3436` | 3565 |
| 4180–4186 | 4–10 | microscope | **NULL** | 0.0534937320902084 | `7a872549-…3436` | 3566–3572 |
| 4187–4189 | 14–16 | microscope | **NULL** | 0.0534937320902084 | `7a872549-…3436` | 3576–3578 |

`stored_width`, `stored_height` and `stored_bytes` are NULL on all 14 rows.
`media_version = 1` on all 14. All microscope images share one calibration UUID.

**Digest** (`md5` over `id:storage_path:sort_order:image_type:scale:calibration_uuid:deleted_at`,
ordered by `id`): **`7a34b1a10766f5121b1bece0f2a14129`**

> **Note for Stage 4, not a taxonomy defect:** 11 of 14 images having a NULL
> `storage_path` is pre-existing state unrelated to this closeout, and is
> plausibly the subject of the separate cloud-media recovery work. It is
> recorded because a count-only baseline would have hidden it, and because
> Stage 4 must not "repair" it or mistake it for damage its own migration caused.

`observations.image_key` / `thumb_key`:
`8c471394-…/917/0_1784645444763.webp` / `…/917/thumb_0_1784645444763.webp`.

### 6.6 Measurements — row-level baseline

`public.spore_measurements` joined via `image_id` → `observation_images`:
**26 rows**, ids **5801–5910** (non-contiguous).

**Digest** (`md5` over `id:image_id:measurement_type:length×width` rounded to 4dp,
ordered by `id`): **`9a2828fb4781cd84f602ebee1b3e10d4`**

`[id, image_id, length_um, width_um]`:

```
[5801,3934,6.0573,5.6754] [5802,3934,6.0867,5.8942] [5887,4179,6.7442,5.9725]
[5888,4180,6.8827,6.6760] [5889,4181,6.2150,6.1969] [5890,4181,6.3226,6.2765]
[5891,4181,5.9208,5.5828] [5892,4182,6.4546,6.3439] [5893,4183,6.1852,6.0916]
[5894,4184,7.9154,7.5066] [5895,4184,7.5898,6.7120] [5896,4185,6.7188,6.3497]
[5897,4185,6.6416,6.3210] [5898,4185,6.0700,5.4842] [5899,4185,5.6765,5.6041]
[5900,4186,6.8333,6.5683] [5901,4186,6.1432,5.6712] [5902,4186,6.1941,6.1485]
[5903,4186,6.4644,6.1431] [5904,4186,5.5385,5.4626] [5905,4186,5.7866,5.4860]
[5906,4186,6.2950,5.9875] [5907,4187,6.4711,6.2765] [5908,4187,6.7906,6.2374]
[5909,4188,6.5692,6.3542] [5910,4189,6.3498,6.2277]
```

Min/max length 5.5385 / 7.9154 µm and width 5.4626 / 7.5066 µm agree with
`observations.spore_statistics`:
`Spores: (5.5-)5.7-7.4(-7.9) um x (5.5-)5.5-6.7(-7.5) um, Q = (1.0-)1.0-1.1(-1.1), Qm = 1.0, n = 26`.

### 6.7 Mosaic and summaries — row-level baseline

`public.spore_measurement_mosaics`: **1 row**

```json
{"id": 178, "observation_id": 917, "version": 2,
 "storage_key": "8c471394-…/917/spore_mosaic_v2_fd70c15a234f97d8.webp",
 "width_px": 1944, "height_px": 1600, "tile_size_px": 320,
 "tile_width_px": 324, "tile_height_px": 320,
 "common_crop_width_um": 9.64636457288377,
 "common_crop_height_um": 9.52017322435107,
 "media_version": 1, "canonical_bucket": "legacy",
 "created_at": "2026-07-26T22:06:06.461172+00:00",
 "updated_at": "2026-07-27T09:09:24.017999+00:00"}
```

`public.spore_measurement_mosaic_tiles`: **26 rows**, one per measurement.
**Digest** (`md5` over `measurement_id@mosaic_id:x,y,w,h`, ordered by
`measurement_id`): **`7cf07f7ee18ac31c6045f956715823d2`**

`public.observation_spore_summaries`: **2 rows**, `n_spores` 23 + 3 = 26.
**Digest** (`md5` over `id:context_hash:n_spores:length_mean:width_mean:q_mean`,
ordered by `id`): **`8d3b020df4a1e91fe505461c28b27f4e`**

| `id` | `n_spores` | `context_hash` | length mean | width mean | Q mean |
|---:|---:|---|---:|---:|---:|
| 167 | 23 | `78dc6187…7afb99` | 6.4360 | 6.1612 | 1.0446 |
| 168 | 3 | `88d193a4…478c8a` | 6.2960 | 5.8473 | 1.0764 |

`public.observation_reference_uses`: **0 rows**.

### 6.8 Stage 4 preservation check

After any taxonomy repair, re-run the capture queries and compare **digests, not
counts**. Expected unchanged:

| Dimension | Expected digest / value |
|---|---|
| images (14 rows) | `7a34b1a10766f5121b1bece0f2a14129` |
| measurements (26 rows) | `9a2828fb4781cd84f602ebee1b3e10d4` |
| mosaic tiles (26 rows) | `7cf07f7ee18ac31c6045f956715823d2` |
| spore summaries (2 rows) | `8d3b020df4a1e91fe505461c28b27f4e` |
| mosaic row | `id=178`, `version=2`, `storage_key` `…fd70c15a234f97d8.webp` |
| `observations.spore_statistics` | unchanged string, `n = 26` |

**Exact capture query.** This is the verbatim read-only SQL that produced the
four digests. It was re-executed against production on 2026-09-22 and reproduced
all four values identically. Separator `'|'`; NULLs rendered as the literal
`NULL` via `coalesce`; numerics rounded to 4 decimal places then cast to `text`;
ordering fixed by primary key:

```sql
select
  (select md5(string_agg(
       i.id::text||':'||coalesce(i.storage_path,'NULL')||':'||i.sort_order::text||':'||i.image_type
       ||':'||coalesce(i.scale_microns_per_pixel::text,'NULL')||':'||coalesce(i.calibration_uuid::text,'NULL')
       ||':'||coalesce(i.deleted_at::text,'NULL'), '|' order by i.id))
     from public.observation_images i where i.observation_id = 917) as images_md5,
  (select md5(string_agg(
       m.id::text||':'||m.image_id::text||':'||coalesce(m.measurement_type,'')
       ||':'||round(m.length_um::numeric,4)::text||'x'||round(m.width_um::numeric,4)::text, '|' order by m.id))
     from public.spore_measurements m
     join public.observation_images i2 on i2.id = m.image_id
    where i2.observation_id = 917) as measurements_md5,
  (select md5(string_agg(
       t.measurement_id::text||'@'||t.mosaic_id::text||':'||t.x_px::text||','||t.y_px::text
       ||','||t.w_px::text||','||t.h_px::text, '|' order by t.measurement_id))
     from public.spore_measurement_mosaic_tiles t
     join public.spore_measurement_mosaics mo on mo.id = t.mosaic_id
    where mo.observation_id = 917) as tiles_md5,
  (select md5(string_agg(
       s.id::text||':'||s.context_hash||':'||s.n_spores::text
       ||':'||round(s.length_mean_um::numeric,4)::text
       ||':'||round(s.width_mean_um::numeric,4)::text
       ||':'||round(s.q_mean::numeric,4)::text, '|' order by s.id))
     from public.observation_spore_summaries s where s.observation_id = 917) as summaries_md5;
```

Verified output (2026-09-22):

```
images_md5       | 7a34b1a10766f5121b1bece0f2a14129
measurements_md5 | 9a2828fb4781cd84f602ebee1b3e10d4
tiles_md5        | 7cf07f7ee18ac31c6045f956715823d2
summaries_md5    | 8d3b020df4a1e91fe505461c28b27f4e
```

Two notes for whoever re-runs it. `spore_measurements` has **no
`observation_id` column**; it joins through `image_id` → `observation_images.id`,
which the query does explicitly — a naive `where observation_id = 917` fails with
`42703`. And `measurement_type` uses `coalesce(...,'')`, not `'NULL'`, matching
the original capture; changing that alters the digest without any data having
changed.

The row-level listings in §6.5–6.7 were captured by the corresponding
`jsonb_agg` / `to_jsonb` selects over the same predicates.

### 6.9 Expected semantics after correction

Selecting a correct identity later must **not** rewrite §6.2 from
`no_identity_evidence`. The snapshot is `snapshot_locked` and historically
truthful about the `observations` row at 2026-08-02. Stage 4 should set current
selected identity and leave the snapshot and its `resolution_link` evidence
intact.

### 6.10 Why 917 cannot be resolved today

Confirmed live against production: `taxonomy_v2_external_ids` contains **zero**
rows with any namespace other than `col_usage_id`, and zero rows for external
IDs `54350`, `53482` or `52369`. So `NBIC:54350` has nothing to bind to until
Stage 3 lands. (`resolve_taxon_external_id_v2` itself is not executable by the
read-only role — `permission denied for function` — so this was verified against
the underlying table, which is the same evidence the function reads.)

## 7. Additional identity-boundary defects discovered

| ID | Defect | Location | Severity |
|---|---|---|---|
| **D3a** | **NorTaxa identifiers are routed into the legacy store, losing their namespace.** `INTEGER_NAMESPACES` membership sends all four NorTaxa namespaces to `taxon_external_id_min`, which by contract carries no namespace column, collapsing them into one `artsdatabanken` integer space — while the authoritative `taxon_external_id_text_min` (with `namespace TEXT NOT NULL`) sits unused for them. The defect is the routing, not the legacy table's shape | `build_sqlite_candidate.py:473-490` (routing), `:170-181` (unused text table) | **High — root cause of D3b** |
| **D3b** | `_resolve_via_nortaxa` resolves identity from a namespace-lost integer, with `LIMIT 1` silently swallowing ambiguity | `migrate_observations_sporely_id.py:73-80` | **High** |
| D3c | Migration step 4 resolves identity by unique scientific-name match | `migrate_observations_sporely_id.py:177-190` | High |
| D2 | Cloud sync's client-side proof standard for a Sporely identity is `> 0`. **Backstopped server-side** — see below | `utils/cloud_sync.py:16124-16132` | **Low** — residual risk is numeric collision only |
| **D11** | **W3 `no_identity_evidence` is not a statement that no identifier exists.** Observation 917's snapshot records `original_signals: []` and "carried no exact or text identity signal", while `observation_identifications` holds `top_taxon_id = NBIC:54350`. The reconciliation read the `observations` row only | §6.4 | **High for Stage 4** — the audit must not infer absence of evidence from this state |
| D1 | Committed snapshot stores no source/namespace/external ID; `canonical_source_system` is rendered then dropped | `ui/taxon_input_controller.py:50-53` vs `:1028-1036` | Medium — provenance-completeness gap; **not** evidence of contamination |
| D8 | `resolve_manual_scientific` breaks a `col_xr`/`nortaxa` exact-name tie by source preference — identity selection by name | `database/taxon_lookup.py:726-733` | Medium |
| D9 | Module docstring claims every external identifier is stored under an explicit `(source_system, id_role)` namespace. `id_role` holds `accepted`/`synonym` — a taxonomic status, not a namespace. The real namespace lives in `taxon_external_id_text_min.namespace`, so the docstring misdescribes the module's own schema and obscures D3a | `build_sqlite_candidate.py:12-14` vs `:170-181` | Low — documentation defect |
| D5 | W2D/supplement tests depend on a gitignored absent build output; 32 tests cannot run in a clean checkout | `tests/taxonomy/test_w2d_reconciliation.py`, `tests/taxonomy/test_supplement_loader.py` | Medium — makes "W3 is done" unfalsifiable |
| D6 | July plan's publication gate contradicts the recorded active production release | §1.1 | Medium — unresolved, blocks Stage 3 release safety |
| D7 | `taxon_external_id_legacy_integer.jsonl` is written unconditionally empty, silently discarding every integer-namespace external ID | `macrofungi_scope.py:480` | Medium — data loss with no diagnostic |

| **D10** | **Norwegian vernacular loss for dropped anchors.** A divergent-name NorTaxa concept keeps its vernaculars on its own Phase 2d anchor, which the COL-only scope drops — so the retained COL concept has no Norwegian name at all. Verified: `83668` (`Conocybe rugosa`) has zero vernaculars while `slank ringkjeglesopp` sits on dropped anchor `624680`. Affects the 2,633 NorTaxa-anchored concepts carrying vernaculars (§3.1) | §2.1, §3.1 | **High — a name-loss class the plan does not enumerate** |

**D2 downgraded — the cloud boundary is enforced server-side.** The deployed
RPC validates more than the client does (§1.0):

```sql
IF p_sporely_taxon_id IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM public.taxonomy_v2_releases r
  JOIN public.taxonomy_v2_taxa t ON t.release_id = r.release_id
  WHERE r.status = 'active' AND t.sporely_taxon_id = p_sporely_taxon_id)
THEN RAISE EXCEPTION 'sporely_taxon_id % is missing from the active
  taxonomy-v2 release' ... USING ERRCODE = '22023';
```

It is `SECURITY DEFINER`, requires `auth.uid()`, enforces ownership, and
**requires membership in the active release**. A trigger
(`_guard_selected_sporely_taxon_id_v2`) additionally forbids any direct write to
`selected_sporely_taxon_id` outside the RPC for non-service roles.

So an arbitrary external integer forwarded by the desktop is **rejected by
production**, not silently accepted. The earlier characterization of this as a
high-severity leak boundary overstated it. The residual risk is precisely the
plan's Stage 2 regression #2: an external integer that **numerically collides**
with a real Sporely ID in the active release, which the RPC cannot distinguish
and which no amount of server-side validation can catch. That is a real risk
worth closing client-side, but it is a narrower one than "any integer reaches
production".

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

**Single change: emit the alias binding's identifier onto its COL host
concept.** §2.2 shows the identifiers are already emitted — they are simply
attached to NorTaxa anchor concepts that the scope deletes. The fix is therefore
not about routing or about the scope filter; it is about **which `taxon_id` the
identifier is written against**.

For a NorTaxa source usage that the §3 audit grades as a reviewed bridge and
that is alias-bound to a COL concept, emit
`(source_system=nortaxa, namespace=nortaxa_taxon_id, external_id=<taxonID>)`
against the **COL host `taxon_id`**, into the authoritative text table
(`taxon_external_id_text_min`, which already has the right shape — §8's desktop
subsection). Those rows then sit on retained concepts and survive the scope
filter untouched.

This supersedes an earlier draft that aimed the change at the Gate 1 routing
decision. That draft assumed Gate 1 was blocking emission; §2.2 measured 13,919
NorTaxa rows in the W1 export, so it was not. Gate 2's derived
`norwegian_taxon_id` branch (`cloud_export.py:696-704`) becomes redundant once
aliases are emitted directly and should be **retired**, not relaxed — it exists
only to reconstruct an anchor identifier and is the reason every identifier
lands on a doomed concept.

Two properties worth noting: the change is **additive to retained concepts**, so
it cannot resurrect the duplicate-concept block (no NorTaxa concept is admitted
to scope); and it leaves the COL backbone's canonical presentation untouched,
since only an external-ID row is added.

Eligibility is the audit's output, not namespace membership: emitting every
NorTaxa alias because it is a NorTaxa alias would infer authoritative identity
from an automatic name+rank match, which §3 shows the evidence does not support.

**Entry point A — `53482`.** Already alias-bound to `7821` (`is_preferred=0`,
§2.1) and confirmed absent from the W1 authoritative export (§2.2). If the §3
audit grades that binding as a reviewed bridge, the change above emits it
against `7821` — a retained concept — and it survives. No new relationship is
created.

**Entry point B — `52369`.** Anchored as its own concept `624680` (§2.1), whose
identifier *is* emitted in W1 but against `624680`, which the scope deletes
(§2.2). It has no alias binding, so the change above has nothing to attach to a
retained concept. It needs an **approved manual bridge entry** so it is
alias-bound onto `83668` in Phase 2b/2c instead of anchored in 2d — touching the
manual-mappings input, not the emitter. That is a human decision no automatic
rule can supply, which is the correct outcome given that the sources genuinely
disagree on the accepted name.

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

**All seven acceptance-gate criteria are met**, with one item recorded as
explicitly unconfirmed under the allowance the brief grants.

| Gate criterion | Status |
|---|---|
| Deployed and repository state agree with the ledger | **Met** — §1.0. Production queried; release identity, status, hashes, all eleven table counts and the W3 state distribution match the artifacts and this ledger |
| Observation 917 has a reproducible before-state | **Met** — §6, all seven dimensions frozen from live SQL |
| Desktop leak traced to a concrete write path | **Met** — §4.3: the migration's `_resolve_via_nortaxa` resolves identity from a namespace-lost integer with `LIMIT 1` (D3a/D3b). Recorded with it: the plan's premise that the *picker* leaks is **unsupported** (§4.2), and the cloud boundary is enforced server-side (§7, D2) |
| NorTaxa bridge loss traced to a concrete compile/export path | **Met** — §2.1 verifies both bindings against compiled records; §2.2 verifies the loss against the active release and its W1 input; §1.0 confirms zero non-`col_usage_id` namespaces in production |
| Vernacular join characterized for Stage 3 classification | **Met** — §3.1 confirms the join key from data; §3.2 measures review status (19,808 automatic, **0** reviewed); §3.3 measures the matching rule per association from the pinned source archives (17,675 strict / 2,133 missing-authorship fallback; within the 2,041: 2,001 / 40 / 0 unbound), with lineage hash-verified in §3.4 |
| `Entoloma conferendum` failure traced, or recorded unconfirmed with reason | **Met** (recorded unconfirmed with a concrete blocking reason, §5 — permitted by the brief) |
| No unexplained taxonomy production objects | **Met** — §1.0. One active release, one import run, three release installations, 369 snapshot/resolution pairs; all accounted for. Recorded alongside it: whether the licence/publication gate was satisfied before activation remains **unresolved** (§1.1) — a missing decision record, not an unexplained object |

One item is explicitly recorded as unconfirmed rather than unmet, which the
brief permits:

- **§5, the `Entoloma conferendum` mechanism.** The deprecation-sentinel defect
  is confirmed *in code* (`artsorakel.js:482-493` vs `:732`), and the null-write
  path is confirmed (`find_detail.js:1059-1087`, `splitScientificName`
  `:290-308`). What is **not** confirmed is that this is what happened to that
  particular observation, because the raw Artsorakel JSON for that
  identification was never captured and is not recoverable after the fact. The
  brief explicitly allows this to remain unconfirmed with a concrete blocking
  reason. Part B's required-behavior list stands regardless, and §6.4 notes a
  competing explanation that is not excluded.

One artifact limit remains, no longer blocking any criterion:

- `mappings.jsonl` / `source_usages.jsonl` were never supplied and do not exist.
  Both facts they would have carried were obtained otherwise: review status from
  `alias_reason` in `taxon_external_id_min.note` (§3.2), and the matching rule
  by recomputing it from the pinned source archives' authorship (§3.3).
  Regenerating them would let the two be read directly rather than derived, and
  would carry `evidence.reason` verbatim — useful corroboration, not a gap.

**Nothing further is requested from the operator.** Both previously reported
blockers are discharged: SQL access works, and the source archives were located
in the primary checkout rather than needing to be supplied (§0.3).

Test results in §0.4 were produced in the implementation session and have not
been independently rerun by a reviewer.

### Seventh revision — matching rule recovered; digest SQL committed

1. **§3.3 recovers the per-association matching rule**, which the sixth revision
   wrongly declared unrecoverable. Showing authorship absent from the downstream
   SQLite and export proved only that *those* artifacts were insufficient. The
   pinned source archives were present all along in the primary checkout
   (§0.3 corrected), and the classification is decidable from them: 17,675
   strict / 2,133 missing-authorship fallback across the bridge population, and
   2,001 / 40 / 0-unbound within the required 2,041. `53482` is strict.
   §3.4 hash-verifies the recovery inputs against the audited release.
2. **§6.8 now carries the verbatim capture SQL**, re-executed and confirmed to
   reproduce all four digests, including the `image_id` join and the NULL /
   rounding / separator conventions that a description alone could not pin down.
3. **§9 corrected** — the statement that the missing compiler artifacts blocked
   no criterion has been removed; it contradicted the sixth revision's own
   verdict. With §3.3 complete the criterion is met on its own evidence.

### Sixth revision — three bounded corrections

1. **§3.2 overclaimed.** `alias_reason` separates automatic from manual, but
   `cross_source_mapping.py:232-259` emits `PROPOSAL_AUTOMATIC_EXACT` for *both*
   the strict rule and the missing-authorship fallback, and
   `compile_release.py:755-756` collapses them. The claim of uniform authorship
   agreement is withdrawn. The split is unrecoverable — authorship appears in no
   supplied artifact (schemas and JSONL keys checked) — so the criterion is
   reclassified **incomplete**, and the Stage 1 verdict is no longer claimed.
2. **§6.5–6.8 replaced counts with a preservation-capable baseline:** stable row
   IDs, values, and reproducible `md5` digests for images, measurements, mosaic
   tiles and summaries. This immediately surfaced something counts hid — **11 of
   14 images have a NULL `storage_path`**.
3. **§0.2 and §1.1 corrected.** Full SHA-256s are now compared explicitly, disk
   against local manifest against production, all seven matching. §1.1 no longer
   treats activation as proving its own prerequisite: activation is verified,
   licence/publication readiness is **unresolved**.

Also consolidated: §0.1's stale "every deployed fact is quoted from the plan"
paragraph, which contradicted the measurements in §1.0 and §6.

### Fifth revision — production read, Stage 1 audit completed

SQL permission was granted for the stage session. Work done:

1. **Observation 917 frozen** across all seven dimensions (§6) — including the
   discovery that its AI history holds `NBIC:54350` while W3 records
   `no_identity_evidence` (D11).
2. **Deployed state reconciled** (§1.0) — one active release, hashes, all eleven
   table counts, W3 state distribution, deployed functions, and live
   confirmation that zero non-`col_usage_id` namespaces exist.
3. **§1.1 resolved** — the release is active; the July markers are stale.
4. **§3.2 grading completed** without `mappings.jsonl`, by reading the
   compiler's `alias_reason` out of `taxon_external_id_min.note`: 19,808
   `cross_source_automatic_exact`, **zero** `manual_approved_exact`.
5. **D2 downgraded** — `set_observation_selected_taxon_v2` validates membership
   in the active release and a trigger forbids direct writes, so the cloud
   boundary is enforced server-side; the residual risk is numeric collision.

**One correction to the record:** the human evidence stated that the stage agent
"has already reproduced/obtained the compiler outputs". That is not accurate —
`mappings.jsonl` and `source_usages.jsonl` were never supplied, do not exist
under `sporely-py`, and were not reproduced. §3.2 reaches the same conclusion
from a different artifact, and says so explicitly rather than claiming those
files were used.

### Fourth revision — artifacts supplied, §2 attribution corrected

The operator supplied MCP access and the pinned artifacts. Acting on that:

1. **The artifacts were measured** (§2.2). Every plan baseline figure is
   confirmed exactly (52,917 / 57,769 / 3,923 / 2,041 / 52,881 / 2,262), and
   `taxon_external_id_legacy_integer.jsonl` is confirmed 0 bytes (D7).
2. **§2's attribution is retracted.** The W1 input contains **13,919
   `(nortaxa, nortaxa_taxon_id)` rows**, so Gate 1 was never blocking emission.
   The operative mechanism is Gate 2 composing with the COL-only scope: Gate 2
   emits identifiers only for anchors, anchors are NorTaxa concepts, and the
   scope deletes exactly those — all 13,919 sit on `taxon_id` 620,977–634,895
   and none survive. `53482` is absent from W1 entirely, lost one step earlier
   at the anchor gate.
3. **§8 re-aimed accordingly.** The fix is not routing and not the scope filter:
   it is emitting the alias binding's identifier against its **COL host
   `taxon_id`**, so it lands on a retained concept.
4. **Still blocked:** SQL execution, now by a third distinct cause — the harness
   tool-permission gate, not authentication and not the Supabase approval policy
   (§0.1) — and §3's grading, because `mappings.jsonl` / `source_usages.jsonl`
   were not among the supplied artifacts (§0.2).

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

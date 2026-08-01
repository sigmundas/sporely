# Taxonomy UI menu — scope recommendation

Status: deferred product recommendation. Not part of the current W2–W5 taxonomy integration critical path.

## Audiences

- **Observer** (default): status, activation toggle, "is my data OK?" — read
  and one reversible switch.
- **Maintainer / power user**: refresh, verify, inspect — actions that touch
  bytes on disk.

Taxonomy has hard invariants (append-only registry, immutable source releases,
SHA-256-bound approvals, byte-exact manifests). Anything that could violate
them stays CLI-only under an approval envelope; it does not belong in a GUI.

## Recommended: `Help → Taxonomy` submenu (observer-safe)

Six items. Read-first. Only two write, both reversible.

| Item | Behavior | Reads / writes |
|---|---|---|
| Show taxonomy status… | Modal: schema version, active release ID, bundled vs installed vs candidate, SHA-256 short prefixes, install date, "using fallback DB: yes/no", registry shard count + concatenated SHA-256 short. | Reads `desktop-compatibility.json`, bundled `manifest.json`, install receipt. |
| Verify installed database… | Full-hash the extracted SQLite via the `SPORELY_TAXONOMY_V2_VERIFY=1` restart path; report pass/fail. | Read-only. Needs progress UI (~310 MB). |
| Reveal in Finder / Explorer… | Opens the app-data taxonomy directory. | No mutation. |
| Copy diagnostics to clipboard | Status snapshot + last N log lines for support tickets, path-sanitized. | Read-only. |
| Enable taxonomy v2 (experimental) — toggle | Writes `taxonomy_v2_activation: true/false` in `app_settings.json`; offers restart. | Reversible; any verify failure falls back to bundled legacy DB. |
| Reset & reinstall from bundle | Deletes installed SQLite + receipt; next start re-extracts from bundled `.gz`. | Idempotent — bundle is version-controlled + SHA-256-verified before extract. |

Nothing else belongs in the observer menu.

## Explicitly NOT in any menu

Each of these looks convenient and has a non-obvious failure mode. Keep them
CLI-only.

- **Refresh sources.** `acquire_col_xr.py` / `acquire_nortaxa.py` require a
  separately reviewed approval JSON binding proposal + request SHA-256s,
  ceiling, and redirect hosts. A menu click cannot construct that envelope,
  and each script consumes its authorization on failure.
- **Compile release.** Requires a `tax-YYYY.MM.DD-NN` release ID, a
  registry-path decision (dry-run vs canonical), and manual-mapping review.
  A GUI trigger encourages repeat-click behavior that breaks the append-only
  registry contract.
- **Promote candidate → published.** Gated by
  `policies/release_thresholds.yml` hard failures and severities. Threshold
  review is a maintainer conversation, not a checkbox.
- **Edit manual mappings.** `policies/manual_mappings.yml` requires reviewer,
  rationale, evidence references, and source-release range on every entry.
  Free-form GUI editing is how "just add one row" becomes a corrupt policy
  file.
- **Delete or merge a taxon.** The registry is append-only. There is no
  delete. A UI that appears to offer it is a liability.
- **Import a national source.** The `national_source.py` CLI runs four
  distinct validation phases (`init` → `inspect` → `validate` → `normalize`).
  Collapsing them into one button hides the checks that catch bad profiles.

## Optional developer sub-menu (dev flag only, read-only)

Only if there is actual demand. Gate on `SPORELY_DEV=1` or equivalent. No
writes to `database/taxonomy/` — that directory is version-controlled and its
contents are byte-hashed elsewhere in the repo.

- Show source releases on disk: `sources/<code>/<release>/manifest.json`
  `state` field (`planned`, `downloaded`, `quarantined`, `validated`,
  `promoted`) and archive SHA-256 short prefixes.
- Show registry health: shard file sizes, line counts, concatenated SHA-256
  vs the manifest's declared value. A mismatch is repository corruption; the
  UI should tell the user to stop and file an issue rather than "repair."
- Show last compile diagnostics: read a local `diagnostics.json` if present.
- Run the offline policy validator: invoke
  `database/taxonomy/validate_policies.py`; report pass/fail + counts.
- Open this README: deep-link to `database/taxonomy/README.md`.

## Design notes

1. **Never surface `sporely_taxon_id` as user-facing identity.** It is a
   stable internal integer per `docs/identity-contract.md`. Render
   `canonical_scientific_name` + rank + source/family. Sporely IDs belong in
   the diagnostics blob only.
2. **Every "verify" surface names its fallback.** "If verification fails,
   Sporely will use the currently bundled taxonomy DB. Your observations are
   safe." Users otherwise panic at hash mismatches.
3. **Do not re-implement the receipt fast-path.** `utils/taxonomy_v2.py`
   already caches a valid install and only re-verifies on the
   `SPORELY_TAXONOMY_V2_VERIFY` flag. The menu's Verify item should set that
   flag and restart, not roll its own hasher.
4. **Show source releases, not just the compiled artifact.** When something
   is off, "what was the input" is the first question. Two lines — COL XR
   release + NorTaxa version — with SHA-256 short prefixes covers most
   debugging.
5. **One state phrase beats five numbers.** e.g. *"Taxonomy v2 candidate
   (tax-2026.07.29-01) — installed and verified."* The five raw numbers can
   live in the copy-diagnostics blob.
6. **Always indicate fallback state.** Silent fallback to the bundled legacy
   DB is the correct behavior, but users need to know they are on it —
   otherwise a broken activation looks like missing new taxa.

## Non-goals

- No acquisition, compile, promote, mapping-edit, or delete surfaces at any
  tier.
- No writes to `database/taxonomy/` from any UI.
- No UI-side hashing that duplicates `utils/taxonomy_v2.py` verification.
- No maintenance the CLI cannot already perform under an approval envelope.

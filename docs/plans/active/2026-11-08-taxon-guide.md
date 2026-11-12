# Taxon guide: edibility and species notes plan

Status: Draft for owner review, revised after a Codex plan review on
2026-11-12. Cross-repo: sporely-py builds the content, sporely-web shows it.

## Agent handoff

- Status: Proposed. Owner decisions D1–D7 below are open; D3 must be settled
  before the content ships.
- Last completed stage: None. Feasibility spikes only (scratch scripts, nothing
  committed).
- Current/next stage: Stage 1 — Normlisten source and name resolution.
- Important decisions:
  - Content is keyed by `sporely_taxon_id` and versioned apart from taxonomy
    releases.
  - Edibility shown to users comes from the NSNF Normlisten only. The owner's
    notes add descriptions, not edibility verdicts.
  - The app never resolves identity by name. A taxon without a proven Sporely
    identity gets no card.
- Do not:
  - Put guide content into `taxon_min` or the taxonomy release identity
    pipeline.
  - Show an edibility verdict as a recommendation to eat.
  - Let opening a card change an observation's identification.
  - Write to production without explicit approval.

## Goal

An info link on a taxon (AI candidate, search result, observation detail) that
opens a short guide card in the mobile app:

- NSNF Normlisten status (spiselig, spiselig*, ikke matsopp, giftig, meget
  giftig) with its comment, year and "pensum" flag.
- Macroscopic description from the owner's study notes for NSNF
  "pensumarter": cap, gills, stem, ring, spore print, smell, taste, habitat,
  ecology, toxin and remarks.
- Genus-level descriptions for the most common genera, shown for species that
  have no description of their own.
- A reference image: the Artsorakel `picture_url` when the card is opened from
  an Artsorakel result (credit rules in D4/D6).

## Sources

| Source | Shape | Role |
|---|---|---|
| Normlisten 2026 PDF (NSNF, Fagmykologisk råd, March 2026) | 222 rows; text tables, parsed with `pdfplumber`; listed three times in the PDF (by Norwegian name, scientific name and status); red text marks changes from 2025 | Authoritative edibility status |
| `Soppkurs pensum.xlsx` (owner's notes) | 151 rows, 79 genera, 24 columns: names (no/en/da), Hatt, Skiver, Stilk, Edibility, Sporepulver, Skiveform, Ring, colours, Voksested, Merknad, Lukt, Smak, Toksin, Culinary value, Mycorrhizal | Descriptions; edibility only as a cross-check |
| Genus notes (to be written) | Same fields, at genus level | Fallback description per genus |
| Artsorakel response | `picture_url` and `species_url` per candidate. The saved copy keeps `picture_url` (`sporely-web/src/ai-identification.js:1432`); the live normalised prediction (`ai-identification.js:531`) does not. No photographer or licence fields are extracted. | Reference image at runtime |

## Feasibility findings (2026-11-08)

Matched against taxonomy release `tax-2026.10.31-01` (NorTaxa 1.292):

- **214 of 222 rows match one concept.** They resolve to exactly one Sporely
  concept through NorTaxa names, and the Norwegian name agrees in every case
  (211 via NorTaxa accepted names, 3 via NorTaxa synonyms).
- **Where NorTaxa names are stored.** NorTaxa usages merged into a COL concept
  are found in `taxon_external_id_text_min` (`nortaxa/nortaxa_taxon_id`, name
  in `external_name`). NorTaxa-only concepts are followed through
  `taxon_association_min` (`member_taxon_id` → `primary_taxon_id`).
- **Genus rows.** The 13 `spp.` rows resolve to genus concepts.
- **`coll.` rows are not parents.** The 6 `coll.` rows match NorTaxa's own
  `… coll.` concepts. These are species-rank concepts with no parent. The
  member species do not sit under them: *Cuphophyllus pratensis* (234) has its
  genus as parent, not `C. pratensis coll.` (627043). A `coll.` status
  therefore never reaches its member species without an explicit member list.
- **Groups without a scientific name.** 8 group rows have none: Beite-, Åker-,
  Blod- and Giftsjampinjong-gruppen, Kamfluesopp-, Matriske-, Rundmorkel- and
  Spissmorkel-gruppen. The workbook names some anchors (`Agaricus arvensis
  sensu lato`, `Amanita vaginata sensu lato`, `Lactarius fennoscandicus,
  quieticolor`). Norwegian-name suffixes propose member species.
- **One name-sense conflict.** *Lepiota brunneoincarnata* is a NorTaxa synonym
  of *L. subincarnata* (24133), but COL also has a separate *L.
  brunneoincarnata* (104913) with the same Norwegian name (D7).
- **Workbook mismatches.** 18 workbook rows don't match a Normlisten Norwegian
  name. These are mostly typos ("Grå blekksop", "Vintersop"), "+" suffixes,
  genus rows with "(No specific species listed)", and two *Hydnum* groups that
  Normlisten lists as `Hydnum spp.`. The workbook's edibility vocabulary also
  differs ("Spiselig merknad", "Spiselig avkokes").
- **Spike limits; the real matcher must not copy them.**
  - The spike matcher used `accepted or synonym`, which ignores synonym
    targets whenever an accepted target exists.
  - Its change flag looked for red words anywhere on the page and ignored
    comment-only changes.

## Name resolution rule (new, documented exception)

The red-list overlay forbids automatic name matching, because a name used in
another country, or with different authors, can mean a different species.
Normlisten is a Norwegian list using Norwegian name senses, so the rule here
is:

1. Accept a row automatically only when all three hold:
   - The scientific name matches NorTaxa names (accepted and synonym, checked
     together) in the pinned release.
   - The union of all their targets, after following associations, is exactly
     one Sporely concept.
   - The row's Norwegian name is a Norwegian name of that concept.
2. Everything else goes to a reviewed ledger
   (`database/taxon_guide/policies/normlisten_mappings.yml`): groups without a
   scientific name, `coll.` member lists, sense conflicts, ambiguous names,
   misses and Norwegian-name disagreements.
3. Never match a name to COL or Dyntaxa directly.
4. Store how each row was matched: the NorTaxa IDs it hit (accepted or
   synonym role), every association hop with its rule, and the release ID.
   Matching is re-run on every taxonomy release (Stage 3), because a release
   can drop an association while both concept IDs still exist
   (`database/taxonomy/scripts/name_associations.py`, policy `exclusions`).

Record this rule in `database/taxon_guide/docs/name-resolution.md`, and link it
from `redlist-overlay.md` so the stricter red-list rule stays unchanged.

## Proposed decisions

- **Separate content pack, not taxonomy release data.** The pack is keyed by
  `sporely_taxon_id` and versioned as `guide-YYYY.MM.DD-NN`. Its manifest
  records the taxonomy release that names were resolved against. Each new
  taxonomy release triggers a re-match and a diff (Stage 3), not just an
  existence check.
- **Status and description resolve separately.** For a given taxon:
  - **Status:** the nearest of the taxon's own Normlisten row, then a reviewed
    group or `coll.` membership, then a genus `spp.` row. A more specific row
    always wins. If a taxon belongs to two reviewed groups with different
    statuses, the build fails.
  - **Description:** the taxon's own entry, then its group's, then its
    genus's. A genus entry without content is never shown.
  - **Independence:** a description-only species still shows its genus or
    group status. For example, a *Hebeloma* species without a description
    still shows "giftig" from `Hebeloma spp.`.
  - **Infraspecific taxa:** these inherit from their species.
- **Build a direct taxon → card index at build time.** Genus and group
  membership is expanded to concrete taxon IDs in the pinned release. Each
  index entry references a status record and a description record, plus the
  label to show ("Slekt: …", "Gruppe: …"). The app does a single lookup and
  never walks ancestors at runtime. That matters because AI candidates and
  reloaded observations do not carry a parent ID.
- **Cross-check, not a second verdict.** A vocabulary map translates the
  workbook's edibility to Normlisten codes ("Spiselig avkokes" and "Spiselig
  merknad" → `edible_with_caveat`). Disagreements fail the build unless they
  are listed in a reviewed exceptions file. Workbook rows without a Normlisten
  row get a description but never a status.
- **Authored source in text, not xlsx.** Import the workbook once into
  diffable YAML (one file per genus, with species entries in it). From then
  on, edit the YAML; the xlsx becomes historical input.
- **Edition changes are a full diff.** Each new Normlisten edition is compared
  row by row with the previous one: additions, removals, and status, name and
  comment changes. The red text is a hint, not the review scope. Red is
  attributed by row coordinates, not by matching words on the page.

## Open owner decisions

- **D1 — Delivery to the app.**
  - Option (a), recommended for V1: a static versioned JSON pack bundled into
    the sporely-web build. It works offline, needs no migration and no
    production write, but updates need an app release.
  - Option (b): a cloud table plus a read RPC. Content can be updated without
    an app release, but it needs a migration, a public RLS read policy and a
    production import.
- **D2 — Safety framing.** Proposed:
  - Always show "Never eat a mushroom based on an app; use a soppkontroll."
  - Don't show an "edible" badge directly on AI candidate rows. Show "giftig"
    and "meget giftig" there as warnings.
  - Show the full status only inside the card.
- **D3 — Permission and credit.** Must be settled before the content ships.
  - Ask NSNF before republishing Normlisten statuses and comments in the app,
    and credit "NSNF Normlisten 2026" on every card.
  - Confirm the workbook descriptions are the owner's own wording, not copied
    from a field guide or course material. The sheet's title ("Nice. Can you
    modify this table") suggests an AI tool rewrote it, so the facts also need
    a check.
- **D4 — Reference image source.**
  - V1: the Artsorakel `picture_url`, only when the card opens from an
    Artsorakel result.
  - Later: a chosen reference image (URL, licence, credit) stored per card, so
    an image also shows from search or an observation.
- **D5 — Languages.** V1 Norwegian only. The workbook's English and Danish
  names are kept as data but not shown.
- **D6 — Image without credit.** The Artsorakel response may give no
  photographer or licence. Options:
  - Hide the image.
  - Show it with a generic "Foto: Artsdatabanken".
- **D7 — *Lepiota brunneoincarnata*.** Attach "meget giftig" to both concepts
  (24133 and 104913), as proposed, or only to the NorTaxa-resolved one.

## Data model

Pack entries are records. The index maps every covered taxon ID to records.

```yaml
status_records:
  nl2026-ametystsopp:
    edition: 2026
    status_code: not_food   # edible | edible_with_caveat | not_food | poisonous | deadly
    status_raw: Ikke matsopp
    comment: Tar opp arsen.
    pensum: true
    source_row: {norsk: Ametystsopp, scientific: Laccaria amethystina}
    resolution:
      release_id: tax-2026.10.31-01
      rule: nortaxa_exact              # or reviewed_ledger
      nortaxa_hits: [{id: "53909", role: accepted}]
      association_hops: []             # [{member, primary, rule}]
description_records:
  laccaria-amethystina:
    level: species                     # species | group | genus
    cap: …
    gills: …
    stem: …
    ring: …
    spore_print: Hvitt
    smell: …
    taste: …
    habitat: …
    ecology: mycorrhizal               # mycorrhizal | saprotroph | saprotroph_wood | parasite
    toxin: …
    remarks: …
    culinary_value: low                # high | moderate | low
index:
  "21213":
    status: nl2026-ametystsopp
    status_via: own                    # own | group:<code> | genus:<id>
    description: laccaria-amethystina
    description_via: own
    names_snapshot: {sci: Laccaria amethystina, no: Ametystsopp}
```

The manifest holds:
- pack ID;
- Normlisten PDF SHA-256 and URL;
- workbook/YAML content hash;
- taxonomy release ID used for resolution;
- row and index counts;
- generator version.

## Stages

### Stage 1 — Normlisten source and name resolution (sporely-py)

- Pin the PDF under `database/taxon_guide/sources/normlisten/2026/`, with its
  SHA-256 in `taxon_guide/recipe.json`. Gitignore the PDF if its size or
  licence requires it, as with the red-list workbook.
- **Normaliser:**
  - Parse the "sortert etter norske navn" section.
  - Check that the other two sections contain the same 222 rows with the same
    status.
  - Attribute red text by row coordinates.
  - Map status to `status_code`.
- **Resolver:** implements the rule above, recording how each row matched.
  - Reviewed ledger for: the 8 unnamed groups, the 6 `coll.` member lists, and
    D7.
- **Tests:**
  - Parser fixture with a wrapped comment, a comment-only change and a
    repeated red word on an unrelated row.
  - Resolver cases: accepted name; synonym; a name hitting both an accepted
    and a synonym target on different concepts (must go to review); `spp.`;
    `coll.`; an association hop; ambiguity; a miss; a Norwegian-name
    disagreement.
  - The build fails on an unreviewed non-unique row.
- **Acceptance:** 222/222 rows are either resolved automatically or covered by
  the ledger, and a report lists every non-automatic row.

### Stage 2 — Thin end-to-end slice

Prove the whole path with about 10 hand-picked taxa before importing all
notes. The slice must cover:

- a species with its own status and description;
- a species with only a description, which inherits a genus status
  (*Hebeloma*);
- a `spp.` genus;
- a `coll.` member;
- an unnamed group member;
- an unresolved provider candidate;
- a saved observation reloaded from the database.

Steps:

- Pack builder (merge status, descriptions and the index, write the manifest;
  output byte-identical when rebuilt from the same inputs).
- A minimal app card for one entry point (AI result rows) reading the bundled
  pack.
- **Acceptance:** each case shows the right card, or no card, in the app;
  rebuilding the pack is deterministic.

### Stage 3 — Release compatibility check (sporely-py)

- For a new taxonomy release:
  - re-run name resolution;
  - re-expand group and genus membership;
  - diff the result against the current pack's recorded matches.
- **Fail the check when:**
  - a match changes target, gains or loses a target, or loses an association
    hop;
  - an index entry's status or description changes source without a ledger
    change;
  - a taxon ends up with two conflicting statuses;
  - a superseded concept (`taxon_supersession_min`) still holds an entry.
- **Tests:** association removed while both IDs remain, a retired ID, a
  supersession, and a group with conflicting member statuses.

### Stage 4 — Notes import and genus descriptions (sporely-py)

- One-off importer from the workbook to YAML, one file per genus:
  - Normalise typos and spelling variants ("Hvitt"/"Hvit", "Mycorhiza").
  - Turn "(No specific species listed)" rows into genus entries.
  - Split "fennoscandicus, quieticolor".
- Resolve species with the Stage 1 resolver, and genera through NorTaxa genus
  names.
- Cross-check edibility with the vocabulary map; disagreements go to the
  reviewed exceptions file.
- Add genus entries for the most common genera (at least the 79 already in the
  workbook). Empty ones stay hidden until filled in.
- **Acceptance:** every YAML entry resolves to one concept, and the owner has
  reviewed the exceptions file.

### Stage 5 — App card, split by entry point (sporely-web)

**Shared, for all three entry points:**
- One lookup by `sporely_taxon_id` in the bundled index.
- No card when the identity is legacy or unresolved.
- No name matching at runtime.
- Opening a card is read-only.
- Reuse the dialog pattern in `src/publish-notice.js` (Escape and backdrop
  dismiss, focus moves in and returns).

**5a — AI result rows** (next to the species link and red-list badge in
`src/ai-identification.js`):
- Resolve the candidate with `externalTaxonomySelectionForCandidate` and
  `resolveExternalTaxonomySelection`.
- That resolver makes no request when cloud access is unavailable
  (`src/taxonomy-v2.js:280`). Artsorakel needs the network anyway, but a
  resolution that fails or returns no single match shows no card.
- Ignore stale results when the candidate list changes.
- Read the image from the saved `picture_url`, not from the live normalised
  prediction.
- Warning badges per D2.

**5b — Taxon search results:**
- Search rows select the taxon on `mousedown`
  (`src/screens/find_detail.js:4533`). The info button must stop that event,
  so opening a card never selects a taxon.

**5c — Observation detail** (`src/screens/find_detail.js`):
- Use the observation's `selected_sporely_taxon_id`.
- Rows with an unresolved external identity
  (`taxonomySelectionFromObservationRow`) show no card.

**Tests:**
- lookup, and status and description inheritance;
- unresolved and legacy identities;
- resolver error and offline;
- stale async results;
- a malformed or missing pack;
- an info click that does not change the selection;
- escaped content;
- image missing or failing to load, and missing credit (D6);
- keyboard dismissal and focus return.

**Review:**
- Codex review.
- A security review only if D1 = cloud table.

### Stage 6 — Release (and cloud table if D1 = b)

- **D1 = a:** the content ships with the next app build; record the pack ID in
  the release notes.
- **D1 = b:**
  - a migration, following the sporely-web migration rules and
    `deploy-exceptions.json`;
  - a public read-only RPC;
  - an import runbook;
  - an explicitly approved production import.

### Stage 7 — Yearly update runbook

For a new Normlisten edition:
1. Pin the new PDF.
2. Run the full edition diff (additions, removals, status, name and comment
   changes).
3. Review the diff and any ledger misses.
4. Run the Stage 3 compatibility check.
5. Bump the pack version.

## Out of scope

- Generating edibility from AI or from any source other than Normlisten.
- Look-alike comparison logic. Comments that name look-alikes are shown as
  text only.
- Translations beyond Norwegian (D5).
- A desktop card in sporely-py. It can reuse the pack later.
- Bundling a provider-ID index for resolving AI candidates offline.

## Review history

- **2026-11-12, Codex plan review.** Changes adopted:
  - `coll.` member lists;
  - re-matching on every taxonomy release instead of existence checks;
  - separate status and description inheritance;
  - a build-time taxon → card index;
  - an edition diff instead of red-text-only review;
  - cross-check exceptions instead of a hard fail;
  - no card for unresolved identities;
  - the search-row `mousedown` hazard;
  - reuse of the dialog pattern;
  - a thin end-to-end slice before the full import;
  - Stage 5 split by entry point;
  - the desktop card dropped.

  Partly rejected: the claim that saved AI results drop `picture_url`. The
  saved copy keeps it; only the live normalised prediction lacks it.

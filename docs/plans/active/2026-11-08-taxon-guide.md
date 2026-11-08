# Taxon guide: edibility and species notes plan

Status: Draft for owner review. Cross-repo (sporely-py builds the content,
sporely-web shows it).

## Agent handoff

- Status: Proposed; owner decisions D1–D5 below are open.
- Last completed stage: None. Feasibility spikes only (scratch scripts, nothing
  committed).
- Current/next stage: Stage 1 — Normlisten source and name resolution.
- Important decisions: Content is keyed by `sporely_taxon_id` and versioned
  apart from taxonomy releases. Edibility shown to users comes from the NSNF
  Normlisten only; the owner's notes add descriptions, not edibility verdicts.
- Do not: Put guide content into `taxon_min` or the taxonomy release identity
  pipeline; show an edibility verdict as a recommendation to eat; write to
  production without explicit approval.

## Goal

An info link on a taxon (AI candidate, search result, observation detail) that
opens a short guide card in the mobile app:

- NSNF Normlisten status (spiselig, spiselig*, ikke matsopp, giftig, meget
  giftig) with its comment, year and "pensum" flag.
- Macroscopic description from the owner's study notes for NSNF
  "pensumarter": cap, gills, stem, ring, spore print, smell, taste, habitat,
  ecology, toxin, remarks.
- Genus-level cards for the most common genera, shown for species without
  their own card.
- A reference image: the Artsorakel `picture_url` when the card is opened from
  an Artsorakel result, with attribution.

## Sources

| Source | Shape | Role |
|---|---|---|
| Normlisten 2026 PDF (NSNF, Fagmykologisk råd, March 2026) | 222 rows; text tables, parsed with `pdfplumber`; listed three times in the PDF (by Norwegian name, scientific name and status); red text marks changes from 2025 | Authoritative edibility status |
| `Soppkurs pensum.xlsx` (owner's notes) | 151 rows, 79 genera, 24 columns: names (no/en/da), Hatt, Skiver, Stilk, Edibility, Sporepulver, Skiveform, Ring, colours, Voksested, Merknad, Lukt, Smak, Toksin, Culinary value, Mycorrhizal | Descriptions; edibility only as a cross-check |
| Genus notes (to be written) | Same fields, at genus level | Fallback card per genus |
| Artsorakel response | `picture_url` and `species_url` per candidate, already normalised in `sporely-web/src/artsorakel.js` | Reference image and link at runtime |

## Feasibility findings (2026-11-08)

Matched against taxonomy release `tax-2026.10.31-01` (NorTaxa 1.292):

- 214 of 222 Normlisten rows resolve to exactly one Sporely concept through
  NorTaxa names, and the Norwegian name agrees in every case (211 via NorTaxa
  accepted names, 3 via NorTaxa synonyms).
- NorTaxa usages merged into a COL concept are found in
  `taxon_external_id_text_min` (`nortaxa/nortaxa_taxon_id`, name in
  `external_name`); NorTaxa-only concepts are followed through
  `taxon_association_min` (`member_taxon_id` → `primary_taxon_id`).
- `spp.` rows (13) resolve to genus concepts; `coll.` rows (6) match NorTaxa's
  own `… coll.` concepts.
- 8 group rows have no scientific name (Beite-, Åker-, Blod- and
  Giftsjampinjong-gruppen, Kamfluesopp-, Matriske-, Rundmorkel- and
  Spissmorkel-gruppen). The workbook names some anchors (`Agaricus arvensis
  sensu lato`, `Amanita vaginata sensu lato`, `Lactarius fennoscandicus,
  quieticolor`); Norwegian name suffixes propose member species.
- Sense conflict: *Lepiota brunneoincarnata* is a NorTaxa synonym of *L.
  subincarnata* (24133), but COL also has a separate *L. brunneoincarnata*
  (104913) with the same Norwegian name.
- Workbook: 18 rows don't match a Normlisten Norwegian name. These are mostly
  typos ("Grå blekksop", "Vintersop"), "+" suffixes, genus rows with
  "(No specific species listed)", and two *Hydnum* groups that Normlisten
  lists as `Hydnum spp.`. Its edibility vocabulary differs ("Spiselig
  merknad", "Spiselig avkokes").

## Name resolution rule (new, documented exception)

The red-list overlay forbids automatic name matching, because a name used in
another country, or with different authors, can mean a different species.
Normlisten is a Norwegian list using Norwegian name senses, so the rule here
is:

1. Accept automatically only when the scientific name matches a NorTaxa name
   (accepted or synonym) in the pinned release, the match gives exactly one
   Sporely concept after following associations, and the row's Norwegian name
   is a Norwegian name of that concept.
2. Everything else goes to a reviewed ledger
   (`database/taxon_guide/policies/normlisten_mappings.yml`): groups without a
   scientific name, sense conflicts, ambiguous names, misses and Norwegian-name
   disagreements.
3. Never match a name to COL or Dyntaxa directly.

Record this rule in `database/taxon_guide/docs/name-resolution.md`, and link it
from `redlist-overlay.md` so the stricter red-list rule stays unchanged.

## Proposed decisions

- **Separate content pack, not taxonomy release data.** Sporely concept IDs
  stay stable across releases, so guide content can be keyed by
  `sporely_taxon_id` and versioned on its own (`guide-YYYY.MM.DD-NN`). The
  manifest records which taxonomy release was used to resolve names. Each new
  taxonomy release re-validates the pack: every ID still exists, and
  superseded concepts are followed via `taxon_supersession_min`.
- **Edibility precedence.** Show the Normlisten status as the edibility. The
  workbook's edibility is a build-time cross-check only, and the build fails
  if the two disagree.
- **Inheritance.** A taxon without its own card shows the nearest ancestor's
  card, labelled "Slekt: …" or "Gruppe: …". A more specific Normlisten row
  always overrides a genus (`spp.`) row. Infraspecific taxa inherit their
  species card.
- **Authored source in text, not xlsx.** Import the workbook once into
  diffable YAML (one file per genus, with species entries in it). From then
  on, edit the YAML; the xlsx becomes historical input.
- **Group cards.** A Normlisten group becomes a card attached to each reviewed
  member species, unless that species has its own row.

## Open owner decisions

- **D1 — Delivery to the app.**
  - Option (a), recommended for V1: a static versioned JSON pack (about 250
    entries, well under 1 MB) bundled into the sporely-web build. It works
    offline, needs no migration and no production write, but updates need an
    app release.
  - Option (b): a cloud table plus a read RPC. Content can be updated without
    an app release, but it needs a migration, a public RLS read policy and a
    production import.
- **D2 — Safety framing.** Proposed:
  - Always show "Never eat a mushroom based on an app; use a soppkontroll."
  - Don't show an "edible" badge directly on AI candidate rows. Show "giftig"
    and "meget giftig" there as warnings.
  - Show the full status only inside the card.
- **D3 — Permission and credit.**
  - Ask NSNF before republishing Normlisten statuses and comments in the app,
    and credit "NSNF Normlisten 2026" on every card.
  - Confirm the workbook descriptions are your own wording, not copied from a
    field guide or course material. The sheet's title suggests an AI tool
    rewrote it, so its facts also need a check.
- **D4 — Reference image.**
  - V1: show only the Artsorakel `picture_url` when the card opens from an
    Artsorakel result, with the Artsdatabanken attribution or licence shown
    with it.
  - Later: store a chosen reference image (URL, licence, credit) per card, so
    an image also shows when the card opens from search or an observation.
- **D5 — Languages.** V1 Norwegian only. Workbook English and Danish names are
  kept as data but not shown.

## Data model (pack entry)

```yaml
sporely_taxon_id: 21213
rank: species            # species | genus | group
names_snapshot: {sci: Laccaria amethystina, no: Ametystsopp}
normlisten:
  edition: 2026
  status_code: not_food  # edible | edible_with_caveat | not_food | poisonous | deadly
  status_raw: Ikke matsopp
  comment: Tar opp arsen.
  pensum: true
  changed_from_previous: false
  source_row: {norsk: Ametystsopp, scientific: Laccaria amethystina}
  resolution: {rule: nortaxa_accepted_exact, nortaxa_taxon_id: "53909"}
description:            # from authored YAML, all optional
  cap: …
  gills: …
  stem: …
  ring: …
  spore_print: Hvitt
  smell: …
  taste: …
  habitat: …
  ecology: mycorrhizal   # mycorrhizal | saprotroph | saprotroph_wood | parasite
  toxin: …
  remarks: …
  culinary_value: low    # high | moderate | low
group: {code: matriske, label_no: Matriske-gruppen}   # when from a group row
```

The manifest holds: pack ID, Normlisten PDF SHA-256 and URL, workbook/YAML
content hash, taxonomy release ID used for resolution, row counts and the
generator version.

## Stages

### Stage 1 — Normlisten source and name resolution (sporely-py)

- Pin the PDF under `database/taxon_guide/sources/normlisten/2026/`, with its
  SHA-256 in `taxon_guide/recipe.json`. Gitignore the PDF if size or licence
  requires it, as with the red-list workbook.
- Normaliser: parse the "sortert etter norske navn" section; cross-check that
  the other two sections contain the same 222 rows with the same status;
  record the red-text change flag; map status to `status_code`.
- Resolver implementing the rule above; reviewed ledger for the 8 groups and
  the *Lepiota* conflict. For the *Lepiota* conflict, the proposal is to
  attach the status to both concepts.
- Tests: parser fixture covering a wrapped comment; resolver cases for an
  accepted name, a synonym, `spp.`, `coll.`, an association hop, ambiguity, a
  miss and a Norwegian-name disagreement; the build fails on an unreviewed
  non-unique row.
- Acceptance: 222/222 rows either resolved automatically or covered by the
  ledger; report lists every non-automatic row.

### Stage 2 — Notes import and genus cards (sporely-py)

- One-off importer: workbook → YAML per genus. Normalise typos and spelling
  variants ("Hvitt"/"Hvit", "Mycorhiza"); turn "(No specific species listed)"
  rows into genus entries; split "fennoscandicus, quieticolor".
- Resolve species with the same resolver, and genera through NorTaxa genus
  names.
- Cross-check the workbook's edibility against Normlisten and report any
  disagreement.
- Add skeleton genus entries for the most common genera (at least the 79
  already in the workbook), to be filled in by the owner.
- Acceptance: every YAML entry resolves to one concept; the cross-check report
  is reviewed by the owner.

### Stage 3 — Pack builder and validator (sporely-py)

- Merge Normlisten, descriptions and groups into `taxon-guide.json` plus a
  manifest; the output must be byte-identical when rebuilt from the same
  inputs.
- Validator against a given taxonomy release: IDs exist, supersessions are
  followed, inheritance has no cycles, and a single taxon never has two
  conflicting statuses.
- Optional: a desktop card in the species views, reading the same pack.

### Stage 4 — App card (sporely-web)

- Load the pack (per D1) and look up a card by `sporely_taxon_id` with ancestor
  fallback. An ancestor chain is needed for the fallback: bundle a small
  `taxon → parent` map for the covered genera, or add `parent_taxon_id` to
  search results, which already return it.
- Info button where a taxon is shown:
  - AI result rows (next to the existing species link and red-list badge in
    `src/ai-identification.js`), resolving the candidate through
    `resolve_taxon_external_id_v2`, now that the active release carries
    `nortaxa_taxon_id` mappings;
  - taxon search results;
  - observation detail (`src/screens/find_detail.js`).
- Card with the status badge, comment, description, safety text (D2) and
  credits; Artsorakel image when available (D4).
- Tests: lookup and fallback, unresolved candidates show no button, warning
  badges on AI rows, card rendering, offline load.
- Codex review; a security review only if D1 = cloud table.

### Stage 5 — Release (and cloud table if D1 = b)

- D1 = a: content ships with the next app build; record the pack ID in the
  release notes.
- D1 = b: a migration (following the sporely-web migration rules and
  `deploy-exceptions.json`), a public read-only RPC, an import runbook, and an
  explicitly approved production import.

### Stage 6 — Yearly update runbook

- New Normlisten edition: pin, rebuild, review only red-marked changes and
  ledger misses, then bump the pack.

## Out of scope

- Generating edibility from AI or from any source other than Normlisten.
- Look-alike comparison logic. Comments that name look-alikes are shown as
  text only.
- Translations beyond Norwegian (D5).

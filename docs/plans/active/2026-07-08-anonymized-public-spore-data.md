# Anonymized public spore data plan

Status: Proposed; implementation not found.

## Agent handoff

- Status: Proposed; implementation not found.
- Last completed stage: None verified.
- Current/next stage: Resolve the privacy/cohort questions before desktop or RPC work.
- Important decisions: Keep underlying-table RLS private and expose only deliberately stripped aggregate data through a public RPC.
- Do not: Expose observation identity, location, exact dates, or direct hidden-observation access.
- Remaining acceptance criteria: The policy, constraints, and rollout requirements below.

Goal:
Let users contribute spore measurements to the community dataset
without exposing the observation itself. Motivating cases: matsutake
sites, rare taxa, and psychoactive species where the finder does not
want the location tied to their name.

The schema already separates `spore_data_visibility` from
`visibility`, so "hidden observation, public measurements" is a
valid combination on the desktop side today — what is missing is a
public RPC that reads it and a landing surface that consumes it.

Model (desktop):

- Keep the existing `observations.visibility` (`private` /
  `friends` / `public`) and `spore_data_visibility`
  (`private` / `public`) as-is.
- The desktop Preferences dialog gets an explicit control: "Share
  spore measurements from private observations anonymously". When
  on, private observations still push measurements + mosaic tiles to
  the cloud through Stage J's metadata-only image path.
- The desktop helper that already creates metadata-only microscope
  image rows is unchanged — it fires when the observation is public
  OR when `spore_data_visibility='public'`, which is already the
  gate we use.

Model (cloud, sporely-web):

- New (or extended) public RPC — e.g.
  `search_public_anonymous_spore_points(taxon_slug, country, ...)` —
  that reads observations where `spore_data_visibility='public'`
  regardless of `visibility`. Projection intentionally strips:
  observation id, observer, GPS, exact date, unshared image URLs.
- Kept: `genus`, `species`, `length_um`, `width_um`, `q`,
  `country_code`, optionally `year_month` (`YYYY-MM`) but only when
  the (species, country, month) cohort has at least N points to
  avoid re-identification of rare taxa; otherwise coarsen to year
  only.
- Mosaic tile access: allow the tile URL + tile rect via a companion
  RPC, but drop `observationId` from the returned row so the tile
  cannot be linked back to the observation. Keep the polygon
  overlay.

Constraints:

- No new columns on `observations` — reuse existing visibility
  fields.
- RLS on `observations`, `observation_images`,
  `spore_measurements`, `spore_measurement_mosaics`, and
  `spore_measurement_mosaic_tiles` must continue to reject direct
  reads of hidden observations by anonymous / stranger roles. The
  new visibility comes only through the RPC, not through the
  underlying tables.
- Landing must not expose observation-level detail pages for
  anonymized points; those clicks land on the species aggregate
  chart instead.

Follow-up questions before implementing:

- Minimum cohort size for month-year vs year-only. Rough starting
  point: month-year only when `count(species, country, month) >= 5`,
  else year, else omit.
- Whether anonymized points should also feed `search_public_species`
  observation counts (probably no — count "publicly shared
  observations" separately from "anonymously shared spore points"
  in the UI).
- UX copy for the opt-in checkbox: "Share only my spore data. Your
  observation stays private; the community sees the measurements
  without any location or identity."

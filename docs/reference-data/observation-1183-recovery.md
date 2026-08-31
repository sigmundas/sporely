# Observation 1183 reference recovery

Observation `1183` must not be backfilled or presented as having a frozen
historical Funga Nordica attachment. The production audit found all three
authoritative records needed for that history absent:

- no cloud `observation_reference_use`;
- no current saved local `observation_reference_uses` row;
- both cloud exact-taxonomy fields are null.

The earlier comparison was display-only state. Its source revision and frozen
snapshot cannot now be reconstructed without inventing scientific history.
Genus/species text, citation text, publication names, slugs, or fuzzy matches
are not sufficient evidence for an exact taxon or attachment.

Recovery is therefore an explicit user workflow after the forward-path fix:

1. Resolve or reselect the exact taxon when it is still unresolved.
2. Plot or attach the Funga Nordica library reference again.
3. Sync the observation.

Only that new deliberate action creates the current frozen attachment and
allows shared same-species discovery.

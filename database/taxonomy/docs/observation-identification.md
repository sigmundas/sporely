# Observation identification history contract

An identification records both current resolution and historical selection:

- current resolved `sporely_taxon_id`, nullable;
- immutable selected/source usage reference, nullable for manual entries;
- originally identified display/scientific name;
- source code;
- source identifier namespace and raw text value;
- pinned source release;
- identification timestamp;
- state: `resolved`, `unresolved`, `provisional`, or `manual`.

Current presentation may change after a rename, synonymization, split, or merge.
The recorded “identified as” snapshot does not. A later resolution appends or
updates resolution evidence without deleting the original selection.

For unresolved Artsorakel results, preserve the raw `NBIC:` identifier and every
returned accepted/former name used in fallback diagnostics. Never strip the
prefix and assume the suffix is a NorTaxa or Sporely ID.

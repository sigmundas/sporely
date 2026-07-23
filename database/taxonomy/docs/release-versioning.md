# Taxonomy release and versioning contract

`TAXONOMY_SCHEMA_VERSION = 2`.

A schema version changes only when a logical or physical contract consumed by
SQLite, cloud publication, or an application changes incompatibly or requires
new consumer behavior. Taxonomy content may change without a schema change.

Content release IDs use `tax-YYYY.MM.DD-NN`, for example
`tax-2026.07.23-01`. The sequence distinguishes candidates produced on the same
date. A correction gets a new ID; published artifacts and manifests are never
altered in place.

Source releases have independent IDs of the form
`<source-code>:<upstream-version-or-issued-date>`, such as
`col_xr:2026-07-17`. Immutable archives use
`<source>-<filesystem-safe-version>-<sha256-prefix>.<extension>` and retain the
full hash in their generated manifest. Floating `latest` is forbidden as a
compiler input. Application release, taxonomy schema, taxonomy content, and
source versions are separate dimensions. Generated manifests bind them together
and must not be hand-edited.

Each consumer declares a supported schema range and tested content release.
Desktop additionally records its bundled SQLite hash; cloud consumers record
the tested active cloud release. Additive fields and compatible content changes
do not require a schema increment. Removing/changing fields, identity semantics,
or required search behavior is breaking and requires a new schema version plus
a compatibility/rollback plan.

The machine contract is `policies/release_contract.yml`.

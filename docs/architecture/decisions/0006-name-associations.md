# ADR 0006: Name associations

- Status: Proposed (candidate `tax-2026.10.31-01`, not released)
- Date: 2026-10-31
- Amends: ADR 0002 ("name equality alone never establishes identity")

## Context

COL, NorTaxa and Dyntaxa were kept apart unless a reviewed bridge joined
them. Users saw the same species twice, Artsorakel ids stayed unlinked and
Norwegian/Swedish names were missing on web. The owner decided (2026-10-31)
to merge by default when scientific names match, accepting a small rate of
wrong associations for usability.

## Decision

Each release may associate a national Fungi usage with the live concept of
the same canonical name and rank, or through a unique synonym, preferring the
COL-backed concept (`policies/name_association_policy.json`). Genuinely
ambiguous cases are blocked: a name that two concepts of one source carry,
several candidate concepts, a qualified concept (coll., s.lat., s.str.,
sensu…), a candidate another usage of the same source already claims, a
continuity hold, or a listed exclusion.

An association is release content, not identity: it never allocates, retires
or renumbers a Sporely id. It is recorded in `taxon_association_min` and
projected onto the primary concept (ids, names, vernaculars, Red List), each
row labelled `name_association:<rule>`. Clients present member and primary as
one taxon; new selections bind the primary and record the association in
identity provenance.

## Consequences

An exclusion in the policy reverses an association in the next release.
Observations bound to the member keep their id; observations bound through
the association are listed with a recovery proposal by
`scripts/association_reversal_audit.py` (read-only). Reviewed supersessions
(Group B) stay as they are. The cloud accepts `name_association:` provenance
(sporely-web `20261031120000`).

## Rejected alternatives

- Thousands of permanent supersessions: irreversible, and they rewrite which
  id is current.
- Manual review of every pair: did not finish, and kept the picker confusing.

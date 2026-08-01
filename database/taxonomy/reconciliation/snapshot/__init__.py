"""W2D-R anonymised-snapshot source-recovery tooling.

Modules:
    pseudonym    HMAC-keyed deterministic observation-reference pseudonymisation.
    validator    Schema + privacy validator for the snapshot JSONL contract.
    transformer  Offline transformation from an authorised raw export to the
                 anonymised snapshot contract.
    cli          Command-line entry points for validate and transform.

No module in this package connects to production. See
`database/taxonomy/docs/w2d-input-snapshot-contract.md` and
`database/taxonomy/docs/w2d-source-recovery-runbook.md`.
"""

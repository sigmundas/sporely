"""Hard-fail invariant errors for the W2D reconciliation engine.

See Stage W2D §14 for the full invariant list. Any triggered invariant is a
build-time failure — the engine refuses to emit a manifest that violates
these guarantees.
"""

from __future__ import annotations


class ReconciliationError(Exception):
    """Base class for engine-side errors."""


class ReconciliationInvariantError(ReconciliationError):
    """Raised when a result set violates a Stage W2D §14 invariant.

    Examples:

    * two records share the same ``observation_id``;
    * a record reports more than one primary reconciliation state;
    * a record is ``resolved_*`` but has no ``resolution_evidence`` chain
      steps;
    * a record is unresolved but carries a non-null
      ``resolved_sporely_taxon_id``.
    """


class PolicyValidationError(ReconciliationError):
    """Raised when the loaded policy JSON is inconsistent with the contract.

    The engine refuses to load a policy that is missing a required primary
    state, resolution method, or namespace rule.
    """


class ReleaseValidationError(ReconciliationError):
    """Raised when the pinned release directory is missing a required file
    or contains a malformed JSONL row. The engine will not silently degrade
    to a partial release.
    """

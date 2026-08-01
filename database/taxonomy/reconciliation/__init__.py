"""Stage W2D historical-reconciliation engine.

The engine is deterministic, read-only, and takes an anonymised
``ReconciliationInput`` stream as input. It emits ``ReconciliationResult``
records and a manifest that a downstream cloud driver applies.

See ``database/taxonomy/docs/w2d-reconciliation-contract.md`` for the
contract. Nothing in this package touches production Supabase or the
observations database directly.
"""

from database.taxonomy.reconciliation.errors import (
    ReconciliationError,
    ReconciliationInvariantError,
)
from database.taxonomy.reconciliation.input_model import (
    Candidate,
    ChainStep,
    RawSignal,
    ReconciliationInput,
    ReconciliationResult,
)
from database.taxonomy.reconciliation.namespace_rules import (
    NamespaceRuleSet,
    load_policy,
)
from database.taxonomy.reconciliation.resolver import Resolver
from database.taxonomy.reconciliation.sources import PinnedRelease

__all__ = [
    "Candidate",
    "ChainStep",
    "NamespaceRuleSet",
    "PinnedRelease",
    "RawSignal",
    "ReconciliationError",
    "ReconciliationInput",
    "ReconciliationInvariantError",
    "ReconciliationResult",
    "Resolver",
    "load_policy",
]

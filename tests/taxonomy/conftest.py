"""Shared fixtures for W2D reconciliation tests.

Every test is self-contained and reads only the pinned macrofungi
release plus the accepted W2D policy. No test opens the observations
database, no test connects to Supabase, no test writes into the
canonical registry.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from database.taxonomy.reconciliation.namespace_rules import load_policy
from database.taxonomy.reconciliation.resolver import Resolver
from database.taxonomy.reconciliation.sources import PinnedRelease


REPO_ROOT = Path(__file__).resolve().parents[2]
RELEASE_DIR = (
    REPO_ROOT
    / "database"
    / "reference_data"
    / "generated"
    / "taxonomy_v2"
    / "global_macrofungi_tax-2026.08.01-01"
)
POLICY_PATH = (
    REPO_ROOT
    / "database"
    / "taxonomy"
    / "policies"
    / "w2d-reconciliation-policy.json"
)
FIXTURES_DIR = (
    REPO_ROOT
    / "database"
    / "taxonomy"
    / "reconciliation"
    / "fixtures"
)


@pytest.fixture(scope="session")
def rule_set():
    """Load the policy once per test session."""
    return load_policy(POLICY_PATH)


@pytest.fixture(scope="session")
def release():
    """Load the pinned macrofungi release once per test session."""
    return PinnedRelease.load(RELEASE_DIR)


@pytest.fixture()
def resolver(release, rule_set):
    return Resolver(release=release, rule_set=rule_set)


@pytest.fixture()
def fixtures_dir():
    return FIXTURES_DIR

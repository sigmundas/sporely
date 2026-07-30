"""Tests for the append-only Sporely taxonomy identity registry."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from identity_registry import (  # noqa: E402
    ENTRY_KIND_ALIAS,
    ENTRY_KIND_ANCHOR,
    IdentityRegistry,
    RegistryError,
)


def _fresh(path: Path) -> IdentityRegistry:
    reg = IdentityRegistry(path)
    reg.load()
    return reg


def test_allocate_assigns_monotonic_ids(tmp_path: Path) -> None:
    reg = _fresh(tmp_path / "registry.jsonl")
    a = reg.allocate(source="col_xr", namespace="col_usage_id", identifier="A",
                     allocated_in_release="tax-2026.07.28-01",
                     first_seen_source_release="2026-07-17")
    b = reg.allocate(source="col_xr", namespace="col_usage_id", identifier="B",
                     allocated_in_release="tax-2026.07.28-01",
                     first_seen_source_release="2026-07-17")
    assert (a.sporely_taxon_id, b.sporely_taxon_id) == (1, 2)


def test_allocate_is_idempotent(tmp_path: Path) -> None:
    reg = _fresh(tmp_path / "registry.jsonl")
    a1 = reg.allocate(source="s", namespace="n", identifier="i",
                      allocated_in_release="tax-2026.07.28-01",
                      first_seen_source_release="v1")
    a2 = reg.allocate(source="s", namespace="n", identifier="i",
                      allocated_in_release="tax-2027.01.01-01",
                      first_seen_source_release="v9")
    assert a1 == a2


def test_flush_and_reload_are_deterministic(tmp_path: Path) -> None:
    path = tmp_path / "registry.jsonl"
    reg1 = _fresh(path)
    for i in ("Z", "A", "M"):
        reg1.allocate(source="s", namespace="n", identifier=i,
                      allocated_in_release="tax-2026.07.28-01",
                      first_seen_source_release="v1")
    reg1.flush()
    payload1 = path.read_bytes()

    reg2 = _fresh(path)
    reg2.flush()  # loaded state → same bytes
    payload2 = path.read_bytes()

    assert payload1 == payload2
    # And the header sits on line 1 with anchors ordered by sporely_taxon_id.
    lines = payload1.decode("utf-8").rstrip("\n").split("\n")
    header = json.loads(lines[0])
    assert header["registry_schema_version"] == 1
    body = [json.loads(line) for line in lines[1:]]
    assert [entry["sporely_taxon_id"] for entry in body] == [1, 2, 3]


def test_bind_alias_shares_identity(tmp_path: Path) -> None:
    reg = _fresh(tmp_path / "registry.jsonl")
    anchor = reg.allocate(source="col_xr", namespace="col_usage_id",
                          identifier="X",
                          allocated_in_release="tax-2026.07.28-01",
                          first_seen_source_release="v1")
    alias = reg.bind_alias(
        existing_sporely_taxon_id=anchor.sporely_taxon_id,
        source="nortaxa", namespace="nortaxa_taxon_id", identifier="42",
        allocated_in_release="tax-2026.07.28-01",
        first_seen_source_release="1.284",
    )
    assert alias.sporely_taxon_id == anchor.sporely_taxon_id
    assert alias.kind == ENTRY_KIND_ALIAS
    resolved = reg.lookup("nortaxa", "nortaxa_taxon_id", "42")
    assert resolved is not None
    assert resolved.sporely_taxon_id == anchor.sporely_taxon_id


def test_alias_conflict_fails_closed(tmp_path: Path) -> None:
    reg = _fresh(tmp_path / "registry.jsonl")
    a = reg.allocate(source="col_xr", namespace="col_usage_id", identifier="X",
                     allocated_in_release="tax-2026.07.28-01",
                     first_seen_source_release="v1")
    b = reg.allocate(source="col_xr", namespace="col_usage_id", identifier="Y",
                     allocated_in_release="tax-2026.07.28-01",
                     first_seen_source_release="v1")
    reg.bind_alias(
        existing_sporely_taxon_id=a.sporely_taxon_id,
        source="nortaxa", namespace="nortaxa_taxon_id", identifier="1",
        allocated_in_release="tax-2026.07.28-01",
        first_seen_source_release="1.284",
    )
    with pytest.raises(RegistryError, match="alias conflict"):
        reg.bind_alias(
            existing_sporely_taxon_id=b.sporely_taxon_id,
            source="nortaxa", namespace="nortaxa_taxon_id", identifier="1",
            allocated_in_release="tax-2026.07.28-01",
            first_seen_source_release="1.284",
        )


def test_existing_registry_preserves_ids_when_new_source_appears(tmp_path: Path) -> None:
    path = tmp_path / "registry.jsonl"
    reg1 = _fresh(path)
    a = reg1.allocate(source="col_xr", namespace="col_usage_id", identifier="X",
                      allocated_in_release="tax-2026.07.28-01",
                      first_seen_source_release="v1")
    reg1.flush()

    reg2 = _fresh(path)
    # Adding a fresh nortaxa allocation must not renumber X.
    reg2.allocate(source="nortaxa", namespace="nortaxa_taxon_id", identifier="42",
                  allocated_in_release="tax-2026.07.29-01",
                  first_seen_source_release="1.284")
    reg2.flush()

    reg3 = _fresh(path)
    still = reg3.lookup("col_xr", "col_usage_id", "X")
    assert still is not None and still.sporely_taxon_id == a.sporely_taxon_id
    assert still.kind == ENTRY_KIND_ANCHOR


def test_registry_rejects_hand_edits_that_introduce_duplicates(tmp_path: Path) -> None:
    path = tmp_path / "registry.jsonl"
    reg = _fresh(path)
    reg.allocate(source="s", namespace="n", identifier="A",
                 allocated_in_release="tax-2026.07.28-01",
                 first_seen_source_release="v1")
    reg.flush()
    with path.open("a", encoding="utf-8") as handle:
        # Duplicate key with the same anchor sporely_taxon_id — forbidden.
        handle.write(json.dumps({
            "sporely_taxon_id": 1, "source": "s", "namespace": "n",
            "identifier": "A", "allocated_in_release": "tax-2026.07.28-01",
            "first_seen_source_release": "v1", "kind": "anchor",
        }, sort_keys=True) + "\n")
    reloaded = IdentityRegistry(path)
    with pytest.raises(RegistryError, match="duplicates key"):
        reloaded.load()

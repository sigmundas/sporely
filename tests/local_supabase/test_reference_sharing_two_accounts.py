"""Two synthetic accounts: default reference sharing end to end.

Real desktop code for both accounts (sync, catalogue reader, copy, the client
methods the My shared references dialog calls); real server functions.
SQL is used only for setup without a client path: the registry species and
the resolved taxon production's resolver would assign. Moderation uses
``public.moderate_shared_reference_contribution`` as service_role, as the
moderation tooling does.

Relationship role (step 7): the role belongs to the observation-reference
*use*, not to the publication. The public envelope reports the owner's
current relationship (``relationship_roles``, live state, never frozen
provenance; ``database/curated_reference_forks.py`` _LIVE_ONLY_SHARED_KEYS).
A copy creates a fresh private work/treatment/set and no use, so it carries
no role at all.
"""
from __future__ import annotations

import json
import random

import pytest
import requests

from tests.local_supabase.conftest import SERVICE_KEY, URL, sql


def _setup_species(owner_id: str) -> int:
    taxon = random.randint(900_000, 999_999)
    sql("INSERT INTO taxonomy_v3.registry_concept (sporely_taxon_id, canonical_name, rank, "
        f"cache_state, first_materialized_from_release) VALUES ({taxon}, 'Russula paludosa', "
        "'species', 'in_cache', 'harness') ON CONFLICT DO NOTHING")
    sql("BEGIN; SET LOCAL ROLE service_role; "
        "SELECT set_config('request.jwt.claims', '{\"role\":\"service_role\"}', true); "
        f"UPDATE public.observations SET resolved_sporely_taxon_id = {taxon} "
        f"WHERE user_id = '{owner_id}'; COMMIT;")
    return taxon


def _server_graph(owner_id: str) -> str:
    """Every server row of A's reference graph, uses and observations."""
    parts = []
    for table in ("reference_works", "reference_taxon_treatments", "reference_measurement_sets",
                  "observation_reference_uses", "observations"):
        rows = sql(f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY t.id), '[]') "
                   f"FROM public.{table} t WHERE t.user_id = '{owner_id}'")
        parts.append(f"{table}={rows[0][0]}")
    return "\n".join(parts)


def _moderate(contribution_id: str, action: str, reason: str | None) -> dict:
    response = requests.post(
        f"{URL}/rest/v1/rpc/moderate_shared_reference_contribution",
        headers={"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}"},
        json={"p_contribution_id": contribution_id, "p_action": action, "p_reason": reason},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


FORK_BUG = (
    "sporely-web public.sync_reference_curated_fork (20260830120000) accepts only "
    "legacy curated publications; a copy of a shared contribution is refused with "
    "invalid_source on every sync of the copying account"
)


def _assert_only_fork_bug(errors: list[str]) -> None:
    """B's sync is clean except for the known fork-provenance refusal."""
    assert all("remote status invalid_source" in error and error.startswith("curated fork ")
               for error in errors), errors


def _ids(search: dict) -> list[str]:
    return [bundle["contribution_id"] for bundle in search["bundles"]]


def test_two_account_default_sharing_lifecycle(make_user, device):
    owner, reader = make_user(), make_user()
    a, b = device("a"), device("b")

    # 1. A attaches a reference with role `contradicts` to a public,
    #    non-draft observation of a registry species -> shared by default.
    created = a.run("create_set")
    a.run("attach_public_use", set_id=created["set_id"], role="contradicts")
    sync_a = a.run("sync", **owner.credentials)
    assert sync_a["errors"] == []
    taxon = _setup_species(owner.id)
    assert sql("SELECT status, hidden_at IS NULL FROM private.shared_reference_contributions "
               f"WHERE owner_id = '{owner.id}'") == [["shared", "t"]]

    # A pulls the server-side resolution once, so later comparisons of A's
    # local rows see only what B's actions could change.
    assert a.run("sync", **owner.credentials)["errors"] == []

    # 2. B finds it through the desktop catalogue (signed in, opt-in [1,2]).
    found = b.run("search", taxon_id=taxon, **reader.credentials)
    assert len(found["bundles"]) == 1
    contribution = found["bundles"][0]["contribution_id"]
    # 7a. The envelope reports the owner's relationship as contradicting.
    assert found["bundles"][0]["relationship_roles"] == ["contradicts"]
    assert found["bundles"][0]["relationship_label"] == "Contradicts"

    server_before = _server_graph(owner.id)
    a_local_before = a.run("graph")["tables"]

    # 3. B copies it into B's own library; B's copy is owned by B.
    copied = b.run("search", taxon_id=taxon, copy_contribution_id=contribution,
                   **reader.credentials)["copy"]
    assert copied["created"] is True
    assert copied["set_id"] != created["set_id"]
    _assert_only_fork_bug(b.run("sync", **reader.credentials)["errors"])
    owners = sql("SELECT user_id::text FROM public.reference_measurement_sets "
                 f"WHERE id = '{copied['set_id']}'")
    assert owners == [[reader.id]]
    # 7b. The copy carries no role: no observation use exists for it.
    b_graph = b.run("graph")["tables"]
    assert b_graph["observation_reference_uses"] == []
    assert [fork["reference_measurement_set_id"] for fork in b_graph["curated_reference_forks"]] == [
        copied["set_id"]
    ]

    # 8. B's later edit + sync leaves A's server and local rows untouched.
    b.run("edit_set", set_id=copied["set_id"], notes="reader's own note")
    _assert_only_fork_bug(b.run("sync", **reader.credentials)["errors"])
    assert a.run("sync", **owner.credentials)["errors"] == []
    assert _server_graph(owner.id) == server_before
    a_local_after = a.run("graph")["tables"]
    for table in ("reference_works", "reference_taxon_treatments",
                  "reference_measurement_sets", "observation_reference_uses", "observations"):
        # ``observations.synced_at`` is A's own sync bookkeeping (A synced
        # again to pull anything B could have caused); every other column must
        # be identical.
        strip = (lambda rows: [{k: v for k, v in row.items() if k != "synced_at"} for row in rows])
        assert strip(a_local_after[table]) == strip(a_local_before[table]), table

    # 4. A stops sharing (My shared references client method) -> gone for B and anon.
    listed = a.run("sharing", op="list", **owner.credentials)["result"]
    assert listed["status"] == "ok"
    stop = a.run("sharing", op="stop", set_id=created["set_id"], **owner.credentials)
    assert stop["ok"] is True, stop
    assert _ids(b.run("search", taxon_id=taxon, **reader.credentials)) == []
    assert _ids(b.run("search", taxon_id=taxon)) == []

    # 5. A shares again -> back in the catalogue.
    again = a.run("sharing", op="share_again", set_id=created["set_id"], **owner.credentials)
    assert again["ok"] is True, again
    assert _ids(b.run("search", taxon_id=taxon, **reader.credentials)) == [contribution]
    assert _ids(b.run("search", taxon_id=taxon)) == [contribution]

    # 6. Moderation hide -> unavailable; the owner's share-again cannot lift it.
    hidden = _moderate(contribution, "hide", "abuse")
    assert hidden.get("status") not in {"invalid_payload", "not_found"}, hidden
    assert _ids(b.run("search", taxon_id=taxon, **reader.credentials)) == []
    assert _ids(b.run("search", taxon_id=taxon)) == []
    a.run("sharing", op="stop", set_id=created["set_id"], **owner.credentials)
    retry = a.run("sharing", op="share_again", set_id=created["set_id"], **owner.credentials)
    assert _ids(b.run("search", taxon_id=taxon)) == [], json.dumps(retry["result"])
    assert sql("SELECT hidden_reason FROM private.shared_reference_contributions "
               f"WHERE id = '{contribution}'") == [["abuse"]]


@pytest.mark.xfail(reason=FORK_BUG, strict=True)
def test_copy_of_a_shared_contribution_syncs_cleanly(make_user, device):
    owner, reader = make_user(), make_user()
    a, b = device("a"), device("b")
    created = a.run("create_set")
    a.run("attach_public_use", set_id=created["set_id"], role="contradicts")
    assert a.run("sync", **owner.credentials)["errors"] == []
    taxon = _setup_species(owner.id)
    contribution = b.run("search", taxon_id=taxon)["bundles"][0]["contribution_id"]
    b.run("search", taxon_id=taxon, copy_contribution_id=contribution)
    assert b.run("sync", **reader.credentials)["errors"] == []

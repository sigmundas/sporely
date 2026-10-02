"""Desktop reference sync against a real local Supabase (Stage M scenarios)."""
from __future__ import annotations

import pytest

from tests.local_supabase.conftest import sql


def _devices(user_id: str) -> list[list[str]]:
    return sql(
        "SELECT device_id::text, client, reference_snapshot_versions::text "
        f"FROM public.reference_client_devices WHERE user_id = '{user_id}' ORDER BY device_id"
    )


def _remote_set(set_id: str) -> list[list[str]]:
    return sql(
        "SELECT id::text, (deleted_at IS NOT NULL)::text "
        f"FROM public.reference_measurement_sets WHERE id = '{set_id}'"
    )


def _assert_clean(result: dict) -> None:
    assert result["errors"] == [], result["errors"]


@pytest.fixture()
def owner(make_user):
    return make_user()


def test_1_sign_in_and_sync_registers_one_capable_device(owner, device):
    a = device("a")
    a.run("create_set")
    result = a.run("sync", **owner.credentials)
    _assert_clean(result)
    assert result["rpc_calls"].get("record_reference_client_capabilities") == 1
    rows = _devices(owner.id)
    assert rows == [[a.device_id, "desktop_app", "{1,2}"]]
    assert all(row[0] != "00000000-0000-0000-0000-000000000000" for row in rows)


def test_2_no_change_sync_keeps_one_device(owner, device):
    a = device("a")
    a.run("create_set")
    _assert_clean(a.run("sync", **owner.credentials))
    second = a.run("sync", **owner.credentials)
    _assert_clean(second)
    assert len(_devices(owner.id)) == 1
    assert second["rpc_calls"].get("record_reference_client_capabilities") == 1


@pytest.mark.xfail(reason="issue #11: pre-existing reference re-push on a no-change sync", strict=False)
def test_2b_no_change_sync_sends_no_reference_writes(owner, device):
    a = device("a")
    created = a.run("create_set")
    a.run("attach_public_use", set_id=created["set_id"])
    _assert_clean(a.run("sync", **owner.credentials))
    second = a.run("sync", **owner.credentials)
    writes = {
        name: count for name, count in second["rpc_calls"].items()
        if name.startswith("sync_reference_") or name == "sync_observation_reference_use"
    }
    assert writes == {}


def test_3_download_from_cloud_receives_the_set_without_writes(owner, device):
    a, b = device("a"), device("b")
    created = a.run("create_set")
    _assert_clean(a.run("sync", **owner.credentials))

    pulled = b.run("pull_only", **owner.credentials)

    _assert_clean(pulled)
    assert created["set_id"] in b.run("local_sets")["sets"]
    assert pulled["cloud_writes_completed"] == 0
    assert pulled["blocked_write_attempts"] == []
    assert "record_reference_client_capabilities" not in pulled["rpc_calls"]
    assert [row[0] for row in _devices(owner.id)] == [a.device_id]


def test_4_deletion_on_b_arrives_on_a(owner, device):
    a, b = device("a"), device("b")
    created = a.run("create_set")
    _assert_clean(a.run("sync", **owner.credentials))
    _assert_clean(b.run("sync", **owner.credentials))
    assert created["set_id"] in b.run("local_sets")["sets"]

    b.run("delete_set", set_id=created["set_id"])
    _assert_clean(b.run("sync", **owner.credentials))
    assert _remote_set(created["set_id"]) == [[created["set_id"], "true"]]

    _assert_clean(a.run("sync", **owner.credentials))
    assert created["set_id"] not in a.run("local_sets")["sets"]
    assert sorted(row[0] for row in _devices(owner.id)) == sorted([a.device_id, b.device_id])


def test_5_catalogue_search_and_copy_of_a_default_shared_contribution(owner, device):
    """A public, exact-species observation use is shared by default; another
    device finds it through the desktop's anon catalogue reader and copies it."""
    import random

    a, b = device("a"), device("b")
    created = a.run("create_set")
    a.run("attach_public_use", set_id=created["set_id"])
    _assert_clean(a.run("sync", **owner.credentials))

    # Server-side setup the local stack lacks: one species concept in the
    # taxonomy registry, and the resolved taxon production's resolver would
    # assign (as service_role, which the sharing trigger accepts).
    taxon = random.randint(900_000, 999_999)
    sql("INSERT INTO taxonomy_v3.registry_concept (sporely_taxon_id, canonical_name, rank, "
        f"cache_state, first_materialized_from_release) VALUES ({taxon}, 'Russula paludosa', "
        "'species', 'in_cache', 'harness') ON CONFLICT DO NOTHING")
    sql("BEGIN; SET LOCAL ROLE service_role; "
        "SELECT set_config('request.jwt.claims', '{\"role\":\"service_role\"}', true); "
        f"UPDATE public.observations SET resolved_sporely_taxon_id = {taxon} "
        f"WHERE user_id = '{owner.id}'; COMMIT;")
    assert sql("SELECT status FROM private.shared_reference_contributions "
               f"WHERE owner_id = '{owner.id}'") == [["shared"]]

    found = b.run("catalogue", taxon_id=taxon, copy=True)

    assert len(found["contributions"]) == 1
    assert found["created"] is True
    assert found["copied_set_id"] in b.run("local_sets")["sets"]


def test_6_undeclared_write_registers_the_legacy_pseudo_device(owner, device):
    """An older desktop (no p_client_capabilities) is recorded as undeclared."""
    a = device("a")
    a.run("create_set")
    _assert_clean(a.run("sync", **owner.credentials))
    work_id = sql(
        "SELECT id::text, row_version FROM public.reference_works "
        f"WHERE user_id = '{owner.id}' LIMIT 1"
    )[0]
    # Same RPC a released desktop calls, without the capability parameter,
    # as the owner (request.jwt.claims), through the real wrapper.
    sql(
        "BEGIN; SET LOCAL ROLE authenticated; "
        f"SELECT set_config('request.jwt.claims', '{{\"sub\":\"{owner.id}\",\"role\":\"authenticated\"}}', true); "
        "SELECT public.sync_reference_measurement_set("
        "(SELECT to_jsonb(m) - 'user_id' - 'row_version' - 'created_at' - 'updated_at' - 'deleted_at' "
        f"   || jsonb_build_object('deleted', false) FROM public.reference_measurement_sets m WHERE m.user_id = '{owner.id}' LIMIT 1), "
        f"(SELECT row_version FROM public.reference_measurement_sets WHERE user_id = '{owner.id}' LIMIT 1)); "
        "COMMIT;"
    )
    rows = _devices(owner.id)
    assert ["00000000-0000-0000-0000-000000000000", "undeclared", "{1}"] in rows
    assert work_id


@pytest.mark.skip(reason="withheld v2 content needs enhanced sets; measurement-content gates are closed "
                         "and the v2 snapshot migration is deferred in production")
def test_6b_withheld_v2_content():
    pass

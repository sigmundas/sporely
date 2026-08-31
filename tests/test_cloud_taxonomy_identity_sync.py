from __future__ import annotations

from utils.cloud_sync import SporelyCloudClient


def _client_with_existing_observation(remote: dict):
    client = object.__new__(SporelyCloudClient)
    client.user_id = "00000000-0000-4000-8000-000000000001"
    client._resolve_existing_observation_for_push = lambda _obs, remote_obs=None: "1184"
    client._patch = lambda *_args, **_kwargs: None
    calls: list[tuple[str, dict]] = []
    client._rpc = lambda name, payload: calls.append((name, payload))
    return client, calls


def test_taxonomy_aware_observation_syncs_exact_selected_taxon():
    remote = {
        "id": 1184,
        "selected_sporely_taxon_id": None,
    }
    client, calls = _client_with_existing_observation(remote)

    client.push_observation(
        {
            "id": 42,
            "cloud_id": "1184",
            "sporely_taxon_id": 634856,
        },
        remote_obs=remote,
    )

    assert calls == [
        (
            "set_observation_selected_taxon_v2",
            {
                "p_observation_id": 1184,
                "p_sporely_taxon_id": 634856,
            },
        )
    ]


def test_exact_taxon_sync_skips_noop_and_never_infers_from_name_text():
    matching = {"id": 1184, "selected_sporely_taxon_id": 634856}
    client, calls = _client_with_existing_observation(matching)
    client.push_observation(
        {
            "id": 42,
            "cloud_id": "1184",
            "sporely_taxon_id": 634856,
            "genus": "Amanita",
            "species": "muscaria",
        },
        remote_obs=matching,
    )
    assert calls == []

    unresolved = {"id": 1185, "selected_sporely_taxon_id": None}
    client, calls = _client_with_existing_observation(unresolved)
    client._resolve_existing_observation_for_push = (
        lambda _obs, remote_obs=None: "1185"
    )
    client.push_observation(
        {
            "id": 43,
            "cloud_id": "1185",
            "sporely_taxon_id": None,
            "genus": "Amanita",
            "species": "muscaria",
        },
        remote_obs=unresolved,
    )
    assert calls == []

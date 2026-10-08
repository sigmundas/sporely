"""Frozen taxonomy-identity golden for the cloud-sync extraction.

The fixture ``tests/fixtures/cloud_sync_taxonomy_identity_golden.json`` was
captured from the code at the extraction base. The test asserts the current
code reproduces it exactly, so relocating the identity helpers cannot change
what they compute (invariant 5 of the extraction plan).

The fixture must not change while the extraction stages run. Regenerating it
(``python tests/test_cloud_sync_taxonomy_identity_golden.py --write``) is only
legitimate for an intentional, reviewed identity behavior change.
"""

from __future__ import annotations

import dataclasses
import json
import sys
from pathlib import Path

if __name__ == "__main__":  # allow ``python tests/<this file> --write``
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils import cloud_sync
from utils.taxon_identity import TaxonIdentity

GOLDEN_PATH = Path(__file__).parent / "fixtures" / "cloud_sync_taxonomy_identity_golden.json"

#: Stands in for ``_IDENTITY_BASELINE_UNKNOWN`` in the JSON fixture.
BASELINE_UNKNOWN = "<IDENTITY_BASELINE_UNKNOWN>"

#: A Sporely id that the installed taxonomy does not contain. None of the
#: helpers under test consult the taxonomy artifact, so the outputs must not
#: depend on whether one is installed.
ABSENT_SPORELY_ID = 987654321

PROVEN_ID = 634856
OTHER_ID = 112233


def _proven(sporely_id: int) -> dict:
    return TaxonIdentity.from_taxonomy_v2_artifact(sporely_id).to_row()


def _cloud_selected(sporely_id: int) -> dict:
    return TaxonIdentity.from_cloud_selection(
        sporely_id, local_release_id="taxonomy-v2-test-release", cloud_state="sporely_v2"
    ).to_row()


def _external(source_system: str, namespace: str, external_id: str, raw: str | None = None) -> dict:
    return TaxonIdentity.unresolved_external(
        source_system=source_system,
        namespace=namespace,
        external_id=external_id,
        raw_external_id=raw,
    ).to_row()


def _local(identity: dict, **fields) -> dict:
    row = {
        "id": 42,
        "cloud_id": "1184",
        "genus": "Flammulina",
        "species": "velutipes",
        "date": "2026-05-01",
    }
    row.update(identity)
    row.update(fields)
    return row


def _remote(**fields) -> dict:
    row = {
        "id": 1184,
        "desktop_id": 42,
        "genus": "Flammulina",
        "species": "velutipes",
        "date": "2026-05-01",
    }
    row.update(fields)
    return row


def _remote_sporely(sporely_id: int, **fields) -> dict:
    return _remote(
        selected_sporely_taxon_id=sporely_id,
        taxon_identity_state="sporely_v2",
        taxon_identity_source_system="sporely",
        taxon_identity_namespace="sporely_taxon_id",
        taxon_identity_external_id=str(sporely_id),
        **fields,
    )


def _remote_external(source_system, namespace, external_id, raw=None, **fields) -> dict:
    return _remote(
        selected_sporely_taxon_id=None,
        taxon_identity_state="external_unresolved",
        taxon_identity_source_system=source_system,
        taxon_identity_namespace=namespace,
        taxon_identity_external_id=external_id,
        taxon_identity_raw_external_id=raw,
        **fields,
    )


def _remote_none(**fields) -> dict:
    return _remote(selected_sporely_taxon_id=None, taxon_identity_state=None, **fields)


def _baseline(key: str | None) -> dict:
    """A snapshot observation that recorded identity ``key``; ``None`` = pre-identity."""
    row = {"id": 1184, "desktop_id": 42, "genus": "Flammulina", "species": "velutipes"}
    if key is not None:
        row[cloud_sync.TAXON_IDENTITY_SYNC_FIELD] = key
    return row


NORTAXA = ("nortaxa", "nortaxa_taxon_id")
SPORELY_K = f"sporely:{PROVEN_ID}"


def _cases() -> dict[str, dict]:
    """Named (local, remote, baseline) rows. Row classes required by Stage S2 are noted."""
    return {
        # proven Sporely identity (sporely_taxon_id + source + namespace)
        "proven_sporely_matches_remote": dict(
            local=_local(_proven(PROVEN_ID)), remote=_remote_sporely(PROVEN_ID), baseline=_baseline(SPORELY_K)),
        "proven_sporely_local_change": dict(
            local=_local(_proven(OTHER_ID)), remote=_remote_sporely(PROVEN_ID), baseline=_baseline(SPORELY_K)),
        "proven_sporely_remote_change_conflict": dict(
            local=_local(_proven(OTHER_ID)), remote=_remote_sporely(555), baseline=_baseline(SPORELY_K)),
        "proven_sporely_shared_move": dict(
            local=_local(_proven(OTHER_ID)), remote=_remote_sporely(OTHER_ID), baseline=_baseline(SPORELY_K)),
        "proven_sporely_remote_none": dict(
            local=_local(_proven(PROVEN_ID)), remote=_remote_none(), baseline=_baseline("")),
        # cloud-selected unverified identity
        "cloud_selected_unverified_matches": dict(
            local=_local(_cloud_selected(PROVEN_ID)), remote=_remote_sporely(PROVEN_ID), baseline=_baseline(SPORELY_K)),
        "cloud_selected_unverified_remote_change": dict(
            local=_local(_cloud_selected(PROVEN_ID)), remote=_remote_sporely(OTHER_ID), baseline=_baseline(SPORELY_K)),
        # legacy unverified integer (no state column)
        "legacy_unverified_integer": dict(
            local=_local({"sporely_taxon_id": PROVEN_ID}), remote=_remote_sporely(PROVEN_ID), baseline=_baseline(SPORELY_K)),
        # external-only evidence
        "external_only_local_remote_none": dict(
            local=_local(_external(*NORTAXA, "53482", "NBIC:53482")), remote=_remote_none(), baseline=_baseline("")),
        # unresolved external evidence on the remote
        "external_unresolved_remote_local_none": dict(
            local=_local(TaxonIdentity.none().to_row()),
            remote=_remote_external(*NORTAXA, "53482", "NBIC:53482"), baseline=_baseline("")),
        "external_unresolved_both_match": dict(
            local=_local(_external(*NORTAXA, "53482")),
            remote=_remote_external(*NORTAXA, "53482"), baseline=_baseline("external:nortaxa:nortaxa_taxon_id:53482")),
        "external_remote_incomplete_tuple": dict(
            local=_local(TaxonIdentity.none().to_row()),
            remote=_remote_external("nortaxa", None, "53482"), baseline=_baseline("")),
        # an external integer colliding numerically with a Sporely id
        "external_integer_collides_with_sporely_id": dict(
            local=_local(_external(*NORTAXA, str(PROVEN_ID))),
            remote=_remote_sporely(PROVEN_ID), baseline=_baseline("")),
        "external_integer_collides_remote_side": dict(
            local=_local(_proven(PROVEN_ID)),
            remote=_remote_external(*NORTAXA, str(PROVEN_ID)), baseline=_baseline(SPORELY_K)),
        # Sporely-namespace external (unconfirmable cloud Sporely id)
        "sporely_namespace_external": dict(
            local=_local(_external("sporely", "sporely_taxon_id", str(PROVEN_ID))),
            remote=_remote_sporely(PROVEN_ID), baseline=_baseline(SPORELY_K)),
        # no identity
        "no_identity_both": dict(
            local=_local(TaxonIdentity.none().to_row()), remote=_remote_none(), baseline=_baseline("")),
        "no_identity_remote_without_identity_columns": dict(
            local=_local(TaxonIdentity.none().to_row()), remote=_remote(), baseline=_baseline("")),
        "manual_text_local": dict(
            local=_local(TaxonIdentity.manual_text("Flammulina velutipes").to_row()),
            remote=_remote_sporely(PROVEN_ID), baseline=_baseline("")),
        # an id absent from the installed taxonomy
        "absent_id_proven_local": dict(
            local=_local(_proven(ABSENT_SPORELY_ID)), remote=_remote_none(), baseline=_baseline("")),
        "absent_id_remote_only": dict(
            local=_local(TaxonIdentity.none().to_row()),
            remote=_remote_sporely(ABSENT_SPORELY_ID), baseline=_baseline("")),
        # a pre-identity snapshot (baseline -> _IDENTITY_BASELINE_UNKNOWN)
        "pre_identity_snapshot_remote_claim": dict(
            local=_local(TaxonIdentity.none().to_row()), remote=_remote_sporely(PROVEN_ID), baseline=_baseline(None)),
        "pre_identity_snapshot_two_claims_disagree": dict(
            local=_local(_proven(OTHER_ID)), remote=_remote_sporely(PROVEN_ID), baseline=_baseline(None)),
        "pre_identity_snapshot_proven_local_remote_none": dict(
            local=_local(_proven(PROVEN_ID)), remote=_remote_none(), baseline=_baseline(None)),
        "pre_identity_snapshot_agree": dict(
            local=_local(_proven(PROVEN_ID)), remote=_remote_sporely(PROVEN_ID), baseline=_baseline(None)),
        "no_baseline_at_all": dict(
            local=_local(_proven(PROVEN_ID)), remote=_remote_sporely(OTHER_ID), baseline=None),
        # rows differing only in display / scientific / vernacular text
        "text_only_difference": dict(
            local=_local(
                _proven(PROVEN_ID), genus="Collybia", species="velutipes",
                scientific_name="Collybia velutipes", vernacular_name="vinterjuvelsopp",
                species_guess="Collybia velutipes",
            ),
            remote=_remote_sporely(
                PROVEN_ID, genus="Flammulina", species="velutipes",
                scientific_name="Flammulina velutipes", vernacular_name="Vinterjuvelsopp",
                species_guess="Flammulina velutipes",
            ),
            baseline=_baseline(SPORELY_K)),
        # ... or only in case and whitespace
        "case_whitespace_only_difference": dict(
            local=_local(_proven(PROVEN_ID), genus=" flammulina ", species="VELUTIPES "),
            remote=_remote_sporely(PROVEN_ID, genus="Flammulina", species="velutipes"),
            baseline=_baseline(SPORELY_K)),
        "external_case_whitespace_only": dict(
            local=_local(_external(" nortaxa ", "nortaxa_taxon_id", " 53482 ")),
            remote=_remote_external("nortaxa", " nortaxa_taxon_id ", "53482"),
            baseline=_baseline("external:nortaxa:nortaxa_taxon_id:53482")),
    }


#: Direct `_identity_sync_key` inputs: (kind, sporely_taxon_id, source, namespace, external_id).
_SYNC_KEY_INPUTS = [
    ("sporely", PROVEN_ID, None, None, None),
    ("sporely", "634856", "sporely", "sporely_taxon_id", "634856"),
    ("sporely", 0, None, None, None),
    ("sporely", None, "sporely", "sporely_taxon_id", "1"),
    ("external", None, "nortaxa", "nortaxa_taxon_id", "53482"),
    ("external", PROVEN_ID, "nortaxa", "nortaxa_taxon_id", str(PROVEN_ID)),
    ("external", None, "nortaxa", None, "53482"),
    ("none", PROVEN_ID, "nortaxa", "nortaxa_taxon_id", "53482"),
    ("legacy", PROVEN_ID, None, None, None),
]


def _baseline_out(value):
    return BASELINE_UNKNOWN if value is cloud_sync._IDENTITY_BASELINE_UNKNOWN else value


class _FakeSettingsDB:
    """In-memory stand-in for ``SettingsDB`` so the persisted snapshot text is captured."""

    def __init__(self):
        self.values: dict[str, str] = {}

    def get_setting(self, key, default=None):
        return self.values.get(key, default)

    def set_setting(self, key, value):
        self.values[key] = value


def _persisted_snapshot(snapshot: str) -> dict:
    """Store and reload ``snapshot`` through the production snapshot helpers."""
    fake = _FakeSettingsDB()
    original = cloud_sync.SettingsDB
    cloud_sync.SettingsDB = fake
    try:
        cloud_sync._store_cloud_observation_snapshot("1184", snapshot)
        stored = dict(fake.values)
        loaded = cloud_sync._load_cloud_observation_snapshot("1184")
    finally:
        cloud_sync.SettingsDB = original
    return {"stored_settings": stored, "loaded": loaded}


def _compute_case(case: dict) -> dict:
    local, remote, baseline = case["local"], case["remote"], case["baseline"]
    claim = cloud_sync._remote_identity_claim(remote)
    field = cloud_sync.TAXON_IDENTITY_SYNC_FIELD
    snapshot = cloud_sync._cloud_observation_snapshot(
        remote, [], [], include_images=False, include_measurements=False
    )
    return {
        "remote_identity_claim": None if claim is None else {**dataclasses.asdict(claim), "key": claim.key},
        "local_identity_sync_key": cloud_sync._local_identity_sync_key(local),
        "baseline_identity_key": _baseline_out(cloud_sync._baseline_identity_key(baseline)),
        "classify_identity_sync_change": {
            "identification_locally_owned=False": cloud_sync._classify_identity_sync_change(
                local, remote, baseline, identification_locally_owned=False),
            "identification_locally_owned=True": cloud_sync._classify_identity_sync_change(
                local, remote, baseline, identification_locally_owned=True),
        },
        "remote_identity_changed_since": cloud_sync._remote_identity_changed_since(remote, baseline),
        "compare_payload_identity": {
            "local": cloud_sync._observation_compare_payload(local, local=True)[field],
            "remote": cloud_sync._observation_compare_payload(remote, local=False)[field],
            "baseline": cloud_sync._baseline_observation_compare_payload(baseline).get(field, BASELINE_UNKNOWN),
        },
        "observation_snapshot_json": snapshot,
        "persisted_observation_snapshot": _persisted_snapshot(snapshot),
        "snapshot_round_trip_baseline_identity_key": _baseline_out(
            cloud_sync._baseline_identity_key(json.loads(snapshot)["observation"])
        ),
    }


def compute_golden() -> dict:
    return {
        "identity_sync_key": [
            {"args": list(args), "key": cloud_sync._identity_sync_key(*args)} for args in _SYNC_KEY_INPUTS
        ],
        "cases": {name: _compute_case(case) for name, case in _cases().items()},
    }


def test_taxonomy_identity_golden_matches_base():
    expected = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    actual = json.loads(json.dumps(compute_golden()))
    assert actual["identity_sync_key"] == expected["identity_sync_key"]
    assert sorted(actual["cases"]) == sorted(expected["cases"])
    for name in expected["cases"]:
        assert actual["cases"][name] == expected["cases"][name], name


def test_taxonomy_identity_golden_covers_required_row_classes():
    names = set(json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))["cases"])
    required_prefixes = [
        "proven_sporely", "cloud_selected_unverified", "external_only", "external_unresolved",
        "external_integer_collides", "no_identity", "absent_id", "pre_identity_snapshot",
        "text_only_difference", "case_whitespace_only_difference",
    ]
    for prefix in required_prefixes:
        assert any(name.startswith(prefix) for name in names), prefix


def test_baseline_unknown_sentinel_is_distinct():
    assert cloud_sync._baseline_identity_key(_baseline(None)) is cloud_sync._IDENTITY_BASELINE_UNKNOWN
    assert cloud_sync._baseline_identity_key(_baseline("")) == ""


if __name__ == "__main__":
    if "--write" not in sys.argv:
        raise SystemExit("usage: python tests/test_cloud_sync_taxonomy_identity_golden.py --write")
    GOLDEN_PATH.write_text(json.dumps(compute_golden(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {GOLDEN_PATH}")

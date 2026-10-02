"""One simulated desktop "device" action, run in its own process.

Invoked by the ``local_supabase`` scenarios as
``python tests/local_supabase/device.py <action> <json-args>`` with
``SPORELY_APP_DATA_DIR`` set to that device's own temp profile. It runs the
desktop's real code (sync_all, reference repositories, catalogue reader)
against the LOCAL stack: the Supabase URL/key module constants are pointed at
``SPORELY_LOCAL_SUPABASE_URL`` before any request, and this refuses to run
for any non-local URL. Prints one JSON object on the last stdout line.
"""
from __future__ import annotations

import collections
import json
import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from urllib.parse import urlsplit  # noqa: E402

LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost"})


def is_local_url(url: str) -> bool:
    try:
        parts = urlsplit(str(url))
    except ValueError:
        return False
    return (
        parts.scheme in {"http", "https"}
        and parts.hostname in LOCAL_HOSTS
        and parts.username is None and parts.password is None
    )


URL = os.environ["SPORELY_LOCAL_SUPABASE_URL"].rstrip("/")
if not is_local_url(URL):
    raise SystemExit("refusing a non-local Supabase URL")
if not os.environ.get("SPORELY_APP_DATA_DIR"):
    raise SystemExit("a device needs its own SPORELY_APP_DATA_DIR")
os.environ["PYTHON_KEYRING_BACKEND"] = "keyring.backends.fail.Keyring"
os.environ.setdefault("SPORELY_TAXONOMY_V2", "0")

import utils.cloud_sync as cloud_sync  # noqa: E402
import utils.sporely_cloud_auth as cloud_auth  # noqa: E402

cloud_sync.SUPABASE_URL = URL
cloud_sync.SUPABASE_KEY = os.environ["SPORELY_LOCAL_SUPABASE_ANON_KEY"]
cloud_auth.SUPABASE_URL = URL
# Computed at import from the production URL; repoint them too.
cloud_auth._AUTH_URL = f"{URL}/auth/v1/oauth/authorize"
cloud_auth._TOKEN_URL = f"{URL}/auth/v1/oauth/token"

# Fail closed: every HTTP request this device makes must go to the local
# stack. A non-local request raises (and is reported) instead of being sent.
import requests  # noqa: E402

NON_LOCAL_REQUESTS: list[str] = []
_real_session_request = requests.Session.request


def _local_only_request(self, method, url, *args, **kwargs):
    if not is_local_url(url):
        NON_LOCAL_REQUESTS.append(str(urlsplit(str(url)).hostname))
        raise RuntimeError(f"harness blocked a non-local request to {urlsplit(str(url)).hostname}")
    return _real_session_request(self, method, url, *args, **kwargs)


requests.Session.request = _local_only_request
cloud_sync.set_cloud_sync_source_app_version("0.9.99-harness")

from database import schema  # noqa: E402

schema.init_database()

RPC_CALLS: collections.Counter = collections.Counter()
_real_rpc = cloud_sync.SporelyCloudClient._rpc


def _counting_rpc(self, function_name, payload=None):
    RPC_CALLS[str(function_name)] += 1
    return _real_rpc(self, function_name, payload)


cloud_sync.SporelyCloudClient._rpc = _counting_rpc


def _login(args):
    return cloud_sync.SporelyCloudClient.login(args["email"], args["password"])


def _sync(args, *, pull_only=False):
    client = _login(args)
    result = cloud_sync.sync_all(
        client, sync_images=False, materialize_remote_images=False,
        full_pull=True, pull_only=pull_only,
    )
    return {
        "errors": [str(item) for item in result.get("errors") or []],
        "reference_sync": result.get("reference_sync"),
        "cloud_writes_completed": result.get("cloud_writes_completed"),
        "blocked_write_attempts": result.get("blocked_write_attempts"),
    }


def _local_sets(_args):
    from database.reference_library import MeasurementSetRepository

    conn = schema.get_reference_connection()
    try:
        ids = [row[0] for row in conn.execute("SELECT id FROM reference_measurement_sets")]
    finally:
        conn.close()
    return {"sets": sorted(ids), "repository": MeasurementSetRepository.__name__}


def _create_set(args):
    from database.reference_library import (
        MeasurementSet, MeasurementSetRepository, ReferenceWork,
        ReferenceWorkRepository, TaxonTreatment, TaxonTreatmentRepository,
    )

    suffix = uuid.uuid4().hex[:8]
    work = ReferenceWorkRepository.create(ReferenceWork(
        str(uuid.uuid4()), "book", f"Harness work {suffix}", f"Harness {suffix}",
        authors_json='[{"family":"Harness"}]',
    ))
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(str(uuid.uuid4()), work.id, args.get("name", "Russula paludosa"))
    )
    measurement_set = MeasurementSetRepository.create(MeasurementSet(
        str(uuid.uuid4()), treatment.id, "spore_size", "range",
        raw_text="7-9 x 5-6 um", length_min=7.0, length_max=9.0, width_min=5.0, width_max=6.0,
    ))
    return {"set_id": measurement_set.id, "work_id": work.id, "treatment_id": treatment.id}


def _delete_set(args):
    from database.reference_library import MeasurementSetRepository

    MeasurementSetRepository.delete(args["set_id"])
    return {"deleted": args["set_id"]}


def _delete_treatment(args):
    from database.reference_library import TaxonTreatmentRepository

    TaxonTreatmentRepository.delete(args["treatment_id"])
    return {"deleted": args["treatment_id"]}


def _local_treatments(_args):
    conn = schema.get_reference_connection()
    try:
        ids = [row[0] for row in conn.execute("SELECT id FROM reference_taxon_treatments")]
    finally:
        conn.close()
    return {"treatments": sorted(ids)}


def _local_set_row(args):
    conn = schema.get_reference_connection()
    try:
        row = conn.execute(
            "SELECT q_core_min, q_core_max FROM reference_measurement_sets WHERE id=?",
            (args["set_id"],),
        ).fetchone()
    finally:
        conn.close()
    return {"row": list(row) if row else None}


def _attach_public_use(args):
    from database.models import ObservationDB
    from database.reference_library import ObservationReferenceUseRepository

    observation_id = ObservationDB.create_observation(
        "2026-10-02", genus="Russula", species="paludosa",
        is_draft=False, sharing_scope="public",
    )
    use = ObservationReferenceUseRepository.attach(
        observation_id, args["set_id"], role=args.get("role", "supports_identification"),
    )
    return {"observation_id": observation_id, "use_id": use.id}


def _use_state(_args):
    """Per-use sync state with local vs accepted payload differences."""
    from database.reference_use_sync_reconciliation import _local_payload

    conn = schema.get_connection()
    conn.row_factory = __import__("sqlite3").Row
    try:
        rows = conn.execute(
            "SELECT * FROM observation_reference_use_cloud_sync_state ORDER BY use_id"
        ).fetchall()
        out = []
        for row in rows:
            local = _local_payload(conn, row["use_id"])
            accepted = json.loads(row["accepted_payload_json"] or "null")
            diff = sorted(
                key for key in set(local or {}) | set(accepted or {})
                if (local or {}).get(key) != (accepted or {}).get(key)
            )
            out.append({
                "use_id": row["use_id"], "sync_status": row["sync_status"],
                "remote_identity_state": row["remote_identity_state"],
                "differs": {k: [(local or {}).get(k), (accepted or {}).get(k)] for k in diff},
            })
    finally:
        conn.close()
    return {"uses": out}


def _status_rows(_args):
    """Raw status tables (every column) for byte-for-byte comparisons."""
    import sqlite3

    observation = schema.get_connection()
    reference = schema.get_reference_connection()
    observation.row_factory = reference.row_factory = sqlite3.Row
    try:
        return {
            "uses": [dict(row) for row in observation.execute(
                "SELECT * FROM observation_reference_use_cloud_sync_state ORDER BY use_id"
            )],
            "library": [dict(row) for row in reference.execute(
                "SELECT * FROM reference_cloud_sync_state ORDER BY entity_type, entity_id"
            )],
        }
    finally:
        observation.close()
        reference.close()


def _age_status_rows(_args):
    """Preset old timestamps so a same-second rewrite is still visible."""
    for table, connect in (
        ("observation_reference_use_cloud_sync_state", schema.get_connection),
        ("reference_cloud_sync_state", schema.get_reference_connection),
    ):
        conn = connect()
        try:
            conn.execute(
                f"UPDATE {table} SET updated_at='2000-01-01 00:00:00', "
                "last_attempted_at='2000-01-01T00:00:00+00:00'"
            )
            conn.commit()
        finally:
            conn.close()
    return {}


def _set_use_note(args):
    from database.reference_library import ObservationReferenceUseRepository

    ObservationReferenceUseRepository.update(args["use_id"], note=args["note"])
    return {"use_id": args["use_id"]}


def _catalogue(args):
    from database.curated_reference_forks import (
        copy_curated_bundle_to_personal_library,
        search_shared_reference_contributions,
    )

    anon = cloud_sync.SporelyCloudClient(cloud_sync.SUPABASE_KEY, "")
    bundles = search_shared_reference_contributions(anon, int(args["taxon_id"]))
    out = {"contributions": [bundle.contribution_id for bundle in bundles]}
    if args.get("copy") and bundles:
        fork = copy_curated_bundle_to_personal_library(bundles[0])
        out["copied_set_id"] = fork.reference_measurement_set_id
        out["created"] = fork.created
    return out


def _search(args):
    """Shared catalogue search as the desktop dialog runs it, signed in
    (``email``/``password``) or anon; optionally copy one contribution."""
    from database.curated_reference_forks import (
        copy_curated_bundle_to_personal_library,
        search_shared_reference_contributions,
    )
    from ui.curated_reference_catalogue_dialog import relationship_label

    client = (
        _login(args) if args.get("email")
        else cloud_sync.SporelyCloudClient(cloud_sync.SUPABASE_KEY, "")
    )
    bundles = search_shared_reference_contributions(client, int(args["taxon_id"]))
    out = {"bundles": [
        {
            "contribution_id": bundle.contribution_id,
            "revision": bundle.bundle_revision,
            "relationship_roles": list(bundle.relationship_roles),
            "relationship_label": relationship_label(bundle.relationship_roles),
            "raw_text": bundle.snapshot.get("raw_text"),
        }
        for bundle in bundles
    ]}
    wanted = args.get("copy_contribution_id")
    if wanted:
        bundle = next(b for b in bundles if b.contribution_id == wanted)
        fork = copy_curated_bundle_to_personal_library(bundle)
        out["copy"] = {
            "set_id": fork.reference_measurement_set_id,
            "treatment_id": fork.taxon_treatment_id,
            "work_id": fork.reference_work_id,
            "created": fork.created,
        }
    return out


def _sharing(args):
    """Owner sharing actions through the client methods the My shared
    references dialog calls (list / stop / share again)."""
    from ui.reference_sharing_dialogs import action_result_message

    client = _login(args)
    action = args["op"]
    if action == "list":
        return {"result": client.list_my_reference_sharing()}
    call = {
        "stop": client.stop_sharing_reference_set,
        "share_again": client.share_reference_set_again,
    }[action]
    result = call(args["set_id"])
    ok, message = action_result_message(action, result)
    return {"result": result, "ok": ok, "message": message}


def _graph(_args):
    """Every local reference/use/observation row, for before/after equality."""
    tables = {}
    conn = schema.get_reference_connection()
    try:
        for table in ("reference_works", "reference_taxon_treatments",
                      "reference_measurement_sets", "curated_reference_forks"):
            cur = conn.execute(f"SELECT * FROM {table} ORDER BY 1")
            names = [d[0] for d in cur.description]
            tables[table] = [dict(zip(names, row)) for row in cur.fetchall()]
    finally:
        conn.close()
    conn = schema.get_connection()
    try:
        for table in ("observation_reference_uses", "observations"):
            cur = conn.execute(f"SELECT * FROM {table} ORDER BY 1")
            names = [d[0] for d in cur.description]
            tables[table] = [dict(zip(names, row)) for row in cur.fetchall()]
    finally:
        conn.close()
    return {"tables": tables}


def _edit_set(args):
    from database.reference_library import MeasurementSetRepository

    MeasurementSetRepository.update(args["set_id"], {"notes": args["notes"]})
    return {"set_id": args["set_id"]}


ACTIONS = {
    "search": _search,
    "sharing": _sharing,
    "graph": _graph,
    "edit_set": _edit_set,
    "sync": _sync,
    "pull_only": lambda args: _sync(args, pull_only=True),
    "local_sets": _local_sets,
    "create_set": _create_set,
    "delete_set": _delete_set,
    "delete_treatment": _delete_treatment,
    "local_treatments": _local_treatments,
    "local_set_row": _local_set_row,
    "attach_public_use": _attach_public_use,
    "catalogue": _catalogue,
    "use_state": _use_state,
    "set_use_note": _set_use_note,
    "status_rows": _status_rows,
    "age_status_rows": _age_status_rows,
}


if __name__ == "__main__":
    action, raw = sys.argv[1], (sys.argv[2] if len(sys.argv) > 2 else "{}")
    output = ACTIONS[action](json.loads(raw))
    output["rpc_calls"] = dict(RPC_CALLS)
    output["non_local_requests"] = NON_LOCAL_REQUESTS
    print(json.dumps(output, default=str))

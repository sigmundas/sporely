import io
import argparse
import types

import requests
from PIL import Image

from tools import repair_supabase_storage_media as repair


class FakeR2Client:
    def __init__(self):
        self.uploaded_keys = {}

    def put_bytes(self, data, key, content_type=None, cache_control=None):
        self.uploaded_keys[key] = {
            "data": bytes(data),
            "content_type": content_type,
            "cache_control": cache_control,
        }


class FakeCurrentClient:
    def __init__(self):
        self.access_token = "current-user-token"
        self.user_id = "current-user"
        self._s = requests.Session()


class FakeAdminClient:
    def __init__(self, access_token):
        self.access_token = access_token
        self.user_id = "service-role"
        self.supabase_api_key = access_token
        self._s = requests.Session()
        self.r2 = FakeR2Client()
        self.patches = []

    def _get_r2(self):
        return self.r2

    def _patch(self, path, payload):
        self.patches.append((path, payload))


def _row_report(
    *,
    row_id="1589",
    observation_id="617",
    user_id="af912ffe-bdde-4f4a-a003-7938bf4f3504",
    storage_path="8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp",
    r2_original="missing_404",
    r2_thumb="missing_404",
    supabase_original="exists",
    supabase_thumb="exists",
):
    cloud_row = {
        "id": row_id,
        "observation_id": observation_id,
        "user_id": user_id,
        "storage_path": storage_path,
    }
    probes = {
        "r2_original": {"status": r2_original},
        "r2_thumb": {"status": r2_thumb},
        "supabase_original": {"status": supabase_original},
        "supabase_thumb": {"status": supabase_thumb},
    }
    return repair._prepare_row_report(cloud_row, probes)


def _jpeg_bytes(size=(64, 48), color=(120, 20, 20)) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", size, color).save(buffer, format="JPEG")
    return buffer.getvalue()


def test_storage_object_urls_keep_the_original_and_thumb_keys_stable():
    storage_path = "8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp"

    assert repair._storage_object_url(storage_path, backend="r2") == (
        "https://media.sporely.no/8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp"
    )
    assert repair._storage_object_url(storage_path, backend="r2", variant="thumb") == (
        "https://media.sporely.no/8c471394-b274-4933-b830-59805820d93c/617/thumb_0_1780071867059.webp"
    )
    assert repair._storage_object_url(storage_path, backend="supabase") == (
        "https://zkpjklzfwzefhjluvhfw.supabase.co/storage/v1/object/authenticated/observation-images/8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp"
    )


def test_load_client_uses_service_role_key_only_when_admin_flag_is_explicit(monkeypatch):
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "service-token")

    current_client = FakeCurrentClient()
    monkeypatch.setattr(repair.SporelyCloudClient, "from_stored_credentials", staticmethod(lambda: current_client))

    admin_calls = []

    class CapturingAdminClient(FakeAdminClient):
        def __init__(self, access_token):
            admin_calls.append(access_token)
            super().__init__(access_token)

    monkeypatch.setattr(repair, "AdminServiceRoleClient", CapturingAdminClient)

    client, api_key = repair._load_client(argparse.Namespace(admin_service_role=False))
    assert client is current_client
    assert api_key == repair.SUPABASE_KEY
    assert admin_calls == []

    client, api_key = repair._load_client(argparse.Namespace(admin_service_role=True))
    assert isinstance(client, CapturingAdminClient)
    assert api_key == "service-token"
    assert admin_calls == ["service-token"]


def test_resolve_row_scope_rejects_legacy_admin_modes_without_the_explicit_flag():
    client = types.SimpleNamespace(user_id="current-user")
    args = argparse.Namespace(
        storage_path="af912ffe-bdde-4f4a-a003-7938bf4f3504/59/558_cloud_0002.jpg",
        legacy_upload_mode_null=False,
        cloud_observation_id=None,
        local_observation_id=None,
        all=False,
        limit=0,
        repair=False,
        soft_delete_stale_metadata=False,
        output_format="text",
        admin_service_role=False,
    )

    try:
        repair._resolve_row_scope(args, client, admin_mode=False)
    except RuntimeError as exc:
        assert "--admin-service-role" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected admin gating to reject legacy repair mode")


def test_resolve_row_scope_builds_admin_storage_path_and_legacy_filters():
    client = types.SimpleNamespace(user_id="current-user")

    storage_args = argparse.Namespace(
        storage_path="observation-images/af912ffe-bdde-4f4a-a003-7938bf4f3504/59/558_cloud_0002.jpg",
        legacy_upload_mode_null=False,
        cloud_observation_id=None,
        local_observation_id=None,
        all=False,
        limit=0,
        repair=False,
        soft_delete_stale_metadata=False,
        output_format="text",
        admin_service_role=True,
    )
    filters, scope = repair._resolve_row_scope(storage_args, client, admin_mode=True)
    assert filters[0] == "deleted_at=is.null"
    assert filters[1] == "storage_path=eq.af912ffe-bdde-4f4a-a003-7938bf4f3504%2F59%2F558_cloud_0002.jpg"
    assert scope["storage_path"] == "af912ffe-bdde-4f4a-a003-7938bf4f3504/59/558_cloud_0002.jpg"

    batch_args = argparse.Namespace(
        storage_path=None,
        legacy_upload_mode_null=True,
        cloud_observation_id=None,
        local_observation_id=None,
        all=False,
        limit=0,
        repair=False,
        soft_delete_stale_metadata=False,
        output_format="text",
        admin_service_role=True,
    )
    filters, scope = repair._resolve_row_scope(batch_args, client, admin_mode=True)
    assert filters == ["deleted_at=is.null", "upload_mode=is.null"]
    assert scope["legacy_upload_mode_null"] is True


def test_repair_rows_reuploads_missing_objects_into_r2(monkeypatch):
    uploaded_keys = set()
    source_downloads: list[str] = []

    r2 = FakeR2Client()
    fake_client = types.SimpleNamespace(
        access_token="token-123",
        _get_r2=lambda: r2,
    )
    original_bytes = _jpeg_bytes()

    def fake_download_supabase_object(url, session, access_token, *, supabase_api_key):
        assert access_token == "token-123"
        assert supabase_api_key == "service-token"
        source_downloads.append(url)
        if "thumb_" in url:
            raise AssertionError("thumb should be generated locally from the original")
        return original_bytes, "image/jpeg"

    def fake_probe_media_backends(storage_path, session, access_token, *, supabase_api_key):
        key = repair.normalize_media_key(storage_path)
        return {
            "r2_original": {"status": "exists" if key in uploaded_keys else "missing_404"},
            "r2_thumb": {"status": "exists" if repair.media_variant_key(key, "thumb") in uploaded_keys else "missing_404"},
            "supabase_original": {"status": "exists"},
            "supabase_thumb": {"status": "missing_404"},
        }

    def fake_put_bytes(data, key, content_type=None, cache_control=None):
        uploaded_keys.add(key)
        r2.uploaded_keys[key] = {
            "data": bytes(data),
            "content_type": content_type,
            "cache_control": cache_control,
        }

    r2.put_bytes = fake_put_bytes

    monkeypatch.setattr(repair, "_download_supabase_object", fake_download_supabase_object)
    monkeypatch.setattr(repair, "_probe_media_backends", fake_probe_media_backends)

    row = _row_report(supabase_thumb="missing_404")
    assert row["action"] == "needs_repair"
    repaired = repair._repair_rows(
        fake_client,
        [row],
        object(),
        supabase_api_key="service-token",
        repair=True,
        soft_delete_stale_metadata=False,
    )
    repaired_row = repaired[0]

    assert repaired_row["action"] == "repaired"
    assert repaired_row["error"] is None
    assert repaired_row["r2_original"] == "exists"
    assert repaired_row["r2_thumb"] == "exists"
    assert row["storage_path"] in uploaded_keys
    assert repair.media_variant_key(row["storage_path"], "thumb") in uploaded_keys
    assert len(source_downloads) == 1
    assert "thumb_" not in source_downloads[0]


def test_repair_rows_soft_deletes_stale_metadata_only_when_requested(monkeypatch):
    fake_client = FakeAdminClient("service-token")

    def fake_probe_media_backends(storage_path, session, access_token, *, supabase_api_key):
        return {
            "r2_original": {"status": "missing_404"},
            "r2_thumb": {"status": "missing_404"},
            "supabase_original": {"status": "missing_404"},
            "supabase_thumb": {"status": "missing_404"},
        }

    monkeypatch.setattr(repair, "_probe_media_backends", fake_probe_media_backends)

    row = _row_report(
        r2_original="missing_404",
        r2_thumb="missing_404",
        supabase_original="missing_404",
        supabase_thumb="missing_404",
    )
    repaired = repair._repair_rows(
        fake_client,
        [row],
        fake_client._s,
        supabase_api_key="service-token",
        repair=False,
        soft_delete_stale_metadata=True,
    )
    repaired_row = repaired[0]

    assert repaired_row["action"] == "soft_deleted_stale_metadata"
    assert repaired_row["error"] is None
    assert len(fake_client.patches) == 1
    patch_path, patch_payload = fake_client.patches[0]
    assert patch_path == "observation_images?id=eq.1589"
    assert isinstance(patch_payload.get("deleted_at"), str)
    assert "T" in patch_payload["deleted_at"]


def test_run_audit_json_mode_keeps_stdout_clean(monkeypatch, capsys):
    fake_client = FakeAdminClient("service-token")

    args = argparse.Namespace(
        storage_path=None,
        legacy_upload_mode_null=False,
        cloud_observation_id="617",
        local_observation_id=None,
        all=False,
        limit=0,
        repair=False,
        soft_delete_stale_metadata=False,
        output_format="json",
        admin_service_role=True,
    )

    monkeypatch.setattr(
        repair,
        "_fetch_cloud_rows",
        lambda client, filters, limit: [
            {
                "id": "1589",
                "observation_id": "617",
                "user_id": "af912ffe-bdde-4f4a-a003-7938bf4f3504",
                "storage_path": "8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp",
            }
        ],
    )
    monkeypatch.setattr(
        repair,
        "_probe_media_backends",
        lambda storage_path, session, access_token, *, supabase_api_key: {
            "r2_original": {"status": "exists"},
            "r2_thumb": {"status": "exists"},
            "supabase_original": {"status": "exists"},
            "supabase_thumb": {"status": "exists"},
        },
    )

    report = repair.run_audit(
        args,
        client=fake_client,
        probe_session=fake_client._s,
        supabase_api_key="service-token",
    )
    captured = capsys.readouterr()

    assert captured.out == ""
    assert report["rows"][0]["action"] == "noop"

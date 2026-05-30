import types

from tools import repair_supabase_storage_media as repair


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


def test_prepare_row_report_marks_rows_repairable_when_supabase_has_the_objects():
    cloud_row = {
        "id": "1589",
        "observation_id": "617",
        "storage_path": "8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp",
    }
    probes = {
        "r2_original": {"status": "missing_404"},
        "r2_thumb": {"status": "missing_404"},
        "supabase_original": {"status": "exists"},
        "supabase_thumb": {"status": "exists"},
    }

    report = repair._prepare_row_report(cloud_row, probes)

    assert report["repairable"] == "yes"
    assert report["repairable_variants"] == "original,thumb"
    assert report["thumb_key"] == "8c471394-b274-4933-b830-59805820d93c/617/thumb_0_1780071867059.webp"


def test_repair_rows_reuploads_missing_objects_into_r2(monkeypatch):
    uploaded_keys = {}

    class FakeR2Client:
        def put_bytes(self, data, key, content_type=None, cache_control=None):
            uploaded_keys[key] = {
                "data": bytes(data),
                "content_type": content_type,
                "cache_control": cache_control,
            }

    fake_client = types.SimpleNamespace(
        access_token="token-123",
        _get_r2=lambda: FakeR2Client(),
    )

    def fake_download_supabase_object(url, session, access_token):
        assert access_token == "token-123"
        key = repair.normalize_media_key(url)
        return f"payload:{key}".encode("utf-8"), "image/webp"

    def fake_probe(url, session, headers=None):
        key = repair.normalize_media_key(url)
        return {
            "status": "exists" if key in uploaded_keys else "missing_404",
            "http_status": 200 if key in uploaded_keys else 404,
            "url": url,
            "detail": None,
        }

    monkeypatch.setattr(repair, "_download_supabase_object", fake_download_supabase_object)
    monkeypatch.setattr(repair, "_probe_url", fake_probe)

    row = {
        "cloud_image_id": "1589",
        "repairable": "yes",
        "repairable_variants": "original,thumb",
        "storage_path": "8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp",
        "thumb_key": "8c471394-b274-4933-b830-59805820d93c/617/thumb_0_1780071867059.webp",
    }

    repaired = repair._repair_rows(fake_client, [row], object())
    repaired_row = repaired[0]

    assert repaired_row["repair_status"] == "repaired"
    assert repaired_row["repair_uploaded_original_key"] == row["storage_path"]
    assert repaired_row["repair_uploaded_thumb_key"] == row["thumb_key"]
    assert repaired_row["repair_post_original_status"] == "exists"
    assert repaired_row["repair_post_thumb_status"] == "exists"
    assert row["storage_path"] in uploaded_keys
    assert row["thumb_key"] in uploaded_keys

from tools import cleanup_orphaned_r2_media as cleanup


def test_reference_inventory_keeps_full_thumb_original_and_mosaic(monkeypatch):
    values = {
        ("observation_images", "storage_path"): {"user/obs/full.webp"},
        ("observation_images", "original_storage_path"): {"user/obs/originals/42/source.heic"},
        ("spore_measurement_mosaics", "storage_key"): {"user/obs/spore_mosaic.webp"},
    }
    monkeypatch.setattr(
        cleanup,
        "_fetch_supabase_column",
        lambda table, column, token: values.get((table, column), set()),
    )

    referenced = cleanup._referenced_media_keys("token")

    assert "user/obs/full.webp" in referenced
    assert "user/obs/thumb_full.webp" in referenced
    assert "user/obs/originals/42/source.heic" in referenced
    assert "user/obs/spore_mosaic.webp" in referenced
    assert "user/obs/originals/42/thumb_source.heic" not in referenced
    assert "user/obs/thumb_spore_mosaic.webp" not in referenced


def test_configured_clients_include_optional_private_role_without_mode_switch(monkeypatch):
    monkeypatch.setattr(
        cleanup.R2Config,
        "from_env",
        classmethod(lambda cls: cleanup.R2Config("id", "secret", "https://r2.test")),
    )
    monkeypatch.setenv("R2_PRIVATE_BUCKET_NAME", "sporely-media-private")

    clients = cleanup._configured_r2_clients()

    assert [(role, client.config.bucket_name) for role, client in clients] == [
        ("legacy", "sporely-media"),
        ("private", "sporely-media-private"),
    ]

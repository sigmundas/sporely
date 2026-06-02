import json
from pathlib import Path
from types import SimpleNamespace

from utils import cloud_sync


class _FakeR2:
    def __init__(self, payload: bytes):
        self._payload = payload

    def download_to_file(self, storage_key, dest_path, timeout=120):
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self._payload)
        return dest


class _ProfiledClient(cloud_sync.SporelyCloudClient):
    def __init__(self, remote_images, remote_measurements, payload: bytes = b'cloud-bytes'):
        super().__init__('access-token', 'user-id')
        self._remote_images = [dict(row or {}) for row in remote_images]
        self._remote_measurements = [dict(row or {}) for row in remote_measurements]
        self._payload = payload

    def _download_public_media_file(self, storage_path, dest_path, *, timeout=120):
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self._payload)
        return dest

    def _get(self, path: str):
        if str(path or '').startswith('observation_images?'):
            return [dict(row) for row in self._remote_images]
        if str(path or '').startswith('spore_measurements?'):
            return [dict(row) for row in self._remote_measurements]
        return []

    def set_desktop_id(self, *args, **kwargs):
        return None

    def set_image_desktop_id(self, *args, **kwargs):
        return None

    def set_measurement_desktop_id(self, *args, **kwargs):
        return None


def test_sync_all_profile_emits_structured_lines_and_phase_metrics(monkeypatch, capsys):
    monkeypatch.setenv('SPORELY_CLOUD_SYNC_PROFILE', '1')

    monkeypatch.setattr(cloud_sync, 'ensure_database_linked_to_cloud_user', lambda client: 'linked-user')
    pull_all_kwargs = {}
    monkeypatch.setattr(
        cloud_sync,
        'push_calibrations',
        lambda *args, **kwargs: {'pushed': 1, 'total': 1, 'errors': []},
    )
    monkeypatch.setattr(
        cloud_sync,
        'push_all',
        lambda *args, **kwargs: {
            'pushed': 2,
            'pulled': 0,
            'calibrations_pushed': 0,
            'calibrations_pulled': 0,
            'errors': [],
            'deleted_remote': [],
        },
    )
    monkeypatch.setattr(
        cloud_sync,
        'pull_all',
        lambda *args, **kwargs: pull_all_kwargs.update(kwargs) or {
            'pushed': 0,
            'pulled': 3,
            'calibrations_pushed': 0,
            'calibrations_pulled': 0,
            'errors': [],
            'deleted_remote': ['cloud-obs-removed'],
        },
    )
    monkeypatch.setattr(
        cloud_sync,
        'pull_calibrations',
        lambda *args, **kwargs: {'pulled': 4, 'total': 4, 'errors': []},
    )

    client = SimpleNamespace(
        list_remote_observations=lambda: [{'id': 'obs-1'}],
        list_remote_calibrations=lambda: [{'id': 'cal-1'}],
    )

    result = cloud_sync.sync_all(client, sync_images=False, materialize_remote_images=False)
    output_lines = [line for line in capsys.readouterr().out.splitlines() if line.startswith('[cloud_sync_profile]')]
    assert len(output_lines) == 8

    phase_events = []
    summary_event = None
    for line in output_lines:
        payload = json.loads(line.split(' ', 1)[1])
        if payload['event'] == 'phase':
            phase_events.append(payload['phase'])
        elif payload['event'] == 'summary':
            summary_event = payload

    assert phase_events == [
        'ensure_database_linked_to_cloud_user',
        'list_remote_observations',
        'list_remote_calibrations',
        'push_calibrations',
        'push_all',
        'pull_all',
        'pull_calibrations',
    ]
    assert summary_event is not None
    assert summary_event['status'] == 'ok'
    assert set(summary_event['phases_ms']) == set(phase_events)
    assert summary_event['metrics']['download_image_file']['calls'] == 0
    assert pull_all_kwargs['materialize_remote_images'] is False
    assert result == {
        'pushed': 2,
        'pulled': 3,
        'calibrations_pushed': 1,
        'calibrations_pulled': 4,
        'errors': [],
        'deleted_remote': ['cloud-obs-removed'],
    }


def test_cloud_sync_profiler_records_media_metrics_and_snapshot_fetches(monkeypatch, tmp_path):
    profiler = cloud_sync.CloudSyncProfiler()
    remote_images = [
        {
            'id': 'cloud-image-1',
            'observation_id': 'cloud-obs-1',
            'storage_path': 'bucket/image-1.jpg',
            'original_filename': 'image-1.jpg',
            'original_storage_path': 'bucket/originals/image-1.tif',
        },
        {
            'id': 'cloud-image-2',
            'observation_id': 'cloud-obs-1',
            'storage_path': 'bucket/image-2.jpg',
            'original_filename': 'image-2.jpg',
            'original_storage_path': None,
        },
    ]
    remote_measurements = [
        {
            'id': 'cloud-measurement-1',
            'image_id': 'cloud-image-1',
        },
        {
            'id': 'cloud-measurement-2',
            'image_id': 'cloud-image-2',
        },
    ]
    client = _ProfiledClient(remote_images, remote_measurements, payload=b'cloud-bytes')

    monkeypatch.setattr(cloud_sync, 'generate_all_sizes', lambda *args, **kwargs: {'sizes': []})
    monkeypatch.setattr(cloud_sync, '_store_cloud_observation_snapshot', lambda *args, **kwargs: None)

    download_path = tmp_path / 'downloaded.jpg'
    with cloud_sync._cloud_sync_profile_scope(profiler):
        client.download_image_file('bucket/image-1.jpg', download_path)
        cloud_sync._profile_generate_all_sizes(str(download_path), 123)
        client.pull_bulk_image_metadata(['cloud-obs-1'])
        cloud_sync._store_remote_snapshot(
            client,
            'cloud-obs-1',
            remote={'id': 'cloud-obs-1'},
            remote_images=None,
            remote_measurements=None,
        )

    assert profiler.download_image_file_calls == 1
    assert profiler.download_image_file_bytes == len(b'cloud-bytes')
    assert profiler.generate_all_sizes_calls == 1
    assert profiler.pull_bulk_image_metadata_calls == 1
    assert profiler.pull_bulk_image_metadata_rows == len(remote_images)
    assert profiler.pull_measurements_for_images_calls == 1
    assert profiler.pull_measurements_for_images_rows == len(remote_measurements)
    assert profiler.store_remote_snapshot_fetch_images_count == 1
    assert profiler.store_remote_snapshot_fetch_measurements_count == 1


def test_store_remote_snapshot_uses_prefetched_rows_without_fallback_fetches(monkeypatch):
    profiler = cloud_sync.CloudSyncProfiler()
    remote_images = [
        {
            'id': 'cloud-image-1',
            'desktop_id': 101,
            'observation_id': 'cloud-obs-1',
            'storage_path': 'bucket/image-1.jpg',
            'original_filename': 'image-1.jpg',
            'image_type': 'field',
            'sort_order': 0,
            'original_storage_path': 'bucket/originals/image-1.tif',
        },
        {
            'id': 'cloud-image-2',
            'desktop_id': None,
            'observation_id': 'cloud-obs-1',
            'storage_path': 'bucket/image-2.jpg',
            'original_filename': 'image-2.jpg',
            'image_type': 'field',
            'sort_order': 1,
            'original_storage_path': None,
        },
    ]
    remote_measurements = [
        {
            'id': 'cloud-measurement-1',
            'desktop_id': 201,
            'image_id': 'cloud-image-1',
            'length_um': 12.3,
            'width_um': 4.5,
        },
        {
            'id': 'cloud-measurement-2',
            'desktop_id': None,
            'image_id': 'cloud-image-2',
            'length_um': 6.7,
            'width_um': 8.9,
        },
    ]
    snapshot_payload = {}

    client = _ProfiledClient(remote_images, remote_measurements)
    monkeypatch.setattr(
        cloud_sync,
        '_store_cloud_observation_snapshot',
        lambda cloud_id, snapshot: snapshot_payload.update({'cloud_id': cloud_id, 'snapshot': snapshot}),
    )

    with cloud_sync._cloud_sync_profile_scope(profiler):
        cloud_sync._store_remote_snapshot(
            client,
            'cloud-obs-1',
            remote={'id': 'cloud-obs-1', 'desktop_id': 1, 'date': '2026-05-01'},
            remote_images=remote_images,
            remote_measurements=remote_measurements,
        )

    assert profiler.store_remote_snapshot_fetch_images_count == 0
    assert profiler.store_remote_snapshot_fetch_measurements_count == 0
    snapshot = json.loads(snapshot_payload['snapshot'])
    assert snapshot_payload['cloud_id'] == 'cloud-obs-1'
    assert len(snapshot['images']) == 2
    assert len(snapshot['measurements']) == 2
    assert snapshot['images'][0]['id'] == 'cloud-image-1'
    assert snapshot['images'][1]['id'] == 'cloud-image-2'
    assert snapshot['images'][0]['original_storage_path'] == 'bucket/originals/image-1.tif'
    assert 'original_storage_path' not in snapshot['images'][1]

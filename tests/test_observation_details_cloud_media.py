from ui.observations_tab import _cloud_media_materialization_should_launch


def test_cloud_media_materialization_should_launch_respects_state_and_running():
    ready_state = {
        "status": "needs_materialization",
        "needs_materialization": True,
        "can_auto_start": True,
    }
    already_state = {
        "status": "already_materialized",
        "needs_materialization": False,
        "can_auto_start": False,
    }

    assert _cloud_media_materialization_should_launch(ready_state, worker_running=False) is True
    assert _cloud_media_materialization_should_launch(ready_state, worker_running=True) is False
    assert _cloud_media_materialization_should_launch(already_state, worker_running=False) is False

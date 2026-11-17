import asyncio

import pytest

from test_disk_pipeline import setup, enqueue, until


async def test_free_worker_starts_next_fight_before_slow_worker_finishes(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    pipeline.download_concurrency = 2
    slow_started, release = asyncio.Event(), asyncio.Event()
    original = client.fetch_all_fight_events
    starts = []
    logs = []

    async def fetch(code, fid, start, end):
        starts.append(fid)
        if fid == 1:
            slow_started.set()
            await release.wait()
        return await original(code, fid, start, end)

    monkeypatch.setattr(client, 'fetch_all_fight_events', fetch)
    for fid in (1, 2, 3):
        enqueue(db, fid)
    try:
        await pipeline.drain_queue_batch(batch_size=2, continuous=True, on_progress=logs.append)
        await until(lambda: 3 in starts)
        assert slow_started.is_set() and not release.is_set()
        assert starts[:3] == [1, 2, 3]
        assert any('worker #1' in item.get('log', '') and 'Started downloading Report_1' in item['log'] for item in logs)
        assert any('worker #2' in item.get('log', '') and 'Started downloading Report_3' in item['log'] for item in logs)
    finally:
        release.set()
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_ready_fight_starts_without_waiting_for_report_preparation(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    db.enqueue_fight('Younger', 1, 'mplus', 9, 5, 42, 'boss')
    preparing, release = asyncio.Event(), asyncio.Event()

    async def unarchive(code):
        preparing.set()
        await release.wait()
        return True

    monkeypatch.setattr(client, 'unarchive_report', unarchive)
    try:
        await pipeline.drain_queue_batch(batch_size=2, continuous=True)
        await until(lambda: ('Report', 1) in client.downloads)
        await until(preparing.is_set)
        assert not release.is_set()
    finally:
        release.set()
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_start_log_is_visible_while_network_is_still_blocked(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    entered, release = asyncio.Event(), asyncio.Event()
    original = client.fetch_all_fight_events
    logs = []

    async def fetch(*args):
        entered.set()
        await release.wait()
        return await original(*args)

    monkeypatch.setattr(client, 'fetch_all_fight_events', fetch)
    downloading = asyncio.create_task(pipeline.drain_queue_batch(batch_size=1, on_progress=logs.append))
    try:
        await until(entered.is_set)
        assert any('Started downloading Report_1' in item.get('log', '') and 'worker #1' in item['log'] for item in logs)
        assert not downloading.done()
    finally:
        release.set()
        await downloading
        await pipeline.finish_background_packing()
        pipeline.close()


async def test_live_resize_preserves_active_work_and_refills_only_available_slots(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    pipeline.download_concurrency = 2
    releases = {fid: asyncio.Event() for fid in range(1, 7)}
    started, finished = [], []
    original = client.fetch_all_fight_events

    async def fetch(code, fid, start, end):
        started.append(fid)
        await releases[fid].wait()
        result = await original(code, fid, start, end)
        finished.append(fid)
        return result

    monkeypatch.setattr(client, 'fetch_all_fight_events', fetch)
    for fid in releases:
        enqueue(db, fid)
    try:
        await pipeline.drain_queue_batch(continuous=True)
        await until(lambda: len(started) == 2)
        pipeline.download_concurrency = 3
        await until(lambda: len(started) == 3)
        pipeline.download_concurrency = 1
        releases[2].set()
        releases[3].set()
        await until(lambda: len(finished) == 2)
        await until(lambda: pipeline.get_staging_stats()['downloading_fights'] == 1)
        assert started == [1, 2, 3]
        assert pipeline.get_staging_stats()['downloading_fights'] == 1
        releases[1].set()
        await until(lambda: 4 in started)
        assert 5 not in started
    finally:
        for event in releases.values():
            event.set()
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_newly_discovered_key_takes_next_slot_before_queued_raids(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    pipeline.download_concurrency = 1
    release = asyncio.Event()
    started = []
    original = client.fetch_all_fight_events

    async def fetch(code, fid, start, end):
        started.append(fid)
        if fid == 1:
            await release.wait()
        return await original(code, fid, start, end)

    monkeypatch.setattr(client, 'fetch_all_fight_events', fetch)
    enqueue(db, 1, 'raid')
    enqueue(db, 2, 'raid')
    try:
        await pipeline.drain_queue_batch(continuous=True)
        await until(lambda: started == [1])
        enqueue(db, 3)
        release.set()
        await until(lambda: len(started) >= 3)
        assert started == [1, 3, 2]
    finally:
        release.set()
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_idle_scheduler_picks_up_new_discovery_without_another_drain_call(tmp_path):
    db, pipeline, client = setup(tmp_path)
    try:
        await pipeline.drain_queue_batch(continuous=True)
        enqueue(db, 1)
        await until(lambda: ('Report', 1) in client.downloads)
    finally:
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_continuous_stop_awaits_inflight_staging_writer(tmp_path, monkeypatch):
    import threading
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    entered, release = threading.Event(), threading.Event()
    original = pipeline.staging_store.stage_fight

    def stage(*args):
        entered.set()
        assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(pipeline.staging_store, 'stage_fight', stage)
    stopping = None
    try:
        await pipeline.drain_queue_batch(continuous=True)
        await until(entered.is_set)
        stopping = asyncio.create_task(pipeline.finish_background_packing(cancel=True))
        await asyncio.sleep(.05)
        assert not stopping.done()
        release.set()
        await stopping
        row = db.get_fight('Report', 1)
        assert row['status'] in ('STAGED', 'PACKED')
        if row['status'] == 'STAGED':
            assert (tmp_path / row['staging_rel_path']).exists()
        assert pipeline.get_staging_stats()['downloading_fights'] == 0
        assert pipeline.get_staging_stats()['preparing_reports'] == 0
    finally:
        release.set()
        if stopping:
            await stopping
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_continuous_scheduler_error_is_visible_and_cleanup_can_finish(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    def fail(*args, **kwargs):
        raise OSError('download queue unavailable')
    monkeypatch.setattr(db, 'get_unarchived_pending_fights', fail)
    try:
        with pytest.raises(RuntimeError, match='download queue unavailable'):
            await pipeline.drain_queue_batch(continuous=True)
    finally:
        with pytest.raises(OSError, match='download queue unavailable'):
            await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_stop_predicate_then_cleanup_cannot_cancel_a_staging_writer_twice(tmp_path, monkeypatch):
    import threading
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    entered, release = threading.Event(), threading.Event()
    original = pipeline.staging_store.stage_fight
    stopped = False

    def stage(*args):
        entered.set()
        assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(pipeline.staging_store, 'stage_fight', stage)
    stopping = None
    try:
        await pipeline.drain_queue_batch(continuous=True, should_stop=lambda: stopped)
        await until(entered.is_set)
        stopped = True
        pipeline._download_wakeup.set()
        await until(lambda: any(task.cancelling() for uid, task in pipeline._continuous_downloads.values()))
        stopping = asyncio.create_task(pipeline.finish_background_packing(cancel=True))
        await asyncio.sleep(.05)
        assert not stopping.done(), 'cleanup returned while the disk writer was still running'
        release.set()
        await stopping
        row = db.get_fight('Report', 1)
        assert row['status'] in ('STAGED', 'PACKED')
    finally:
        release.set()
        if stopping:
            await stopping
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_scheduler_can_restart_after_transient_queue_read_failure(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    original = db.get_unarchived_pending_fights
    calls = 0
    def fail_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError('temporary catalogue read failure')
        return original(*args, **kwargs)
    monkeypatch.setattr(db, 'get_unarchived_pending_fights', fail_once)
    try:
        with pytest.raises(RuntimeError, match='temporary catalogue read failure'):
            await pipeline.drain_queue_batch(continuous=True)
        with pytest.raises(OSError, match='temporary catalogue read failure'):
            await pipeline.stop_background_downloading()
        await pipeline.drain_queue_batch(continuous=True)
        await until(lambda: ('Report', 1) in client.downloads)
    finally:
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_continuous_network_failure_respects_attempt_budget(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    attempts = []
    logs = []
    async def fail(*args):
        attempts.append(1)
        raise RuntimeError('HTTP 503')
    monkeypatch.setattr(client, 'fetch_all_fight_events', fail)
    try:
        await pipeline.drain_queue_batch(continuous=True, on_progress=logs.append)
        await until(lambda: db.get_fight('Report', 1)['status'] == 'FAILED')
        assert len(attempts) == db.MAX_WORK_ATTEMPTS
        assert any('Download/staging failed for Report_1 (download worker #1)' in item.get('log', '') for item in logs)
    finally:
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()

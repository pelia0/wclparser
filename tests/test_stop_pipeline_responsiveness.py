import asyncio
import threading

import pytest

from test_disk_pipeline import enqueue, until
from test_hybrid_pipeline import setup


@pytest.mark.parametrize('reader', ['ready', 'reports', 'browser', 'packer', 'counts'])
async def test_catalogue_reads_leave_event_loop_responsive(tmp_path, monkeypatch, reader):
    db, pipeline, _ = setup(tmp_path)
    loop = asyncio.get_running_loop()
    release, finished = threading.Event(), threading.Event()
    callback = asyncio.Event()
    observations = []
    stopped = False

    def tick():
        nonlocal stopped
        observations.append(not finished.is_set())
        stopped = True
        callback.set()
        release.set()

    def slow_read(*args, **kwargs):
        watchdog.start()
        loop.call_soon_threadsafe(tick)
        try:
            assert release.wait(2), 'read watchdog failed'
            if reader == 'counts':
                return dict.fromkeys(('pending_reports', 'queued_fights', 'retryable_reports',
                                      'retryable_fights', 'staged_fights', 'api_staged_fights'), 0)
            return []
        finally:
            finished.set()

    # A watchdog releases the legacy synchronous read so a regression fails
    # deterministically instead of hanging the test runner's application loop.
    watchdog = threading.Timer(.3, release.set)
    pipeline._packing_wakeup = asyncio.Event()
    pipeline._browser_wakeup = asyncio.Event()
    pipeline._download_wakeup = asyncio.Event()
    pipeline._download_progress = asyncio.Event()
    if reader in ('ready', 'reports'):
        name = ('get_unarchived_pending_fights' if reader == 'ready'
                else 'get_pending_reports_for_unarchive')
        monkeypatch.setattr(db, name, slow_read)
        run = pipeline._continuous_download_loop(lambda: stopped)
    elif reader == 'browser':
        monkeypatch.setattr(db, 'get_api_staged_fights', slow_read)
        pipeline._browser_finish = True
        run = pipeline._browser_manager()
    elif reader == 'packer':
        monkeypatch.setattr(db, 'get_staged_fights', slow_read)
        pipeline._packing_finish = True
        run = pipeline._background_pack_worker()
    else:
        monkeypatch.setattr(db, 'get_queue_counts', slow_read)

        async def empty_drain(**kwargs):
            return 0

        async def empty_rankings(**kwargs):
            return []

        monkeypatch.setattr(pipeline, 'drain_queue_batch', empty_drain)
        monkeypatch.setattr(pipeline, '_fetch_rankings_refs', empty_rankings)
        run = pipeline._run_discovery_pool([{
            'zone_id': 9, 'encounter_id': 42, 'encounter_slug': 'boss', 'partition': 5,
            'difficulty': 0, 'content_type': 'mplus', 'class_name': 'Mage',
            'spec_name': 'Fire', 'role': 'dps', 'label_prefix': 'M+', 'title': 'Boss',
        }], pages_per_spec=1, pack_as_discovered=True)
    task = asyncio.create_task(run)
    try:
        await asyncio.wait_for(callback.wait(), 3)
        await asyncio.wait_for(task, 3)
        assert observations == [True], 'the catalogue read blocked an application-loop callback'
    finally:
        release.set()
        watchdog.cancel()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        pipeline.close()


async def test_stop_freezes_admission_and_cancels_browser_before_api_writer_finishes(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    pack_entered, pack_release = threading.Event(), threading.Event()
    writer_entered, writer_release = threading.Event(), threading.Event()
    browser_cancelled = asyncio.Event()
    packed = []
    original_fetch = client.fetch_all_fight_events
    original_pack = pipeline._pack_staged_cpu
    original_stage = pipeline.staging_store.stage_fight
    original_enrich = client.enrich_staged_fight

    async def fetch(code, fid, start, end):
        events = await original_fetch(code, fid, start, end)
        if fid in (3, 4):
            client.archive._completed[(code, fid)]['manifest'].pop('hybrid_pending')
        return events

    def pack(path, row):
        packed.append(row['fight_id'])
        if row['fight_id'] == 3:
            pack_entered.set()
            assert pack_release.wait(5)
        return original_pack(path, row)

    def stage(archive, row, *args, **kwargs):
        if row['fight_id'] == 2:
            writer_entered.set()
            assert writer_release.wait(5)
        return original_stage(archive, row, *args, **kwargs)

    async def enrich(*args):
        try:
            return await original_enrich(*args)
        except asyncio.CancelledError:
            browser_cancelled.set()
            raise

    monkeypatch.setattr(client, 'fetch_all_fight_events', fetch)
    monkeypatch.setattr(client, 'enrich_staged_fight', enrich)
    monkeypatch.setattr(pipeline, '_pack_staged_cpu', pack)
    monkeypatch.setattr(pipeline.staging_store, 'stage_fight', stage)
    finishing = None
    try:
        for fid in (3, 4):
            enqueue(db, fid)
        assert await pipeline.drain_queue_batch(batch_size=2) == 2
        await until(pack_entered.is_set)
        enqueue(db, 1)
        assert await pipeline.drain_queue_batch(batch_size=1) == 1
        await asyncio.wait_for(client.entered.wait(), 2)
        enqueue(db, 2)
        row = db.get_fight('Report', 2)
        pipeline._download_scheduler_task = asyncio.create_task(pipeline._download_rows_to_staging([row]))
        await until(writer_entered.is_set)
        finishing = asyncio.create_task(pipeline.finish_background_packing(cancel=True))
        await asyncio.sleep(.03)
        frozen = pipeline._packing_stop and pipeline._browser_stop
        cancelled = browser_cancelled.is_set()
        pack_release.set()
        await until(lambda: db.get_fight('Report', 3)['status'] == 'PACKED')
        await asyncio.sleep(.03)
        admitted = list(packed)
        assert not finishing.done(), 'Stop detached the active API snapshot writer'
        finishing.cancel()
        await asyncio.sleep(0)
        finishing.cancel()
        await asyncio.sleep(0)
        assert not finishing.done(), 'repeated cancellation detached cleanup'
        writer_release.set()
        await asyncio.gather(finishing, return_exceptions=True)
        assert frozen, 'Stop waited for API publication before freezing pack/browser admission'
        assert cancelled, 'browser network activity continued while Stop waited for an API writer'
        assert admitted == [3], 'a queued pack job started while Stop waited for an API writer'
        assert db.get_fight('Report', 4)['status'] == 'STAGED'
        assert db.get_fight('Report', 1)['status'] == 'API_STAGED'
        assert db.get_fight('Report', 2)['status'] == 'API_STAGED'
        assert len(pipeline.staging_store.list_jobs()) == 3
    finally:
        pack_release.set()
        writer_release.set()
        client.release.set()
        if finishing is not None:
            await asyncio.gather(finishing, return_exceptions=True)
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_cancelled_catalogue_reads_remain_owned_and_bound_until_they_finish(tmp_path):
    _, pipeline, _ = setup(tmp_path)
    release = threading.Event()
    lock = threading.Lock()
    running = 0
    maximum = 0
    entered = threading.Event()

    def slow_read():
        nonlocal running, maximum
        with lock:
            running += 1
            maximum = max(maximum, running)
            if running == 2:
                entered.set()
        try:
            assert release.wait(5)
            return []
        finally:
            with lock:
                running -= 1

    tasks = [asyncio.create_task(pipeline._db_read(slow_read)) for _ in range(12)]
    try:
        await until(entered.is_set)
        for task in tasks:
            task.cancel()
        await asyncio.sleep(0)
        for task in tasks:
            task.cancel()
        await asyncio.sleep(0)
        assert sum(not task.done() for task in tasks) == 2
        with pytest.raises(RuntimeError, match='catalogue reads'):
            pipeline.close()
        release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
        assert maximum == 2
        assert running == 0
        assert pipeline._db_reads == 0
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        pipeline.close()


async def test_normal_drain_can_start_another_finite_batch_after_finishing(tmp_path):
    db, pipeline, client = setup(tmp_path)
    client.release.set()
    try:
        for fid in (1, 2):
            enqueue(db, fid)
            assert await pipeline.drain_queue_batch(batch_size=1) == 1
            await pipeline.finish_background_packing()
            assert db.get_fight('Report', fid)['status'] == 'PACKED'
    finally:
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_stop_discards_pack_selectors_waiting_for_a_db_slot(tmp_path, monkeypatch):
    db, pipeline, _ = setup(tmp_path)
    release = threading.Event()
    calls = []

    def slow_read(**kwargs):
        calls.append(threading.get_ident())
        assert release.wait(5)
        return []

    monkeypatch.setattr(db, 'get_staged_fights', slow_read)
    pipeline.pack_workers = 32
    pipeline._packing_wakeup = asyncio.Event()
    pipeline._background_pack_tasks = [asyncio.create_task(pipeline._background_pack_worker())
                                       for _ in range(pipeline.pack_workers)]
    finishing = None
    try:
        await until(lambda: len(calls) == 2 and pipeline._db_reads == 32)
        finishing = asyncio.create_task(pipeline.finish_background_packing(cancel=True))
        await until(lambda: pipeline._packing_stop)
        release.set()
        await asyncio.wait_for(finishing, 2)
        assert len(calls) == 2, 'Stop submitted obsolete catalogue reads from waiting packers'
    finally:
        release.set()
        if finishing is not None:
            await asyncio.gather(finishing, return_exceptions=True)
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_request_stop_freezes_admission_before_returning_to_api_caller(tmp_path):
    _, pipeline, _ = setup(tmp_path)

    async def pending_network():
        await asyncio.Event().wait()

    tasks = [asyncio.create_task(pending_network()) for _ in range(4)]
    pipeline._download_scheduler_task, pipeline._browser_manager_task = tasks[:2]
    pipeline._continuous_downloads[1] = ('Report_1', tasks[2])
    pipeline._browser_tasks[1] = tasks[3]
    pipeline._packing_wakeup = asyncio.Event()
    try:
        # No yield between API request_stop and these checks: a 202 response
        # must never precede the admission fences or cancellation requests.
        pipeline.request_stop()
        assert pipeline._packing_stop and pipeline._browser_stop and pipeline._download_stop
        assert pipeline._packing_wakeup.is_set()
        assert [task.cancelling() for task in tasks] == [1, 1, 1, 1]
        pipeline.request_stop()
        assert [task.cancelling() for task in tasks] == [1, 1, 1, 1]
    finally:
        for task in tasks:
            if not task.done() and not task.cancelling():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        pipeline._download_scheduler_task = pipeline._browser_manager_task = None
        pipeline._continuous_downloads.clear()
        pipeline._browser_tasks.clear()
        pipeline.close()

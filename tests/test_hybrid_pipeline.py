import asyncio
from copy import deepcopy
import threading

import pytest

from test_disk_pipeline import CapturingClient, enqueue, until
from wcl_archiver.codec.fight_archive import FightArchive, json_bytes
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.harvester.pipeline import HarvesterPipeline


class PendingClient(CapturingClient):
    requires_browser_enrichment = True

    def __init__(self, archive):
        super().__init__(archive)
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.enriched = []
        self.error = None

    async def fetch_all_fight_events(self, code, fid, start, end):
        self.downloads.append((code, fid))
        result = [{'timestamp': 1, 'type': 'unknown', 'unknown': [None, {'all': True}]}]
        with self.archive.fight_capture(code, fid) as fetch:
            self.archive.capture('POST', '/api/v2/client', json_bytes({
                'data': {'reportData': {'report': {'events': {'data': result, 'nextPageTimestamp': None}}}}}),
                request_json={'variables': {'code': code, 'fightId': fid, 'startTime': start, 'endTime': end}})
            self.archive.complete_fight(code, fid, fetch, start, end, len(result))
        self.archive._completed[(code, fid)]['manifest']['hybrid_pending'] = True
        return result

    async def fetch_report_master_and_fights(self, code):
        master = {'fights': [{'id': i, 'start_time': 0, 'end_time': 100} for i in range(1, 9)],
                  'friendlies': [], 'abilities': []}
        self.archive.capture('POST', '/api/v2/client', json_bytes({'data': {'reportData': {'report': master}}}),
                             request_json={'variables': {'code': code}})
        return master

    async def enrich_staged_fight(self, job, restored):
        self.entered.set()
        await self.release.wait()
        if self.error:
            raise self.error
        code, fid = job['fight_meta']['report_code'], job['fight_meta']['fight_id']
        web = FightArchive(restored.root)
        master = {'fights': [{'id': fid, 'start_time': 0, 'end_time': 100, 'name': 'Browser fight'}],
                  'friendlies': [{'id': 1, 'name': 'Browser actor'}], 'abilities': []}
        web.capture('GET', f'/reports/fights-and-participants/{code}/0', json_bytes(master))
        with web.fight_capture(code, fid) as fetch:
            web.capture('GET', f'/reports/summary-events/{code}/{fid}/0/100', json_bytes({'events': job['raw_events']}))
            web.capture('GET', f'/reports/replaysegment/{code}/99/0/100', json_bytes({'events': job['raw_events']}))
            web.complete_fight(code, fid, fetch, 0, 100, len(job['raw_events']))
        restored.merge_hybrid_fight(web, code, fid)
        self.enriched.append(fid)
        return master


def setup(tmp_path, concurrency=1):
    archive = FightArchive(tmp_path / 'unused', compression='fast')
    db = IndexDB(tmp_path / 'index.db')
    client = PendingClient(archive)
    pipeline = HarvesterPipeline(tmp_path, db, MasterDictDB(tmp_path / 'master.db'), client,
                                 download_concurrency=concurrency, pack_workers=1, raw_archive=archive)
    return db, pipeline, client


def test_api_staged_priority_and_failed_retry_preserve_pending_phase(tmp_path):
    db = IndexDB(tmp_path / 'index.db')
    for code, kind in [('raid', 'raid'), ('key1', 'mplus'), ('key2', 'mplus')]:
        db.enqueue_fight(code, 1, kind, 9, 5, 42, 'boss')
        db.mark_fight_api_staged(code, 1, f'staging/{code}.wcls')
    assert [row['report_code'] for row in db.get_api_staged_fights()] == ['key1', 'key2', 'raid']
    assert db.get_staged_fights() == []
    assert db.count_api_staged_fights() == 3
    db.mark_fight_failed('key1', 1, 'browser quota')
    db.retry_failed_work()
    row = db.get_fight('key1', 1)
    assert row['status'] == 'API_STAGED' and row['needs_browser'] == 1
    assert row['staging_rel_path'] == 'staging/key1.wcls'
    db.mark_fight_staged('key1', 1, 'staging/full.wcls')
    assert db.get_fight('key1', 1)['needs_browser'] == 0


async def test_blocked_browser_never_occupies_api_download_slot(tmp_path):
    db, pipeline, client = setup(tmp_path)
    for fid in (1, 2):
        enqueue(db, fid)
    try:
        assert await pipeline.drain_queue_batch(batch_size=1) == 1
        await asyncio.wait_for(client.entered.wait(), 2)
        assert await asyncio.wait_for(pipeline.drain_queue_batch(batch_size=1), 2) == 1
        assert client.downloads == [('Report', 1), ('Report', 2)]
        assert db.get_queue_counts()['api_staged_fights'] == 2
        assert db.get_queue_counts()['staged_fights'] == 0
        assert pipeline.get_staging_stats()['browser_enriching_fights'] == 1
        assert pipeline.raw_archive.stats()['buffered_complete_fights'] == 0
        client.release.set()
        await pipeline.finish_background_packing()
        assert db.get_fight('Report', 2)['status'] == 'PACKED'
        assert pipeline.staging_store.list_jobs() == []
    finally:
        client.release.set()
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_browser_failure_restart_reuses_valid_api_without_refetch(tmp_path):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    client.error = RuntimeError('browser quota')
    client.release.set()
    await pipeline.drain_queue_batch(batch_size=1)
    await pipeline.finish_background_packing()
    row = db.get_fight('Report', 1)
    assert row['status'] == 'FAILED' and row['needs_browser'] == 1
    raw = tmp_path / row['staging_rel_path']
    assert raw.exists()
    pipeline.close()
    _, restarted, client = setup(tmp_path)
    db.retry_failed_work()
    client.release.set()
    try:
        await restarted.start_background_packing()
        await restarted.finish_background_packing()
        assert client.downloads == []
        assert db.get_fight('Report', 1)['status'] == 'PACKED'
        assert not raw.exists()
    finally:
        await restarted.finish_background_packing(cancel=True)
        restarted.close()


async def test_cancel_browser_network_keeps_pending_api_snapshot(tmp_path):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    try:
        await pipeline.drain_queue_batch(batch_size=1)
        await asyncio.wait_for(client.entered.wait(), 2)
        await asyncio.wait_for(pipeline.finish_background_packing(cancel=True), 2)
        row = db.get_fight('Report', 1)
        assert row['status'] == 'API_STAGED' and row['needs_browser'] == 1
        job, restored = pipeline.staging_store.load(tmp_path / row['staging_rel_path'])
        assert job['manifest']['hybrid_pending'] is True
        restored.clear()
    finally:
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def stage_pending(db, pipeline, client, fid=1):
    enqueue(db, fid)
    row = db.get_fight('Report', fid)
    master = await client.fetch_report_master_and_fights('Report')
    out, meta, actors, abilities, start, end = pipeline._prepare_fight_payload(row, master)
    await client.fetch_all_fight_events('Report', fid, start, end)
    path = pipeline.staging_store.stage_fight(pipeline.raw_archive, row, meta, actors, abilities, out)
    return path


async def stage_full(pipeline, client, path):
    job, restored = pipeline.staging_store.load(path)
    client.release.set()
    master = await client.enrich_staged_fight(job, restored)
    out, meta, actors, abilities, _, _ = pipeline._prepare_fight_payload(job['fight_row'], master)
    return pipeline.staging_store.stage_fight(restored, job['fight_row'], meta, actors, abilities, out)


async def test_orphan_api_recovers_as_pending_without_packer_access(tmp_path):
    db, pipeline, client = setup(tmp_path)
    path = await stage_pending(db, pipeline, client)
    client.downloads.clear()
    try:
        assert pipeline._recover_staging() == []
        row = db.get_fight('Report', 1)
        assert row['status'] == 'API_STAGED' and row['needs_browser'] == 1
        assert db.get_staged_fights() == []
        assert client.downloads == [] and path.exists()
    finally:
        pipeline.close()


@pytest.mark.parametrize('linked', [False, True])
async def test_full_orphan_wins_matching_api_crash_window(tmp_path, linked):
    db, pipeline, client = setup(tmp_path)
    pending = await stage_pending(db, pipeline, client)
    if linked:
        db.mark_fight_api_staged('Report', 1, pending.relative_to(tmp_path).as_posix())
        db.mark_fight_failed('Report', 1, 'database link interrupted')
    full = await stage_full(pipeline, client, pending)
    assert full != pending and pending.exists()
    try:
        pipeline._recover_staging()
        row = db.get_fight('Report', 1)
        assert row['status'] == 'STAGED' and row['needs_browser'] == 0
        assert tmp_path / row['staging_rel_path'] == full
        assert full.exists() and not pending.exists()
        await pipeline.start_background_packing()
        await pipeline.finish_background_packing()
        assert db.get_fight('Report', 1)['status'] == 'PACKED'
    finally:
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_recovery_retains_mismatched_api_attempt(tmp_path):
    db, pipeline, client = setup(tmp_path)
    pending = await stage_pending(db, pipeline, client)
    full = await stage_full(pipeline, client, pending)
    another = await stage_pending(db, pipeline, client)
    db.mark_fight_api_staged('Report', 1, another.relative_to(tmp_path).as_posix())
    try:
        pipeline._recover_staging()
        row = db.get_fight('Report', 1)
        assert row['status'] == 'API_STAGED'
        assert tmp_path / row['staging_rel_path'] == another
        assert another.exists() and pending.exists() and full.exists()
    finally:
        pipeline.close()


@pytest.mark.parametrize('phase', ['api', 'hybrid'])
async def test_failed_database_link_recovers_without_api_redownload(tmp_path, monkeypatch, phase):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    client.release.set()
    marker = 'mark_fight_api_staged' if phase == 'api' else 'mark_fight_staged'
    original = getattr(db, marker)
    def fail(*args):
        raise OSError('link crashed')
    monkeypatch.setattr(db, marker, fail)
    await pipeline.drain_queue_batch(batch_size=1)
    await pipeline.finish_background_packing()
    assert pipeline.staging_store.list_jobs()
    pipeline.close()
    monkeypatch.setattr(db, marker, original)
    _, restarted, restarted_client = setup(tmp_path)
    restarted_client.release.set()
    try:
        db.retry_failed_work()
        await restarted.start_background_packing()
        await restarted.finish_background_packing()
        assert restarted_client.downloads == []
        assert db.get_fight('Report', 1)['status'] == 'PACKED'
        assert restarted.staging_store.list_jobs() == []
    finally:
        await restarted.finish_background_packing(cancel=True)
        restarted.close()


async def test_stop_and_repeated_cancel_wait_for_active_hybrid_atomic_writer(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    client.release.set()
    entered, release = threading.Event(), threading.Event()
    original = pipeline.staging_store.stage_fight
    def block(archive, *args):
        if archive.fight_manifest('Report', 1).get('hybrid'):
            entered.set()
            assert release.wait(5)
        return original(archive, *args)
    monkeypatch.setattr(pipeline.staging_store, 'stage_fight', block)
    finishing = None
    try:
        await pipeline.drain_queue_batch(batch_size=1)
        await until(entered.is_set)
        finishing = asyncio.create_task(pipeline.finish_background_packing(cancel=True))
        await asyncio.sleep(.03)
        finishing.cancel()
        await asyncio.sleep(.03)
        finishing.cancel()
        await asyncio.sleep(.03)
        assert not finishing.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await finishing
        row = db.get_fight('Report', 1)
        assert row['status'] in ('STAGED', 'PACKED')
        assert row['needs_browser'] == 0
        if row['status'] == 'STAGED':
            job, restored = pipeline.staging_store.load(tmp_path / row['staging_rel_path'])
            assert job['manifest'].get('hybrid')
            restored.clear()
        assert pipeline.get_staging_stats()['browser_enriching_fights'] == 0
    finally:
        release.set()
        if finishing and not finishing.done():
            await finishing
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_live_browser_concurrency_is_independent_of_api_workers(tmp_path):
    db, pipeline, client = setup(tmp_path, concurrency=2)
    for fid in (1, 2, 3):
        enqueue(db, fid)
    try:
        await pipeline.drain_queue_batch(batch_size=3)
        await until(lambda: len(pipeline._browser_claims) == 2)
        assert len(client.downloads) == 3
        assert db.count_api_staged_fights() == 3
        pipeline.download_concurrency = 3
        await until(lambda: len(pipeline._browser_claims) == 3)
        client.release.set()
        await pipeline.finish_background_packing()
        assert sorted(client.enriched) == [1, 2, 3]
    finally:
        client.release.set()
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_provider_retry_restarts_browser_manager_with_existing_packers(tmp_path):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    try:
        await pipeline.drain_queue_batch(batch_size=1)
        await asyncio.wait_for(client.entered.wait(), 2)
        packers = list(pipeline._background_pack_tasks)
        await pipeline.stop_background_downloading()
        await pipeline.stop_background_enrichment(cancel=True)
        assert db.get_fight('Report', 1)['status'] == 'API_STAGED'
        replacement = PendingClient(pipeline.raw_archive)
        replacement.release.set()
        pipeline.client = replacement
        await pipeline.start_background_packing()
        assert pipeline._background_pack_tasks == packers
        await pipeline.finish_background_packing()
        assert replacement.downloads == [] and replacement.enriched == [1]
        assert client.enriched == []
        assert db.get_fight('Report', 1)['status'] == 'PACKED'
    finally:
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_nonhybrid_finite_wait_returns_incomplete_with_pending_browser_work(tmp_path):
    db, pipeline, client = setup(tmp_path)
    path = await stage_pending(db, pipeline, client)
    db.mark_fight_api_staged('Report', 1, path.relative_to(tmp_path).as_posix())
    pipeline.client = CapturingClient(pipeline.raw_archive)
    try:
        rows = [db.get_fight('Report', 1)]
        assert await asyncio.wait_for(pipeline._wait_for_staged_rows(rows), .25) == []
        assert db.get_fight('Report', 1)['status'] == 'API_STAGED'
    finally:
        pipeline.close()


async def test_cancel_finite_processing_aborts_browser_network_and_keeps_api_raw(tmp_path):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    processing = asyncio.create_task(pipeline._process_rows_pipeline([db.get_fight('Report', 1)]))
    try:
        await asyncio.wait_for(client.entered.wait(), 2)
        processing.cancel()
        await asyncio.sleep(.15)
        assert processing.done(), 'Finite processing cancellation must abort browser network'
        with pytest.raises(asyncio.CancelledError):
            await processing
        row = db.get_fight('Report', 1)
        assert row['status'] == 'API_STAGED' and row['needs_browser'] == 1
        assert (tmp_path / row['staging_rel_path']).exists()
    finally:
        await pipeline.stop_background_enrichment(cancel=True)
        client.release.set()
        await asyncio.gather(processing, return_exceptions=True)
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_pending_cleanup_failure_recovers_only_after_matching_packed_hybrid(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    client.release.set()
    original = pipeline.staging_store.delete
    def fail_pending(path):
        job, restored = pipeline.staging_store.load(path)
        restored.clear()
        if job['manifest'].get('hybrid_pending'):
            raise OSError('API cleanup unavailable')
        return original(path)
    monkeypatch.setattr(pipeline.staging_store, 'delete', fail_pending)
    await pipeline.drain_queue_batch(batch_size=1)
    await pipeline.finish_background_packing()
    row = db.get_fight('Report', 1)
    assert row['status'] == 'PACKED' and row['staging_rel_path'] is None
    pending = pipeline.staging_store.list_jobs()[0]
    # A different pending attempt for the same fight must survive cleanup.
    different = await stage_pending(db, pipeline, client)
    pipeline.close()
    _, restarted, _ = setup(tmp_path)
    try:
        restarted._recover_staging()
        assert not pending.exists()
        assert different.exists()
        assert db.get_fight('Report', 1)['status'] == 'PACKED'
    finally:
        restarted.close()


async def test_hybrid_manifest_copy_does_not_block_api_event_loop(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    client.release.set()
    main_thread = threading.get_ident()
    original = FightArchive.fight_manifest
    def checked(archive, code, fid):
        completed = archive._completed.get((code, fid))
        if completed and completed['manifest'].get('hybrid'):
            assert threading.get_ident() != main_thread, 'Large hybrid manifest copying blocks API scheduling'
        return original(archive, code, fid)
    monkeypatch.setattr(FightArchive, 'fight_manifest', checked)
    try:
        await pipeline.drain_queue_batch(batch_size=1)
        await pipeline.finish_background_packing()
        row = db.get_fight('Report', 1)
        assert row['status'] == 'PACKED', row['last_error']
    finally:
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()

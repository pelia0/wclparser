import asyncio
from copy import deepcopy
from decimal import Decimal
import json
import threading

import pytest

from test_disk_pipeline import enqueue, until
from test_hybrid_pipeline import setup, stage_pending, stage_full
from test_staging import captured, stage
from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader
from wcl_archiver.codec.staging import StagingStore
from wcl_archiver.config import AppConfig
from wcl_archiver.harvester.pipeline import HarvesterPipeline


def test_non_negative_processing_budget(tmp_path):
    assert AppConfig().processing_concurrency == 2
    with pytest.raises(ValueError, match='processing_concurrency'):
        HarvesterPipeline(tmp_path, None, None, None, processing_concurrency=-1)
    with pytest.raises(ValueError, match='processing_concurrency'):
        AppConfig(processing_concurrency=-1)


def test_lean_load_preserves_exact_archive_without_event_view(tmp_path, monkeypatch):
    store = StagingStore(tmp_path)
    args = captured(tmp_path)
    completed = deepcopy(args[0]._completed[('Report1', 1)])
    path = stage(store, args, tmp_path)
    def no_event_view(*args):
        raise AssertionError('eager event materialization')
    monkeypatch.setattr(FightArchive, 'full_fight_events', no_event_view)
    job, archive = store.load(path, include_events=False)
    assert 'raw_events' not in job
    out = tmp_path / 'exact.wclf'
    archive.write_fight(out, job['fight_meta'], 'Report1', 1)
    sources = PackedFightReader.read_sections(out)['metadata']['manifest']
    assert sources['event_count'] == 1
    assert PackedFightReader.read_raw_events(out) == [{
        'timestamp': 1, 'n': Decimal('0.12345678901234567890123456789'),
        'large': 184467440737095516160, 'unknown': [None, False, {'x': Decimal('1.00')}]}]
    for cap in [completed['master'], *completed['pages'], *completed['replay']]:
        assert json.loads(PackedFightReader.read_source_response(out, cap['info']['id']),
                          parse_float=Decimal) == cap['body']
    archive.clear()


def test_runtime_stats_never_collect_disk_or_catalogue(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    def forbidden(*args):
        raise AssertionError('runtime counters performed slow collection')
    monkeypatch.setattr(pipeline.staging_store, 'stats', forbidden)
    monkeypatch.setattr(db, 'count_api_staged_fights', forbidden)
    try:
        assert pipeline.get_runtime_stats()['processing_active'] == 0
        assert pipeline.get_runtime_stats()['processing_waiting'] == 0
    finally:
        pipeline.close()


async def test_recovery_and_unlinked_restore_do_not_materialize_events(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    path = await stage_pending(db, pipeline, client)
    def no_event_view(*args):
        raise AssertionError('eager event materialization')
    monkeypatch.setattr(FightArchive, 'full_fight_events', no_event_view)
    try:
        assert pipeline._recover_staging() == []
        assert db.get_fight('Report', 1)['status'] == 'API_STAGED'
        enqueue(db, 2)
        path2 = await stage_pending(db, pipeline, client, 2)
        pipeline._unlinked_staging['Report_2'] = path2
        assert await pipeline._download_one_to_staging(db.get_fight('Report', 2), 1) == 1
        assert db.get_fight('Report', 2)['status'] == 'API_STAGED'
    finally:
        pipeline.close()


async def test_browser_budget_waiters_stop_without_loading_and_api_progresses(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path, concurrency=8)
    # Packing owns a separate pool and cannot consume a browser processing slot.
    first = await stage_pending(db, pipeline, client, 1)
    full = await stage_full(pipeline, client, first)
    pipeline.staging_store.delete(first)
    db.mark_fight_staged('Report', 1, full.relative_to(tmp_path).as_posix())
    client.release.clear()
    client.entered.clear()
    entered, release = threading.Event(), threading.Event()
    original = pipeline._pack_staged_cpu
    def blocked(*args):
        entered.set()
        assert release.wait(10)
        return original(*args)
    monkeypatch.setattr(pipeline, '_pack_staged_cpu', blocked)
    loaded = []
    load = pipeline.staging_store.load
    def tracked(path, **kwargs):
        loaded.append(path)
        return load(path, **kwargs)
    monkeypatch.setattr(pipeline.staging_store, 'load', tracked)
    try:
        await pipeline.start_background_packing()
        await until(entered.is_set)
        assert pipeline.get_runtime_stats()['packing_fights'] == 1
        assert loaded == []  # the linked snapshot only needs a header check at startup
        browser_paths = []
        for fid in (2, 3, 4):
            path = await stage_pending(db, pipeline, client, fid)
            browser_paths.append(path)
            db.mark_fight_api_staged('Report', fid, path.relative_to(tmp_path).as_posix())
        pipeline._browser_wakeup.set()
        await asyncio.wait_for(client.entered.wait(), 2)
        await until(lambda: pipeline.get_runtime_stats()['processing_waiting'] >= 1 and len(loaded) == 2)
        stats = pipeline.get_runtime_stats()
        assert stats['processing_active'] == 2
        assert stats['processing_concurrency'] == 2
        assert stats['packing_fights'] == 1 and stats['browser_enriching_fights'] == 2
        # Only admitted browser work restores snapshots. Waiting rows stay on disk.
        assert len(loaded) == 2
        assert set(loaded).issubset(browser_paths)
        enqueue(db, 5)
        assert await asyncio.wait_for(pipeline.drain_queue_batch(batch_size=1), 2) == 1
        assert db.get_fight('Report', 5)['status'] == 'API_STAGED'
        before_stop = len(loaded)
        finishing = asyncio.create_task(pipeline.finish_background_packing(cancel=True))
        await until(lambda: pipeline.get_runtime_stats()['processing_waiting'] == 0)
        assert not finishing.done()
        release.set()
        await asyncio.wait_for(finishing, 3)
        assert len(loaded) == before_stop + 1  # active pack load, no waiter load
        assert pipeline.get_runtime_stats()['processing_active'] == 0
        for fid in (2, 3, 4, 5):
            row = db.get_fight('Report', fid)
            assert row['status'] == 'API_STAGED'
            assert (tmp_path / row['staging_rel_path']).exists()
    finally:
        release.set()
        client.release.set()
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_cancel_waiter_releases_accounting_and_permits(tmp_path):
    db, pipeline, client = setup(tmp_path, concurrency=8)
    tasks = []
    pipeline._browser_wakeup = asyncio.Event()
    pipeline._packing_wakeup = asyncio.Event()
    try:
        for fid in (1, 2, 3):
            path = await stage_pending(db, pipeline, client, fid)
            db.mark_fight_api_staged('Report', fid, path.relative_to(tmp_path).as_posix())
        tasks = [asyncio.create_task(pipeline._enrich_one_staged(db.get_fight('Report', fid), fid))
                 for fid in (1, 2, 3)]
        await until(lambda: pipeline.get_runtime_stats()['processing_waiting'] == 1)
        waiter = tasks[-1]
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        assert pipeline.get_runtime_stats()['processing_waiting'] == 0
        assert pipeline.get_runtime_stats()['processing_active'] == 2
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert pipeline.get_runtime_stats()['processing_active'] == 0
        # A fresh run must have the full budget, rather than leaked permits.
        client.release.set()
        await pipeline.start_background_packing()
        await pipeline.finish_background_packing()
        assert all(db.get_fight('Report', fid)['status'] == 'PACKED' for fid in (1, 2, 3))
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()

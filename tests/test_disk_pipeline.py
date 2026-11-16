import asyncio
import threading

import pytest

from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader, json_bytes
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.harvester.pipeline import HarvesterPipeline


class CapturingClient:
    def __init__(self, archive):
        self.archive, self.downloads = archive, []

    async def unarchive_report(self, code):
        return True

    async def fetch_report_master_and_fights(self, code):
        master = {'fights': [{'id': i, 'boss': 42, 'start_time': 0, 'end_time': 100} for i in range(1, 9)],
                  'friendlies': [], 'abilities': [], 'unknown': {'value': None}}
        self.archive.capture('GET', f'/reports/fights-and-participants/{code}/0', json_bytes(master))
        return master

    async def fetch_all_fight_events(self, code, fid, start, end):
        self.downloads.append((code, fid))
        events = [{'timestamp': 1, 'type': 'unknown', 'unknown': [None, {'all': True}]}]
        with self.archive.fight_capture(code, fid) as fetch:
            self.archive.capture('GET', f'/reports/summary-events/{code}/{fid}/0/100', json_bytes({'events': events}))
            self.archive.complete_fight(code, fid, fetch, start, end, len(events))
        return events


def setup(tmp_path, workers=1):
    archive = FightArchive(tmp_path / 'unused', compression='fast')
    db = IndexDB(tmp_path / 'index.db')
    client = CapturingClient(archive)
    pipeline = HarvesterPipeline(tmp_path, db, MasterDictDB(tmp_path / 'master.db'), client,
                                 pack_workers=workers, raw_archive=archive)
    return db, pipeline, client


def enqueue(db, fid, kind='mplus'):
    db.enqueue_fight('Report', fid, kind, 9, 5, 42, 'boss')
    db.mark_report_unarchived('Report')


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(.01)
    await asyncio.wait_for(wait(), 5)


async def test_blocked_pack_does_not_block_next_download_batch(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    entered, release = threading.Event(), threading.Event()
    original = FightArchive.write_fight
    def block(*args):
        entered.set()
        assert release.wait(5)
        return original(*args)
    monkeypatch.setattr(FightArchive, 'write_fight', block)
    for fid in (1, 2):
        enqueue(db, fid)
    try:
        await pipeline.start_background_packing()
        assert await asyncio.wait_for(pipeline.drain_queue_batch(batch_size=1), 2) == 1
        await until(entered.is_set)
        assert await asyncio.wait_for(pipeline.drain_queue_batch(batch_size=1), 2) == 1
        assert client.downloads == [('Report', 1), ('Report', 2)]
        assert db.get_queue_counts()['staged_fights'] == 2
        assert pipeline.get_staging_stats()['staging_files'] == 2
        assert pipeline.raw_archive.stats()['buffered_complete_fights'] == 0
    finally:
        release.set()
        await pipeline.finish_background_packing()
        pipeline.close()
    assert db.get_fight('Report', 2)['status'] == 'PACKED'
    assert pipeline.get_staging_stats()['staging_files'] == 0


async def test_stop_waits_active_writer_and_retains_queued_raw(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    entered, release = threading.Event(), threading.Event()
    original = FightArchive.write_fight
    def block(*args):
        entered.set()
        assert release.wait(5)
        return original(*args)
    monkeypatch.setattr(FightArchive, 'write_fight', block)
    for fid in (1, 2, 3):
        enqueue(db, fid)
    await pipeline.start_background_packing()
    try:
        assert await pipeline.drain_queue_batch(batch_size=3) == 3
        await until(entered.is_set)
        finishing = asyncio.create_task(pipeline.finish_background_packing(cancel=True))
        await asyncio.sleep(.05)
        assert not finishing.done()
        release.set()
        await finishing
        assert db.get_queue_counts()['staged_fights'] == 2
        assert pipeline.get_staging_stats()['staging_files'] == 2
    finally:
        release.set()
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()
    _, restarted, restarted_client = setup(tmp_path)
    try:
        await restarted.start_background_packing()
        await restarted.finish_background_packing()
        assert restarted_client.downloads == []
        assert db.get_queue_counts()['staged_fights'] == 0
        assert restarted.get_staging_stats()['staging_files'] == 0
    finally:
        restarted.close()


async def test_orphan_raw_recovers_after_database_link_failure(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    def fail(*args):
        raise OSError('database link failed')
    monkeypatch.setattr(db, 'mark_fight_staged', fail)
    await pipeline.start_background_packing()
    await pipeline.drain_queue_batch(batch_size=1)
    await pipeline.finish_background_packing(cancel=True)
    assert pipeline.get_staging_stats()['staging_files'] == 1
    pipeline.close()
    _, restarted, restarted_client = setup(tmp_path)
    try:
        await restarted.start_background_packing()
        await restarted.finish_background_packing()
        assert restarted_client.downloads == []
        assert db.get_fight('Report', 1)['status'] == 'PACKED'
    finally:
        restarted.close()


@pytest.mark.parametrize('failure', ['pack', 'commit', 'delete'])
async def test_pack_commit_or_deletion_failure_retains_recoverable_raw(tmp_path, monkeypatch, failure):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    def fail(*args, **kwargs):
        raise OSError(f'{failure} failed')
    if failure == 'pack':
        monkeypatch.setattr(FightArchive, 'write_fight', fail)
    elif failure == 'commit':
        monkeypatch.setattr(db, 'mark_fight_packed', fail)
    else:
        monkeypatch.setattr(pipeline.staging_store, 'delete', fail)
    try:
        await pipeline.start_background_packing()
        await pipeline.drain_queue_batch(batch_size=1)
        await pipeline.finish_background_packing()
        row = db.get_fight('Report', 1)
        assert row['status'] == ('PACKED' if failure == 'delete' else 'FAILED')
        assert row['staging_rel_path']
        assert pipeline.get_staging_stats()['staging_files'] == 1
        job, restored = pipeline.staging_store.load(tmp_path / row['staging_rel_path'])
        assert job['raw_events'][0]['unknown'] == [None, {'all': True}]
    finally:
        pipeline.close()


@pytest.mark.parametrize('damage', ['corrupt', 'missing'])
async def test_invalid_staged_raw_never_redownloads_or_claims_packed(tmp_path, monkeypatch, damage):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    # Stage without letting the background worker claim it yet.
    row = db.get_fight('Report', 1)
    master = await client.fetch_report_master_and_fights('Report')
    out, meta, actors, abilities, start, end = pipeline._prepare_fight_payload(row, master)
    await client.fetch_all_fight_events('Report', 1, start, end)
    path = pipeline.staging_store.stage_fight(pipeline.raw_archive, row, meta, actors, abilities, out)
    db.mark_fight_staged('Report', 1, path.relative_to(tmp_path).as_posix())
    if damage == 'corrupt':
        path.write_bytes(b'broken')
    else:
        path.unlink()
    client.downloads.clear()
    try:
        await pipeline.start_background_packing()
        await pipeline.finish_background_packing()
        assert await pipeline.drain_queue_batch() == 0
        assert client.downloads == []
        assert db.get_fight('Report', 1)['status'] == 'FAILED'
        assert path.exists() == (damage == 'corrupt')
    finally:
        pipeline.close()


async def test_four_workers_never_pack_same_fight_twice(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path, workers=4)
    seen, lock = [], threading.Lock()
    original = FightArchive.write_fight
    def record(archive, out, summary, code, fid):
        with lock:
            seen.append(fid)
        return original(archive, out, summary, code, fid)
    monkeypatch.setattr(FightArchive, 'write_fight', record)
    for fid in range(1, 9):
        enqueue(db, fid)
    try:
        await pipeline.start_background_packing()
        await pipeline.drain_queue_batch(batch_size=8)
        await pipeline.finish_background_packing()
        assert sorted(seen) == list(range(1, 9))
        assert db.get_queue_counts()['staged_fights'] == 0
    finally:
        pipeline.close()


async def test_cancelled_download_awaits_stage_write_before_return(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    entered, release = threading.Event(), threading.Event()
    original = pipeline.staging_store.stage_fight
    def block(*args):
        entered.set()
        assert release.wait(5)
        return original(*args)
    monkeypatch.setattr(pipeline.staging_store, 'stage_fight', block)
    try:
        await pipeline.start_background_packing()
        downloading = asyncio.create_task(pipeline.drain_queue_batch(batch_size=1))
        await until(entered.is_set)
        downloading.cancel()
        await asyncio.sleep(.05)
        assert not downloading.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await downloading
        await pipeline.finish_background_packing(cancel=True)
        row = db.get_fight('Report', 1)
        assert row['status'] in ('STAGED', 'PACKED')
        if row['status'] == 'STAGED':
            assert (tmp_path / row['staging_rel_path']).exists()
    finally:
        release.set()
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_database_link_retry_reuses_published_raw_in_same_run(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    original = db.mark_fight_staged
    calls = 0
    def fail_once(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError('catalogue temporarily busy')
        return original(*args)
    monkeypatch.setattr(db, 'mark_fight_staged', fail_once)
    try:
        await pipeline.start_background_packing()
        await pipeline.drain_queue_batch(batch_size=1)
        await pipeline.drain_queue_batch(batch_size=1)
        await pipeline.finish_background_packing()
        assert client.downloads == [('Report', 1)]
        assert db.get_fight('Report', 1)['status'] == 'PACKED'
        assert pipeline.get_staging_stats()['staging_files'] == 0
    finally:
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


async def test_worker_catalogue_failure_surfaces_to_download_loop(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    def fail(*args, **kwargs):
        raise OSError('catalogue read failed')
    monkeypatch.setattr(db, 'get_staged_fights', fail)
    await pipeline.start_background_packing()
    await asyncio.sleep(.01)
    try:
        with pytest.raises(RuntimeError, match='catalogue read failed'):
            await pipeline.drain_queue_batch()
    finally:
        with pytest.raises(RuntimeError, match='catalogue read failed'):
            await pipeline.finish_background_packing(cancel=True)
        pipeline.close()


@pytest.mark.parametrize('replace_archive', [False, True])
async def test_packed_raw_leftovers_delete_only_after_matching_full_verification(tmp_path, monkeypatch, replace_archive):
    db, pipeline, client = setup(tmp_path)
    enqueue(db, 1)
    def fail(*args):
        raise OSError('cleanup unavailable')
    monkeypatch.setattr(pipeline.staging_store, 'delete', fail)
    try:
        await pipeline.start_background_packing()
        await pipeline.drain_queue_batch(batch_size=1)
        await pipeline.finish_background_packing()
        row = db.get_fight('Report', 1)
        raw = tmp_path / row['staging_rel_path']
        assert raw.exists()
        if replace_archive:
            # Another valid archive, with a different fetch ID, cannot certify
            # deletion of this source attempt even for the same report/fight.
            await client.fetch_report_master_and_fights('Report')
            await client.fetch_all_fight_events('Report', 1, 0, 100)
            pipeline.raw_archive.write_fight(tmp_path / row['file_rel_path'], {}, 'Report', 1)
    finally:
        pipeline.close()
    _, restarted, restarted_client = setup(tmp_path)
    try:
        await restarted.start_background_packing()
        await restarted.finish_background_packing()
        assert raw.exists() == replace_archive
        assert restarted_client.downloads == []
        assert db.get_fight('Report', 1)['status'] == 'PACKED'
        assert bool(db.get_fight('Report', 1)['staging_rel_path']) == replace_archive
    finally:
        restarted.close()

from decimal import Decimal
import asyncio
import json
import re
import threading

import pytest

from wcl_archiver.codec.browser_staging import BrowserWork


def test_immutable_response_resume_precision_and_tamper(tmp_path):
    work = BrowserWork(tmp_path, "a.wcls", "1" * 64)
    work.initialize("ABC", 1, 0, 20, "fetch")
    path = work.master_path
    raw = b'{"fights":[{"id":1,"boss":2}],"n":1.1234567890123456789,"missing":null}'
    work.save_response(path, raw)
    reopened = BrowserWork(tmp_path, "a.wcls", "1" * 64)
    assert reopened.response(path)["n"] == Decimal("1.1234567890123456789")
    with pytest.raises(ValueError):
        reopened.save_response(path, b"{}")
    target = reopened.response_file(path)
    target.write_bytes(target.read_bytes()[:-1])
    with pytest.raises(ValueError):
        reopened.response(path)


def test_attempt_isolation_and_path_binding(tmp_path):
    original = BrowserWork(tmp_path, "a.wcls", "1" * 64)
    original.initialize("ABC", 2, 0, 99, "fetch")
    original.save_response(original.master_path, b'{"fights":[]}')
    other = BrowserWork(tmp_path, "a.wcls", "2" * 64)
    assert other.phase() == "initialize"
    with pytest.raises(ValueError):
        original.initialize("XYZ", 2, 0, 99, "fetch")

async def test_two_blocked_processors_leave_32_network_workers_and_fight_33(tmp_path):
    from test_hybrid_pipeline import setup
    from test_disk_pipeline import until

    db, pipeline, client = setup(tmp_path, concurrency=32)
    activity = []
    pipeline._pack_log = activity.append
    entered = []
    release = asyncio.Event()
    cpu_release = threading.Event()
    cpu_entered = [threading.Event(), threading.Event()]
    cpu_tasks = []
    def block_cpu(entered):
        entered.set()
        assert cpu_release.wait(20)
    async def download(work, phase):
        assert phase == "replay"
        code, fid, start, end, _ = work.read("descriptor")
        entered.append(code)
        await release.wait()
        master = {"fights": [{"id": fid, "start_time": start, "end_time": end, "boss": 99}]}
        work.save_response(work.master_path, json.dumps(master).encode())
        paths = work.replay_paths(master)
        for path in paths:
            work.save_response(path, b'{"events":[]}')
        work.write("replay", paths)

    client.download_supplement_phase = download
    for i in range(34):
        code = f"Fight{i}"
        db.enqueue_fight(code, 1, "mplus", 9, 5, 42, "boss")
        db.mark_report_unarchived(code)
    try:
        for i in range(33):
            row = db.get_fight(f"Fight{i}", 1)
            await pipeline._download_one_to_staging(row, 1)
        async with pipeline._processing_admission(set(), "a", lambda: False):
            async with pipeline._processing_admission(set(), "b", lambda: False):
                cpu_tasks = [asyncio.create_task(pipeline._raw_io(block_cpu, event))
                             for event in cpu_entered]
                await until(lambda: all(event.is_set() for event in cpu_entered))
                await pipeline.start_background_packing()
                await until(lambda: len(entered) == 32)
                assert pipeline.get_runtime_stats()["browser_enriching_fights"] == 32
                assert pipeline.get_runtime_stats()["processing_active"] == 2
                starts = [m for line in activity if (m := re.fullmatch(
                    r"Started browser replay for (Fight\d+_1) \(browser worker #(\d+)\)", line))]
                assert {m[1] for m in starts} == {code + "_1" for code in entered}
                assert {int(m[2]) for m in starts} == set(range(1, 33))
                assert not any("Completed browser replay" in line for line in activity)
                assert await pipeline._download_one_to_staging(db.get_fight("Fight33", 1), 1) == 1
                release.set()
                await until(lambda: len(entered) == 34)
                await until(lambda: pipeline.get_runtime_stats()["browser_enriching_fights"] == 0)
                assert db.count_api_staged_fights() == 34
                for m in starts:
                    assert f"Completed browser replay for {m[1]} (browser worker #{m[2]})" in activity
                assert pipeline.raw_archive.stats()["buffered_complete_fights"] == 0
                assert all(work.phase() == "plan" for work in
                           (pipeline.staging_store.browser_work(p) for p in pipeline.staging_store.list_jobs()))
                await pipeline.finish_background_packing(cancel=True)
    finally:
        release.set()
        cpu_release.set()
        await asyncio.gather(*cpu_tasks)
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()

@pytest.mark.parametrize("resume_after", ["descriptor", "replay", "windows", "canonical",
                                          "stop_writer", "stop_plan", "stop_merge", "stop_publish"])
async def test_restart_drains_disk_phases_to_verified_archive(tmp_path, resume_after, monkeypatch):
    from test_hybrid_pipeline import setup
    from test_disk_pipeline import enqueue
    from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader
    from wcl_archiver.wcl_client.browser_client import BrowserClient

    db, pipeline, client = setup(tmp_path)
    browser = BrowserClient(tmp_path / "profile", raw_archive=FightArchive(tmp_path / "unused-browser"))
    paths = []
    class LocalPage:
        url = BrowserClient.BASE_URL
        def is_closed(self):
            return False

        async def evaluate(self, script, arg):
            path = arg["url"]
            paths.append(path)
            if "fights-and-participants" in path:
                body = {"fights": [{"id": 1, "boss": 99, "start_time": 0, "end_time": 100}]}
            elif "replaysegment" in path:
                body = {"events": []}
            else:
                body = {"events": [{"timestamp": 1, "type": "unknown", "new": [None, True]}]}
            return {"status": 200, "text": json.dumps(body), "retryAfter": ""}

    browser._context, browser._page = object(), LocalPage()
    client.download_supplement_phase = browser.download_supplement_phase
    enqueue(db, 1)
    try:
        assert await pipeline._download_one_to_staging(db.get_fight("Report", 1), 1) == 1
        path, = pipeline.staging_store.list_jobs()
        work = pipeline.staging_store.browser_work(path)
        if resume_after.startswith("stop_"):
            entered, release = threading.Event(), threading.Event()
            def gate(fn, predicate):
                def wrapped(*args, **kwargs):
                    if predicate(*args, **kwargs):
                        entered.set()
                        assert release.wait(10)
                    return fn(*args, **kwargs)
                return wrapped
            with monkeypatch.context() as patch:
                if resume_after == "stop_writer":
                    patch.setattr(BrowserWork, "save_response", gate(
                        BrowserWork.save_response, lambda self, key, raw: "replaysegment" in key))
                elif resume_after == "stop_publish":
                    store = pipeline.staging_store
                    patch.setattr(store, "stage_fight", gate(store.stage_fight, lambda *args: True))
                else:
                    patch.setattr(BrowserWork, "process", gate(
                        BrowserWork.process, lambda *args, merge: merge == (resume_after == "stop_merge")))
                await pipeline.start_background_packing()
                try:
                    assert await asyncio.to_thread(entered.wait, 6)
                    pipeline.request_stop()
                    stop_task = asyncio.create_task(pipeline.finish_background_packing(cancel=True))
                    await asyncio.sleep(.05)
                    assert not stop_task.done()
                finally:
                    release.set()
                await asyncio.wait_for(stop_task, 5)
        elif resume_after != "descriptor":
            await browser.download_supplement_phase(work, "replay")
        if resume_after in ("windows", "canonical"):
            job, archive = pipeline.staging_store.load(path)
            work.process(job, archive, merge=False)
            del job, archive
        if resume_after == "canonical":
            await browser.download_supplement_phase(work, "canonical")
        pipeline.close()
        db, pipeline, client = setup(tmp_path)
        client.download_supplement_phase = browser.download_supplement_phase
        await pipeline.start_background_packing()
        await pipeline.finish_background_packing()
        row = db.get_fight("Report", 1)
        assert row["status"] == "PACKED", row
        packed = tmp_path / row["file_rel_path"]
        assert PackedFightReader.read_full_events(packed) == [
            {"timestamp": 1, "type": "unknown", "unknown": [None, {"all": True}], "new": [None, True]}]
        assert len(paths) == len(set(paths)) == 3
        assert not work.root.exists()
        assert pipeline.staging_store.list_jobs() == []
    finally:
        await pipeline.finish_background_packing(cancel=True)
        pipeline.close()
        await browser.stop()
        if browser._bg_loop is not None:
            browser._bg_loop.call_soon_threadsafe(browser._bg_loop.stop)
            await asyncio.to_thread(browser._bg_thread.join, 2)

def test_indexed_dispatch_skips_large_cpu_prefix_and_bounds_each_pool(tmp_path):
    from wcl_archiver.db.schema import IndexDB
    db = IndexDB(tmp_path / "index.db")
    with db._connect() as conn:
        conn.execute("""WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<2000)
            INSERT INTO fights(fight_uid,report_code,fight_id,content_type,zone_id,partition,encounter_id,
                encounter_slug,status,browser_cpu)
            SELECT 'R_'||n,'R',n,'mplus',1,1,1,'boss','API_STAGED',1 FROM ids""")
    db.enqueue_fight("Late", 1, "raid", 9, 5, 42, "boss")
    db.mark_fight_api_staged("Late", 1, "staging/late.wcls")
    assert [row["fight_uid"] for row in db.get_browser_dispatch(1, 2)] == ["Late_1", "R_1", "R_2"]
    assert db.count_browser_cpu_jobs() == 2000
    with db._connect() as conn:
        plan = conn.execute("""EXPLAIN QUERY PLAN SELECT * FROM fights
            WHERE status='API_STAGED' AND browser_cpu=0 AND failure_count < 3
            ORDER BY CASE WHEN content_type='mplus' THEN 0 ELSE 1 END, rowid ASC LIMIT 32""").fetchall()
    assert any("idx_browser_phase_dispatch" in row["detail"] for row in plan)

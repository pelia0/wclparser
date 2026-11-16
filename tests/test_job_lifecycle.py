import asyncio
import httpx
import pytest

from wcl_archiver.api.app import create_app
from wcl_archiver.db.schema import IndexDB
from wcl_archiver.harvester.pipeline import HarvesterPipeline


def enqueue(db, fight_id):
    db.enqueue_fight("abcdefghij", fight_id, "raid", 10, 1, 1853, "nythendra", duration_ms=1000)


async def wait_until_idle(client):
    for _ in range(100):
        status = (await client.get("/api/harvester/status")).json()
        if not status["running"]:
            return status
        await asyncio.sleep(0.01)
    pytest.fail("job did not finish")


async def test_explorer_defaults_to_packed_and_exposes_second_page(tmp_path):
    db = IndexDB(tmp_path / "index.db")
    for fight_id in (1, 2, 3):
        enqueue(db, fight_id)
    for fight_id in (1, 2):
        db.mark_fight_packed("abcdefghij", fight_id, f"{fight_id}.wclz", 20, 1)
    app = create_app(tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        rows = (await client.get("/api/fights")).json()
        assert len(rows) == 2
        assert {row["status"] for row in rows} == {"PACKED"}
        page1 = (await client.get("/api/fights?limit=1")).json()
        page2 = (await client.get("/api/fights?limit=1&offset=1")).json()
        assert page1[0]["fight_uid"] != page2[0]["fight_uid"]
        assert len((await client.get("/api/fights?status=all")).json()) == 3
        assert len((await client.get("/api/fights?status=QUEUED")).json()) == 1


async def test_zero_pack_batch_keeps_pending_work_and_reports_terminal_failures(tmp_path, monkeypatch):
    calls = []
    counts = dict(pending_reports=1, queued_fights=1, retryable_reports=0,
                  retryable_fights=0, failed_reports=0, failed_fights=0)
    async def discover(self, **kwargs):
        return 0
    async def drain(self, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            counts.update(pending_reports=0, queued_fights=0, failed_fights=1)
        return 0
    monkeypatch.setattr(HarvesterPipeline, "discover_and_enqueue_raids", discover)
    monkeypatch.setattr(HarvesterPipeline, "drain_queue_batch", drain)
    monkeypatch.setattr(IndexDB, "get_queue_counts", lambda self: counts.copy(), raising=False)
    monkeypatch.setattr(IndexDB, "retry_failed_work", lambda self: None, raising=False)
    app = create_app(tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        await client.post("/api/harvester/start", json={"mode": "raids", "rankings_source": "protocol"})
        status = await wait_until_idle(client)
        assert len(calls) == 2
        assert status["failed_fights"] == 1
        assert status["last_error"]
        assert "incomplete" in status["current_task"].lower()
        assert status["progress_pct"] < 100


async def test_start_defaults_to_two_raid_pages_and_resets_retry_budget_once(tmp_path, monkeypatch):
    pages = []
    resets = []
    async def discover(self, **kwargs):
        pages.append(kwargs["pages_per_spec"])
        return 0
    async def drain(self, **kwargs):
        return 0
    monkeypatch.setattr(HarvesterPipeline, "discover_and_enqueue_raids", discover)
    monkeypatch.setattr(HarvesterPipeline, "drain_queue_batch", drain)
    monkeypatch.setattr(IndexDB, "retry_failed_work", lambda self: resets.append(1), raising=False)
    app = create_app(tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        await client.post("/api/harvester/start", json={"mode": "raids", "rankings_source": "protocol"})
        await wait_until_idle(client)
    assert pages == [2]
    assert resets == [1]


async def test_failed_discovery_page_cannot_report_complete(tmp_path,monkeypatch):
    async def discover(self,**kwargs):
        self.index_db.mark_page_failed(10,1853,1,'Monk','Windwalker',1,difficulty=5,reason='timeout')
        return 0
    async def drain(self,**kwargs): return 0
    monkeypatch.setattr(HarvesterPipeline,'discover_and_enqueue_raids',discover)
    monkeypatch.setattr(HarvesterPipeline,'drain_queue_batch',drain)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)),base_url='http://test') as client:
        await client.post('/api/harvester/start',json={'mode':'raids','rankings_source':'protocol'})
        status=await wait_until_idle(client)
        assert status['failed_scan_pages']==1
        assert 'incomplete' in status['current_task'].lower()
        assert status['progress_pct']<100


async def test_stop_waits_for_worker_cleanup_and_rejects_competing_jobs(tmp_path, monkeypatch):
    entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def discover(self, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()
    monkeypatch.setattr(HarvesterPipeline, "discover_and_enqueue_raids", discover)
    app = create_app(tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        payload = {"mode": "raids", "rankings_source": "protocol"}
        await client.post("/api/harvester/start", json=payload)
        await asyncio.wait_for(entered.wait(), 1)
        assert (await client.post("/api/harvester/start", json=payload)).status_code == 409
        assert (await client.post("/api/harvester/import-report", json={"report": "abcdefghij"})).status_code == 409
        stop = asyncio.create_task(client.post("/api/harvester/stop"))
        another_stop = None
        try:
            await asyncio.wait_for(cleaning.wait(), 1)
            assert not stop.done()
            assert (await client.post("/api/harvester/start", json=payload)).status_code == 409
            another_stop = asyncio.create_task(client.post("/api/harvester/stop"))
            await asyncio.sleep(0)
            assert not another_stop.done()
        finally:
            release.set()
            await stop
            if another_stop is not None:
                await another_stop
        status = (await client.get("/api/harvester/status")).json()
        assert status["running"] is False
        assert status["stopping"] is False


async def test_import_owns_job_and_shutdown_cleans_worker_and_pipeline(tmp_path, monkeypatch):
    entered, finished = asyncio.Event(), asyncio.Event()
    closed = []
    async def import_report(self, *args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            finished.set()
    monkeypatch.setattr(HarvesterPipeline, "import_report_by_code", import_report)
    monkeypatch.setattr(HarvesterPipeline, "close", lambda self: closed.append(True))
    app = create_app(tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        async with app.router.lifespan_context(app):
            request = asyncio.create_task(client.post("/api/harvester/import-report", json={
                "report": "abcdefghij", "rankings_source": "protocol"}))
            await asyncio.wait_for(entered.wait(), 1)
            response = await client.post("/api/harvester/start", json={"rankings_source": "protocol"})
            assert response.status_code == 409
        await asyncio.gather(request, return_exceptions=True)
    assert finished.is_set()
    assert closed == [True]


async def test_shutdown_closes_open_browser_even_when_no_job_was_started(tmp_path, monkeypatch):
    from wcl_archiver.wcl_client.browser_client import BrowserClient
    closed = []
    async def open_browser(self):
        self.state = "ready"
        self.status_message = "Ready"
    async def close_browser(self):
        closed.append(True)
    monkeypatch.setattr(BrowserClient, "start", open_browser)
    monkeypatch.setattr(BrowserClient, "stop", close_browser)
    app = create_app(tmp_path)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            response = await client.post("/api/browser/open")
            assert response.status_code == 200
    assert closed == [True]


@pytest.mark.parametrize('cancel', [False, True])
async def test_job_awaits_pack_quiescence_before_clearing_capture_buffers(tmp_path, monkeypatch, cancel):
    from wcl_archiver.codec.fight_archive import FightArchive
    working, work_release = asyncio.Event(), asyncio.Event()
    cleaning, cleanup_release = asyncio.Event(), asyncio.Event()
    finishes, cleared = [], []
    async def discover(self, **kwargs):
        working.set()
        await work_release.wait()
    async def drain(self, **kwargs):
        return 0
    async def start_packing(self, on_progress=None):
        pass
    async def finish_packing(self, cancel=False):
        finishes.append(cancel)
        cleaning.set()
        await cleanup_release.wait()
    monkeypatch.setattr(HarvesterPipeline, 'discover_and_enqueue_raids', discover)
    monkeypatch.setattr(HarvesterPipeline, 'drain_queue_batch', drain)
    monkeypatch.setattr(HarvesterPipeline, 'start_background_packing', start_packing)
    monkeypatch.setattr(HarvesterPipeline, 'finish_background_packing', finish_packing)
    monkeypatch.setattr(FightArchive, 'clear', lambda self: cleared.append(True))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://test') as client:
        payload = {'mode': 'raids', 'rankings_source': 'protocol'}
        await client.post('/api/harvester/start', json=payload)
        await working.wait()
        stop = None
        if cancel:
            stop = asyncio.create_task(client.post('/api/harvester/stop'))
        else:
            work_release.set()
        try:
            await asyncio.wait_for(cleaning.wait(), 1)
            assert finishes == [cancel]
            assert cleared == []
            assert (await client.post('/api/harvester/start', json=payload)).status_code == 409
            assert (await client.post('/api/harvester/import-report', json={'report': 'abcdefghij'})).status_code == 409
            if stop is not None:
                assert not stop.done()
        finally:
            cleanup_release.set()
            if stop is not None:
                await stop
            for _ in range(100):
                if cleared:
                    break
                await asyncio.sleep(0.01)
        assert cleared == [True]

import asyncio
import threading

import httpx
from fastapi.testclient import TestClient

from wcl_archiver.api.app import create_app
from wcl_archiver.db.schema import IndexDB
from wcl_archiver.harvester.pipeline import HarvesterPipeline


async def test_cached_http_status_keeps_runtime_and_control_fresh(tmp_path, monkeypatch):
    calls = []
    entered, release = threading.Event(), threading.Event()
    original = IndexDB.get_queue_counts
    def blocked(self):
        calls.append(1)
        if len(calls) > 1:
            entered.set()
            release.wait(3)
        return original(self)
    monkeypatch.setattr(IndexDB, 'get_queue_counts', blocked)
    app = create_app(tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
        first = (await client.get('/api/harvester/status')).json()
        assert first['processing_concurrency'] == 2
        app.state.status_cache.ttl = 0
        try:
            stale = await asyncio.wait_for(client.get('/api/harvester/status'), .3)
            assert stale.json()['status_cache_refreshing']
            await asyncio.to_thread(entered.wait, 1)
            monkeypatch.setattr(HarvesterPipeline, 'get_runtime_stats', lambda self: {'processing_active': 2, 'processing_waiting': 8})
            live = (await asyncio.wait_for(client.get('/api/harvester/status'), .3)).json()
            assert live['processing_active'] == 2 and live['processing_waiting'] == 8
            stop = await asyncio.wait_for(client.post('/api/harvester/stop?wait=false'), .3)
            assert stop.status_code == 202
            assert len(calls) == 2
        finally:
            release.set()
            await app.state.status_cache.close()


def test_status_lifespan_drains_executor(tmp_path):
    app = create_app(tmp_path)
    with TestClient(app) as client:
        assert client.get('/api/harvester/status').status_code == 200
    assert app.state.status_cache._closed
    assert app.state.status_cache._future is None

async def test_legacy_source_totals_are_cached_but_buffers_are_live(tmp_path, monkeypatch):
    from wcl_archiver.codec.raw_archive import RawArchive
    from wcl_archiver.codec.fight_archive import FightArchive
    root = tmp_path / 'raw'
    root.mkdir()
    (root / 'manifest.db').touch()
    calls = []
    def legacy(self):
        calls.append(1)
        return {'source_bytes': 12345, 'source_responses': 88}
    buffered = {'buffered_event_captures': 1}
    monkeypatch.setattr(RawArchive, 'stats', legacy)
    monkeypatch.setattr(FightArchive, 'stats', lambda self: {**buffered, 'source_bytes': 0, 'source_responses': 0})
    app = create_app(tmp_path)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
            first = (await client.get('/api/harvester/status')).json()
            buffered['buffered_event_captures'] = 5
            second = (await client.get('/api/harvester/status')).json()
            assert first['source_bytes'] == second['source_bytes'] == 12345
            assert second['source_responses'] == 88
            assert second['buffered_event_captures'] == 5
            assert len(calls) == 1
    finally:
        await app.state.status_cache.close()

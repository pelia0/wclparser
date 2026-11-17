import asyncio

import httpx
import pytest

from wcl_archiver.api.app import create_app
from wcl_archiver.db.schema import IndexDB
from wcl_archiver.harvester.pipeline import HarvesterPipeline


async def test_async_stop_acknowledges_without_waiting_for_cleanup_or_statistics(tmp_path, monkeypatch):
    entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    cancellations = []
    admission_fences = []

    async def discover(self, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancellations.append(True)
            admission_fences.append((self._packing_stop, self._browser_stop, self._download_stop))
            cleaning.set()
            await release.wait()

    monkeypatch.setattr(HarvesterPipeline, 'discover_and_enqueue_raids', discover)
    app = create_app(tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
        payload = {'mode': 'raids', 'rankings_source': 'protocol'}
        await client.post('/api/harvester/start', json=payload)
        await asyncio.wait_for(entered.wait(), 1)
        original_counts = IndexDB.get_queue_counts
        monkeypatch.setattr(IndexDB, 'get_queue_counts', lambda self: (_ for _ in ()).throw(
            AssertionError('Stop acknowledgement must not scan catalogue statistics')))
        request = asyncio.create_task(client.post('/api/harvester/stop?wait=false'))
        try:
            await asyncio.wait_for(cleaning.wait(), 1)
            response = await asyncio.wait_for(asyncio.shield(request), .5)
            assert response.status_code == 202
            state = response.json()
            assert state['stopping'] is True and state['running'] is False
            assert state['stop_request_accepted'] is True
            again = await asyncio.wait_for(client.post('/api/harvester/stop?wait=false'), .5)
            assert again.status_code == 202 and again.json()['stopping'] is True
            assert cancellations == [True]
            assert admission_fences == [(True, True, True)]
            assert (await client.post('/api/harvester/start', json=payload)).status_code == 409
            assert (await client.post('/api/harvester/import-report', json={'report': 'abcdefghij'})).status_code == 409
        finally:
            monkeypatch.setattr(IndexDB, 'get_queue_counts', original_counts)
            release.set()
            await request
        for _ in range(100):
            status = (await client.get('/api/harvester/status')).json()
            if not status['stopping']:
                break
            await asyncio.sleep(.01)
        assert status['running'] is False and status['stopping'] is False
        assert status['supports_async_stop'] is True
        assert status['current_task'] == 'Stopped by user'


async def test_async_stop_cleanup_error_is_visible_in_state(tmp_path, monkeypatch):
    entered = asyncio.Event()

    async def discover(self, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    async def start(self, **kwargs):
        pass

    async def finish(self, **kwargs):
        raise RuntimeError('cleanup failure test')

    monkeypatch.setattr(HarvesterPipeline, 'discover_and_enqueue_raids', discover)
    monkeypatch.setattr(HarvesterPipeline, 'start_background_packing', start)
    monkeypatch.setattr(HarvesterPipeline, 'finish_background_packing', finish)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://test') as client:
        await client.post('/api/harvester/start', json={'mode': 'raids', 'rankings_source': 'protocol'})
        await asyncio.wait_for(entered.wait(), 1)
        response = await asyncio.wait_for(client.post('/api/harvester/stop?wait=false'), .5)
        assert response.status_code == 202
        for _ in range(100):
            status = (await client.get('/api/harvester/status')).json()
            if not status['stopping']:
                break
            await asyncio.sleep(.01)
        assert 'cleanup failure test' in status['last_error']
        assert 'failed' in status['current_task'].lower()

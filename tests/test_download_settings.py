import asyncio
import json

import httpx
import pytest

from wcl_archiver.api.app import create_app
from wcl_archiver.harvester.pipeline import HarvesterPipeline
from wcl_archiver.wcl_client.browser_client import BrowserClient


async def test_settings_default_persist_and_reload(tmp_path):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url="http://test") as client:
        status = (await client.get('/api/harvester/status')).json()
        assert status['download_concurrency'] == 4
        assert status['browser_request_concurrency'] == 8
        assert status['pack_workers'] == 4
        response = await client.post('/api/harvester/settings', json={'download_concurrency': 128})
        assert response.status_code == 200
        status = response.json()
        assert status['download_concurrency'] == 128
        assert status['browser_request_concurrency'] == 256
        assert status['pack_workers'] == 4
        assert status['staging_bytes'] == status['staging_files'] == status['packing_fights'] == 0
    assert json.loads((tmp_path / 'harvester_settings.json').read_text()) == {'download_concurrency': 128}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url="http://test") as client:
        assert (await client.get('/api/harvester/status')).json()['download_concurrency'] == 128


@pytest.mark.parametrize('invalid', [0, -1, True, False, 1.5, '10', None])
async def test_invalid_settings_have_no_side_effects(tmp_path, invalid):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url="http://test") as client:
        response = await client.post('/api/harvester/settings', json={'download_concurrency': invalid})
        assert response.status_code == 422
        assert not (tmp_path / 'harvester_settings.json').exists()
        assert (await client.get('/api/harvester/status')).json()['download_concurrency'] == 4


@pytest.mark.parametrize('invalid', ['{', '{"download_concurrency": true}', '{"download_concurrency": 0}'])
async def test_malformed_saved_settings_fall_back_with_log(tmp_path, invalid):
    (tmp_path / 'harvester_settings.json').write_text(invalid)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url="http://test") as client:
        status = (await client.get('/api/harvester/status')).json()
        assert status['download_concurrency'] == 4
        assert any('settings' in log.lower() and '4' in log for log in status['recent_logs'])


async def test_live_apply_preserves_job_and_resizes_existing_browser(tmp_path, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    pipelines, browsers, limits = [], [], []
    async def discover(self, **kwargs):
        pipelines.append(self)
        entered.set()
        await release.wait()
    async def browser_start(self):
        browsers.append(self)
    async def resize(self, limit):
        limits.append(limit)
        self.max_concurrent_requests = limit
    monkeypatch.setattr(HarvesterPipeline, 'discover_and_enqueue_raids', discover)
    monkeypatch.setattr(BrowserClient, 'start', browser_start)
    monkeypatch.setattr(BrowserClient, 'set_request_concurrency', resize, raising=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url="http://test") as client:
        await client.post('/api/harvester/start', json={'mode': 'raids'})
        await entered.wait()
        try:
            for value in [32, 128, 2]:
                response = await client.post('/api/harvester/settings', json={'download_concurrency': value})
                assert response.status_code == 200
                status = response.json()
                assert status['running'] is True
                assert status['download_concurrency'] == pipelines[0].download_concurrency == value
                assert status['browser_request_concurrency'] == 2 * value
            assert len(pipelines) == len(browsers) == 1
            assert limits == [64, 256, 4]
            response = await client.post('/api/harvester/start', json={'mode': 'raids', 'download_concurrency': 16})
            assert response.status_code == 409
            assert pipelines[0].download_concurrency == 2
        finally:
            release.set()
            await client.post('/api/harvester/stop')


async def test_first_browser_uses_previously_selected_concurrency(tmp_path, monkeypatch):
    limits = []
    async def browser_start(self):
        limits.append(self.max_concurrent_requests)
    monkeypatch.setattr(BrowserClient, 'start', browser_start)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url="http://test") as client:
        await client.post('/api/harvester/settings', json={'download_concurrency': 10})
        await client.post('/api/browser/open')
    assert limits == [20]


@pytest.mark.parametrize('invalid', [0, -1, True, 1.5, '10'])
async def test_start_rejects_invalid_worker_setting_without_persisting(tmp_path, invalid):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url="http://test") as client:
        response = await client.post('/api/harvester/start', json={'download_concurrency': invalid})
        assert response.status_code == 422
        assert not (tmp_path / 'harvester_settings.json').exists()
        status = (await client.get('/api/harvester/status')).json()
        assert status['running'] is False
        assert status['download_concurrency'] == 4


async def test_start_optional_download_setting_applies_after_idle_guard(tmp_path, monkeypatch):
    entered = asyncio.Event()
    values = []
    async def discover(self, **kwargs):
        values.append(self.download_concurrency)
        entered.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(HarvesterPipeline, 'discover_and_enqueue_raids', discover)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url="http://test") as client:
        response = await client.post('/api/harvester/start', json={'mode': 'raids', 'rankings_source': 'protocol', 'download_concurrency': 128})
        assert response.status_code == 200
        assert response.json()['download_concurrency'] == 128
        await entered.wait()
        await client.post('/api/harvester/stop')
        assert values == [128]


@pytest.mark.parametrize('value', [17, 32, 128, 1000, 100000])
async def test_settings_accept_positive_counts_above_the_former_cap(tmp_path, value):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://test') as client:
        response = await client.post('/api/harvester/settings', json={'download_concurrency': value})
        assert response.status_code == 200
        status = response.json()
        assert status['download_concurrency'] == value
        assert status['browser_request_concurrency'] == 2 * value
        assert status['pack_workers'] == 4


async def test_start_setting_reserves_job_during_browser_resize(tmp_path, monkeypatch):
    resizing, release = asyncio.Event(), asyncio.Event()
    async def browser_start(self):
        pass
    async def resize(self, limit):
        resizing.set()
        await release.wait()
    async def discover(self, **kwargs):
        await asyncio.Event().wait()
    monkeypatch.setattr(BrowserClient, 'start', browser_start)
    monkeypatch.setattr(BrowserClient, 'set_request_concurrency', resize)
    monkeypatch.setattr(HarvesterPipeline, 'discover_and_enqueue_raids', discover)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url="http://test") as client:
        await client.post('/api/browser/open')
        starting = asyncio.create_task(client.post('/api/harvester/start', json={
            'mode': 'raids', 'download_concurrency': 10}))
        try:
            await resizing.wait()
            assert (await client.post('/api/harvester/start', json={'mode': 'raids'})).status_code == 409
            assert (await client.post('/api/harvester/import-report', json={'report': 'abcdefghij'})).status_code == 409
        finally:
            release.set()
            await starting
            await client.post('/api/harvester/stop')

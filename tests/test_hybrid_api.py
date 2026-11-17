import asyncio

import httpx
import pytest

import wcl_archiver.api.app as api_module
from wcl_archiver.api.app import create_app
from wcl_archiver.db.schema import IndexDB
from wcl_archiver.harvester.pipeline import HarvesterPipeline
from wcl_archiver.wcl_client.auth import WCLAuthManager
from wcl_archiver.wcl_client.browser_client import BrowserClient


@pytest.mark.parametrize('route,payload', [('/api/harvester/start',{'mode':'raids'}),
    ('/api/harvester/import-report',{'report':'Report123456'})])
async def test_hybrid_requires_user_oauth_before_touching_queue(tmp_path, route, payload):
    db=IndexDB(tmp_path/'index.db')
    db.enqueue_fight('Report123456',1,'raid',17,1,42,'boss')
    for _ in range(3):db.mark_fight_failed('Report123456',1,'old failure')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)),base_url='http://test') as client:
        response=await client.post(route,json={**payload,'rankings_source':'hybrid'})
        assert response.status_code==409
        assert 'WCL account' in response.json()['detail']
        assert db.get_fight('Report123456',1)['status']=='FAILED'


async def test_hybrid_client_selection_resize_and_shutdown(tmp_path, monkeypatch):
    entered=asyncio.Event();hybrids=[];starts=[];stops=[];resizes=[]
    class Hybrid:
        def __init__(self,api_client,browser_client,raw_archive,browser_archive):
            assert raw_archive is api_client.raw_archive
            assert browser_archive is browser_client.raw_archive
            assert raw_archive is not browser_archive
            self.browser_client = browser_client
            hybrids.append(self)
    async def discover(self,**kwargs):
        assert self.client is hybrids[0]
        entered.set()
        await asyncio.Event().wait()
    async def start(self):starts.append(self)
    async def stop(self):stops.append(self)
    async def resize(self,limit):resizes.append((self,limit))
    monkeypatch.setattr(api_module,'HybridClient',Hybrid,raising=False)
    monkeypatch.setattr(WCLAuthManager,'get_auth_status',lambda self:{'has_user_oauth_token':True,'oauth_mode':'user'})
    monkeypatch.setattr(HarvesterPipeline,'discover_and_enqueue_raids',discover)
    monkeypatch.setattr(BrowserClient,'start',start)
    monkeypatch.setattr(BrowserClient,'stop',stop)
    monkeypatch.setattr(BrowserClient,'set_request_concurrency',resize)
    app=create_app(tmp_path)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://test') as client:
            response=await client.post('/api/harvester/start',json={'mode':'raids','rankings_source':'hybrid'})
            assert response.status_code==200
            await asyncio.wait_for(entered.wait(),1)
            response=await client.post('/api/harvester/settings',json={'download_concurrency':32})
            assert response.json()['hybrid_strict'] is True
            assert response.json()['hybrid_strategy'] == 'selective'
            assert resizes==[(hybrids[0].browser_client,64)]
            assert starts==[], 'API scheduling must not wait for browser startup'
            await client.post('/api/harvester/stop')
    assert len(hybrids)==len(stops)==1

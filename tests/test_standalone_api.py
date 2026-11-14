import json
from decimal import Decimal
from fastapi.testclient import TestClient
import pytest
import httpx
from wcl_archiver.wcl_client.auth import WCLAuthManager
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient
from wcl_archiver.wcl_client.browser_client import BrowserClient
from wcl_archiver.harvester.pipeline import HarvesterPipeline
from wcl_archiver.db.schema import MasterDictDB

from wcl_archiver.api.app import create_app
from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader
from wcl_archiver.db.schema import IndexDB
from wcl_archiver.harvester.pipeline import _analyze_and_pack_cpu


def buffered_fight(root):
    archive=FightArchive(root/'never-create-this')
    archive.capture('GET','/reports/fights-and-participants/Abc1234567/0',b'{"fights":[{"id":1}],"unknownMaster":{"flag":null}}')
    body=b'{"events":[{"timestamp":1,"type":"damage","sourceID":1,"targetID":2,"abilityGameID":3,"amount":10,"unknown":9007199254740993,"precision":0.1234567890123456789012345,"nullable":null,"classResources":[{"type":1},{"type":2}]}]}'
    with archive.fight_capture('Abc1234567',1) as fetch_id:
        cid=archive.capture('GET','/reports/summary-events/Abc1234567/1/0/10',body)
        archive.complete_fight('Abc1234567',1,fetch_id,0,10,1)
    return archive,cid,body


def test_app_reads_standalone_file_without_creating_raw_storage(tmp_path):
    archive,cid,body=buffered_fight(tmp_path)
    path=tmp_path/'raids'/'fight.wclf'
    archive.write_fight(path,{'report_code':'Abc1234567','fight_id':1,'duration_ms':10,'actors':[]},'Abc1234567',1)
    del archive
    db=IndexDB(tmp_path/'index.db');db.enqueue_fight('Abc1234567',1,'raid',17,1,2092,'argus')
    db.mark_fight_packed('Abc1234567',1,'raids/fight.wclf',path.stat().st_size,1,duration_ms=10)
    with TestClient(create_app(tmp_path)) as client:
        assert not (tmp_path/'raw').exists()
        assert not (tmp_path/'staging').exists()
        assert not (tmp_path/'never-create-this').exists()
        assert client.get('/api/fights/Abc1234567_1/summary').json()['duration_ms']==10
        manifest=client.get('/api/fights/Abc1234567_1/sources').json()
        assert cid in [capture['id'] for capture in manifest['captures']]
        exported=client.get(f'/api/fights/Abc1234567_1/sources/{cid}')
        assert exported.status_code==200
        assert json.loads(exported.content)==json.loads(body)
        assert client.get('/api/fights/Abc1234567_1/full-events').json()==json.loads(body)['events']
        assert b'0.1234567890123456789012345' in client.get('/api/fights/Abc1234567_1/full-events').content
        assert client.get('/api/fights/Abc1234567_1/download').content==path.read_bytes()
        status=client.get('/api/harvester/status').json()
        assert status['archive_mode']=='standalone'
        assert status['source_bytes']==0
        assert status['archive_bytes']==path.stat().st_size


def test_analysis_failure_does_not_discard_full_source_records(tmp_path,monkeypatch):
    archive,cid,body=buffered_fight(tmp_path)
    def broken(*args,**kwargs): raise ValueError('new event schema')
    monkeypatch.setattr('wcl_archiver.harvester.pipeline.CombatStateMachine.analyze_fight',broken)
    path=tmp_path/'fight.wclf'
    _analyze_and_pack_cpu(path,{'report_code':'Abc1234567','fight_id':1,'start_time':0,'end_time':10},[],[],json.loads(body)['events'],9,archive)
    assert PackedFightReader.read_raw_events(path)==json.loads(body,parse_float=Decimal)['events']
    assert PackedFightReader.read_summary_only(path)['analysis_error']=='new event schema'


@pytest.mark.asyncio
async def test_pipeline_metadata_cache_refetches_after_source_buffer_eviction(tmp_path):
    archive=FightArchive(tmp_path)
    client=WCLProtocolClient(WCLAuthManager(tmp_path/'auth.json'),httpx.MockTransport(lambda req:httpx.Response(200,json={'fights':[{'id':1}]})),raw_archive=archive)
    pipeline=HarvesterPipeline(tmp_path,IndexDB(tmp_path/'index.db'),MasterDictDB(tmp_path/'master.db'),client,raw_archive=archive)
    try:
        await pipeline._get_report_master('Abc1234567')
        archive.clear()
        await pipeline._get_report_master('Abc1234567')
        assert archive.stats()['buffered_master_captures']==1
    finally: pipeline.close()


@pytest.mark.asyncio
async def test_browser_metadata_cache_refetches_after_source_buffer_eviction(tmp_path):
    archive=FightArchive(tmp_path)
    class Page:
        url='https://www.warcraftlogs.com/'
        def is_closed(self): return False
        async def title(self): return 'Warcraft Logs'
        async def content(self): return '<html>ok</html>'
        async def evaluate(self,script,arg):
            return {'status':200,'text':'{"fights":[{"id":1}]}','retryAfter':''}
    client=BrowserClient(tmp_path/'profile',min_delay=0,max_delay=0,raw_archive=archive)
    client._context=object();client._page=Page()
    await client.fetch_report_master_and_fights('Abc1234567')
    archive.clear()
    await client.fetch_report_master_and_fights('Abc1234567')
    assert archive.stats()['buffered_master_captures']==1


def test_pipeline_close_releases_unpacked_source_buffers(tmp_path):
    archive,_,_=buffered_fight(tmp_path)
    pipeline=HarvesterPipeline(tmp_path,IndexDB(tmp_path/'index.db'),MasterDictDB(tmp_path/'master.db'),None,raw_archive=archive)
    assert archive.stats()['buffered_complete_fights']==1
    pipeline.close()
    assert archive.stats()['buffered_complete_fights']==0
    assert archive.stats()['buffered_event_captures']==0


@pytest.mark.asyncio
async def test_partial_replay_segment_cannot_be_marked_complete(tmp_path,monkeypatch):
    archive=FightArchive(tmp_path)
    archive.capture('GET','/reports/fights-and-participants/Abc1234567/0',b'{"fights":[{"id":1}]}')
    client=BrowserClient(tmp_path/'profile',min_delay=0,max_delay=0,raw_archive=archive)
    client._fight_boss_cache[('Abc1234567',1)]=99
    async def request(method,path):
        if '/240000/' in path: return 503,None
        payload={'events':[{'timestamp':1,'type':'cast'}]}
        archive.capture(method,path,json.dumps(payload).encode())
        return 200,payload
    monkeypatch.setattr(client,'_request_json_impl',request)
    with pytest.raises(RuntimeError,match='replay'):
        await client.fetch_all_fight_events('Abc1234567',1,0,300000)
    assert archive.stats()['buffered_complete_fights']==0

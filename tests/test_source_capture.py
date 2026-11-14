import json
import httpx
import pytest

from wcl_archiver.codec.raw_archive import RawArchive
from wcl_archiver.wcl_client.auth import WCLAuthManager
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient
from wcl_archiver.wcl_client.browser_client import BrowserClient
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.harvester.pipeline import HarvesterPipeline
from wcl_archiver.codec.raw_archive import SourceFightReader


@pytest.mark.asyncio
async def test_protocol_archives_original_pages_and_rejects_nonadvancing_cursor(tmp_path):
    archive = RawArchive(tmp_path/'raw')
    body = b'{ "events":[{"timestamp":5,"type":"cast","unknown":9007199254740993,"null":null}], "nextPageTimestamp":null }'
    def handler(req):
        if 'fights-and-participants' in req.url.path:
            return httpx.Response(200,content=b'{"fights":[{"id":1,"start_time":0,"end_time":10}]}')
        return httpx.Response(200,content=body)
    client = WCLProtocolClient(WCLAuthManager(tmp_path/'auth.json'),httpx.MockTransport(handler),raw_archive=archive)
    await client.fetch_report_master_and_fights('Abc1234567')
    result = await client.fetch_all_fight_events('Abc1234567',1,0,10)
    assert result[0]['unknown'] == 9007199254740993
    manifest = archive.fight_manifest('Abc1234567',1)
    assert archive.read_body(manifest['event_captures'][0]) == body
    def stuck(req):
        return httpx.Response(200,json={'events':[], 'nextPageTimestamp':0})
    client._transport = httpx.MockTransport(stuck)
    with pytest.raises(RuntimeError,match='cursor'):
        await client.fetch_all_fight_events('Abc1234567',1,0,10)


@pytest.mark.asyncio
async def test_browser_source_uses_canonical_events_even_when_replay_available(tmp_path):
    archive = RawArchive(tmp_path/'raw')
    calls=[]
    class Page:
        url='https://www.warcraftlogs.com/'
        def is_closed(self): return False
        async def title(self): return 'Warcraft Logs'
        async def content(self): return '<html>ok</html>'
        async def evaluate(self,script,arg):
            url=arg['url'];calls.append(url)
            if 'fights-and-participants' in url:
                payload={'fights':[{'id':1,'boss':99,'start_time':0,'end_time':10}]}
            elif 'replaysegment' in url:
                payload={'events':[{'timestamp':2,'type':'damage'}]}
            else:
                payload={'events':[{'timestamp':1,'type':'combatantinfo','custom':{'null':None}}]}
            return {'status':200,'text':json.dumps(payload),'retryAfter':''}
    client=BrowserClient(tmp_path/'profile',min_delay=0,max_delay=0,raw_archive=archive)
    client._context=object();client._page=Page()
    await client.fetch_report_master_and_fights('Abc1234567')
    events=await client.fetch_all_fight_events('Abc1234567',1,0,10)
    assert events == [{'timestamp':1,'type':'combatantinfo','custom':{'null':None}}]
    assert any('summary-events' in url for url in calls)
    assert archive.fight_manifest('Abc1234567',1)['complete'] is True


@pytest.mark.asyncio
async def test_source_write_error_prevents_network_success(tmp_path,monkeypatch):
    archive=RawArchive(tmp_path/'raw')
    def broken(*args,**kwargs): raise OSError('disk full')
    monkeypatch.setattr(archive,'capture',broken)
    client=WCLProtocolClient(WCLAuthManager(tmp_path/'auth.json'),httpx.MockTransport(lambda req:httpx.Response(200,json={'fights':[]})),raw_archive=archive)
    with pytest.raises(OSError,match='disk full'):
        await client.fetch_report_master_and_fights('Abc1234567')


@pytest.mark.asyncio
async def test_pipeline_writes_reference_fight_and_roundtrips_original_fields(tmp_path):
    archive=RawArchive(tmp_path/'raw')
    event={'timestamp':100,'type':'heal','sourceID':1,'targetID':2,'abilityGameID':123,
           'amount':10,'overheal':17,'unknown':{'x':None},'classResources':[{'type':1},{'type':2}]}
    def handler(req):
        if 'fights-and-participants' in req.url.path:
            return httpx.Response(200,json={'fights':[{'id':1,'start_time':0,'end_time':12345}],
                'friendlies':[{'id':1,'type':'Priest'}],'abilities':[]})
        return httpx.Response(200,json={'events':[event]})
    client=WCLProtocolClient(WCLAuthManager(tmp_path/'auth.json'),httpx.MockTransport(handler),raw_archive=archive)
    db=IndexDB(tmp_path/'index.db')
    db.enqueue_fight('Abc1234567',1,'raid',17,1,2092,'argus',duration_ms=1)
    pipeline=HarvesterPipeline(tmp_path,db,MasterDictDB(tmp_path/'master.db'),client,raw_archive=archive)
    try:
        path=await pipeline.process_single_fight(db.search_fights()[0])
        assert path.suffix == '.wclf'
        assert SourceFightReader.read_raw_events(path) == [event]
        assert db.search_fights()[0]['duration_ms'] == 12345
        assert SourceFightReader.read_summary_only(path)['healing_done'][0]['total'] == 10
    finally:
        pipeline.close()


@pytest.mark.asyncio
async def test_partial_graphql_master_is_rejected_and_saved(tmp_path,monkeypatch):
    archive=RawArchive(tmp_path/'raw')
    auth=WCLAuthManager(tmp_path/'auth.json')
    monkeypatch.setattr(auth,'get_auth_status',lambda:{'has_oauth_token':True})
    def handler(req):
        if req.url.path=='/api/v2/client':
            return httpx.Response(200,json={'data':{'reportData':{'report':{'fights':[{'id':1}],'masterData':{'actors':[],'abilities':[]}}}},'errors':[{'message':'actors unavailable'}]})
        return httpx.Response(503)
    client=WCLProtocolClient(auth,httpx.MockTransport(handler),raw_archive=archive)
    with pytest.raises(RuntimeError,match='metadata'):
        await client.fetch_report_master_and_fights('Abc1234567')


@pytest.mark.asyncio
async def test_graphql_fallback_after_first_page_keeps_exactly_one_complete_stream(tmp_path,monkeypatch):
    archive=RawArchive(tmp_path/'raw');auth=WCLAuthManager(tmp_path/'auth.json')
    monkeypatch.setattr(auth,'get_auth_status',lambda:{'has_oauth_token':True})
    archive.capture('GET','/reports/fights-and-participants/Abc1234567/0',b'{"fights":[{"id":1}]}')
    def handler(req):
        if req.method=='POST':
            start=json.loads(req.content)['variables']['startTime']
            events=[{'timestamp':1,'type':'cast'},{'timestamp':6,'type':'cast'}]
            return httpx.Response(200,json={'data':{'reportData':{'report':{'events':{'data':[e for e in events if e['timestamp']>=start],'nextPageTimestamp':None}}}}})
        if '/1/0/10/' in req.url.path:
            return httpx.Response(200,json={'events':[{'timestamp':1,'type':'cast'}],'nextPageTimestamp':5})
        return httpx.Response(403)
    client=WCLProtocolClient(auth,httpx.MockTransport(handler),raw_archive=archive)
    assert await client.fetch_all_fight_events('Abc1234567',1,0,10)==[{'timestamp':1,'type':'cast'},{'timestamp':6,'type':'cast'}]
    assert archive.fight_manifest('Abc1234567',1)['event_count']==2


@pytest.mark.asyncio
async def test_pagination_fetches_page_at_exact_fight_end(tmp_path):
    archive=RawArchive(tmp_path/'raw')
    archive.capture('GET','/reports/fights-and-participants/Abc1234567/0',b'{"fights":[{"id":1}]}')
    def handler(req):
        if '/1/0/10/' in req.url.path:
            return httpx.Response(200,json={'events':[{'timestamp':1,'type':'cast'}],'nextPageTimestamp':10})
        return httpx.Response(200,json={'events':[{'timestamp':10,'type':'damage'}],'nextPageTimestamp':None})
    client=WCLProtocolClient(WCLAuthManager(tmp_path/'auth.json'),httpx.MockTransport(handler),raw_archive=archive)
    events=await client.fetch_all_fight_events('Abc1234567',1,0,10)
    assert [e['timestamp'] for e in events]==[1,10]
    assert len(archive.fight_manifest('Abc1234567',1)['event_captures'])==2

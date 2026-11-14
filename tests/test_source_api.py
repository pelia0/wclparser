import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import threading

from fastapi.testclient import TestClient
import pytest

from wcl_archiver.api.app import create_app
from wcl_archiver.codec.raw_archive import RawArchive
from wcl_archiver.db.schema import IndexDB
from wcl_archiver.harvester.pipeline import _await_pack


def test_source_fight_viewer_and_original_export(tmp_path):
    archive=RawArchive(tmp_path/'raw')
    archive.capture('GET','/reports/fights-and-participants/Abc1234567/0',b'{"fights":[{"id":1}]}')
    body=b'{ "events":[{"timestamp":1,"type":"damage","sourceID":1,"targetID":2,"abilityGameID":3,"amount":10,"unknown":9007199254740993}] }'
    with archive.fight_capture('Abc1234567',1) as fetch_id:
        capture_id=archive.capture('GET','/reports/summary-events/Abc1234567/1/0/10',body)
        archive.complete_fight('Abc1234567',1,fetch_id,0,10,1)
    path=tmp_path/'raid'/'fight.wclf'
    archive.write_fight(path,{'report_code':'Abc1234567','fight_id':1,'duration_ms':10,'actors':[]},'Abc1234567',1)
    db=IndexDB(tmp_path/'index.db');db.enqueue_fight('Abc1234567',1,'raid',17,1,2092,'argus')
    db.mark_fight_packed('Abc1234567',1,'raid/fight.wclf',path.stat().st_size,1,duration_ms=10)
    with TestClient(create_app(tmp_path)) as client:
        assert client.get('/api/fights/Abc1234567_1/summary').json()['duration_ms']==10
        response=client.get('/api/fights/Abc1234567_1/sources')
        assert response.status_code==200
        assert response.json()['event_captures']==[capture_id]
        assert client.get('/api/archive/responses/'+capture_id).content==body
        assert client.get('/api/fights/Abc1234567_1/raw-events').json()[0]['unknown']==9007199254740993
        assert client.get('/api/fights/Abc1234567_1/events').json()['events'][0]['amount']==10
        status=client.get('/api/harvester/status').json()
        assert status['source_responses']==2
        assert status['compressed_mb'] >= status['source_mb']


@pytest.mark.asyncio
async def test_cancelled_pack_waits_for_real_writer_to_finish():
    started=threading.Event();release=threading.Event();finished=threading.Event()
    def write():
        started.set();release.wait(3);finished.set()
    with ThreadPoolExecutor(1) as pool:
        future=asyncio.get_running_loop().run_in_executor(pool,write)
        task=asyncio.create_task(_await_pack(future))
        await asyncio.to_thread(started.wait,1)
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()

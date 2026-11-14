import json
from pathlib import Path

import pytest

from wcl_archiver.codec.raw_archive import RawArchive, SourceFightReader


def test_sources_roundtrip_exact_bytes_deduplicate_and_detect_corruption(tmp_path):
    archive = RawArchive(tmp_path / 'raw')
    body = b'{ "number":9007199254740993, "fraction":1.2300, "null":null, "unknown":[{},false] }'
    one = archive.capture('GET', '/reports/fights-and-participants/Abc123/0', body)
    two = archive.capture('GET', '/reports/fights-and-participants/Abc123/0', body)
    assert archive.read_body(one) == body
    assert archive.read_body(two) == body
    assert len(list((tmp_path / 'raw' / 'objects').rglob('*.zst'))) == 1
    blob = next((tmp_path / 'raw' / 'objects').rglob('*.zst'))
    blob.write_bytes(b'broken')
    with pytest.raises(ValueError, match='Corrupt'):
        archive.read_body(one)


def test_fight_source_keeps_all_fields_and_uses_references_not_duplicate_events(tmp_path):
    archive = RawArchive(tmp_path / 'raw')
    archive.capture('GET', '/reports/fights-and-participants/Abc123/0', b'{"fights":[{"id":1}]}')
    events = [{"timestamp":1000,"type":"heal","sourceID":1,"targetID":2,"abilityGameID":123,
               "amount":10,"overheal":99,"newField":{"nullable":None},
               "classResources":[{"type":1,"amount":3},{"type":2,"amount":4}]}]
    with archive.fight_capture('Abc123', 1) as fetch_id:
        capture = archive.capture('GET', '/reports/summary-events/Abc123/1/0/2000', json.dumps({'events':events}).encode())
        archive.complete_fight('Abc123', 1, fetch_id, 0, 2000, 1)
    path = tmp_path / 'fight.wclf'
    archive.write_fight(path, {'report_code':'Abc123','fight_id':1,'duration_ms':2000,'actors':[]}, 'Abc123', 1)
    assert SourceFightReader.read_raw_events(path) == events
    assert SourceFightReader.read_summary_only(path)['duration_ms'] == 2000
    assert SourceFightReader.source_manifest(path)['event_captures'] == [capture]
    assert not list(tmp_path.glob('*.wclz'))


def test_incomplete_source_cannot_be_certified_or_overwrite_last_complete(tmp_path):
    archive = RawArchive(tmp_path / 'raw')
    with archive.fight_capture('Abc123', 1) as fetch_id:
        archive.capture('GET', '/reports/summary-events/Abc123/1/0/2000', b'{"events":[]}')
        with pytest.raises(ValueError, match='metadata'):
            archive.complete_fight('Abc123', 1, fetch_id, 0, 2000, 0)
    with pytest.raises(ValueError, match='complete'):
        archive.write_fight(tmp_path/'fight.wclf', {}, 'Abc123', 1)


def test_graphql_provenance_has_variables_and_original_response(tmp_path):
    archive = RawArchive(tmp_path/'raw')
    body = b'{"data":{"reportData":{"report":{"events":{"data":[],"nextPageTimestamp":null}}}}}'
    request = {'query':'query($code:String!){reportData{report(code:$code){title}}}', 'variables':{'code':'Abc123','fightId':1}}
    capture = archive.capture('POST','/api/v2/client',body,request_json=request)
    assert archive.read_body(capture) == body
    assert archive.capture_info(capture)['request_json'] == request

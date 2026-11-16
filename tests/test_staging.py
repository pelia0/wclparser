from copy import deepcopy
from decimal import Decimal
import hashlib
import json
import os
import struct

import pytest

from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader, _pack, _unpack
from wcl_archiver.codec.staging import StagingStore


def captured(tmp_path, timestamp=1):
    archive = FightArchive(tmp_path / 'unused')
    master_body = b'{"fights":[{"id":1,"start_time":0,"end_time":100}],"extra":{"null":null}}'
    archive.capture('GET', '/reports/fights-and-participants/Report1/0', master_body)
    body = ('{"events":[{"timestamp":%d,"n":0.12345678901234567890123456789,"large":184467440737095516160,"unknown":[null,false,{"x":1.00}]}],"nextPageTimestamp":null,"extra":true}' % timestamp).encode()
    replay = b'{"events":[{"timestamp":2,"x":123,"unknownReplay":{"v":null}}],"other":1.20}'
    with archive.fight_capture('Report1', 1) as fetch:
        archive.capture('GET', '/reports/summary-events/Report1/1/0/100', body)
        archive.capture('GET', '/reports/replaysegment/Report1/99/0/100', replay)
        archive.complete_fight('Report1', 1, fetch, 0, 100, 1)
    row = {'fight_uid': 'Report1_1', 'report_code': 'Report1', 'fight_id': 1, 'unknownRow': {'v': None}}
    meta = {'report_code': 'Report1', 'fight_id': 1, 'start_time': 0, 'end_time': 100, 'unknownMeta': Decimal('1.2300')}
    return archive, row, meta, [{'id': 1, 'unknown': None}], [{'id': 5, 'unknown': True}]


def stage(store, args, tmp_path):
    return store.stage_fight(*args, tmp_path / 'archive' / 'fight.wclf')


def test_disk_stage_fresh_archive_preserves_all_source_responses_and_metadata(tmp_path):
    store = StagingStore(tmp_path)
    args = captured(tmp_path)
    archive, row, meta, actors, abilities = args
    completed = deepcopy(archive._completed[('Report1', 1)])
    path = stage(store, args, tmp_path)
    assert archive.stats()['buffered_complete_fights'] == 0
    assert archive.stats()['buffered_event_captures'] == 0
    assert archive._pinned_masters == {}
    archive.clear()
    job, restored = StagingStore(tmp_path).load(path, compression='fast')
    assert restored is not archive and restored.compression == 'fast'
    assert job['fight_row'] == row and job['fight_meta'] == meta
    assert job['actors'] == actors and job['abilities'] == abilities
    assert job['out_rel_path'] == 'archive/fight.wclf'
    assert job['fetch_id'] == completed['manifest']['fetch_id']
    assert restored._completed[('Report1', 1)] == completed
    expected = completed['pages'][0]['body']['events']
    assert job['raw_events'] == expected
    # The raw event payload occurs once on disk, in its authoritative response.
    assert path.read_bytes().count(b'unknownReplay') == 1
    assert path.read_bytes().count(b'12345678901234567890123456789') == 1
    out = tmp_path / job['out_rel_path']
    restored.write_fight(out, meta, 'Report1', 1)
    assert PackedFightReader.read_raw_events(out) == expected
    for cap in [completed['master'], *completed['pages'], *completed['replay']]:
        assert json.loads(PackedFightReader.read_source_response(out, cap['info']['id']), parse_float=Decimal) == cap['body']
    assert store.list_jobs() == [path]
    assert store.stats() == {'staging_files': 1, 'staging_bytes': path.stat().st_size}
    store.delete(path)
    assert store.stats() == {'staging_files': 0, 'staging_bytes': 0}


@pytest.mark.parametrize('operation', ['fsync', 'replace'])
def test_failed_durable_write_preserves_capture_and_previous_snapshot(tmp_path, monkeypatch, operation):
    store = StagingStore(tmp_path)
    old = stage(store, captured(tmp_path), tmp_path)
    old_bytes = old.read_bytes()
    args = captured(tmp_path, timestamp=3)
    completed = deepcopy(args[0]._completed)
    def fail(*args):
        raise OSError('disk write failure')
    monkeypatch.setattr('wcl_archiver.codec.staging.os.' + operation, fail)
    with pytest.raises(OSError, match='disk write failure'):
        stage(store, args, tmp_path)
    assert args[0]._completed == completed
    assert args[0].stats()['buffered_event_captures'] == 2
    assert old.read_bytes() == old_bytes
    assert store.list_jobs() == [old]
    assert not list((tmp_path / 'staging').glob('*.tmp'))


def test_distinct_fetches_never_overwrite_previous_raw(tmp_path):
    store = StagingStore(tmp_path)
    old = stage(store, captured(tmp_path), tmp_path)
    old_bytes = old.read_bytes()
    new = stage(store, captured(tmp_path, timestamp=8), tmp_path)
    assert new != old and old.read_bytes() == old_bytes
    assert len(store.list_jobs()) == 2


def test_stats_tolerates_a_packer_deleting_a_listed_snapshot(tmp_path, monkeypatch):
    from pathlib import Path
    store = StagingStore(tmp_path)
    path = stage(store, captured(tmp_path), tmp_path)
    original_stat = Path.stat
    deleted = []
    def disappearing(self, *args, **kwargs):
        if self == path and kwargs.get('follow_symlinks', True) and not deleted:
            path.unlink()
            deleted.append(True)
            raise FileNotFoundError(path)
        return original_stat(self, *args, **kwargs)
    monkeypatch.setattr(Path, 'stat', disappearing)
    assert store.stats() == {'staging_files': 0, 'staging_bytes': 0}


def test_corruption_is_retained_including_when_delete_requested(tmp_path):
    store = StagingStore(tmp_path)
    path = stage(store, captured(tmp_path), tmp_path)
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)
    with pytest.raises(ValueError):
        store.load(path)
    with pytest.raises(ValueError):
        store.delete(path)
    assert path.exists() and path in store.list_jobs()


def test_output_and_staging_path_escape_are_rejected(tmp_path):
    store = StagingStore(tmp_path)
    args = captured(tmp_path)
    with pytest.raises(ValueError):
        store.stage_fight(*args, tmp_path.parent / 'outside.wclf')
    assert args[0].stats()['buffered_complete_fights'] == 1
    with pytest.raises(ValueError):
        store.load('../outside.wcls')
    with pytest.raises(ValueError):
        store.delete(tmp_path.parent / 'outside.wcls')


def test_symlink_or_reparse_directory_is_rejected(tmp_path):
    outside = tmp_path / 'outside'
    outside.mkdir()
    link = tmp_path / 'staging'
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError:
        if os.name != 'nt':
            raise
        import subprocess
        subprocess.run(['cmd', '/c', 'mklink', '/J', str(link), str(outside)], check=True, capture_output=True)
    with pytest.raises(ValueError):
        stage(StagingStore(tmp_path), captured(tmp_path), tmp_path)
    assert not list(outside.iterdir())


@pytest.mark.parametrize('field,value', [('fight_uid', 'Other_1'), ('report_code', 'Other'), ('fight_id', 2)])
def test_wrong_row_identity_never_certifies_capture(tmp_path, field, value):
    args = captured(tmp_path)
    args[1][field] = value
    with pytest.raises(ValueError):
        stage(StagingStore(tmp_path), args, tmp_path)
    assert args[0].stats()['buffered_complete_fights'] == 1


@pytest.mark.parametrize('mutation', ['fetch', 'count', 'bounds', 'page_report', 'capture_ids', 'output'])
def test_digest_valid_semantic_mismatch_is_retained(tmp_path, mutation):
    store = StagingStore(tmp_path)
    path = stage(store, captured(tmp_path), tmp_path)
    data = path.read_bytes()
    header_size = 5 + 8 + 32
    payload = _unpack(data[header_size:])
    if mutation == 'fetch':
        payload['capture']['pages'][0]['info']['fetch_id'] = 'different'
    elif mutation == 'count':
        payload['capture']['manifest']['event_count'] += 1
    elif mutation == 'bounds':
        payload['job']['fight_meta']['end_time'] += 1
    elif mutation == 'page_report':
        payload['capture']['pages'][0]['info']['report_code'] = 'Other'
    elif mutation == 'capture_ids':
        payload['capture']['manifest']['event_captures'] = ['wrong']
    else:
        payload['job']['out_rel_path'] = '../escape.wclf'
    raw = _pack(payload)
    path.write_bytes(data[:5] + struct.pack('<Q', len(raw)) + hashlib.sha256(raw).digest() + raw)
    with pytest.raises(ValueError):
        store.load(path)
    assert path.exists()


def test_graphql_metadata_and_nested_page_survive_master_cache_destruction(tmp_path):
    archive = FightArchive(tmp_path / 'unused')
    archive.capture('POST', '/api/v2/client', b'{"data":{"reportData":{"report":{"fights":[{"id":1}],"newMetadata":null}}},"extensions":{"cost":1.01}}', request_json={'variables': {'code': 'Report1'}})
    with archive.fight_capture('Report1', 1) as fetch:
        archive.capture('POST', '/api/v2/client', b'{"data":{"reportData":{"report":{"events":{"data":[{"timestamp":10,"nested":{"n":1.0000000000000000001}}],"nextPageTimestamp":null}}}},"unknown":true}', request_json={'variables': {'code': 'Report1', 'fightId': 1, 'startTime': 0, 'endTime': 100}})
        archive.complete_fight('Report1', 1, fetch, 0, 100, 1)
    _, row, meta, actors, abilities = captured(tmp_path)
    expected = deepcopy(archive._completed[('Report1', 1)])
    store = StagingStore(tmp_path)
    path = store.stage_fight(archive, row, meta, actors, abilities, tmp_path / 'graph.wclf')
    archive.clear()
    job, restored = store.load(path)
    assert restored._completed[('Report1', 1)] == expected
    assert job['raw_events'][0]['nested']['n'] == Decimal('1.0000000000000000001')


def test_verification_failure_retains_all_capture_buffers(tmp_path, monkeypatch):
    store = StagingStore(tmp_path)
    args = captured(tmp_path)
    original = deepcopy(args[0]._completed)
    def fail(*args, **kwargs):
        raise ValueError('verification failed')
    monkeypatch.setattr(store, '_read', fail)
    with pytest.raises(ValueError, match='verification failed'):
        stage(store, args, tmp_path)
    assert args[0]._completed == original
    assert args[0].stats()['buffered_event_captures'] == 2
    assert not store.list_jobs()


@pytest.mark.parametrize('kind', ['master', 'replay'])
def test_source_url_report_mismatch_is_rejected(tmp_path, kind):
    args = captured(tmp_path)
    capture = args[0]._completed[('Report1', 1)]
    cap = capture['master'] if kind == 'master' else capture['replay'][0]
    cap['info']['url'] = cap['info']['url'].replace('/Report1/', '/Other/')
    with pytest.raises(ValueError):
        stage(StagingStore(tmp_path), args, tmp_path)
    assert args[0].stats()['buffered_complete_fights'] == 1

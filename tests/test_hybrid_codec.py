"""Strict hybrid keeps every source while exposing an API-first full view."""
from copy import deepcopy
from decimal import Decimal
import hashlib
import json

import pytest

from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader, json_bytes
from wcl_archiver.codec.staging import StagingStore


def captured(tmp_path, provider, events, *, code='Hybrid1', start=0, end=100, replay=None):
    archive = FightArchive(tmp_path / provider)
    master_body = {'fights': [{'id': 1, 'start_time': start, 'end_time': end}],
                   'providerUnknown': {provider: Decimal('1.0000000000000000000001')}}
    if provider == 'api':
        master_body = {'data': {'reportData': {'report': master_body}}, 'extensions': {'extra': True}}
        mid = archive.capture('POST', '/api/v2/client', json_bytes(master_body),
                              request_json={'variables': {'code': code}})
    else:
        mid = archive.capture('GET', f'/reports/fights-and-participants/{code}/0', json_bytes(master_body))
    with archive.fight_capture(code, 1) as fetch:
        body = {'events': deepcopy(events), 'nextPageTimestamp': None, 'pageUnknown': [False, None, Decimal('2.300')]}
        if provider == 'api':
            body = {'data': {'reportData': {'report': {'events': {'data': deepcopy(events), 'nextPageTimestamp': None}}}},
                    'extensions': {'extra': Decimal('2.300')}}
            eid = archive.capture('POST', '/api/v2/client', json_bytes(body), request_json={
                'variables': {'code': code, 'fightId': 1, 'startTime': start, 'endTime': end}})
        else:
            eid = archive.capture('GET', f'/reports/summary-events/{code}/1/{start}/{end}', json_bytes(body))
        rid = None
        if replay is not None:
            rid = archive.capture('GET', f'/reports/replaysegment/{code}/99/{start}/{end}', json_bytes({'events': replay, 'unknownReplay': None}))
        archive.complete_fight(code, 1, fetch, start, end, len(events))
    return archive, [cid for cid in (mid, eid, rid) if cid]


def event(**extra):
    return {'timestamp': 10, 'type': 'damage', 'sourceID': 1, 'targetID': 2, 'abilityGameID': 3, **extra}


def test_hybrid_full_view_and_every_original_response_survive_fresh_disk_stage(tmp_path):
    api, aids = captured(tmp_path, 'api', [event(amount=7, nested={'api': True}, null=None), event(amount=8)])
    web, bids = captured(tmp_path, 'web', [event(amount=9, nested={'browser': Decimal('0.12345678901234567890123456789')}, null='conflict'), event(amount=10, second='canonical')],
                         replay=[event(amount=99, x=Decimal('1.000000000000000000001')), event(amount=100, x=2)])
    originals = {cid: json.loads(archive.read_body(cid), parse_float=Decimal)
                 for archive, ids in ((api, aids), (web, bids)) for cid in ids}
    browser_manifest = web.fight_manifest('Hybrid1', 1)
    api_manifest = api.fight_manifest('Hybrid1', 1)
    api.merge_hybrid_fight(web, 'Hybrid1', 1)
    merged = api.full_fight_events('Hybrid1', 1)
    assert [row['amount'] for row in merged] == [7, 8]
    assert [row['x'] for row in merged] == [Decimal('1.000000000000000000001'), 2]
    assert merged[0]['nested'] == {'api': True, 'browser': Decimal('0.12345678901234567890123456789')}
    assert merged[0]['null'] is None and merged[1]['second'] == 'canonical'
    assert web.stats()['buffered_complete_fights'] == 0
    manifest = api.fight_manifest('Hybrid1', 1)
    assert manifest['hybrid']['api_manifest'] == api_manifest
    assert manifest['hybrid']['browser_manifest'] == browser_manifest
    assert any(row['index'] == 0 and row['path'] == ['amount'] for row in manifest['hybrid']['conflicts'])
    row = {'fight_uid': 'Hybrid1_1', 'report_code': 'Hybrid1', 'fight_id': 1}
    meta = {'report_code': 'Hybrid1', 'fight_id': 1, 'start_time': 0, 'end_time': 100}
    store = StagingStore(tmp_path)
    staged = store.stage_fight(api, row, meta, [], [], tmp_path / 'out.wclf')
    assert b'raw_events' not in staged.read_bytes()
    api.clear()
    web.clear()
    job, restored = StagingStore(tmp_path).load(staged)
    assert job['raw_events'] == merged
    restored.write_fight(tmp_path / 'out.wclf', meta, 'Hybrid1', 1)
    path = tmp_path / 'out.wclf'
    assert PackedFightReader.read_raw_events(path) == originals[aids[1]]['data']['reportData']['report']['events']['data']
    assert PackedFightReader.read_full_events(path) == merged
    assert json.loads(PackedFightReader.full_events_json(path), parse_float=Decimal) == merged
    for cid, body in originals.items():
        assert json.loads(PackedFightReader.read_source_response(path, cid), parse_float=Decimal) == body
    rows = PackedFightReader.source_manifest(path)['captures']
    for cap in rows:
        if cap['id'] in bids:
            assert cap['original_fetch_id'] == (None if cap['id'] == bids[0] else browser_manifest['fetch_id'])


@pytest.mark.parametrize('change', ['bounds', 'count', 'timestamp', 'type', 'sourceID', 'targetID', 'abilityGameID', 'order'])
def test_mismatch_rejects_hybrid_without_mutating_valid_attempts(tmp_path, change):
    rows = [event(amount=7), event(timestamp=11, amount=8)]
    other = deepcopy(rows)
    end = 100
    if change == 'bounds':
        end = 101
    elif change == 'count':
        other.pop()
    elif change == 'order':
        other.reverse()
    elif change == 'type':
        other[0][change] = 'heal'
    else:
        other[0][change] += 1
    api, _ = captured(tmp_path, 'api', rows)
    web, _ = captured(tmp_path, 'web', other, end=end)
    before_api, before_web = deepcopy(api._completed), deepcopy(web._completed)
    with pytest.raises(ValueError):
        api.merge_hybrid_fight(web, 'Hybrid1', 1)
    assert api._completed == before_api and web._completed == before_web


def test_alias_identity_and_discard_only_target_attempt(tmp_path):
    api, _ = captured(tmp_path, 'api', [event()])
    browser_event = {'timestamp': 10, 'type': 'damage', 'source': {'id': 1, 'unknown': True},
                     'target': {'id': 2}, 'ability': {'guid': 3}, 'extra': 'retained'}
    web, _ = captured(tmp_path, 'web', [browser_event])
    unrelated, _ = captured(tmp_path, 'other', [event()], code='Other')
    # A separate completed fight and master must survive discard/merge.
    api._completed.update(unrelated._completed)
    web._completed.update(unrelated._completed)
    api.merge_hybrid_fight(web, 'Hybrid1', 1)
    assert api.full_fight_events('Hybrid1', 1)[0]['extra'] == 'retained'
    api.discard_fight('Hybrid1', 1)
    assert ('Other', 1) in api._completed and ('Other', 1) in web._completed
    assert web.has_report_metadata('Hybrid1')


@pytest.mark.parametrize('field', ['fetch_id', 'report_code', 'kind', 'body'])
def test_staging_rejects_corrupt_auxiliary_source_binding(tmp_path, field):
    api, _ = captured(tmp_path, 'api', [event()])
    web, _ = captured(tmp_path, 'web', [event(extra=True)])
    api.merge_hybrid_fight(web, 'Hybrid1', 1)
    auxiliary = api._completed[('Hybrid1', 1)]['auxiliary'][-1]
    if field == 'body':
        auxiliary['body']['events'] = []
    else:
        auxiliary['info'][field] = 'corrupt'
    row = {'fight_uid': 'Hybrid1_1', 'report_code': 'Hybrid1', 'fight_id': 1}
    meta = {'report_code': 'Hybrid1', 'fight_id': 1, 'start_time': 0, 'end_time': 100}
    with pytest.raises(ValueError):
        StagingStore(tmp_path).stage_fight(api, row, meta, [], [], tmp_path / 'bad.wclf')


def test_recursive_list_fields_are_filled_without_replacing_api_values(tmp_path):
    api, _ = captured(tmp_path, 'api', [event(resources=[{'id': 1, 'value': 7}, None])])
    web, _ = captured(tmp_path, 'web', [event(resources=[{'id': 1, 'value': 9, 'unknown': {'precise': Decimal('1.000000000000000001')}}, {'extra': True}, {'tail': True}])])
    api.merge_hybrid_fight(web, 'Hybrid1', 1)
    assert api.full_fight_events('Hybrid1', 1)[0]['resources'] == [
        {'id': 1, 'value': 7, 'unknown': {'precise': Decimal('1.000000000000000001')}}, None, {'tail': True}]
    assert any(row['path'] == ['resources', 0, 'value'] for row in api.fight_manifest('Hybrid1', 1)['hybrid']['conflicts'])


def test_derived_replay_retains_unmatched_replay_positions(tmp_path):
    api, _ = captured(tmp_path, 'api', [event(amount=7)])
    web, _ = captured(tmp_path, 'web', [event(amount=9)], replay=[event(timestamp=20, x=100, y=200)])
    api.merge_hybrid_fight(web, 'Hybrid1', 1)
    path = tmp_path / 'derived.wclf'
    api.write_fight(path, {'actors': [{'id': 1, 'name': 'Player', 'type': 'Monk'}]}, 'Hybrid1', 1)
    frames = PackedFightReader.read_replay_only(path)['frames']
    assert any(frame['t'] == 20 for frame in frames)


@pytest.mark.parametrize('corruption', ['missing_terminal', 'api_bounds', 'api_report', 'web_gap', 'missing_replay', 'replay_gap', 'identity_alias'])
def test_strict_completeness_checks_payloads_and_requests_not_just_complete_flag(tmp_path, corruption):
    api, _ = captured(tmp_path, 'api', [event()])
    web, _ = captured(tmp_path, 'web', [event()], replay=[event(x=100)])
    api_capture, web_capture = api._completed[('Hybrid1', 1)], web._completed[('Hybrid1', 1)]
    if corruption == 'missing_terminal':
        api_capture['pages'][0]['body']['data']['reportData']['report']['events']['nextPageTimestamp'] = 50
    elif corruption == 'api_bounds':
        api_capture['pages'][0]['info']['request_json']['variables']['endTime'] = 99
    elif corruption == 'api_report':
        api_capture['pages'][0]['info']['request_json']['variables']['code'] = 'Other'
    elif corruption == 'web_gap':
        web_capture['pages'][0]['info']['url'] = '/reports/summary-events/Hybrid1/1/1/100'
    elif corruption == 'missing_replay':
        web_capture['master']['body']['fights'][0]['boss'] = 99
        web_capture['replay'] = []
        web_capture['manifest']['supplemental_captures'] = []
    elif corruption == 'replay_gap':
        web_capture['replay'][0]['info']['url'] = '/reports/replaysegment/Hybrid1/99/1/100'
    else:
        web_capture['pages'][0]['body']['events'][0]['source'] = {'id': 99}
    before_api, before_web = deepcopy(api._completed), deepcopy(web._completed)
    with pytest.raises(ValueError):
        api.merge_hybrid_fight(web, 'Hybrid1', 1)
    assert api._completed == before_api and web._completed == before_web


def pending_stage(tmp_path):
    api, ids = captured(tmp_path, 'api', [event(amount=7, apiUnknown=Decimal('0.12345678901234567890123456789'))])
    api._completed[('Hybrid1', 1)]['manifest']['hybrid_pending'] = True
    row = {'fight_uid': 'Hybrid1_1', 'report_code': 'Hybrid1', 'fight_id': 1}
    meta = {'report_code': 'Hybrid1', 'fight_id': 1, 'start_time': 0, 'end_time': 100}
    path = StagingStore(tmp_path).stage_fight(api, row, meta, [], [], tmp_path / 'out.wclf')
    return path, ids


def test_pending_api_snapshot_loads_but_cannot_be_packed(tmp_path):
    path, _ = pending_stage(tmp_path)
    job, restored = StagingStore(tmp_path).load(path)
    before = deepcopy(restored._completed)
    assert job['manifest']['hybrid_pending'] is True
    assert job['raw_events'][0]['apiUnknown'] == Decimal('0.12345678901234567890123456789')
    with pytest.raises(ValueError, match='(?i)pending|browser|enrich'):
        restored.write_fight(tmp_path / 'out.wclf', job['fight_meta'], 'Hybrid1', 1)
    assert restored._completed == before
    assert not (tmp_path / 'out.wclf').exists()
    assert path.exists()


def test_hybrid_enrichment_stages_new_phase_without_replacing_pending_api_snapshot(tmp_path):
    api_path, _ = pending_stage(tmp_path)
    original_bytes = api_path.read_bytes()
    original_hash = hashlib.sha256(original_bytes).digest()
    store = StagingStore(tmp_path)
    job, restored = store.load(api_path)
    original_manifest = deepcopy(job['manifest'])
    web, _ = captured(tmp_path, 'web', [event(amount=9, browserUnknown={'nested': True})], replay=[event(x=100)])
    restored.merge_hybrid_fight(web, 'Hybrid1', 1)
    manifest = restored.fight_manifest('Hybrid1', 1)
    assert 'hybrid_pending' not in manifest
    assert manifest['hybrid']['api_manifest'] == original_manifest
    hybrid_path = store.stage_fight(restored, job['fight_row'], job['fight_meta'], job['actors'], job['abilities'], tmp_path / job['out_rel_path'])
    assert hybrid_path != api_path
    assert hashlib.sha256(api_path.read_bytes()).digest() == original_hash
    api_job, _ = StagingStore(tmp_path).load(api_path)
    hybrid_job, final_archive = StagingStore(tmp_path).load(hybrid_path)
    assert api_job['manifest']['hybrid_pending'] is True
    assert 'hybrid_pending' not in hybrid_job['manifest']
    assert hybrid_job['raw_events'][0]['browserUnknown'] == {'nested': True}
    assert 'browserUnknown' not in api_job['raw_events'][0]
    final_archive.write_fight(tmp_path / 'out.wclf', job['fight_meta'], 'Hybrid1', 1)
    assert api_path.read_bytes() == original_bytes
    assert PackedFightReader.read_full_events(tmp_path / 'out.wclf') == hybrid_job['raw_events']


def test_failed_enrichment_leaves_original_api_snapshot_retryable(tmp_path):
    api_path, _ = pending_stage(tmp_path)
    original_bytes = api_path.read_bytes()
    store = StagingStore(tmp_path)
    job, restored = store.load(api_path)
    web, _ = captured(tmp_path, 'web', [event(timestamp=11)])
    with pytest.raises(ValueError):
        restored.merge_hybrid_fight(web, 'Hybrid1', 1)
    assert restored.fight_manifest('Hybrid1', 1)['hybrid_pending'] is True
    assert api_path.read_bytes() == original_bytes
    assert store.load(api_path)[0]['raw_events'] == job['raw_events']
    assert store.list_jobs() == [api_path]


def test_pending_and_complete_hybrid_markers_cannot_coexist(tmp_path):
    api, _ = captured(tmp_path, 'api', [event()])
    web, _ = captured(tmp_path, 'web', [event()])
    api.merge_hybrid_fight(web, 'Hybrid1', 1)
    api._completed[('Hybrid1', 1)]['manifest']['hybrid_pending'] = True
    row = {'fight_uid': 'Hybrid1_1', 'report_code': 'Hybrid1', 'fight_id': 1}
    meta = {'report_code': 'Hybrid1', 'fight_id': 1, 'start_time': 0, 'end_time': 100}
    with pytest.raises(ValueError, match='(?i)pending|phase|hybrid'):
        StagingStore(tmp_path).stage_fight(api, row, meta, [], [], tmp_path / 'bad.wclf')


def test_null_hybrid_phase_cannot_select_enriched_filename(tmp_path):
    api, _ = captured(tmp_path, 'api', [event()])
    api._completed[('Hybrid1', 1)]['manifest']['hybrid'] = None
    row = {'fight_uid': 'Hybrid1_1', 'report_code': 'Hybrid1', 'fight_id': 1}
    meta = {'report_code': 'Hybrid1', 'fight_id': 1, 'start_time': 0, 'end_time': 100}
    with pytest.raises(ValueError, match='(?i)hybrid|phase'):
        StagingStore(tmp_path).stage_fight(api, row, meta, [], [], tmp_path / 'bad.wclf')


def test_staging_filename_must_match_authoritative_capture_phase(tmp_path):
    api_path, _ = pending_stage(tmp_path)
    wrong_path = api_path.with_name(api_path.stem + '-hybrid.wcls')
    wrong_path.write_bytes(api_path.read_bytes())
    with pytest.raises(ValueError, match='filename'):
        StagingStore(tmp_path).load(wrong_path)
    wrong_path.unlink()
    store = StagingStore(tmp_path)
    job, restored = store.load(api_path)
    web, _ = captured(tmp_path, 'web', [event(extra=True)])
    restored.merge_hybrid_fight(web, 'Hybrid1', 1)
    hybrid_path = store.stage_fight(restored, job['fight_row'], job['fight_meta'], [], [], tmp_path / 'out.wclf')
    # Swapping a final payload into the old phase name must also fail validation.
    api_path.write_bytes(hybrid_path.read_bytes())
    with pytest.raises(ValueError, match='filename'):
        StagingStore(tmp_path).load(api_path)


def test_failed_enriched_publication_preserves_api_snapshot_and_restored_sources(tmp_path, monkeypatch):
    api_path, _ = pending_stage(tmp_path)
    original_bytes = api_path.read_bytes()
    store = StagingStore(tmp_path)
    job, restored = store.load(api_path)
    web, _ = captured(tmp_path, 'web', [event(extra=True)])
    restored.merge_hybrid_fight(web, 'Hybrid1', 1)
    completed = deepcopy(restored._completed)
    def fail(*args):
        raise OSError('enriched publication failed')
    monkeypatch.setattr('wcl_archiver.codec.staging.os.replace', fail)
    with pytest.raises(OSError, match='enriched publication'):
        store.stage_fight(restored, job['fight_row'], job['fight_meta'], [], [], tmp_path / 'out.wclf')
    assert api_path.read_bytes() == original_bytes
    assert restored._completed == completed
    assert store.list_jobs() == [api_path]
    assert store.load(api_path)[0]['manifest']['hybrid_pending'] is True

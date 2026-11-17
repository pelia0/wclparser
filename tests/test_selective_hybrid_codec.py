"""Selective supplements preserve received records without a full canonical fetch."""
from copy import deepcopy
from decimal import Decimal
import json

import pytest

from wcl_archiver.codec import fight_archive as codec
from wcl_archiver.codec.staging import StagingStore
from test_hybrid_codec import captured, event


def supplement(tmp_path, primary, replay, canonical=(), *, windows=None, boss=99,
               replay_range=(0, 100), finalize=True):
    archive = codec.FightArchive(tmp_path / 'supplement')
    ids = [archive.capture('GET', '/reports/fights-and-participants/Hybrid1/0', codec.json_bytes({
        'fights': [{'id': 1, 'boss': boss, 'start_time': 0, 'end_time': 100}],
        'unknownMaster': {'precise': Decimal('1.000000000000000001')}}))]
    with archive.fight_capture('Hybrid1', 1) as fetch:
        ids.append(archive.capture('GET', f'/reports/replaysegment/Hybrid1/99/{replay_range[0]}/{replay_range[1]}',
                                  codec.json_bytes({'events': replay, 'unknownReplay': None})))
        for left, right, rows in canonical:
            ids.append(archive.capture('GET', f'/reports/summary-events/Hybrid1/1/{left}/{right}',
                                      codec.json_bytes({'events': rows, 'nextPageTimestamp': None,
                                                        'unknownPage': [None, Decimal('2.300')]})))
        if finalize:
            assert hasattr(archive, 'complete_supplement'), 'Selective supplement finalizer is missing'
            archive.complete_supplement('Hybrid1', 1, fetch, 0, 100, primary)
            if windows is not None:
                assert archive.fight_manifest('Hybrid1', 1)['canonical_windows'] == windows
    return archive, ids


def test_windows_cover_deaths_missing_identities_and_duplicate_occurrences():
    assert hasattr(codec, 'supplement_windows'), 'Selective window planner is missing'
    api = [event(timestamp=30), event(timestamp=10, type='Death'),
           event(timestamp=20), event(timestamp=20), event(timestamp=10, type='Death')]
    replay = [event(timestamp=30), event(timestamp=20)]
    assert codec.supplement_windows(api, replay) == [(10, 10), (20, 20)]


def test_replay_extraction_is_scoped_to_exact_attempt(tmp_path):
    archive = codec.FightArchive(tmp_path)
    with archive.fight_capture('Hybrid1', 1) as fetch:
        archive.capture('GET', '/reports/replaysegment/Hybrid1/99/0/100', codec.json_bytes({'events': [event()]}))
        assert hasattr(archive, 'supplement_replay_events'), 'Scoped replay extraction is missing'
        assert archive.supplement_replay_events('Hybrid1', 1, fetch, 0, 100) == [event()]
        with pytest.raises(ValueError):
            archive.supplement_replay_events('Hybrid1', 2, fetch, 0, 100)


@pytest.mark.parametrize('invalid', [True, Decimal('1.5'), None, '10'])
def test_planner_rejects_nonintegral_timestamps_in_every_source(invalid):
    assert hasattr(codec, 'supplement_windows'), 'Selective window planner is missing'
    with pytest.raises(ValueError):
        codec.supplement_windows([event()], [event(timestamp=invalid)])


def test_zero_canonical_pages_stage_and_export_exact_sources(tmp_path):
    rows = [event(amount=7, nested={'api': None})]
    api, aids = captured(tmp_path, 'api', rows)
    web, bids = supplement(tmp_path, rows, [event(amount=9, nested={'browser': Decimal('0.123456789012345678901')})], windows=[])
    original = {cid: json.loads(archive.read_body(cid), parse_float=Decimal)
                for archive, ids in ((api, aids), (web, bids)) for cid in ids}
    manifest = web.fight_manifest('Hybrid1', 1)
    assert manifest['source_mode'] == 'browser_supplement_v1'
    assert manifest['event_count'] == 1 and manifest['event_captures'] == []
    api.merge_hybrid_fight(web, 'Hybrid1', 1)
    merged = api.full_fight_events('Hybrid1', 1)
    assert merged == [event(amount=7, nested={'api': None, 'browser': Decimal('0.123456789012345678901')})]
    store = StagingStore(tmp_path)
    path = store.stage_fight(api, {'fight_uid': 'Hybrid1_1', 'report_code': 'Hybrid1', 'fight_id': 1},
                            {'report_code': 'Hybrid1', 'fight_id': 1, 'start_time': 0, 'end_time': 100}, [], [], tmp_path / 'out.wclf')
    job, restored = store.load(path)
    assert job['raw_events'] == merged
    restored.write_fight(tmp_path / 'out.wclf', job['fight_meta'], 'Hybrid1', 1)
    assert codec.PackedFightReader.read_full_events(tmp_path / 'out.wclf') == merged
    for cid, body in original.items():
        assert json.loads(codec.PackedFightReader.read_source_response(tmp_path / 'out.wclf', cid), parse_float=Decimal) == body


def test_targeted_canonical_precedes_replay_preserves_unknown_fields_and_duplicates(tmp_path):
    rows = [event(amount=1), event(amount=2), event(timestamp=20, type='death', extra=None)]
    api, _ = captured(tmp_path, 'api', rows)
    replay = [event(amount=99, replayOnly={'x': Decimal('1.00000000000000001')}),
              event(timestamp=20, type='death', killer=8, replayNull=None)]
    canonical = [(10, 10, [event(amount=4, canonicalOnly=[None, {'x': True}]), event(amount=5, second=True)]),
                 (20, 20, [event(timestamp=20, type='death', killer=7, killingAbility={'id': 12}, extra='conflict')])]
    web, _ = supplement(tmp_path, rows, replay, canonical, windows=[[10, 10], [20, 20]])
    api.merge_hybrid_fight(web, 'Hybrid1', 1)
    merged = api.full_fight_events('Hybrid1', 1)
    assert [r['amount'] for r in merged[:2]] == [1, 2]
    assert merged[0]['canonicalOnly'] == [None, {'x': True}]
    assert merged[0]['replayOnly']['x'] == Decimal('1.00000000000000001')
    assert merged[1]['second'] is True
    assert merged[2]['killer'] == 7 and merged[2]['extra'] is None
    assert merged[2]['killingAbility'] == {'id': 12} and merged[2]['replayNull'] is None
    assert any(c['path'] == ['killer'] for c in api.fight_manifest('Hybrid1', 1)['hybrid']['conflicts'])


@pytest.mark.parametrize('problem', ['missing_window', 'unneeded_window', 'unknown_event', 'missing_duplicate',
                                     'replay_gap', 'boss_zero', 'boss_mismatch', 'out_of_bounds'])
def test_incomplete_or_unbound_supplement_is_rejected(tmp_path, problem):
    rows = [event(type='death'), event(type='death')]
    replay = [event(type='death')]
    canonical = [(10, 10, deepcopy(rows))]
    opts = {}
    if problem == 'missing_window':
        canonical = []
    elif problem == 'unneeded_window':
        canonical.append((30, 30, []))
    elif problem == 'unknown_event':
        canonical[0][2].append(event(type='heal'))
    elif problem == 'missing_duplicate':
        canonical[0][2].pop()
    elif problem == 'replay_gap':
        opts['replay_range'] = (1, 100)
    elif problem == 'boss_zero':
        opts['boss'] = 0
    elif problem == 'boss_mismatch':
        opts['boss'] = 98
    else:
        replay.append(event(timestamp=101))
    with pytest.raises(ValueError):
        supplement(tmp_path, rows, replay, canonical, **opts)


@pytest.mark.parametrize('corruption', ['windows', 'window_float', 'terminal', 'missing_page', 'unknown_event', 'missing_replay'])
def test_merge_revalidates_supplement_and_leaves_primary_intact(tmp_path, corruption):
    rows = [event(type='death')]
    api, _ = captured(tmp_path, 'api', rows)
    web, _ = supplement(tmp_path, rows, rows, [(10, 10, rows)])
    cap = web._completed[('Hybrid1', 1)]
    if corruption == 'windows':
        cap['manifest']['canonical_windows'] = []
    elif corruption == 'window_float':
        cap['manifest']['canonical_windows'] = [[10.0, 10.0]]
    elif corruption == 'terminal':
        cap['pages'][0]['body']['nextPageTimestamp'] = 11
    elif corruption == 'missing_page':
        cap['pages'] = []
    elif corruption == 'unknown_event':
        cap['pages'][0]['body']['events'].append(event(type='heal'))
    else:
        cap['replay'] = []
    before_api, before_web = deepcopy(api._completed), deepcopy(web._completed)
    with pytest.raises(ValueError):
        api.merge_hybrid_fight(web, 'Hybrid1', 1)
    assert api._completed == before_api and web._completed == before_web


def test_supplement_cannot_be_exposed_as_a_complete_standalone_archive(tmp_path):
    rows = [event()]
    web, _ = supplement(tmp_path, rows, rows)
    with pytest.raises(ValueError, match='(?i)primary|supplement'):
        web.full_fight_events('Hybrid1', 1)
    with pytest.raises(ValueError, match='(?i)primary|supplement'):
        web.write_fight(tmp_path / 'partial.wclf', {}, 'Hybrid1', 1)
    assert not (tmp_path / 'partial.wclf').exists()


def test_targeted_canonical_cannot_substitute_unbound_graphql_request(tmp_path):
    rows = [event(type='death')]
    api, _ = captured(tmp_path, 'api', rows)
    web, _ = supplement(tmp_path, rows, rows, [(10, 10, rows)])
    page = web._completed[('Hybrid1', 1)]['pages'][0]
    page['info']['url'] = '/api/v2/client'
    page['info']['request_json'] = {'variables': {'code': 'Hybrid1', 'fightId': 1, 'startTime': 0, 'endTime': 100}}
    with pytest.raises(ValueError):
        api.merge_hybrid_fight(web, 'Hybrid1', 1)


def test_failed_selective_stage_keeps_pending_api_snapshot(tmp_path, monkeypatch):
    from test_hybrid_codec import pending_stage
    pending, _ = pending_stage(tmp_path)
    original = pending.read_bytes()
    store = StagingStore(tmp_path)
    job, restored = store.load(pending)
    web, _ = supplement(tmp_path, job['raw_events'], [event(browserUnknown=True)])
    restored.merge_hybrid_fight(web, 'Hybrid1', 1)
    before = deepcopy(restored._completed)
    def fail(*args):
        raise OSError('selective publication failed')
    monkeypatch.setattr('wcl_archiver.codec.staging.os.replace', fail)
    with pytest.raises(OSError):
        store.stage_fight(restored, job['fight_row'], job['fight_meta'], [], [], tmp_path / 'out.wclf')
    assert pending.read_bytes() == original and restored._completed == before
    assert store.load(pending)[0]['manifest']['hybrid_pending'] is True


@pytest.mark.parametrize('corruption', ['windows', 'source_mode', 'replay_gap'])
def test_staging_revalidates_selective_manifest(tmp_path, corruption):
    rows = [event(type='death')]
    api, _ = captured(tmp_path, 'api', rows)
    web, _ = supplement(tmp_path, rows, rows, [(10, 10, rows)])
    api.merge_hybrid_fight(web, 'Hybrid1', 1)
    completed = api._completed[('Hybrid1', 1)]
    if corruption == 'windows':
        completed['manifest']['hybrid']['browser_manifest']['canonical_windows'] = []
    elif corruption == 'source_mode':
        completed['manifest']['hybrid']['browser_manifest']['source_mode'] = 'unrecognized'
    else:
        completed['replay'][0]['info']['url'] = '/reports/replaysegment/Hybrid1/99/1/100'
    with pytest.raises(ValueError):
        StagingStore(tmp_path).stage_fight(api, {'fight_uid': 'Hybrid1_1', 'report_code': 'Hybrid1', 'fight_id': 1},
                                         {'report_code': 'Hybrid1', 'fight_id': 1, 'start_time': 0, 'end_time': 100},
                                         [], [], tmp_path / 'out.wclf')


@pytest.mark.parametrize('corruption', ['windows', 'missing_page', 'replay_gap'])
def test_reader_revalidates_selective_source_payloads(tmp_path, corruption):
    rows = [event(type='death')]
    api, _ = captured(tmp_path, 'api', rows)
    web, _ = supplement(tmp_path, rows, rows, [(10, 10, rows)])
    api.merge_hybrid_fight(web, 'Hybrid1', 1)
    path = tmp_path / 'reader.wclf'
    api.write_fight(path, {}, 'Hybrid1', 1)
    sections = codec.PackedFightReader.read_sections(path)
    manifest = sections['metadata']['manifest']
    if corruption == 'windows':
        manifest['hybrid']['browser_manifest']['canonical_windows'] = []
    elif corruption == 'missing_page':
        cid = manifest['hybrid']['browser_manifest']['event_captures'][0]
        sections['metadata']['captures'] = [cap for cap in sections['metadata']['captures'] if cap['id'] != cid]
    else:
        cid = manifest['hybrid']['browser_manifest']['supplemental_captures'][0]
        next(cap for cap in sections['metadata']['captures'] if cap['id'] == cid)['url'] = '/reports/replaysegment/Hybrid1/99/1/100'
    codec.FightArchive.write_sections(path, sections)
    with pytest.raises(ValueError):
        codec.PackedFightReader.read_full_events(path)


@pytest.mark.parametrize('extra', [event(timestamp=25, type='death', browserOnly=True), event(browserOnly=True)])
def test_replay_excess_occurrences_reject_completion_without_changing_api_sources(tmp_path, extra):
    rows = [event()]
    api, ids = captured(tmp_path, 'api', rows)
    before = deepcopy(api._completed)
    originals = [api.read_body(cid) for cid in ids]
    with pytest.raises(ValueError, match='(?i)replay|occurrence|primary'):
        supplement(tmp_path, rows, [event(), extra])
    assert api._completed == before
    assert [api.read_body(cid) for cid in ids] == originals


@pytest.mark.parametrize('extra', [event(timestamp=25, type='death', browserOnly=True), event(browserOnly=True)])
def test_replay_excess_occurrences_reject_merge_without_mutating_provider_attempts(tmp_path, extra):
    rows = [event()]
    api, ids = captured(tmp_path, 'api', rows)
    web, _ = supplement(tmp_path, rows, rows)
    web._completed[('Hybrid1', 1)]['replay'][0]['body']['events'].append(extra)
    before_api, before_web = deepcopy(api._completed), deepcopy(web._completed)
    originals = [api.read_body(cid) for cid in ids]
    with pytest.raises(ValueError, match='(?i)replay|occurrence|primary'):
        api.merge_hybrid_fight(web, 'Hybrid1', 1)
    assert api._completed == before_api and web._completed == before_web
    assert [api.read_body(cid) for cid in ids] == originals

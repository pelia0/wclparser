import asyncio
import json
import time
from decimal import Decimal

import httpx
import pytest

from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader
from wcl_archiver.wcl_client.auth import WCLAuthManager
from wcl_archiver.parser.combat_state_machine import CombatStateMachine
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient


class UserAuth:
    def __init__(self):
        self.calls = 0

    def get_headers_and_cookies(self):
        return {'Authorization': 'Bearer stale-client'}, {}

    def get_auth_status(self):
        return {'has_oauth_token': True, 'has_user_oauth_token': True}

    async def get_api_auth(self):
        self.calls += 1
        return 'https://www.warcraftlogs.com/api/v2/user', f'fresh-{self.calls}'


def master_payload():
    return {'data': {'reportData': {'report': {
        'fights': [{'id': 1, 'startTime': 0, 'endTime': 10, 'encounterID': 99}],
        'masterData': {'logVersion': 19, 'gameVersion': 1, 'lang': 'en',
                       'actors': [{'id': 1, 'name': 'Player', 'type': 'Player',
                                   'subType': 'Mage', 'server': 'Test', 'gameID': 12,
                                   'petOwner': None}],
                       'abilities': []}, 'unknown': {'future': True}}}}}


@pytest.mark.asyncio
async def test_user_endpoint_captures_full_decimal_source_and_refreshes_each_request(tmp_path):
    auth = UserAuth()
    archive = FightArchive(tmp_path)
    requests = []
    event_body = b'{"data":{"reportData":{"report":{"events":{"data":[{"timestamp":1,"amount":0.12345678901234567890123456789,"unknown":{"nested":[null,9007199254740993]}}],"nextPageTimestamp":null,"future":1.0000000000000000001}}}},"extensions":{"future":true}}'

    def handler(req):
        requests.append(req)
        assert req.url.path == '/api/v2/user'
        assert req.headers['Authorization'] == f'Bearer fresh-{len(requests)}'
        if 'masterData' in json.loads(req.content)['query']:
            return httpx.Response(200, json=master_payload())
        return httpx.Response(200, content=event_body)

    client = WCLProtocolClient(auth, httpx.MockTransport(handler), archive)
    assert await client.unarchive_report('Report1234') is True
    master = await client.fetch_report_master_and_fights('Report1234')
    assert master['friendlies'][0]['name'] == 'Player'
    assert len(requests) == 1  # Unarchive validation retains captured metadata.
    metadata_query = json.loads(requests[0].content)['query']
    assert all(field in metadata_query for field in ('logVersion', 'gameVersion', 'lang', 'petOwner', 'gameID', 'server'))
    events = await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    assert events[0]['amount'] == Decimal('0.12345678901234567890123456789')
    query = json.loads(requests[-1].content)
    assert 'limit: 20000' in query['query']
    assert 'includeResources: true' in query['query']
    assert query['variables']['fightId'] == 1
    path = tmp_path / 'fight.wclf'
    archive.write_fight(path, {}, 'Report1234', 1)
    assert PackedFightReader.read_raw_events(path) == events
    manifest = PackedFightReader.source_manifest(path)
    event_capture = next(c for c in manifest['captures'] if c['kind'] == 'events')
    assert event_capture['url'].endswith('/api/v2/user')
    assert event_capture['request_json']['variables']['fightId'] == 1
    assert json.loads(PackedFightReader.read_source_response(path, event_capture['id']), parse_float=Decimal) == json.loads(event_body, parse_float=Decimal)


@pytest.mark.asyncio
async def test_user_metadata_cache_refetches_when_source_is_evicted(tmp_path):
    archive = FightArchive(tmp_path)
    requests = []

    def handler(req):
        requests.append(req)
        return httpx.Response(200, json=master_payload())

    client = WCLProtocolClient(UserAuth(), httpx.MockTransport(handler), archive)
    await client.unarchive_report('Report1234')
    archive.clear()
    await client.fetch_report_master_and_fights('Report1234')
    assert len(requests) == 2
    assert archive.has_report_metadata('Report1234')


@pytest.mark.asyncio
@pytest.mark.parametrize('payload', [None, [], {'errors': [{'message': 'Bearer SECRET'}]},
    {'data': {'reportData': {'report': None}}},
    {'data': {'reportData': {'report': {'events': {'data': 'invalid'}}}}}])
async def test_user_event_denial_and_malformed_payload_never_use_browser(tmp_path, payload):
    requests = []

    def handler(req):
        requests.append(req)
        return httpx.Response(200, content=json.dumps(payload).encode())

    client = WCLProtocolClient(UserAuth(), httpx.MockTransport(handler))
    with pytest.raises(RuntimeError) as error:
        await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    assert 'SECRET' not in str(error.value)
    assert [req.url.path for req in requests] == ['/api/v2/user']


@pytest.mark.asyncio
@pytest.mark.parametrize('cursor', [0, -1, 11, True, 1.5, '5'])
async def test_graphql_rejects_invalid_cursor(tmp_path, cursor):
    payload = {'data': {'reportData': {'report': {'events': {
        'data': [], 'nextPageTimestamp': cursor}}}}}
    client = WCLProtocolClient(UserAuth(), httpx.MockTransport(lambda req: httpx.Response(200, json=payload)))
    with pytest.raises(RuntimeError, match='cursor'):
        await client.fetch_all_fight_events('Report1234', 1, 0, 10)


@pytest.mark.asyncio
async def test_graphql_pagination_preserves_order_and_fresh_authorization(tmp_path):
    auth = UserAuth()
    requests = []

    def handler(req):
        requests.append(req)
        assert req.headers['Authorization'] == f'Bearer fresh-{len(requests)}'
        start = json.loads(req.content)['variables']['startTime']
        return httpx.Response(200, json={'data': {'reportData': {'report': {'events': {
            'data': [{'timestamp': start, 'unknown': ['ordered', len(requests)]}],
            'nextPageTimestamp': 10 if start == 0 else None}}}}})

    client = WCLProtocolClient(auth, httpx.MockTransport(handler))
    events = await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    assert [e['timestamp'] for e in events] == [0, 10]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_user_rankings_use_selected_endpoint_and_sanitize_graphql_errors(tmp_path):
    def handler(req):
        assert req.url.path == '/api/v2/user'
        return httpx.Response(200, json={'errors': [{'message': 'Bearer SECRET'}]})

    client = WCLProtocolClient(UserAuth(), httpx.MockTransport(handler))
    with pytest.raises(RuntimeError) as error:
        await client.fetch_rankings_fight_refs(9, 123, 1)
    assert 'SECRET' not in str(error.value)


@pytest.mark.asyncio
async def test_application_quota_wait_is_shared_between_concurrent_requests(tmp_path, monkeypatch):
    now = [100.0]
    sleeps = []
    original_sleep = asyncio.sleep
    monkeypatch.setattr(time, 'monotonic', lambda: now[0])

    async def sleep(delay):
        sleeps.append(delay)
        await original_sleep(0)
        now[0] += delay

    monkeypatch.setattr(asyncio, 'sleep', sleep)
    calls = []

    def handler(req):
        calls.append(req)
        assert 'rateLimitData' in json.loads(req.content)['query']
        payload = master_payload()
        if len(calls) == 1:
            payload['data']['rateLimitData'] = {'limitPerHour': 100, 'pointsSpentThisHour': 100, 'pointsResetIn': 2}
        return httpx.Response(200, json=payload)

    client = WCLProtocolClient(UserAuth(), httpx.MockTransport(handler))
    await client.fetch_report_master_and_fights('Report1234')
    await asyncio.gather(client.fetch_report_master_and_fights('Report5678'),
                         client.fetch_report_master_and_fights('Report9012'))
    assert sleeps == [2]
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_http_429_retries_with_bounded_shared_retry_after(tmp_path, monkeypatch):
    now = [100.0]
    sleeps = []
    monkeypatch.setattr(time, 'monotonic', lambda: now[0])

    async def sleep(delay):
        sleeps.append(delay)
        now[0] += delay

    monkeypatch.setattr(asyncio, 'sleep', sleep)
    calls = []

    def handler(req):
        calls.append(req)
        if len(calls) == 1:
            return httpx.Response(429, headers={'Retry-After': '2'}, text='Bearer SECRET')
        return httpx.Response(200, json=master_payload())

    client = WCLProtocolClient(UserAuth(), httpx.MockTransport(handler))
    await client.fetch_report_master_and_fights('Report1234')
    assert sleeps == [2]
    assert len(calls) == 2
    assert calls[-1].headers['Authorization'] == 'Bearer fresh-2'


@pytest.mark.asyncio
async def test_large_retry_after_waits_cancelably_without_failing_queued_reports(tmp_path, monkeypatch):
    calls = []
    sleeps = []
    original_sleep = asyncio.sleep
    blocked = asyncio.Event()

    async def sleep(delay):
        sleeps.append(delay)
        await blocked.wait()

    monkeypatch.setattr(asyncio, 'sleep', sleep)

    def handler(req):
        calls.append(req)
        return httpx.Response(429, headers={'Retry-After': '3600'}, text='Bearer SECRET')

    client = WCLProtocolClient(UserAuth(), httpx.MockTransport(handler))
    tasks = [asyncio.create_task(client.fetch_report_master_and_fights('Report1234')),
             asyncio.create_task(client.fetch_report_master_and_fights('Report5678'))]
    try:
        await original_sleep(0)
        assert all(not task.done() for task in tasks)
        assert len(calls) == 1
        assert len(sleeps) == 1 and 0 < sleeps[0] <= 30
        assert 3500 < client.get_quota_status()['api_quota_wait_seconds'] <= 3600
    finally:
        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_expired_saved_user_token_refreshes_before_direct_graphql_call(tmp_path):
    requests = []

    def handler(req):
        requests.append(req)
        if req.url.path == '/oauth/token':
            return httpx.Response(200, json={'access_token': 'new-user', 'refresh_token': 'new-refresh', 'expires_in': 3600})
        assert req.url.path == '/api/v2/user'
        assert req.headers['Authorization'] == 'Bearer new-user'
        return httpx.Response(200, json=master_payload())

    session = tmp_path / 'auth.json'
    session.write_text(json.dumps({'client_id': 'cid', 'client_secret': 'secret',
        'oauth_token': 'client-stale', 'user_oauth_client_id': 'cid',
        'user_oauth_token': 'expired', 'user_oauth_refresh_token': 'refresh',
        'user_oauth_expires_at': 1}))
    transport = httpx.MockTransport(handler)
    client = WCLProtocolClient(WCLAuthManager(session, transport), transport)
    master = await client.fetch_report_master_and_fights('Report1234')
    assert master['fights'][0]['id'] == 1
    assert [req.url.path for req in requests] == ['/oauth/token', '/api/v2/user']
    assert json.loads(session.read_text())['user_oauth_refresh_token'] == 'new-refresh'


@pytest.mark.asyncio
async def test_rejected_refresh_cannot_silently_downgrade_on_later_calls(tmp_path):
    requests = []

    def handler(req):
        requests.append(req)
        assert req.url.path == '/oauth/token'
        return httpx.Response(400, json={'error': 'invalid_grant', 'error_description': 'SECRET'})

    session = tmp_path / 'auth.json'
    session.write_text(json.dumps({'client_id': 'cid', 'client_secret': 'secret',
        'oauth_token': 'client-stale', 'user_oauth_client_id': 'cid',
        'user_oauth_token': 'expired', 'user_oauth_refresh_token': 'refresh',
        'user_oauth_expires_at': 1, 'cookies': {'wcl_session': 'cookie'}}))
    transport = httpx.MockTransport(handler)
    auth = WCLAuthManager(session, transport)
    client = WCLProtocolClient(auth, transport)
    for _ in range(2):
        with pytest.raises(RuntimeError) as error:
            await client.unarchive_report('Report1234')
        assert 'SECRET' not in str(error.value)
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_user_rankings_denial_does_not_fall_back_to_browser(tmp_path):
    class UserWithoutLegacy(UserAuth):
        def get_auth_status(self):
            return {'has_user_oauth_token': True, 'has_oauth_token': False, 'has_session_cookie': True}

    requests = []

    def handler(req):
        requests.append(req)
        return httpx.Response(403, text='SECRET') if req.url.path == '/api/v2/user' else httpx.Response(200, text='empty')

    client = WCLProtocolClient(UserWithoutLegacy(), httpx.MockTransport(handler))
    with pytest.raises(RuntimeError):
        await client.fetch_rankings_fight_refs(9, 123, 1)
    assert [req.url.path for req in requests] == ['/api/v2/user']


@pytest.mark.asyncio
@pytest.mark.parametrize('field', ['fights', 'actors', 'abilities'])
async def test_malformed_metadata_records_raise_contextual_error(tmp_path, field):
    payload = master_payload()
    report = payload['data']['reportData']['report']
    (report if field == 'fights' else report['masterData'])[field] = [None]
    client = WCLProtocolClient(UserAuth(), httpx.MockTransport(lambda req: httpx.Response(200, json=payload)))
    with pytest.raises(RuntimeError, match='metadata'):
        await client.fetch_report_master_and_fights('Report1234')


@pytest.mark.asyncio
async def test_graphql_rejects_nonobject_event_records(tmp_path):
    payload = {'data': {'reportData': {'report': {'events': {
        'data': [None], 'nextPageTimestamp': None}}}}}
    client = WCLProtocolClient(UserAuth(), httpx.MockTransport(lambda req: httpx.Response(200, json=payload)))
    with pytest.raises(RuntimeError, match='event'):
        await client.fetch_all_fight_events('Report1234', 1, 0, 10)


@pytest.mark.asyncio
async def test_projected_pet_owner_attributes_damage_without_summon_event(tmp_path):
    payload = master_payload()
    actors = payload['data']['reportData']['report']['masterData']['actors']
    pet = {'id': 50, 'name': 'Pet', 'type': 'Pet', 'petOwner': 1, 'gameID': 99,
           'server': 'Test', 'subType': 'NPC', 'future': {'unknown': True}}
    actors.append(pet)
    client = WCLProtocolClient(UserAuth(), httpx.MockTransport(lambda req: httpx.Response(200, json=payload)))
    master = await client.fetch_report_master_and_fights('Report1234')
    projected = master['friendlies'] + master['enemies']
    summary, _, _ = CombatStateMachine.analyze_fight(
        {'report_code': 'Report1234', 'fight_id': 1, 'start_time': 0, 'end_time': 10},
        projected, [], [{'timestamp': 1, 'type': 'damage', 'sourceID': 50,
                         'targetID': 99, 'abilityGameID': 1, 'amount': 25}])
    assert summary['damage_done'][0]['actor_id'] == 1
    assert summary['damage_done'][0]['total'] == 25
    assert next(actor for actor in projected if actor['id'] == 50) == pet
    assert master['friendlies'][0]['server'] == 'Test'


@pytest.mark.asyncio
async def test_archived_inaccessible_metadata_cannot_certify_unarchive_success(tmp_path):
    payload = master_payload()
    payload['data']['reportData']['report']['archiveStatus'] = {'isArchived': True, 'isAccessible': False}
    requests = []

    def handler(req):
        requests.append(req)
        return httpx.Response(200, json=payload)

    client = WCLProtocolClient(UserAuth(), httpx.MockTransport(handler))
    with pytest.raises(RuntimeError, match='archived.*access'):
        await client.unarchive_report('Report1234')
    assert len(requests) == 1
    query = json.loads(requests[0].content)['query']
    assert all(field in query for field in ('archiveStatus', 'isArchived', 'isAccessible'))

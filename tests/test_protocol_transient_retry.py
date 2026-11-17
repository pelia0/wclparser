import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace
import time

import httpx
import pytest

from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader
from wcl_archiver.wcl_client import protocol_parser
from wcl_archiver.wcl_client.hybrid_client import HybridClient
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient


class Auth:
    def __init__(self):
        self.calls = 0

    def get_headers_and_cookies(self):
        return {}, {}

    def get_auth_status(self):
        return {'has_user_oauth_token': True}

    async def get_api_auth(self):
        self.calls += 1
        return 'https://www.warcraftlogs.com/api/v2/user', f'fresh-{self.calls}'


BODY = b'{"data":{"reportData":{"report":{"events":{"data":[{"timestamp":1,"amount":0.12345678901234567890123456789,"unknown":[null,9007199254740993]}],"nextPageTimestamp":null}}}}}'
FAILURES = [httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ReadError,
            httpx.ConnectError, httpx.RemoteProtocolError, 408, 500, 502, 503, 504]


@pytest.fixture
def clock(monkeypatch):
    now, sleeps = [100.0], []
    monkeypatch.setattr(protocol_parser, 'time', SimpleNamespace(
        monotonic=lambda: now[0], time=time.time))

    async def sleep(delay):
        sleeps.append(delay)
        now[0] += delay

    monkeypatch.setattr(asyncio, 'sleep', sleep)
    return now, sleeps


def failure_response(failure, request):
    if isinstance(failure, int):
        return httpx.Response(failure)
    raise failure('', request=request)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', FAILURES)
async def test_transient_failure_retries_with_fresh_auth_and_exact_capture(tmp_path, clock, failure):
    requests = []

    def handler(request):
        if 'masterData' in json.loads(request.content)['query']:
            return httpx.Response(200, json={'data': {'reportData': {'report': {
                'fights': [{'id': 1, 'startTime': 0, 'endTime': 10}],
                'masterData': {'actors': [], 'abilities': []}}}}})
        requests.append(request)
        if len(requests) < 3:
            return failure_response(failure, request)
        return httpx.Response(200, content=BODY)

    archive = FightArchive(tmp_path)
    client = WCLProtocolClient(Auth(), httpx.MockTransport(handler), archive)
    await client.fetch_report_master_and_fights('Report1234')
    client.auth_manager.calls = 0
    events = await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    assert len(requests) == 3
    assert [r.headers['Authorization'] for r in requests] == [
        'Bearer fresh-1', 'Bearer fresh-2', 'Bearer fresh-3']
    assert clock[1] == [1, 2]
    assert events[0]['amount'] == Decimal('0.12345678901234567890123456789')
    path = tmp_path / 'retry.wclf'
    archive.write_fight(path, {}, 'Report1234', 1)
    assert PackedFightReader.read_raw_events(path) == events
    captures = PackedFightReader.source_manifest(path)['captures']
    expected_source = json.loads(BODY, parse_float=Decimal)
    assert any(json.loads(PackedFightReader.read_source_response(path, c['id']),
                          parse_float=Decimal) == expected_source for c in captures)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', FAILURES)
async def test_transient_failure_exhaustion_stops_after_three_attempts(clock, failure):
    requests = []

    def handler(request):
        requests.append(request)
        return failure_response(failure, request)

    client = WCLProtocolClient(Auth(), httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError if isinstance(failure, int) else failure):
        await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    assert len(requests) == 3
    assert clock[1] == [1, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize('status', [400, 401, 403, 404, 422, 200])
async def test_permanent_http_and_graphql_errors_are_not_retried(clock, status):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, json={'errors': [{'message': 'denied'}]})

    client = WCLProtocolClient(Auth(), httpx.MockTransport(handler))
    with pytest.raises(RuntimeError if status == 200 else httpx.HTTPStatusError):
        await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    assert len(requests) == 1
    assert clock[1] == []


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', [httpx.ReadTimeout, 503])
async def test_transient_cooldown_is_shared_cancelable_and_separate_from_quota(monkeypatch, failure):
    blocked, entered = asyncio.Event(), asyncio.Event()
    calls = []

    async def sleep(delay):
        entered.set()
        await blocked.wait()

    monkeypatch.setattr(asyncio, 'sleep', sleep)

    def handler(request):
        calls.append(request)
        if failure == 503:
            return httpx.Response(503, headers={'Retry-After': '3600'})
        return failure_response(failure, request)

    client = WCLProtocolClient(Auth(), httpx.MockTransport(handler))
    first = asyncio.create_task(client.fetch_all_fight_events('Report1234', 1, 0, 10))
    await asyncio.wait_for(entered.wait(), 2)
    second = asyncio.create_task(client.fetch_all_fight_events('Report5678', 1, 0, 10))
    try:
        # Give the second worker an event-loop turn to reach the shared gate.
        await asyncio.wait_for(asyncio.shield(entered.wait()), 2)
        assert len(calls) == 1
        status = client.get_quota_status()
        assert status['api_quota_wait_seconds'] == 0
        assert status['api_retry_wait_seconds'] > 0
    finally:
        first.cancel()
        second.cancel()
        results = await asyncio.gather(first, second, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)
    assert not client._quota_wait_lock.locked()
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_retry_after_and_existing_quota_deadlines_are_both_respected(clock):
    requests = []

    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503, headers={'Retry-After': '7'})
        return httpx.Response(200, content=BODY)

    client = WCLProtocolClient(Auth(), httpx.MockTransport(handler))
    client._quota_until = 104
    await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    assert clock[1] == [4, 7]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_hybrid_blank_transport_exception_identifies_exception_class():
    class Provider:
        async def fetch(self):
            raise httpx.ReadTimeout('')

    with pytest.raises(RuntimeError, match='API.*fetch.*ReadTimeout') as error:
        await HybridClient._source_call('API', Provider(), 'fetch')
    assert isinstance(error.value.__cause__, httpx.ReadTimeout)

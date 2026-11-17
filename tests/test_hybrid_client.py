import asyncio
from copy import deepcopy
import hashlib
import json
import threading
from decimal import Decimal

import httpx
import pytest

from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader
from wcl_archiver.codec.staging import StagingStore
from wcl_archiver.wcl_client.browser_client import BrowserClient
from wcl_archiver.wcl_client.hybrid_client import HybridClient
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient


class Archive:
    """Archive boundary used to check independent acquisition ownership."""

    def __init__(self):
        self.metadata = set()
        self.fights = {}
        self._completed = {}
        self.discarded = []

    def has_report_metadata(self, code):
        return code in self.metadata

    def discard_fight(self, code, fid):
        self.discarded.append((code, fid))
        self.fights.pop((code, fid), None)
        self._completed.pop((code, fid), None)

    def merge_hybrid_fight(self, browser, code, fid):
        key = (code, fid)
        self.fights[key] = [{**browser.fights[key][0], **self.fights[key][0]}]
        self._completed[key]['manifest'].pop('hybrid_pending', None)
        browser.discard_fight(code, fid)

    def full_fight_events(self, code, fid):
        return self.fights[(code, fid)]

    def fight_manifest(self, code, fid):
        return deepcopy(self._completed[(code, fid)]['manifest'])


class Provider:
    def __init__(self, archive, *, browser=False):
        self.raw_archive = archive
        self.master = {'fights': [{'id': 1}], 'browser': browser}
        self.master_calls = []
        self.valid = True
        self.validation_calls = []
        self.fetch_calls = []
        self.fetch = None

    async def unarchive_report(self, code):
        self.validation_calls.append(code)
        return self.valid

    async def fetch_report_master_and_fights(self, code):
        self.master_calls.append(code)
        self.raw_archive.metadata.add(code)
        return self.master

    async def fetch_all_fight_events(self, code, fid, start, end):
        self.fetch_calls.append((code, fid, start, end))
        assert self.raw_archive.has_report_metadata(code)
        if self.fetch:
            return await self.fetch(code, fid, start, end)
        events = [{'timestamp': 1, 'api': True}] if not self.master['browser'] else [
            {'timestamp': 1, 'unknown': Decimal('0.12345678901234567890123456789')}]
        self.raw_archive.fights[(code, fid)] = events
        self.raw_archive._completed[(code, fid)] = {'manifest': {
            'report_code': code, 'fight_id': fid, 'start_time': start, 'end_time': end,
            'complete': True}}
        return events

    async def fetch_rankings_fight_refs(self, **kwargs):
        assert kwargs['difficulty'] == 5
        return [{'report': {'code': 'Report1234', 'fightID': 1}}]


def clients():
    api_archive, browser_archive = Archive(), Archive()
    api, browser = Provider(api_archive), Provider(browser_archive, browser=True)
    return HybridClient(api, browser, api_archive, browser_archive), api, browser


def restored_fight(api, code='Report1234', fid=1):
    restored = Archive()
    restored.fights[(code, fid)] = deepcopy(api.raw_archive.fights[(code, fid)])
    restored._completed[(code, fid)] = deepcopy(api.raw_archive._completed[(code, fid)])
    job = {'fight_row': {'report_code': code, 'fight_id': fid},
           'fight_meta': {'report_code': code, 'fight_id': fid, 'start_time': 0, 'end_time': 10}}
    return job, restored


def test_rejects_shared_or_mismatched_provider_archives():
    primary, secondary = Archive(), Archive()
    with pytest.raises(ValueError, match='separate'):
        HybridClient(Provider(primary), Provider(primary), primary, primary)
    with pytest.raises(ValueError, match='API'):
        HybridClient(Provider(secondary), Provider(secondary), primary, secondary)
    with pytest.raises(ValueError, match='browser'):
        HybridClient(Provider(primary), Provider(primary), primary, secondary)


@pytest.mark.asyncio
async def test_report_preparation_and_download_never_acquire_browser():
    client, api, browser = clients()
    browser.valid = False

    async def forbidden(*args):
        raise AssertionError('API preparation must not contact browser')

    browser.fetch = forbidden
    assert client.requires_browser_enrichment is True
    assert await asyncio.wait_for(client.unarchive_report('Report1234'), 1) is True
    assert await asyncio.wait_for(client.fetch_report_master_and_fights('Report1234'), 1) is api.master
    result = await asyncio.wait_for(client.fetch_all_fight_events('Report1234', 1, 0, 10), 1)
    assert result == [{'timestamp': 1, 'api': True}]
    assert api.raw_archive.fight_manifest('Report1234', 1)['hybrid_pending'] is True
    assert browser.validation_calls == browser.master_calls == browser.fetch_calls == []


@pytest.mark.asyncio
async def test_api_report_denial_is_explicit_and_does_not_contact_browser():
    client, api, browser = clients()
    api.valid = False
    with pytest.raises(RuntimeError, match='API'):
        await client.unarchive_report('Report1234')
    assert browser.validation_calls == browser.master_calls == []


@pytest.mark.asyncio
async def test_api_fetch_refetches_evicted_api_metadata_without_browser_work():
    client, api, browser = clients()
    await client.fetch_report_master_and_fights('Report1234')
    api.raw_archive.metadata.clear()
    assert await client.fetch_all_fight_events('Report1234', 1, 0, 10) == [
        {'timestamp': 1, 'api': True}]
    assert api.master_calls == ['Report1234', 'Report1234']
    assert browser.master_calls == browser.fetch_calls == []


@pytest.mark.asyncio
async def test_discovery_uses_browser_rankings():
    client, _, _ = clients()
    assert await client.fetch_rankings_fight_refs(9, 99, 1, difficulty=5) == [
        {'report': {'code': 'Report1234', 'fightID': 1}}]


@pytest.mark.asyncio
async def test_enrichment_uses_private_restored_archive_and_returns_browser_master():
    client, api, browser = clients()
    await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    job, restored = restored_fight(api)
    master = await client.enrich_staged_fight(job, restored)
    assert master is browser.master
    assert restored.full_fight_events('Report1234', 1) == [
        {'timestamp': 1, 'api': True, 'unknown': Decimal('0.12345678901234567890123456789')}]
    assert 'hybrid_pending' not in restored.fight_manifest('Report1234', 1)
    assert api.raw_archive.full_fight_events('Report1234', 1) == [{'timestamp': 1, 'api': True}]
    assert api.raw_archive.fight_manifest('Report1234', 1)['hybrid_pending'] is True
    assert api.raw_archive.discarded == []
    assert browser.raw_archive.fights == {}


@pytest.mark.asyncio
async def test_enrichment_refetches_browser_metadata_evicted_after_previous_fight():
    client, api, browser = clients()
    for fid in (1, 2):
        await client.fetch_all_fight_events('Report1234', fid, 0, 10)
        job, restored = restored_fight(api, fid=fid)
        await client.enrich_staged_fight(job, restored)
        browser.raw_archive.metadata.clear()
    assert browser.master_calls == ['Report1234', 'Report1234']
    assert api.master_calls == ['Report1234']


@pytest.mark.asyncio
async def test_api_second_fight_completes_while_browser_first_fight_is_blocked():
    client, api, browser = clients()
    await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    job, restored = restored_fight(api)
    started, release = asyncio.Event(), asyncio.Event()
    real_fetch = browser.fetch_all_fight_events

    async def blocked(code, fid, start, end):
        started.set()
        await release.wait()
        browser.fetch = None
        return await real_fetch(code, fid, start, end)

    browser.fetch = blocked
    enriching = asyncio.create_task(client.enrich_staged_fight(job, restored))
    try:
        await asyncio.wait_for(started.wait(), 1)
        second = await asyncio.wait_for(client.fetch_all_fight_events('Report1234', 2, 0, 10), 1)
        assert second == [{'timestamp': 1, 'api': True}]
        assert api.raw_archive.fight_manifest('Report1234', 2)['hybrid_pending'] is True
        assert not enriching.done()
    finally:
        release.set()
        await enriching


def block_merge(restored, *, fail=False):
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    real_merge = restored.merge_hybrid_fight

    def merge(*args):
        started.set()
        try:
            # Bound the old synchronous implementation so a red regression
            # reports failure rather than hanging the whole event loop.
            if not release.wait(1):
                raise AssertionError('Strict merge blocked the event loop')
            if fail:
                raise ValueError('Merge writer failed')
            return real_merge(*args)
        finally:
            finished.set()

    restored.merge_hybrid_fight = merge
    return started, release, finished


@pytest.mark.asyncio
async def test_api_download_advances_while_enrichment_merge_thread_is_blocked():
    client, api, _ = clients()
    await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    job, restored = restored_fight(api)
    started, release, finished = block_merge(restored)
    enriching = asyncio.create_task(client.enrich_staged_fight(job, restored))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        assert not enriching.done()
        second = await asyncio.wait_for(client.fetch_all_fight_events('Report1234', 2, 0, 10), .5)
        assert second == [{'timestamp': 1, 'api': True}]
        assert api.raw_archive.fight_manifest('Report1234', 2)['hybrid_pending'] is True
        assert not finished.is_set()
    finally:
        release.set()
        result, = await asyncio.gather(enriching, return_exceptions=True)
    assert not isinstance(result, BaseException)
    assert finished.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize('writer_fails', [False, True])
async def test_cancelled_enrichment_drains_merge_thread_before_discard(writer_fails):
    client, api, browser = clients()
    await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    job, restored = restored_fight(api)
    started, release, finished = block_merge(restored, fail=writer_fails)
    enriching = asyncio.create_task(client.enrich_staged_fight(job, restored))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        assert not enriching.done()
        enriching.cancel()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(enriching), .02)
        enriching.cancel()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(enriching), .02)
        assert not finished.is_set()
        assert restored.discarded == browser.raw_archive.discarded == []
        assert restored.full_fight_events('Report1234', 1) == [{'timestamp': 1, 'api': True}]
        assert browser.raw_archive.full_fight_events('Report1234', 1) == [
            {'timestamp': 1, 'unknown': Decimal('0.12345678901234567890123456789')}]
    finally:
        release.set()
        result, = await asyncio.gather(enriching, return_exceptions=True)
    assert isinstance(result, asyncio.CancelledError)
    assert finished.is_set()
    assert restored.fights == browser.raw_archive.fights == {}
    assert api.raw_archive.full_fight_events('Report1234', 1) == [{'timestamp': 1, 'api': True}]
    assert api.raw_archive.discarded == []


@pytest.mark.asyncio
async def test_api_failure_discards_only_primary_attempt_and_preserves_browser_fight():
    client, api, browser = clients()
    browser.raw_archive.fights[('Report1234', 1)] = [{'timestamp': 1, 'browser': True}]
    api.raw_archive.fights[('Other12345', 2)] = [{'timestamp': 2}]

    async def fail(*args):
        raise ValueError('API download failed')

    api.fetch = fail
    with pytest.raises(RuntimeError, match='API'):
        await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    assert api.raw_archive.fights == {('Other12345', 2): [{'timestamp': 2}]}
    assert browser.raw_archive.fights == {('Report1234', 1): [{'timestamp': 1, 'browser': True}]}
    assert browser.raw_archive.discarded == []


@pytest.mark.asyncio
async def test_browser_failure_discards_private_restore_but_preserves_global_api_and_siblings():
    client, api, browser = clients()
    await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    job, restored = restored_fight(api)
    browser.raw_archive.fights[('Other12345', 2)] = [{'timestamp': 2}]

    async def fail(*args):
        raise ValueError('browser download failed')

    browser.fetch = fail
    with pytest.raises(RuntimeError, match='browser'):
        await client.enrich_staged_fight(job, restored)
    assert restored.fights == {}
    assert restored.discarded == [('Report1234', 1)]
    assert api.raw_archive.full_fight_events('Report1234', 1) == [{'timestamp': 1, 'api': True}]
    assert api.raw_archive.discarded == []
    assert browser.raw_archive.fights == {('Other12345', 2): [{'timestamp': 2}]}


@pytest.mark.asyncio
async def test_merge_failure_releases_only_private_and_browser_capture():
    client, api, browser = clients()
    await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    job, restored = restored_fight(api)

    def reject_merge(*args):
        raise ValueError('Canonical event identity mismatch')

    restored.merge_hybrid_fight = reject_merge
    with pytest.raises(ValueError, match='identity mismatch'):
        await client.enrich_staged_fight(job, restored)
    assert restored.fights == browser.raw_archive.fights == {}
    assert api.raw_archive.full_fight_events('Report1234', 1) == [{'timestamp': 1, 'api': True}]
    assert api.raw_archive.discarded == []


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['api', 'browser'])
async def test_repeated_cancellation_waits_owned_phase_cleanup(phase):
    client, api, browser = clients()
    if phase == 'browser':
        await client.fetch_all_fight_events('Report1234', 1, 0, 10)
        job, restored = restored_fight(api)
    started, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    cleaned = []

    async def blocked(*args):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()
            cleaned.append(True)

    (api if phase == 'api' else browser).fetch = blocked
    operation = (client.fetch_all_fight_events('Report1234', 1, 0, 10) if phase == 'api'
                 else client.enrich_staged_fight(job, restored))
    task = asyncio.create_task(operation)
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()
    await asyncio.wait_for(cleaning.wait(), 1)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert cleaned == [True]
    if phase == 'browser':
        assert api.raw_archive.full_fight_events('Report1234', 1) == [{'timestamp': 1, 'api': True}]
        assert api.raw_archive.discarded == []
        assert restored.discarded == browser.raw_archive.discarded == [('Report1234', 1)]
    else:
        assert api.raw_archive.discarded == [('Report1234', 1)]
        assert browser.raw_archive.discarded == []


@pytest.mark.asyncio
async def test_enrichment_rejects_global_api_archive_without_discarding_it():
    client, api, browser = clients()
    await client.fetch_all_fight_events('Report1234', 1, 0, 10)
    job, _ = restored_fight(api)
    with pytest.raises(ValueError, match='isolated'):
        await client.enrich_staged_fight(job, api.raw_archive)
    assert api.raw_archive.discarded == browser.raw_archive.discarded == []
    assert browser.fetch_calls == []

@pytest.mark.asyncio
@pytest.mark.parametrize('retry_browser', [False, True])
async def test_real_providers_preserve_independent_complete_decimal_sources(tmp_path, retry_browser):
    class UserAuth:
        def get_headers_and_cookies(self):
            return {}, {}

        def get_auth_status(self):
            return {'has_user_oauth_token': True}

        async def get_api_auth(self):
            return 'https://www.warcraftlogs.com/api/v2/user', 'test-token'

    api_master = {'data': {'reportData': {'report': {
        'fights': [{'id': 1, 'startTime': 0, 'endTime': 10, 'encounterID': 99}],
        'masterData': {'actors': [], 'abilities': []}, 'apiFuture': {'unknown': True}}}}}
    api_events = b'{"data":{"reportData":{"report":{"events":{"data":[{"timestamp":1,"type":"damage","sourceID":1,"targetID":99,"abilityGameID":5,"amount":0.12345678901234567890123456789},{"timestamp":7,"type":"cast","sourceID":1,"abilityGameID":6}],"nextPageTimestamp":null,"apiUnknown":true}}}}}'
    browser_master = b'{"fights":[{"id":1,"start_time":0,"end_time":10,"boss":99}],"friendlies":[],"enemies":[],"abilities":[],"browserUnknown":{"precision":1.0000000000000000001}}'
    browser_first = '{"events":[{"timestamp":1,"type":"damage","sourceID":1,"targetID":99,"abilityGameID":5,"amount":123,"browserUnknown":{"precision":0.9999999999999999999}}],"nextPageTimestamp":null,"pageUnknown":true}'
    browser_last = '{"events":[{"timestamp":7,"type":"cast","sourceID":1,"abilityGameID":6,"browserUnknown":[null,9007199254740993]}],"nextPageTimestamp":null}'
    replay = '{"events":[{"timestamp":1,"type":"damage","sourceID":1,"targetID":99,"abilityGameID":5,"replayUnknown":{"x":3.1415926535897932384626}}],"replayEnvelope":{"future":true}}'
    captured_http = []
    captured_browser = []
    fail_browser_once = [retry_browser]

    def handler(request):
        assert request.url.path == '/api/v2/user'
        assert request.headers['Authorization'] == 'Bearer test-token'
        body = (json.dumps(api_master).encode() if 'masterData' in json.loads(request.content)['query']
                else api_events)
        captured_http.append(body)
        return httpx.Response(200, content=body)

    class FakePage:
        url = 'https://www.warcraftlogs.com/'

        def is_closed(self):
            return False

        async def evaluate(self, script, arg):
            url = arg['url']
            if '/fights-and-participants/' in url:
                text = browser_master.decode()
            elif '/summary-events/Report1234/1/0/5/' in url:
                if fail_browser_once[0]:
                    fail_browser_once[0] = False
                    return {'status': 500, 'text': 'browser unavailable', 'retryAfter': ''}
                text = browser_first
            elif '/summary-events/Report1234/1/6/10/' in url:
                text = browser_last
            elif '/replaysegment/Report1234/99/0/10/' in url:
                text = replay
            else:
                raise AssertionError(f'Unexpected browser request: {url}')
            captured_browser.append(text)
            return {'status': 200, 'text': text, 'retryAfter': ''}

    primary, secondary = FightArchive(tmp_path / 'api'), FightArchive(tmp_path / 'browser')
    api = WCLProtocolClient(UserAuth(), httpx.MockTransport(handler), primary)
    browser = BrowserClient(tmp_path / 'profile', min_delay=0, max_delay=0, raw_archive=secondary)
    browser._context, browser._page = object(), FakePage()
    client = HybridClient(api, browser, primary, secondary)
    try:
        assert await client.unarchive_report('Report1234') is True
        master = await client.fetch_report_master_and_fights('Report1234')
        events = await client.fetch_all_fight_events('Report1234', 1, 0, 10)
        assert events[0]['amount'] == Decimal('0.12345678901234567890123456789')
        assert 'browserUnknown' not in events[0]
        assert captured_browser == []
        assert primary.fight_manifest('Report1234', 1)['hybrid_pending'] is True
        store = StagingStore(tmp_path)
        row = {'fight_uid': 'Report1234_1', 'report_code': 'Report1234', 'fight_id': 1}
        meta = {'report_code': 'Report1234', 'fight_id': 1, 'start_time': 0, 'end_time': 10}
        pending = store.stage_fight(primary, row, meta, [], [], tmp_path / 'hybrid.wclf')
        original_hash = hashlib.sha256(pending.read_bytes()).hexdigest()
        assert primary.stats()['buffered_complete_fights'] == 0
        job, restored = store.load(pending)
        if retry_browser:
            with pytest.raises(RuntimeError, match='browser'):
                await client.enrich_staged_fight(job, restored)
            assert hashlib.sha256(pending.read_bytes()).hexdigest() == original_hash
            assert restored.stats()['buffered_complete_fights'] == 0
            assert secondary.stats()['buffered_complete_fights'] == 0
            assert len(captured_http) == 2
            # Retained browser master is reused; only failed-attempt pages are
            # released. Compare final responses against that retained source.
            captured_browser[:] = [browser_master.decode()]
            job, restored = store.load(pending)
        browser_metadata = await client.enrich_staged_fight(job, restored)
        assert browser_metadata['browserUnknown']['precision'] == Decimal('1.0000000000000000001')
        events = restored.full_fight_events('Report1234', 1)
        assert events == [
            {'timestamp': 1, 'type': 'damage', 'sourceID': 1, 'targetID': 99,
             'abilityGameID': 5, 'amount': Decimal('0.12345678901234567890123456789'),
             'browserUnknown': {'precision': Decimal('0.9999999999999999999')},
             'replayUnknown': {'x': Decimal('3.1415926535897932384626')}},
            {'timestamp': 7, 'type': 'cast', 'sourceID': 1, 'abilityGameID': 6,
             'browserUnknown': [None, 9007199254740993]}]
        assert secondary.stats()['buffered_complete_fights'] == 0
        output = tmp_path / 'hybrid.wclf'
        restored.write_fight(output, {}, 'Report1234', 1)
        assert PackedFightReader.read_full_events(output) == events
        raw = PackedFightReader.read_raw_events(output)
        assert 'browserUnknown' not in raw[0]
        assert raw[0]['amount'] == Decimal('0.12345678901234567890123456789')
        manifest = PackedFightReader.source_manifest(output)
        reconstructed = [json.loads(PackedFightReader.read_source_response(output, record['id']),
                                    parse_float=Decimal) for record in manifest['captures']]
        original = [json.loads(body, parse_float=Decimal)
                    for body in [*captured_http, *captured_browser]]
        assert len(reconstructed) == len(original) == 6
        assert all(body in reconstructed for body in original)
        assert primary.stats()['buffered_complete_fights'] == 0
        assert hashlib.sha256(pending.read_bytes()).hexdigest() == original_hash
    finally:
        await browser.stop()
        if browser._bg_loop is not None:
            browser._bg_loop.call_soon_threadsafe(browser._bg_loop.stop)
            await asyncio.to_thread(browser._bg_thread.join, 2)

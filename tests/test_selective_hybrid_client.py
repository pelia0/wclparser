import asyncio
import json
import threading
from decimal import Decimal

import pytest

from wcl_archiver.codec.fight_archive import FightArchive
from wcl_archiver.wcl_client.browser_client import BrowserClient


CODE = 'Report1234'


class SelectivePage:
    url = 'https://www.warcraftlogs.com/'

    def __init__(self, end, replay, canonical=None, boss=99):
        self.end, self.replay = end, replay
        self.canonical = canonical or {}
        self.boss = boss
        self.calls, self.originals = [], []
        self.replay_status = 200

    def is_closed(self):
        return False

    async def evaluate(self, script, arg):
        path = arg['url']
        self.calls.append(path)
        if '/fights-and-participants/' in path:
            text = json.dumps({'fights': [{'id': 1, 'start_time': 0,
                                          'end_time': self.end, 'boss': self.boss}]})
        elif '/replaysegment/' in path:
            if self.replay_status != 200:
                return {'status': self.replay_status, 'text': '{}', 'retryAfter': ''}
            left, right = map(int, path.rstrip('/').split('/')[-2:])
            records = [row for row in self.replay if left <= row['timestamp'] <= right]
            text = json.dumps({'events': records, 'replayEnvelope': {'future': True}})
        elif '/summary-events/' in path:
            left, right = map(int, path.split('/')[5:7])
            assert left == right, 'Selective acquisition requested a full canonical range'
            assert left in self.canonical, f'Unnecessary canonical request at {left}'
            text = self.canonical[left]
        else:
            raise AssertionError(f'Unexpected browser request: {path}')
        self.originals.append(text)
        return {'status': 200, 'text': text, 'retryAfter': ''}


async def prepared_browser(tmp_path, page):
    archive = FightArchive(tmp_path / 'sources')
    browser = BrowserClient(tmp_path / 'profile', min_delay=0, max_delay=0, raw_archive=archive)
    browser._bg_loop = asyncio.get_running_loop()
    browser._context, browser._page = object(), page
    await browser.fetch_report_master_and_fights(CODE)
    return browser, archive


@pytest.mark.asyncio
@pytest.mark.parametrize('end, segments', [(10, 1), (1_439_999, 6)])
async def test_replay_covered_fights_fetch_only_replay_without_canonical_pages(tmp_path, end, segments):
    api = [{'timestamp': 1, 'type': 'damage', 'sourceID': 1, 'amount': 7}]
    replay = [{**api[0], 'browserUnknown': {'future': [None, 9007199254740993]}}]
    page = SelectivePage(end, replay)
    browser, archive = await prepared_browser(tmp_path, page)
    await browser.fetch_fight_supplement(CODE, 1, 0, end, api)
    assert len(page.calls) == 1 + segments
    assert sum('/replaysegment/' in path for path in page.calls) == segments
    assert not any('/summary-events/' in path for path in page.calls)
    manifest = archive.fight_manifest(CODE, 1)
    assert manifest['source_mode'] == 'browser_supplement_v1'
    assert manifest['canonical_windows'] == []


@pytest.mark.asyncio
async def test_death_and_unmatched_occurrence_fetch_only_sorted_timestamp_windows(tmp_path):
    damage = {'timestamp': 2, 'type': 'damage', 'sourceID': 1}
    death = {'timestamp': 7, 'type': 'death', 'targetID': 2}
    cast = {'timestamp': 4, 'type': 'cast', 'sourceID': 1}
    # Replay contains only one of the two API damage occurrences.
    api = [damage, damage.copy(), cast, death]
    page = SelectivePage(10, [damage, death], {
        2: json.dumps({'events': [damage, damage.copy()]}),
        4: json.dumps({'events': [cast]}),
        7: '{"events":[{"timestamp":7,"type":"death","targetID":2,"killerID":99,"unknown":{"precision":0.12345678901234567890123456789}}],"future":1.00000000000000000001}',
    })
    browser, archive = await prepared_browser(tmp_path, page)
    original_api = json.dumps(api)
    await browser.fetch_fight_supplement(CODE, 1, 0, 10, api)
    assert json.dumps(api) == original_api
    assert '/replaysegment/' in page.calls[1]
    windows = [tuple(map(int, path.split('/')[5:7])) for path in page.calls
               if '/summary-events/' in path]
    assert sorted(windows) == [(2, 2), (4, 4), (7, 7)]
    assert archive.fight_manifest(CODE, 1)['canonical_windows'] == [[2, 2], [4, 4], [7, 7]]
    captures = archive._completed[(CODE, 1)]
    death_body = next(cap['body'] for cap in captures['pages']
                      if cap['body']['events'][0]['timestamp'] == 7)
    assert death_body['events'][0]['unknown']['precision'] == Decimal('0.12345678901234567890123456789')
    assert death_body['future'] == Decimal('1.00000000000000000001')


@pytest.mark.asyncio
@pytest.mark.parametrize('boss, replay_status', [(0, 200), (99, 500)])
async def test_invalid_boss_or_replay_fails_without_full_canonical_fallback(tmp_path, boss, replay_status):
    api = [{'timestamp': 1, 'type': 'damage'}]
    page = SelectivePage(10, api, boss=boss)
    page.replay_status = replay_status
    browser, archive = await prepared_browser(tmp_path, page)
    with pytest.raises((ValueError, RuntimeError)):
        await browser.fetch_fight_supplement(CODE, 1, 0, 10, api)
    assert not any('/summary-events/' in path for path in page.calls)
    assert archive.stats()['buffered_event_captures'] == 0
    assert archive.stats()['buffered_complete_fights'] == 0


@pytest.mark.asyncio
async def test_missing_targeted_occurrence_rejects_capture_without_broader_retry(tmp_path):
    api = [{'timestamp': 4, 'type': 'cast', 'sourceID': 1}]
    page = SelectivePage(10, [], {4: '{"events":[]}'})
    browser, archive = await prepared_browser(tmp_path, page)
    with pytest.raises(ValueError):
        await browser.fetch_fight_supplement(CODE, 1, 0, 10, api)
    assert len(page.calls) == 3
    assert archive.stats()['buffered_event_captures'] == 0
    assert archive.stats()['buffered_complete_fights'] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['replay', 'targeted'])
async def test_failed_sibling_drains_capture_writers_before_attempt_release(tmp_path, phase):
    end = 240_001 if phase == 'replay' else 10
    api = [{'timestamp': 2, 'type': 'cast'}, {'timestamp': 4, 'type': 'cast'}]
    started, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    finished = []

    class BlockingPage(SelectivePage):
        async def evaluate(self, script, arg):
            path = arg['url']
            relevant = ('/replaysegment/' if phase == 'replay' else '/summary-events/') in path
            if not relevant:
                return await super().evaluate(script, arg)
            left = int(path.rstrip('/').split('/')[-2]) if phase == 'replay' else int(path.split('/')[5])
            if left == (0 if phase == 'replay' else 2):
                await started.wait()
                return {'status': 500, 'text': '{}', 'retryAfter': ''}
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release.wait()
                finished.append(True)

    browser, archive = await prepared_browser(tmp_path, BlockingPage(end, []))
    task = asyncio.create_task(browser.fetch_fight_supplement(CODE, 1, 0, end, api))
    try:
        await asyncio.wait_for(cleaning.wait(), 1)
        assert not task.done()
        assert len(archive._attempts) == 1
    finally:
        release.set()
        result, = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(result, RuntimeError)
    assert finished == [True]
    assert archive._attempts == {}
    assert archive.stats()['buffered_event_captures'] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['replay', 'targeted'])
async def test_repeated_cancellation_drains_owned_browser_supplement_before_release(tmp_path, phase):
    started, cleaning, release = threading.Event(), threading.Event(), threading.Event()
    finished = []

    class BlockingPage(SelectivePage):
        async def evaluate(self, script, arg):
            path = arg['url']
            relevant = ('/replaysegment/' if phase == 'replay' else '/summary-events/') in path
            if not relevant:
                return await super().evaluate(script, arg)
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await asyncio.to_thread(release.wait)
                finished.append(True)

    browser, archive = await prepared_browser(tmp_path, BlockingPage(10, []))
    # Run the public operation through the real separate browser loop, which
    # owns remote cancellation and capture teardown under repeated cancellation.
    browser._bg_loop = None
    api = [{'timestamp': 2, 'type': 'cast'}]
    task = asyncio.create_task(browser.fetch_fight_supplement(CODE, 1, 0, 10, api))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        assert await asyncio.to_thread(cleaning.wait, 2)
        task.cancel()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), .02)
        assert finished == []
        assert len(archive._attempts) == 1
    finally:
        release.set()
        result, = await asyncio.gather(task, return_exceptions=True)
        if browser._bg_loop is not None:
            loop = browser._bg_loop
            loop.call_soon_threadsafe(loop.stop)
            await asyncio.to_thread(browser._bg_thread.join, 2)
            loop.close()
    assert isinstance(result, asyncio.CancelledError)
    assert finished == [True]
    assert archive._attempts == {}
    assert archive.stats()['buffered_event_captures'] == 0


def block_codec_phase(monkeypatch, archive, phase, *, fail=False):
    from wcl_archiver.codec import fight_archive

    owner = fight_archive if phase == 'planner' else archive
    name = {'replay': 'supplement_replay_events', 'planner': 'supplement_windows',
            'completion': 'complete_supplement'}[phase]
    original = getattr(owner, name)
    started, release, finished = threading.Event(), threading.Event(), threading.Event()

    def blocked(*args):
        started.set()
        try:
            if not release.wait(1):
                raise AssertionError('Browser codec phase blocked the shared event loop')
            if fail:
                raise ValueError('Supplement writer failed')
            return original(*args)
        finally:
            finished.set()

    monkeypatch.setattr(owner, name, blocked)
    return started, release, finished


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['replay', 'planner', 'completion'])
async def test_browser_network_advances_while_supplement_codec_thread_is_blocked(tmp_path, monkeypatch, phase):
    api = [{'timestamp': 2, 'type': 'cast'}]
    page = SelectivePage(10, api)
    browser, archive = await prepared_browser(tmp_path, page)
    started, release, finished = block_codec_phase(monkeypatch, archive, phase)
    task = asyncio.create_task(browser.fetch_fight_supplement(CODE, 1, 0, 10, api))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        assert not task.done()
        previous = len(page.calls)
        status, _ = await asyncio.wait_for(browser._request_json_impl(
            'GET', f'/reports/fights-and-participants/{CODE}/0?lang=en&'), .2)
        assert status == 200
        assert len(page.calls) == previous + 1
        assert not finished.is_set()
    finally:
        release.set()
        result, = await asyncio.gather(task, return_exceptions=True)
    assert not isinstance(result, BaseException)
    assert archive.fight_manifest(CODE, 1)['complete'] is True


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['replay', 'planner', 'completion'])
@pytest.mark.parametrize('writer_fails', [False, True])
async def test_repeated_cancel_drains_supplement_codec_thread_before_capture_cleanup(
        tmp_path, monkeypatch, phase, writer_fails):
    api = [{'timestamp': 2, 'type': 'cast'}]
    browser, archive = await prepared_browser(tmp_path, SelectivePage(10, api))
    started, release, finished = block_codec_phase(monkeypatch, archive, phase, fail=writer_fails)
    task = asyncio.create_task(browser.fetch_fight_supplement(CODE, 1, 0, 10, api))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        assert not task.done()
        task.cancel()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), .02)
        task.cancel()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), .02)
        assert not finished.is_set()
        assert len(archive._attempts) == 1
        assert archive.stats()['buffered_event_captures'] == 1
    finally:
        release.set()
        result, = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(result, asyncio.CancelledError)
    assert finished.is_set()
    assert archive._attempts == {}
    assert archive.stats()['buffered_event_captures'] == 0
    assert archive.stats()['buffered_complete_fights'] == 0

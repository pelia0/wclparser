import asyncio
import json

import pytest

from wcl_archiver.codec.fight_archive import FightArchive
from wcl_archiver.wcl_client.browser_client import BrowserClient


class MetadataPage:
    url = 'https://www.warcraftlogs.com/'

    def __init__(self, status):
        self.status = status
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.finished = asyncio.Event()
        self.calls = 0

    def is_closed(self):
        return False

    async def evaluate(self, script, arg):
        assert arg['url'] == '/reports/fights-and-participants/Report1234/0?lang=en&'
        self.calls += 1
        self.started.set()
        try:
            await self.release.wait()
            return {'status': self.status, 'retryAfter': '', 'text': json.dumps({
                'fights': [{'id': 1, 'boss': 99, 'start_time': 0, 'end_time': 10}],
                'friendlies': [], 'enemies': [], 'abilities': [], 'unknown': {'future': True}})}
        finally:
            self.finished.set()


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel_creator', [False, True])
@pytest.mark.parametrize('status', [200, 500])
async def test_cancelled_shared_master_waiter_drains_without_cancelling_other_waiter(
        tmp_path, cancel_creator, status):
    archive = FightArchive(tmp_path)
    page = MetadataPage(status)
    browser = BrowserClient(tmp_path / 'profile', min_delay=0, max_delay=0, raw_archive=archive)
    browser._context, browser._page = object(), page
    # Exercise the browser-loop implementation on this test's loop so barriers
    # precisely control network completion while using real capture/cache code.
    creator = asyncio.create_task(browser._fetch_report_master_and_fights_impl('Report1234'))
    await asyncio.wait_for(page.started.wait(), 1)
    follower = asyncio.create_task(browser._fetch_report_master_and_fights_impl('Report1234'))
    await asyncio.sleep(0)
    cancelled, survivor = (creator, follower) if cancel_creator else (follower, creator)
    shared = browser._master_inflight['Report1234']
    try:
        cancelled.cancel()
        # A cancelled waiter cannot return while shared acquisition may still
        # mutate source captures. Its peer must retain the same active task.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(cancelled), .02)
        assert browser._master_inflight['Report1234'] is shared
        assert not shared.done()
        assert not survivor.done()
        assert not page.finished.is_set()
        cancelled.cancel()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(cancelled), .02)
        assert not shared.done()
        assert page.calls == 1
    finally:
        page.release.set()
        results = await asyncio.gather(cancelled, survivor, return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError)
    assert page.finished.is_set()
    assert browser._master_inflight == {}
    if status == 200:
        assert results[1]['unknown'] == {'future': True}
        assert archive.has_report_metadata('Report1234')
        assert await browser._fetch_report_master_and_fights_impl('Report1234') is results[1]
        assert page.calls == 1
    else:
        assert isinstance(results[1], RuntimeError)
        assert 'HTTP 500' in str(results[1])
        assert not archive.has_report_metadata('Report1234')
        # Failure must release the failed shared task so a later retry works.
        page.status = 200
        retried = await browser._fetch_report_master_and_fights_impl('Report1234')
        assert retried['unknown'] == {'future': True}
        assert archive.has_report_metadata('Report1234')
        assert page.calls == 2

import asyncio

import pytest

from wcl_archiver.wcl_client.browser_client import BrowserClient


async def test_live_resize_preserves_active_slots_and_waiters(tmp_path):
    client = BrowserClient(tmp_path, max_concurrent_requests=2)
    # Exercise the actual browser-loop bridge without launching a browser.
    async def scenario():
        limiter, nav, cond = client._ensure_bg_sync()
        await limiter.acquire()
        await limiter.acquire()
        client._cooldown_until = 12345
        cache = client._report_master_cache
        waiter = asyncio.create_task(limiter.acquire())
        await asyncio.sleep(0)
        await client.set_request_concurrency(1)
        assert client._req_sem is limiter
        assert client._nav_lock is nav and client._eval_cond is cond
        assert client._cooldown_until == 12345 and client._report_master_cache is cache
        limiter.release()
        await asyncio.sleep(0)
        assert not waiter.done()
        limiter.release()
        await asyncio.wait_for(waiter, 1)
        waiting = asyncio.create_task(limiter.acquire())
        await asyncio.sleep(0)
        await client.set_request_concurrency(2)
        await asyncio.wait_for(waiting, 1)
        limiter.release()
        limiter.release()
        # Cancelling blocked acquisition must not consume capacity.
        await client.set_request_concurrency(1)
        await limiter.acquire()
        cancelled = asyncio.create_task(limiter.acquire())
        await asyncio.sleep(0)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        limiter.release()
        await asyncio.wait_for(limiter.acquire(), 1)
        limiter.release()
    await client._run_in_browser_loop(scenario())


@pytest.mark.parametrize('invalid', [0, -1, True, 1.5, '2', None])
async def test_resize_rejects_invalid_before_mutation(tmp_path, invalid):
    client = BrowserClient(tmp_path, max_concurrent_requests=4)
    with pytest.raises((TypeError, ValueError)):
        await client.set_request_concurrency(invalid)
    assert client.max_concurrent_requests == 4
    assert client._bg_loop is None

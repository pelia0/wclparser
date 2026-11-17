import asyncio
import threading

import pytest

from wcl_archiver.api.status_cache import StatusSnapshotCache


async def test_first_requests_coalesce_and_cancel_does_not_cancel_collector():
    entered, release = threading.Event(), threading.Event()
    calls = []
    def collect():
        calls.append(1)
        entered.set()
        release.wait(3)
        return {'packed_fights': 9}
    cache = StatusSnapshotCache(collect)
    tasks = [asyncio.create_task(cache.get()) for _ in range(8)]
    try:
        await asyncio.to_thread(entered.wait, 1)
        tasks[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await tasks[0]
        assert len(calls) == 1
        await asyncio.wait_for(asyncio.sleep(0), .1)
        release.set()
        results = await asyncio.gather(*tasks[1:])
        assert all(r['packed_fights'] == 9 for r in results)
        results[0]['packed_fights'] = 100
        assert (await cache.get())['packed_fights'] == 9
    finally:
        release.set()
        await cache.close()


async def test_expired_snapshot_is_immediate_and_error_keeps_good_values():
    release = threading.Event()
    calls = []
    def collect():
        calls.append(1)
        if len(calls) > 1:
            release.wait(3)
            raise ValueError('catalogue unavailable')
        return {'packed_fights': 7}
    cache = StatusSnapshotCache(collect, ttl=0)
    try:
        assert (await cache.get())['packed_fights'] == 7
        stale = await asyncio.wait_for(cache.get(), .1)
        assert stale['packed_fights'] == 7
        assert stale['status_cache_refreshing']
        release.set()
        await cache.close()
        result = await cache.get()
        assert result['packed_fights'] == 7
        assert 'catalogue unavailable' in result['status_cache_error']
    finally:
        release.set()
        await cache.close()


async def test_invalidation_during_refresh_requires_new_collection_and_close_owns_thread():
    entered, release = threading.Event(), threading.Event()
    calls = []
    def collect():
        calls.append(1)
        if len(calls) == 2:
            entered.set()
            release.wait(3)
        return {'packed_fights': len(calls)}
    cache = StatusSnapshotCache(collect, ttl=0)
    try:
        await cache.get()
        await cache.get()
        await asyncio.to_thread(entered.wait, 1)
        cache.invalidate()
        fresh = asyncio.create_task(cache.get())
        await asyncio.sleep(.01)
        assert not fresh.done()
        release.set()
        assert (await fresh)['packed_fights'] == 3
        await cache.get()
        close = asyncio.create_task(cache.close())
        close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close
        await cache.close()
    finally:
        release.set()
        await cache.close()


async def test_first_failure_is_explicit():
    def collect():
        raise OSError('disk unavailable')
    cache = StatusSnapshotCache(collect)
    try:
        with pytest.raises(RuntimeError, match='disk unavailable'):
            await cache.get()
    finally:
        await cache.close()

async def test_close_survives_repeated_cancellation_until_collector_finishes():
    entered, release = threading.Event(), threading.Event()
    def collect():
        entered.set()
        release.wait(3)
        return {'packed_fights': 1}
    cache = StatusSnapshotCache(collect)
    request = asyncio.create_task(cache.get())
    await asyncio.to_thread(entered.wait, 1)
    closing = asyncio.create_task(cache.close())
    try:
        await asyncio.sleep(.01)
        closing.cancel()
        await asyncio.sleep(.01)
        closing.cancel()
        await asyncio.sleep(.01)
        assert not closing.done()
        assert cache._future is not None
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert cache._future is None and cache._executor is None
        assert (await request)['packed_fights'] == 1
    finally:
        release.set()
        await cache.close()
        await asyncio.gather(request, return_exceptions=True)

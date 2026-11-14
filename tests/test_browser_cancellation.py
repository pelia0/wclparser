import asyncio
from contextlib import contextmanager
import threading

import pytest

from wcl_archiver.wcl_client.browser_client import BrowserClient


@contextmanager
def background_client(tmp_path):
    client = BrowserClient(tmp_path / "browser")
    loop = asyncio.new_event_loop()
    ready = threading.Event()
    def run():
        asyncio.set_event_loop(loop)
        loop.call_soon(ready.set)
        loop.run_forever()
    thread = threading.Thread(target=run, daemon=True)
    client._bg_loop, client._bg_thread = loop, thread
    thread.start()
    assert ready.wait(2)
    try:
        yield client, loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(2)
        assert not thread.is_alive()
        loop.close()


async def wait_for_thread_event(event):
    assert await asyncio.to_thread(event.wait, 2), "background coroutine did not reach expected state"


async def test_browser_cancellation_waits_for_remote_finally_and_child_cleanup(tmp_path):
    with background_client(tmp_path) as (client, loop):
        entered, cleaning, finished = threading.Event(), threading.Event(), threading.Event()
        child_cleaning, child_finished = threading.Event(), threading.Event()
        release = asyncio.Event()
        active = []
        async def child():
            active.append("child")
            try:
                await asyncio.Event().wait()
            finally:
                child_cleaning.set()
                await release.wait()
                active.remove("child")
                child_finished.set()
        async def remote():
            task = asyncio.create_task(child())
            await asyncio.sleep(0)
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                finished.set()
        caller = asyncio.create_task(client._run_in_browser_loop(remote()))
        try:
            await wait_for_thread_event(entered)
            caller.cancel()
            await wait_for_thread_event(cleaning)
            await wait_for_thread_event(child_cleaning)
            await asyncio.sleep(0)
            assert not caller.done(), "caller returned before remote finally completed"
            caller.cancel()
            await asyncio.sleep(0)
            assert not caller.done(), "repeated cancellation interrupted the cleanup handshake"
        finally:
            loop.call_soon_threadsafe(release.set)
            await wait_for_thread_event(finished)
            with pytest.raises(asyncio.CancelledError):
                await caller
        assert child_finished.is_set()
        assert active == []


async def test_browser_bridge_preserves_results_and_remote_errors(tmp_path):
    with background_client(tmp_path) as (client, loop):
        async def remote_result():
            assert asyncio.get_running_loop() is loop
            return {"events": [1, 2]}
        async def remote_error():
            raise ValueError("remote failure")
        assert await client._run_in_browser_loop(remote_result()) == {"events": [1, 2]}
        with pytest.raises(ValueError, match="remote failure"):
            await client._run_in_browser_loop(remote_error())


async def test_browser_bridge_same_loop_awaits_directly(tmp_path):
    client = BrowserClient(tmp_path / "browser")
    client._bg_loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    async def remote():
        assert asyncio.current_task() is task
        return "direct"
    assert await client._run_in_browser_loop(remote()) == "direct"

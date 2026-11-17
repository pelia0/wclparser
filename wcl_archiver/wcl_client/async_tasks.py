"""Async ownership boundaries for work that continues outside its caller task."""

import asyncio


async def run_owned_thread(function, *args):
    """Finish a source-mutating thread before cancellation releases its buffers."""
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Cancelling a thread Future cannot stop its writer. Keep its Task alive
        # and drain it even under repeated cancellation before cleanup may run.
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        if not task.cancelled():
            task.exception()
        raise

"""Owned single-flight catalogue snapshots, independent of the default I/O pool."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import time


class StatusSnapshotCache:
    def __init__(self, collector, ttl=5.0):
        self.collector = collector
        self.ttl = ttl
        self._executor = None
        self._future = None
        self._snapshot = None
        self._updated = None
        self._generation = 0
        self._collected_generation = -1
        self._future_generation = -1
        self._error = ''
        self._closed = False

    def invalidate(self):
        self._generation += 1

    def _consume(self):
        if self._future is None or not self._future.done():
            return
        future, generation = self._future, self._future_generation
        self._future = None
        try:
            snapshot = future.result()
        except Exception as exc:
            self._error = f'{type(exc).__name__}: {exc}'
        else:
            # Never let a scan started before terminal invalidation certify it.
            if generation == self._generation:
                self._snapshot = deepcopy(snapshot)
                self._updated = time.monotonic()
                self._collected_generation = generation
                self._error = ''

    def _start(self):
        if self._closed or self._future is not None:
            return
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='wcl-status')
        self._future_generation = self._generation
        self._future = self._executor.submit(self.collector)

    async def get(self):
        self._consume()
        needs_fresh = self._snapshot is None or self._collected_generation != self._generation
        if not self._closed and (needs_fresh or time.monotonic() - self._updated >= self.ttl):
            self._start()
        while needs_fresh and self._future is not None:
            generation = self._future_generation
            try:
                await asyncio.shield(asyncio.wrap_future(self._future))
            except asyncio.CancelledError:
                raise
            except Exception:
                pass  # _consume records the error, retaining the last good snapshot.
            self._consume()
            if generation != self._generation:
                self._start()
                continue
            break
        if self._snapshot is None:
            raise RuntimeError(self._error or 'Status counters unavailable')
        return {**deepcopy(self._snapshot),
                'status_cache_age_seconds': round(time.monotonic() - self._updated, 3),
                'status_cache_refreshing': self._future is not None,
                'status_cache_error': self._error,
                'status_cache_stale': self._collected_generation != self._generation or
                    time.monotonic() - self._updated >= self.ttl}

    async def close(self):
        self._closed = True
        cancelled = False
        if self._future is not None:
            waiting = asyncio.wrap_future(self._future)
            while not waiting.done():
                try:
                    await asyncio.shield(waiting)
                except asyncio.CancelledError:
                    cancelled = True
                except Exception:
                    break
            self._consume()
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
        if cancelled:
            raise asyncio.CancelledError

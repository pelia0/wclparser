"""Independent API acquisition with validated selective browser enrichment."""

import asyncio
from typing import Any, Dict, List, Optional

from wcl_archiver.wcl_client.async_tasks import run_owned_thread as _run_owned_thread


async def _gather_owned(*coroutines):
    """Cancel siblings on failure and finish their cleanup before releasing buffers."""
    tasks = [asyncio.create_task(coroutine) for coroutine in coroutines]
    joined = asyncio.gather(*tasks)
    try:
        # Own cancellation here rather than letting repeated caller cancellation
        # interrupt a provider's asynchronous finally blocks.
        return await asyncio.shield(joined)
    finally:
        interrupted = False
        pending = [task for task in tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            drained = asyncio.gather(*tasks, return_exceptions=True)
            while not drained.done():
                try:
                    await asyncio.shield(drained)
                except asyncio.CancelledError:
                    interrupted = True
        if joined.done() and not joined.cancelled():
            joined.exception()  # Retrieve failure even when caller was cancelled.
        if interrupted:
            raise asyncio.CancelledError()


class HybridClient:
    """Acquire API sources independently, then enrich their durable snapshots.

    Browser replay and selected canonical timestamps supply extra fields.
    Each provider owns its capture scope and bounded master cache;
    this adapter neither wraps their scopes nor duplicates their metadata cache.
    """

    requires_browser_enrichment = True

    def __init__(self, api_client, browser_client, raw_archive, browser_archive):
        if raw_archive is None or browser_archive is None or raw_archive is browser_archive:
            raise ValueError('Hybrid providers require separate source archives')
        if api_client.raw_archive is not raw_archive:
            raise ValueError('Hybrid API client must be bound to the primary source archive')
        if browser_client.raw_archive is not browser_archive:
            raise ValueError('Hybrid browser client must be bound to its separate source archive')
        self.api_client = api_client
        self.browser_client = browser_client
        self.raw_archive = raw_archive
        self.browser_archive = browser_archive

    @staticmethod
    async def _source_call(source, provider, method, *args, **kwargs):
        try:
            return await getattr(provider, method)(*args, **kwargs)
        except Exception as exc:
            detail = str(exc).strip() or type(exc).__name__
            raise RuntimeError(f'Hybrid {source} {method} failed: {detail}') from exc

    async def unarchive_report(self, report_code: str) -> bool:
        valid, = await _gather_owned(
            self._source_call('API', self.api_client, 'unarchive_report', report_code))
        if not valid:
            raise RuntimeError(f'Hybrid API report validation failed for {report_code}')
        return True

    async def _fetch_master(self, source, provider, archive, report_code):
        master = await self._source_call(
            source, provider, 'fetch_report_master_and_fights', report_code)
        if not archive.has_report_metadata(report_code):
            raise RuntimeError(f'Hybrid {source} report metadata source was not retained for {report_code}')
        return master

    async def fetch_report_master_and_fights(self, report_code: str) -> Dict[str, Any]:
        api_master, = await _gather_owned(
            self._fetch_master('API', self.api_client, self.raw_archive, report_code))
        return api_master

    async def fetch_all_fight_events(
        self, report_code: str, fight_id: int, start_time: int, end_time: int,
    ) -> List[Dict[str, Any]]:
        try:
            # API slots never wait on browser acquisition. The pipeline durably
            # stages this capture before scheduling independent enrichment.
            if not self.raw_archive.has_report_metadata(report_code):
                await self.fetch_report_master_and_fights(report_code)
            events, = await _gather_owned(
                self._source_call('API', self.api_client, 'fetch_all_fight_events',
                                  report_code, fight_id, start_time, end_time))
            self.raw_archive._completed[(report_code, int(fight_id))]['manifest']['hybrid_pending'] = True
            return events
        except BaseException:
            self.raw_archive.discard_fight(report_code, fight_id)
            raise

    async def download_supplement_phase(self, work, phase):
        await self._source_call('browser', self.browser_client, 'download_supplement_phase', work, phase)

    async def enrich_staged_fight(self, job, restored_api_archive) -> Dict[str, Any]:
        """Enrich a private restored capture, preserving its durable API original."""
        if restored_api_archive is self.raw_archive or restored_api_archive is self.browser_archive:
            raise ValueError('Browser enrichment requires an isolated restored API archive')
        row, meta = job['fight_row'], job['fight_meta']
        report_code, fight_id = row['report_code'], int(row['fight_id'])
        start, end = int(meta['start_time']), int(meta['end_time'])
        manifest = restored_api_archive.fight_manifest(report_code, fight_id)
        if (meta['report_code'] != report_code or int(meta['fight_id']) != fight_id
                or manifest['report_code'] != report_code or manifest['fight_id'] != fight_id
                or manifest['start_time'] != start or manifest['end_time'] != end
                or manifest.get('hybrid_pending') is not True):
            raise ValueError('Browser enrichment job does not match its pending API capture')
        try:
            # The provider verifies its bounded metadata cache still has its
            # source response. This call also returns full packing metadata.
            browser_master, = await _gather_owned(self._fetch_master(
                'browser', self.browser_client, self.browser_archive, report_code))
            await _gather_owned(self._source_call(
                'browser', self.browser_client, 'fetch_fight_supplement',
                report_code, fight_id, start, end, job['raw_events']))
            await _run_owned_thread(
                restored_api_archive.merge_hybrid_fight, self.browser_archive, report_code, fight_id)
            return browser_master
        except BaseException:
            # The API snapshot on disk and global API acquisition buffers are
            # owned by other phases. Release only this restored/browser fight.
            restored_api_archive.discard_fight(report_code, fight_id)
            self.browser_archive.discard_fight(report_code, fight_id)
            raise

    async def fetch_rankings_fight_refs(
        self, zone_id: int, encounter_id: int, partition: int,
        class_name: str = 'Any', spec_name: str = 'Any', page: int = 1,
        difficulty: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        return await self._source_call(
            'browser', self.browser_client, 'fetch_rankings_fight_refs',
            zone_id=zone_id, encounter_id=encounter_id, partition=partition,
            class_name=class_name, spec_name=spec_name, page=page, difficulty=difficulty)

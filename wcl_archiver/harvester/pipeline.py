import asyncio
import json
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from wcl_archiver.codec.wclz_format import WCLZWriter
from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader
from wcl_archiver.codec.staging import StagingStore
from wcl_archiver.config import (
    LEGION_MPLUS_S5_DUNGEONS,
    LEGION_MPLUS_S5_PARTITION,
    LEGION_MPLUS_ZONE_ID,
    LEGION_RAID_DIFFICULTIES,
    LEGION_RAIDS,
    LEGION_SPECS,
)
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.parser.combat_state_machine import CombatStateMachine
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient
from wcl_archiver.wcl_client.async_tasks import run_owned_thread


def _analyze_and_pack_cpu(
    out_path: Path,
    fight_meta: Dict[str, Any],
    actors: List[Dict[str, Any]],
    abilities: List[Dict[str, Any]],
    raw_events: List[Dict[str, Any]],
    zstd_level: int,
    raw_archive=None,
) -> Tuple[int, int, Dict[str, Any]]:
    standalone = isinstance(raw_archive,FightArchive)
    try:
        summary, norm_events, replay = CombatStateMachine.analyze_fight(
            fight_meta, actors, abilities, raw_events,
            include_normalized_events=not standalone,
            include_replay_frames=not standalone,
        )
    except Exception as exc:
        if not isinstance(raw_archive,FightArchive):
            raise
        # Historical records remain packable even when an unfamiliar field or
        # event prevents this version of the derived analyzer from understanding it.
        summary = {**fight_meta,'duration_ms':max(1,int(fight_meta['end_time'])-int(fight_meta['start_time'])),
                   'actors':actors,'abilities':{},'analysis_error':str(exc)}
        norm_events = raw_events
        replay = {'map_id':fight_meta.get('map_id',0),'pulls':fight_meta.get('pulls',[])}
    if raw_archive is not None:
        summary['map_id'] = replay.get('map_id',0)
        summary['pulls'] = replay.get('pulls',[])
        comp_bytes = raw_archive.write_fight(out_path,summary,fight_meta['report_code'],fight_meta['fight_id'])
    else:
        comp_bytes = WCLZWriter.write_fight(
            out_path, summary, norm_events, replay, zstd_level=zstd_level
        )
    return comp_bytes, len(raw_events) if standalone else len(norm_events), (summary.get("abilities") or {})


async def _await_pack(future):
    """A cancelled asyncio waiter must not leave an archive writer running."""
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        await _finish_cleanup(future)
        raise


async def _finish_cleanup(future):
    """Repeated cancellation must not detach a file writer from its owner."""
    while not future.done():
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError:
            continue
    return future.result()


def _cancel_once(task):
    if task is not None and not task.done() and not task.cancelling():
        task.cancel()


class ImportIncompleteError(RuntimeError):
    def __init__(self, report_code: str, successful_paths: List[Path], failed_rows: List[Dict[str, Any]]):
        self.successful_paths = list(successful_paths)
        self.failed_fight_ids = [int(row["fight_id"]) for row in failed_rows]
        reasons = "; ".join(f"#{row['fight_id']}: {row['last_error']}" for row in failed_rows)
        super().__init__(f"Incomplete import for report {report_code}: {len(failed_rows)} failed fights ({reasons}); "
                         f"{len(successful_paths)} fights saved")


class HarvesterPipeline:
    def __init__(
        self,
        data_dir: Path,
        index_db: IndexDB,
        master_db: MasterDictDB,
        protocol_client: WCLProtocolClient,
        zstd_level: int = 22,
        download_concurrency: int = 4,
        pack_workers: Optional[int] = None,
        scan_concurrency: int = 1,
        raw_archive=None,
        processing_concurrency: int = 2,
    ):
        self.processing_concurrency = int(processing_concurrency)
        if self.processing_concurrency < 1:
            raise ValueError('processing_concurrency must be positive')
        self._processing_slots = asyncio.Semaphore(self.processing_concurrency)
        self._processing_waiting = 0
        self._processing_active = 0
        self._packing_active: set = set()
        self._browser_active: set = set()
        self.data_dir = Path(data_dir)
        self.index_db = index_db
        self.master_db = master_db
        self.client = protocol_client
        self.raw_archive = raw_archive
        self.zstd_level = int(zstd_level)
        self.download_concurrency = max(1, int(download_concurrency))
        default_pack = 4
        self.pack_workers = max(1, int(pack_workers if pack_workers is not None else default_pack))
        self.scan_concurrency = max(1, int(scan_concurrency))
        self._master_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._master_cache_limit = 128
        self._master_inflight: Dict[str, asyncio.Task[Dict[str, Any]]] = {}
        self._seen_spells: Dict[int, str] = {}
        self._scan_retry_base_delay: float = 2.0
        self._pack_executor: Optional[ThreadPoolExecutor] = None
        self.staging_store = StagingStore(self.data_dir)
        self._background_pack_tasks: List[asyncio.Task] = []
        self._packing_claims: set = set()
        self._download_claims: set = set()
        self._unlinked_staging: Dict[str, Path] = {}
        self._packing_stop = False
        self._packing_finish = False
        self._packing_wakeup: Optional[asyncio.Event] = None
        self._packing_progress = None
        self._download_scheduler_task: Optional[asyncio.Task] = None
        self._continuous_downloads: Dict[int, Tuple[str, asyncio.Task]] = {}
        self._report_preparations: Dict[str, asyncio.Task] = {}
        self._download_wakeup: Optional[asyncio.Event] = None
        self._download_progress: Optional[asyncio.Event] = None
        self._download_completed = 0
        self._download_reported = 0
        self._browser_manager_task: Optional[asyncio.Task] = None
        self._browser_tasks: Dict[int, asyncio.Task] = {}
        self._browser_cpu_tasks = {}
        self._browser_cpu_active = set()
        self._browser_claims: set = set()
        self._browser_wakeup: Optional[asyncio.Event] = None
        self._browser_finish = False
        self._browser_stop = False
        self._raw_executor: Optional[ThreadPoolExecutor] = None
        self._db_executor: Optional[ThreadPoolExecutor] = None
        self._db_read_slots = asyncio.Semaphore(2)
        self._db_reads = 0
        self._download_stop = False

    def get_runtime_stats(self) -> Dict[str, int]:
        """In-memory counters only; safe for frequent status polling."""
        return {'processing_concurrency': self.processing_concurrency,
                'processing_active': self._processing_active,
                'processing_waiting': self._processing_waiting,
                'packing_fights': len(self._packing_active),
                'downloading_fights': len(self._download_claims),
                'browser_enriching_fights': len(self._browser_active),
                'browser_network_limit': self.download_concurrency,
                'preparing_reports': len(self._report_preparations)}

    def get_staging_stats(self) -> Dict[str, int]:
        return {**self.staging_store.stats(), **self.get_runtime_stats(),
                'browser_disk_ready': max(0, self.index_db.count_browser_cpu_jobs() - len(self._browser_cpu_active)),
                'api_staged_fights': self.index_db.count_api_staged_fights()}

    @asynccontextmanager
    async def _processing_admission(self, active, uid, should_stop):
        # Claims prevent duplicate selection, while admission bounds resident
        # snapshot work. Stop abandons waiters without restoring their events.
        acquired = False
        self._processing_waiting += 1
        try:
            while not should_stop():
                try:
                    await asyncio.wait_for(self._processing_slots.acquire(), .1)
                    acquired = True
                    break
                except asyncio.TimeoutError:
                    continue
        finally:
            self._processing_waiting -= 1
        admitted = acquired and not should_stop()
        if admitted:
            self._processing_active += 1
            active.add(uid)
        try:
            yield admitted
        finally:
            if admitted:
                active.discard(uid)
                self._processing_active -= 1
            if acquired:
                self._processing_slots.release()

    def _pack_log(self, message: str) -> None:
        if self._packing_progress:
            # Status polling collects disk/DB counters separately. Logging every
            # worker transition must not stall the network loop on those reads.
            self._packing_progress({'log': message})

    def _raise_background_failure(self) -> None:
        tasks = [*self._background_pack_tasks]
        if self._download_scheduler_task is not None:
            tasks.append(self._download_scheduler_task)
        if self._browser_manager_task is not None:
            tasks.append(self._browser_manager_task)
        for task in tasks:
            if task.done() and not task.cancelled() and task.exception() is not None:
                error = task.exception()
                raise RuntimeError(f'Background collection failed: {error}') from error

    def _verify_staged_archive(self, path: Path, job: Dict[str, Any]) -> None:
        sections = PackedFightReader.read_sections(path)
        manifest = sections['metadata']['manifest']
        if manifest != job['manifest']:
            raise ValueError('Packed archive source fetch identity does not match durable raw')
        if sections['events']['count'] != manifest['event_count']:
            raise ValueError('Packed archive event count does not match durable raw')

    def _recover_staging(self) -> List[str]:
        """Recover the file-before-catalogue crash window without network access."""
        messages = []
        # Check already linked files without decoding all queued fights. Orphan
        # replacements must be validated before an older pending file is linked.
        paths = sorted(self.staging_store.list_jobs(), key=lambda path: not path.name.endswith('-hybrid.wcls'))
        for path in paths:
            if self._packing_stop:
                break
            try:
                if not path.exists():  # A verified replacement already cleaned it.
                    continue
                rel = path.relative_to(self.data_dir.resolve()).as_posix()
                linked_row = self.index_db.get_linked_stage(rel)
                if linked_row and linked_row['status'] in ('STAGED', 'API_STAGED'):
                    work = self.staging_store.browser_work(path)
                    if linked_row['status'] == 'API_STAGED' and hasattr(self.client, 'download_supplement_phase'):
                        cpu = work.phase() not in ('replay', 'canonical')
                        if cpu != bool(linked_row['browser_cpu']):
                            self.index_db.set_browser_queue(linked_row['fight_uid'], rel, cpu=cpu)
                    continue
                job, archive = self.staging_store.load(path, include_events=False)
                archive.clear()
                row, manifest = job['fight_row'], job['manifest']
                code, fid = row['report_code'], int(row['fight_id'])
                saved = self.index_db.get_fight(code, fid)
                if saved is None:
                    self.index_db.enqueue_fight(code, fid, row['content_type'], row['zone_id'], row['partition'],
                        row['encounter_id'], row['encounter_slug'], keystone_level=row.get('keystone_level', 0),
                        difficulty=row.get('difficulty', 0), affixes=json.loads(row.get('affixes_json') or '[]'),
                        duration_ms=row.get('duration_ms', 0))
                    saved = self.index_db.get_fight(code, fid)
                if saved['status'] == 'PACKED':
                    out = (self.data_dir / (saved['file_rel_path'] or '')).resolve()
                    out.relative_to(self.data_dir.resolve())
                    sections = PackedFightReader.read_sections(out)
                    packed = sections['metadata']['manifest']
                    if (packed != manifest and not self._hybrid_replaces(packed, manifest)
                            or sections['events']['count'] != packed['event_count']):
                        raise ValueError('Packed archive source fetch identity does not match durable raw')
                    self._delete_replaced_snapshot(path, manifest)
                    self.index_db.clear_fight_staging(code, fid, rel)
                elif saved.get('staging_rel_path') and saved['staging_rel_path'] != rel:
                    other_job, other_archive = self.staging_store.load(
                        self.data_dir / saved['staging_rel_path'], include_events=False)
                    other_archive.clear()
                    linked = other_job['manifest']
                    if self._hybrid_replaces(manifest, linked):
                        self.index_db.mark_fight_staged(code, fid, rel)
                        self.index_db.mark_report_unarchived(code)
                        try:
                            self._delete_replaced_snapshot(self.data_dir / saved['staging_rel_path'], linked)
                        except Exception as exc:
                            messages.append(f'API raw retained for {code}#{fid}: {exc}')
                    elif self._hybrid_replaces(linked, manifest):
                        self._delete_replaced_snapshot(path, manifest)
                    else:
                        messages.append(f'Raw retained for {code}#{fid}: another source fetch is linked')
                elif saved['status'] != 'FAILED' or not saved.get('staging_rel_path'):
                    marker = (self.index_db.mark_fight_api_staged if manifest.get('hybrid_pending') is True
                              else self.index_db.mark_fight_staged)
                    marker(code, fid, rel)
                    if manifest.get('hybrid_pending') and hasattr(self.client, 'download_supplement_phase'):
                        work = self._initialize_browser_work(path, manifest)
                        self.index_db.set_browser_queue(row['fight_uid'], rel, cpu=work.phase() in ('plan', 'merge'))
                    self.index_db.mark_report_unarchived(code)
            except Exception as exc:
                messages.append(f'Raw retained at {path.name}: {exc}')
        return messages

    def _delete_replaced_snapshot(self, path, manifest):
        if manifest.get('hybrid_pending'):
            work = self.staging_store.browser_work(path)
            if work.root.exists():
                work.cleanup()
        self.staging_store.delete(path)

    @staticmethod
    def _hybrid_replaces(full, pending) -> bool:
        return bool(isinstance(full, dict) and isinstance(pending, dict)
                    and pending.get('hybrid_pending') is True
                    and full.get('hybrid_pending') is not True
                    and (full.get('hybrid') or {}).get('api_manifest') == pending)

    async def start_background_packing(self, on_progress=None) -> None:
        if not isinstance(self.raw_archive, FightArchive):
            return
        if on_progress is not None:
            self._packing_progress = on_progress
        if self._background_pack_tasks:
            self._raise_background_failure()
            self._start_browser_enrichment()
            return
        self._packing_stop = self._packing_finish = self._download_stop = False
        self._packing_wakeup = asyncio.Event()
        for message in await run_owned_thread(self._recover_staging):
            self._pack_log(message)
        if self._packing_stop:
            return
        self._background_pack_tasks = [asyncio.create_task(self._background_pack_worker())
                                       for _ in range(self.pack_workers)]
        self._start_browser_enrichment()

    def _start_browser_enrichment(self):
        if (getattr(self.client, 'requires_browser_enrichment', False)
                and self._browser_manager_task is None):
            self._browser_finish = self._browser_stop = False
            self._browser_wakeup = asyncio.Event()
            manager = (self._browser_disk_manager if hasattr(self.client, 'download_supplement_phase')
                       else self._browser_manager)
            self._browser_manager_task = asyncio.create_task(manager())

    def _get_raw_executor(self):
        if self._raw_executor is None:
            self._raw_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='wcl-raw')
        return self._raw_executor

    async def _raw_io(self, func, *args, **kwargs):
        from functools import partial
        future = asyncio.get_running_loop().run_in_executor(self._get_raw_executor(), partial(func, *args, **kwargs))
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            await _finish_cleanup(future)
            raise

    async def _db_read(self, func, *args, _stop_when=None, **kwargs):
        """Bound catalogue reads independently of archive/CPU work and own them until done."""
        from functools import partial
        self._db_reads += 1
        try:
            async with self._db_read_slots:
                # Waiting packers have no active writer to protect. Drop their
                # obsolete selections instead of draining a worker-sized queue
                # of expensive catalogue scans after Stop.
                if _stop_when is not None and _stop_when():
                    return []
                if self._db_executor is None:
                    self._db_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='wcl-db-read')
                future = asyncio.get_running_loop().run_in_executor(
                    self._db_executor, partial(func, *args, **kwargs))
                try:
                    return await asyncio.shield(future)
                except asyncio.CancelledError:
                    await _finish_cleanup(future)
                    raise
        finally:
            self._db_reads -= 1

    def _request_browser_stop(self):
        self._browser_stop = True
        _cancel_once(self._browser_manager_task)
        # The manager can itself be waiting on an owned DB read or publisher.
        # Cancel network children immediately instead of waiting for that work.
        for task in (*self._browser_tasks.values(), *self._browser_cpu_tasks.values()):
            _cancel_once(task)
        if self._browser_wakeup is not None:
            self._browser_wakeup.set()

    def _request_download_stop(self):
        self._download_stop = True
        _cancel_once(self._download_scheduler_task)
        for _, task in self._continuous_downloads.values():
            _cancel_once(task)
        for task in self._report_preparations.values():
            _cancel_once(task)
        if self._download_wakeup is not None:
            self._download_wakeup.set()

    def _request_background_stop(self):
        # Freeze every admission path before awaiting any durable file writer.
        self._packing_stop = True
        self._request_download_stop()
        self._request_browser_stop()
        if self._packing_wakeup is not None:
            self._packing_wakeup.set()

    def request_stop(self) -> None:
        """Freeze worker admission and request cancellation before acknowledging Stop."""
        self._request_background_stop()

    def _link_staging(self, path, code, fid, manifest):
        rel = path.relative_to(self.data_dir.resolve()).as_posix()
        if manifest.get('hybrid_pending') is True:
            self.index_db.mark_fight_api_staged(code, fid, rel)
            if self._browser_wakeup is not None:
                self._browser_wakeup.set()
        else:
            self.index_db.mark_fight_staged(code, fid, rel)
            self._packing_wakeup.set()

    async def _enrich_one_staged(self, row, worker_id):
        uid = row['fight_uid']
        self._browser_claims.add(uid)
        try:
            async with self._processing_admission(
                    self._browser_active, uid, lambda: self._browser_stop) as admitted:
                if admitted:
                    await self._enrich_admitted_staged(row, worker_id)
        finally:
            self._browser_claims.discard(uid)
            self._browser_wakeup.set()

    async def _enrich_admitted_staged(self, row, worker_id):
        uid, code, fid = row['fight_uid'], row['report_code'], int(row['fight_id'])
        archive = None
        try:
            self._pack_log(f'Started browser enrichment for {uid} (browser worker #{worker_id})')
            path = self.data_dir / row['staging_rel_path']
            job, archive = await self._raw_io(self.staging_store.load, path, compression=self.raw_archive.compression)
            if job['fight_row']['fight_uid'] != uid or job['manifest'].get('hybrid_pending') is not True:
                raise ValueError('Browser enrichment requires the matching pending API snapshot')
            api_manifest = job['manifest']
            master = await self.client.enrich_staged_fight(job, archive)
            out, meta, actors, abilities, start, end = self._prepare_fight_payload(row, master)
            if (start, end) != (api_manifest['start_time'], api_manifest['end_time']):
                raise ValueError('Hybrid browser metadata boundaries differ from API snapshot')
            # Hybrid conflict diagnostics can scale with the full event stream.
            # Copying that manifest belongs with disk/restore work, outside the
            # API workers' event loop, after this private archive's merge ends.
            full_manifest = await self._raw_io(archive.fight_manifest, code, fid)
            if not self._hybrid_replaces(full_manifest, api_manifest):
                raise ValueError('Enriched sources do not replace this API snapshot')

            async def publish():
                new_path = await self._raw_io(self.staging_store.stage_fight,
                                             archive, row, meta, actors, abilities, out)
                self.index_db.mark_fight_staged(code, fid, new_path.relative_to(self.data_dir.resolve()).as_posix())
                self._packing_wakeup.set()
                try:
                    await self._raw_io(self.staging_store.delete, path)
                except Exception as exc:
                    self._pack_log(f'Enriched {uid}; API raw retained for cleanup: {exc}')
            task = asyncio.create_task(publish())
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await _finish_cleanup(task)
                raise
            self._pack_log(f'Browser enrichment complete for {uid} (browser worker #{worker_id}); queued for packing')
        except asyncio.CancelledError:
            # Cancellation releases the provider, not the retry budget. A new
            # browser manager can resume the same durable API snapshot directly.
            # An already running atomic publisher may have advanced it to STAGED.
            self._pack_log(f'Browser enrichment cancelled for {uid} (browser worker #{worker_id}); durable source retained')
            raise
        except Exception as exc:
            self.index_db.mark_fight_failed(code, fid, str(exc))
            self._pack_log(f'Browser enrichment failed for {uid} (browser worker #{worker_id}); API raw retained: {exc}')
        finally:
            if archive is not None:
                archive.clear()

    def _initialize_browser_work(self, path, manifest):
        work = self.staging_store.browser_work(path)
        work.initialize(*(manifest[key] for key in
                          ('report_code', 'fight_id', 'start_time', 'end_time', 'fetch_id')))
        return work

    def _browser_disk_state(self, path):
        work = self.staging_store.browser_work(path)
        return work, work.phase()

    async def _run_browser_phase(self, row, slot):
        uid, code, fid = row['fight_uid'], row['report_code'], int(row['fight_id'])
        key = row['staging_rel_path']
        path = self.data_dir / key
        phase = 'inspect'
        activity = None
        try:
            work, phase = await run_owned_thread(self._browser_disk_state, path)
            cpu = phase not in ('replay', 'canonical')
            if cpu != bool(row['browser_cpu']):
                # A crash or Stop can leave the catalogue one disk marker behind.
                self.index_db.set_browser_queue(uid, key, cpu=cpu)
                return
            if phase in ('replay', 'canonical'):
                activity = f'browser {phase} for {uid} (browser worker #{slot})'
                self._browser_active.add(uid)
                self._pack_log(f'Started {activity}')
                await self.client.download_supplement_phase(work, phase)
            else:
                async with self._processing_admission(
                        self._browser_cpu_active, uid, lambda: self._browser_stop) as admitted:
                    if not admitted:
                        return
                    job, archive = await self._raw_io(
                        self.staging_store.load, path, compression=self.raw_archive.compression,
                        include_events=phase != 'initialize')
                    try:
                        manifest = job['manifest']
                        if job['fight_row']['fight_uid'] != uid or not manifest.get('hybrid_pending'):
                            raise ValueError('Browser work does not match pending API snapshot')
                        parent = await self._raw_io(self.staging_store.browser_work, path)
                        if parent.parent != work.parent:
                            raise ValueError('Browser work parent checksum changed')
                        if phase == 'initialize':
                            await self._raw_io(self._initialize_browser_work, path, manifest)
                        elif phase == 'plan':
                            await self._raw_io(work.process, job, archive, merge=False)
                        elif phase == 'merge':
                            master = await self._raw_io(work.process, job, archive, merge=True)
                            out, meta, actors, abilities, start, end = self._prepare_fight_payload(row, master)
                            if (start, end) != (manifest['start_time'], manifest['end_time']):
                                raise ValueError('Browser fight bounds changed')
                            async def publish():
                                merged = await self._raw_io(archive.fight_manifest, code, fid)
                                if not self._hybrid_replaces(merged, manifest):
                                    raise ValueError('Browser merged source parent mismatch')
                                dest = await self._raw_io(self.staging_store.stage_fight,
                                                          archive, row, meta, actors, abilities, out)
                                self.index_db.mark_fight_staged(code, fid, dest.relative_to(self.data_dir.resolve()).as_posix())
                                self._packing_wakeup.set()
                                try:
                                    await self._raw_io(work.cleanup)
                                    await self._raw_io(self.staging_store.delete, path)
                                except Exception as exc:
                                    self._pack_log(f'Published {uid}; temporary sources retained: {exc}')
                            publisher = asyncio.create_task(publish())
                            try:
                                await asyncio.shield(publisher)
                            except asyncio.CancelledError:
                                await _finish_cleanup(publisher)
                                raise
                        else:
                            raise ValueError('Unknown browser staging phase')
                    finally:
                        job.pop('raw_events', None)
                        archive.clear()
            if phase != 'merge':
                self.index_db.set_browser_queue(uid, key, cpu=phase in ('replay', 'canonical'))
            if activity:
                self._pack_log(f'Completed {activity}')
        except asyncio.CancelledError:
            if activity:
                self._pack_log(f'Cancelled {activity}; source retained')
            raise
        except Exception as exc:
            self.index_db.mark_fight_failed(code, fid, str(exc))
            detail = activity or f'browser processing phase {phase} for {uid}'
            self._pack_log(f'Failed {detail}; source retained: {exc}')
        finally:
            self._browser_active.discard(uid)
            self._browser_claims.discard(uid)
            self._browser_wakeup.set()

    async def _browser_disk_manager(self):
        network, cpu = self._browser_tasks, self._browser_cpu_tasks
        try:
            while not self._browser_stop:
                self._browser_wakeup.clear()
                for pool in (network, cpu):
                    for slot, task in list(pool.items()):
                        if task.done():
                            del pool[slot]
                            task.result()
                # The partial dispatch index keeps each pool's M+/FIFO selection
                # independent of the size of the other pool's on-disk backlog.
                rows = await self._db_read(
                    self.index_db.get_browser_dispatch,
                    self.download_concurrency + len(self._browser_claims)
                    if len(network) < self.download_concurrency else 0,
                    self.processing_concurrency + len(self._browser_claims)
                    if len(cpu) < self.processing_concurrency else 0)
                if self._browser_stop:
                    break
                for row in rows:
                    uid = row['fight_uid']
                    if uid in self._browser_claims:
                        continue
                    if not row['browser_cpu']:
                        pool, limit = network, self.download_concurrency
                    else:
                        pool, limit = cpu, self.processing_concurrency
                    if len(pool) >= limit:
                        continue
                    fresh = self.index_db.get_fight(row['report_code'], int(row['fight_id']))
                    if (not fresh or fresh['status'] != 'API_STAGED'
                            or fresh['staging_rel_path'] != row['staging_rel_path']
                            or fresh['browser_cpu'] != row['browser_cpu']):
                        continue
                    slot = next(i for i in range(1, limit+1) if i not in pool)
                    self._browser_claims.add(uid)
                    pool[slot] = asyncio.create_task(self._run_browser_phase(row, slot))
                if self._browser_finish and not rows and not cpu and not network:
                    return
                try:
                    await asyncio.wait_for(self._browser_wakeup.wait(), timeout=.25)
                except asyncio.TimeoutError:
                    pass
        finally:
            tasks = [*network.values(), *cpu.values()]
            for task in tasks:
                _cancel_once(task)
            await _finish_cleanup(asyncio.gather(*tasks, return_exceptions=True))
            network.clear()
            cpu.clear()
            self._browser_claims.clear()

    async def _browser_manager(self):
        try:
            while not self._browser_stop:
                for slot, task in list(self._browser_tasks.items()):
                    if task.done():
                        task.result()
                        del self._browser_tasks[slot]
                limit = self.download_concurrency
                rows = await self._db_read(self.index_db.get_api_staged_fights,
                                           limit=limit + len(self._browser_tasks))
                if self._browser_stop:
                    return
                for row in rows:
                    if row['fight_uid'] in self._browser_claims or len(self._browser_tasks) >= limit:
                        continue
                    fresh = self.index_db.get_fight(row['report_code'], int(row['fight_id']))
                    if not fresh or fresh['status'] != 'API_STAGED' or fresh['staging_rel_path'] != row['staging_rel_path']:
                        continue
                    slot = next(i for i in range(1, limit + 1) if i not in self._browser_tasks)
                    # Claim before scheduling; an entire disk backlog never lives in RAM.
                    self._browser_claims.add(row['fight_uid'])
                    self._browser_tasks[slot] = asyncio.create_task(self._enrich_one_staged(row, slot))
                if self._browser_finish and not self._browser_tasks and not rows:
                    return
                self._browser_wakeup.clear()
                try:
                    await asyncio.wait_for(self._browser_wakeup.wait(), timeout=.25)
                except asyncio.TimeoutError:
                    pass
        finally:
            tasks = list(self._browser_tasks.values())
            for task in tasks:
                if not task.done() and not task.cancelling():
                    task.cancel()
            await _finish_cleanup(asyncio.gather(*tasks, return_exceptions=True))
            self._browser_tasks.clear()
            self._browser_claims.clear()

    async def _finish_browser_enrichment(self, cancel):
        if self._browser_manager_task is None:
            return
        task = self._browser_manager_task
        self._browser_finish = True
        self._browser_stop = self._browser_stop or cancel
        self._browser_wakeup.set()
        if self._browser_stop:
            self._request_browser_stop()
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            if not task.cancelled():
                self._request_browser_stop()
                try:
                    await _finish_cleanup(task)
                except asyncio.CancelledError:
                    pass
                raise
        finally:
            self._browser_manager_task = None

    async def stop_background_enrichment(self, cancel: bool = True) -> None:
        """Quiesce browser-owned requests and durable writers before browser close."""
        if cancel:
            self._request_browser_stop()
        cleanup = asyncio.create_task(self._finish_browser_enrichment(cancel))
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            self._request_browser_stop()
            await _finish_cleanup(cleanup)
            raise

    async def stop_background_downloading(self) -> None:
        # Download cancellation may still be publishing a durable snapshot.
        # Await those writers before allowing packers or capture storage to close.
        self._request_download_stop()
        if self._download_scheduler_task is not None:
            task = self._download_scheduler_task
            try:
                await _finish_cleanup(task)
            except asyncio.CancelledError:
                pass
            finally:
                self._download_scheduler_task = None

    async def finish_background_packing(self, cancel: bool = False) -> None:
        if cancel:
            self.request_stop()
        cleanup = asyncio.create_task(self._finish_background_packing(cancel))
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            self._request_background_stop()
            await _finish_cleanup(cleanup)
            raise

    async def _finish_background_packing(self, cancel: bool = False) -> None:
        download_error = None
        try:
            await self.stop_background_downloading()
        except Exception as exc:
            download_error = exc
        try:
            await self._finish_browser_enrichment(cancel)
        except Exception as exc:
            download_error = download_error or exc
        if not self._background_pack_tasks:
            if download_error:
                raise download_error
            return
        self._packing_stop = self._packing_stop or cancel
        self._packing_finish = True
        self._packing_wakeup.set()
        waiter = asyncio.gather(*self._background_pack_tasks, return_exceptions=True)
        try:
            results = await asyncio.shield(waiter)
            errors = [result for result in results if isinstance(result, BaseException)]
            if errors:
                raise RuntimeError(f'Background packing failed: {errors[0]}') from errors[0]
            if download_error:
                raise download_error
        except asyncio.CancelledError:
            self._packing_stop = True
            self._packing_wakeup.set()
            await _finish_cleanup(waiter)
            raise
        finally:
            self._background_pack_tasks = []

    def _pack_staged_cpu(self, path: Path, expected_row: Dict[str, Any]):
        job, archive = self.staging_store.load(path, compression=self.raw_archive.compression)
        try:
            row = job['fight_row']
            if row['fight_uid'] != expected_row['fight_uid']:
                raise ValueError('Durable raw belongs to a different fight')
            out = self.data_dir / job['out_rel_path']
            result = _analyze_and_pack_cpu(out, job['fight_meta'], job['actors'], job['abilities'],
                                           job['raw_events'], self.zstd_level, archive)
            self._verify_staged_archive(out, job)
            return out, job, result
        finally:
            # Release both source captures and the analyzer's view while still
            # holding admission, including on validation or packing failure.
            job.pop('raw_events', None)
            archive.clear()

    async def _pack_admitted_staged(self, row):
        code, fid = row['report_code'], int(row['fight_id'])
        try:
            if not row.get('staging_rel_path'):
                raise ValueError('Staged fight has no durable raw path')
            path = self.data_dir / row['staging_rel_path']
            out, job, result = await _await_pack(asyncio.get_running_loop().run_in_executor(
                self._get_pack_executor(), self._pack_staged_cpu, path, row))
            comp_bytes, count, summary_abilities = result
            self._commit_fight_result(code, fid, out, job['abilities'], summary_abilities,
                comp_bytes, count, job['fight_meta']['end_time'] - job['fight_meta']['start_time'])
            try:
                await self._raw_io(self.staging_store.delete, path)
                self.index_db.clear_fight_staging(code, fid, row['staging_rel_path'])
            except Exception as exc:
                self._pack_log(f'Packed {code}#{fid}; durable raw retained for cleanup: {exc}')
            self._pack_log(f'Saved {out.name} (packed and verified)')
        except Exception as exc:
            self.index_db.mark_fight_failed(code, fid, str(exc))
            self._pack_log(f'Packing failed for {code}#{fid}; durable raw retained: {exc}')

    async def _background_pack_worker(self) -> None:
        while not self._packing_stop:
            rows = await self._db_read(self.index_db.get_staged_fights, limit=self.pack_workers + 1,
                                       _stop_when=lambda: self._packing_stop)
            if self._packing_stop:
                return
            row = next((row for row in rows if row['fight_uid'] not in self._packing_claims), None)
            if row is not None:
                fresh = self.index_db.get_fight(row['report_code'], int(row['fight_id']))
                if not fresh or fresh['status'] != 'STAGED' or fresh['staging_rel_path'] != row['staging_rel_path']:
                    continue
            if row is None:
                if self._packing_finish:
                    return
                self._packing_wakeup.clear()
                try:
                    await asyncio.wait_for(self._packing_wakeup.wait(), timeout=.25)
                except asyncio.TimeoutError:
                    pass
                continue
            uid = row['fight_uid']
            self._packing_claims.add(uid)
            try:
                async with self._processing_admission(
                        self._packing_active, uid, lambda: self._packing_stop) as admitted:
                    if not admitted:
                        return
                    await self._pack_admitted_staged(row)
            finally:
                self._packing_claims.discard(uid)
                self._packing_wakeup.set()

    async def _download_one_to_staging(self, row, worker_id, should_stop=None) -> int:
        uid = row['fight_uid']
        if uid in self._download_claims:
            return 0
        self._download_claims.add(uid)
        try:
            if self._download_stop or (should_stop and should_stop()):
                return 0
            fresh = self.index_db.get_fight(row['report_code'], int(row['fight_id']))
            if fresh is None or fresh['status'] not in ('QUEUED', 'RETRY'):
                return 0
            if uid in self._unlinked_staging:
                path = self._unlinked_staging[uid]
                job, restored = await self._raw_io(self.staging_store.load, path, include_events=False)
                restored.clear()
                self._link_staging(path, row['report_code'], int(row['fight_id']), job['manifest'])
                self._unlinked_staging.pop(uid)
                return 1
            self._pack_log(f'Started downloading {uid} (download worker #{worker_id}; '
                           f'{row.get("encounter_slug", "fight")})')
            master = await self._get_report_master(row['report_code'])
            out, meta, actors, abilities, start, end = self._prepare_fight_payload(row, master)
            events = await self.client.fetch_all_fight_events(row['report_code'], int(row['fight_id']), start, end)
            del events
            manifest = self.raw_archive.fight_manifest(row['report_code'], int(row['fight_id']))
            async def stage_and_link():
                path = await asyncio.to_thread(self.staging_store.stage_fight,
                    self.raw_archive, row, meta, actors, abilities, out)
                self._unlinked_staging[uid] = path
                if manifest.get('hybrid_pending') and hasattr(self.client, 'download_supplement_phase'):
                    await asyncio.to_thread(self._initialize_browser_work, path, manifest)
                self._link_staging(path, row['report_code'], int(row['fight_id']), manifest)
                self._unlinked_staging.pop(uid)
            task = asyncio.create_task(stage_and_link())
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await _finish_cleanup(task)
                raise
            next_phase = 'browser enrichment' if manifest.get('hybrid_pending') is True else 'packing'
            self._pack_log(f'Downloaded {uid} (download worker #{worker_id}); durable raw queued for {next_phase}')
            return 1
        except Exception as exc:
            self.index_db.mark_fight_failed(row['report_code'], int(row['fight_id']), str(exc))
            self._pack_log(f'Download/staging failed for {uid} (download worker #{worker_id}): {exc}')
            return 0
        finally:
            self._download_claims.discard(uid)

    async def _download_rows_to_staging(self, pending, on_progress=None, should_stop=None) -> int:
        await self.start_background_packing(on_progress=on_progress)
        rows = iter(pending)
        async def worker(worker_id):
            completed = 0
            for row in rows:
                completed += await self._download_one_to_staging(row, worker_id, should_stop)
            return completed
        tasks = [asyncio.create_task(worker(i + 1))
                 for i in range(min(self.download_concurrency, len(pending)))]
        try:
            return sum(await asyncio.gather(*tasks))
        finally:
            # gather may deliver one child's cancellation before its siblings'
            # shielded staging writes finish. Never let capture clear race them.
            for task in tasks:
                if not task.done() and not task.cancelling():
                    task.cancel()
            await _finish_cleanup(asyncio.gather(*tasks, return_exceptions=True))

    async def _prepare_download_report(self, code):
        try:
            if await self.client.unarchive_report(code):
                self.index_db.mark_report_unarchived(code)
                return
            self.index_db.mark_report_failed(code, 'Unarchive request was not successful')
        except Exception as exc:
            self.index_db.mark_report_failed(code, str(exc))

    async def _continuous_download_loop(self, should_stop):
        """Refill individual slots; report preparation never joins a download batch."""
        try:
            while not self._download_stop and not (should_stop and should_stop()):
                for slot, (uid, task) in list(self._continuous_downloads.items()):
                    if task.done():
                        self._download_completed += task.result()
                        del self._continuous_downloads[slot]
                        self._download_progress.set()
                for code, task in list(self._report_preparations.items()):
                    if task.done():
                        task.result()
                        del self._report_preparations[code]
                        self._download_progress.set()

                limit = self.download_concurrency
                ready = await self._db_read(self.index_db.get_unarchived_pending_fights,
                    limit=max(2 * limit, len(self._continuous_downloads) + limit))
                if self._download_stop or (should_stop and should_stop()):
                    return
                claimed = {uid for uid, task in self._continuous_downloads.values()}
                free_slots = iter(i for i in range(1, limit + 1) if i not in self._continuous_downloads)
                for row in ready:
                    if row['fight_uid'] in claimed or len(self._continuous_downloads) >= limit:
                        continue
                    slot = next(free_slots)
                    self._continuous_downloads[slot] = (row['fight_uid'], asyncio.create_task(
                        self._download_one_to_staging(row, slot, should_stop)))
                    claimed.add(row['fight_uid'])

                # Bound metadata lookahead: keep enough prepared fights ahead
                # without unarchiving the entire historical collection in RAM.
                if len(ready) < 2 * limit and len(self._report_preparations) < limit:
                    codes = await self._db_read(self.index_db.get_pending_reports_for_unarchive,
                        limit=limit + len(self._report_preparations))
                    if self._download_stop or (should_stop and should_stop()):
                        return
                    for code in codes:
                        if len(self._report_preparations) >= limit:
                            break
                        if code not in self._report_preparations:
                            self._report_preparations[code] = asyncio.create_task(self._prepare_download_report(code))

                self._download_wakeup.clear()
                wake = asyncio.create_task(self._download_wakeup.wait())
                try:
                    active = [task for uid, task in self._continuous_downloads.values()]
                    active.extend(self._report_preparations.values())
                    await asyncio.wait([*active, wake], timeout=.5, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    wake.cancel()
                    await asyncio.gather(wake, return_exceptions=True)
        finally:
            tasks = [task for uid, task in self._continuous_downloads.values()]
            tasks.extend(self._report_preparations.values())
            for task in tasks:
                if not task.done() and not task.cancelling():
                    task.cancel()
            await _finish_cleanup(asyncio.gather(*tasks, return_exceptions=True))
            self._continuous_downloads.clear()
            self._report_preparations.clear()
            self._download_progress.set()

    async def _continuous_download_step(self, on_progress, should_stop):
        await self.start_background_packing(on_progress=on_progress)
        if self._download_scheduler_task is None:
            self._download_stop = False
            self._download_wakeup, self._download_progress = asyncio.Event(), asyncio.Event()
            self._download_completed = self._download_reported = 0
            self._download_scheduler_task = asyncio.create_task(self._continuous_download_loop(should_stop))
        self._raise_background_failure()
        self._download_wakeup.set()
        if self._download_completed == self._download_reported:
            self._download_progress.clear()
            try:
                await asyncio.wait_for(self._download_progress.wait(), timeout=.25)
            except asyncio.TimeoutError:
                pass
        self._raise_background_failure()
        completed = self._download_completed - self._download_reported
        self._download_reported = self._download_completed
        return completed

    async def _wait_for_staged_rows(self, rows) -> List[Path]:
        waiting = ('STAGED', 'API_STAGED') if getattr(self.client, 'requires_browser_enrichment', False) else ('STAGED',)
        while any((self.index_db.get_fight(row['report_code'], int(row['fight_id'])) or {}).get('status') in waiting
                  for row in rows):
            self._raise_background_failure()
            await asyncio.sleep(.05)
        saved = [self.index_db.get_fight(row['report_code'], int(row['fight_id'])) for row in rows]
        return [self.data_dir / row['file_rel_path'] for row in saved if row and row['status'] == 'PACKED']

    def _get_pack_executor(self) -> ThreadPoolExecutor:
        if self._pack_executor is None:
            self._pack_executor = ThreadPoolExecutor(
                max_workers=self.pack_workers,
                thread_name_prefix="wclz-pack",
            )
        return self._pack_executor

    def close(self) -> None:
        if self._background_pack_tasks or self._download_scheduler_task is not None or self._browser_manager_task is not None:
            raise RuntimeError('Await finish_background_packing() before closing the pipeline')
        if self._db_reads:
            raise RuntimeError('Await catalogue reads before closing the pipeline')
        if self._db_executor is not None:
            self._db_executor.shutdown(wait=True)
            self._db_executor = None
        if self._pack_executor is not None:
            self._pack_executor.shutdown(wait=True)
            self._pack_executor = None
        if self._raw_executor is not None:
            self._raw_executor.shutdown(wait=True)
            self._raw_executor = None
        if isinstance(self.raw_archive,FightArchive):
            self.raw_archive.clear()

    def _cache_master(self, report_code: str, data: Dict[str, Any]) -> None:
        self._master_cache[report_code] = data
        self._master_cache.move_to_end(report_code)
        while len(self._master_cache) > self._master_cache_limit:
            self._master_cache.popitem(last=False)

    async def _get_report_master(self, report_code: str) -> Dict[str, Any]:
        cached = self._master_cache.get(report_code)
        has_source = getattr(self.raw_archive,'has_report_metadata',None)
        if cached is not None and (not callable(has_source) or has_source(report_code)):
            self._master_cache.move_to_end(report_code)
            return cached

        inflight = self._master_inflight.get(report_code)
        if inflight is None:
            async def _fetch() -> Dict[str, Any]:
                data = await self.client.fetch_report_master_and_fights(report_code)
                self._cache_master(report_code, data)
                return data

            inflight = asyncio.create_task(_fetch())
            self._master_inflight[report_code] = inflight
            try:
                return await inflight
            finally:
                if self._master_inflight.get(report_code) is inflight:
                    self._master_inflight.pop(report_code, None)
        return await inflight

    async def warm_unarchive_batch(
        self,
        limit: int = 25,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> int:
        codes = await self._db_read(self.index_db.get_pending_reports_for_unarchive, limit=limit)
        if not codes:
            return 0

        sem = asyncio.Semaphore(self.download_concurrency)

        async def _warm_one(code: str) -> bool:
            if should_stop and should_stop():
                return False
            async with sem:
                if should_stop and should_stop():
                    return False
                try:
                    ok = await self.client.unarchive_report(code)
                except Exception as exc:
                    self.index_db.mark_report_failed(code, str(exc))
                    return False
                if ok:
                    self.index_db.mark_report_unarchived(code, "UNARCHIVED")
                    return True
                self.index_db.mark_report_failed(code, "Unarchive request was not successful")
                return False

        results = await asyncio.gather(*[_warm_one(code) for code in codes])
        return sum(1 for r in results if r)

    def _prepare_fight_payload(
        self,
        fight_row: Dict[str, Any],
        master: Dict[str, Any],
    ) -> Tuple[Path, Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], int, int]:
        report_code = fight_row["report_code"]
        fight_id = int(fight_row["fight_id"])
        encounter_id = int(fight_row["encounter_id"])
        encounter_slug = fight_row["encounter_slug"]
        content_type = fight_row["content_type"]

        fights_list = master.get("fights", [])
        target_fight = next(
            (f for f in fights_list if int(f.get("id", -1)) == fight_id),
            None,
        )
        if not target_fight:
            raise ValueError(f"Fight {fight_id} not found in report {report_code}")

        start_time = int(target_fight.get("start_time", 0))
        end_time = int(target_fight.get("end_time", start_time + 1))

        abilities = master.get("abilities", [])
        actors: List[Dict[str, Any]] = []
        for group_key in ("friendlies", "enemies", "friendlyPets", "enemyPets"):
            for actor in master.get(group_key, []):
                actors.append(actor)

        maps = target_fight.get("maps") or [0]
        map_id = int(maps[0]["id"] if isinstance(maps[0], dict) else maps[0])
        raw_name = str(target_fight.get("name") or "")
        if not raw_name or raw_name.lower() == "unknown zone":
            raw_name = encounter_slug.replace("-", " ").title()
        fight_meta = {
            "report_code": report_code,
            "fight_id": fight_id,
            "start_time": start_time,
            "end_time": end_time,
            "encounter_id": encounter_id,
            "name": raw_name,
            "keystone_level": int(target_fight.get("keystoneLevel", fight_row.get("keystone_level", 0)) or 0),
            "difficulty": int(target_fight.get("difficulty", fight_row.get("difficulty", 0)) or 0),
            "affixes": target_fight.get("affixes", []),
            "map_id": map_id,
            "pulls": target_fight.get("dungeonPulls", []),
        }

        duration_s = max(1, (end_time - start_time) // 1000)
        mins, secs = divmod(duration_s, 60)
        key_lvl = fight_meta["keystone_level"]

        if content_type == "mplus":
            sub_dir = self.data_dir / "mplus_s5" / f"{encounter_id}_{encounter_slug}"
            fname = f"k{key_lvl}_{mins}m{secs:02d}s_{report_code}_f{fight_id}.wclz"
        else:
            zone_id = int(fight_row.get("zone_id", 17))
            sub_dir = self.data_dir / "raids" / f"zone_{zone_id}" / f"{encounter_id}_{encounter_slug}"
            fname = f"{mins}m{secs:02d}s_{report_code}_f{fight_id}.wclz"

        out_path = sub_dir / fname
        if self.raw_archive is not None:
            out_path = out_path.with_suffix('.wclf')
        return out_path, fight_meta, actors, abilities, start_time, end_time

    def _commit_fight_result(
        self,
        report_code: str,
        fight_id: int,
        out_path: Path,
        master_abilities: List[Dict[str, Any]],
        summary_abilities: Dict[str, Any],
        comp_bytes: int,
        event_count: int,
        duration_ms: Optional[int] = None,
    ) -> None:
        spell_batch: List[Tuple[int, str, int, str]] = []
        for ab in master_abilities:
            gid = ab.get("gameID") if "gameID" in ab else ab.get("guid")
            if gid is not None:
                sid = int(gid)
                name = str(ab.get("name", ""))
                if name != self._seen_spells.get(sid):
                    spell_batch.append(
                        (
                            sid,
                            name,
                            int(ab.get("type", 1) or 1),
                            str(ab.get("icon") or ab.get("abilityIcon") or ""),
                        )
                    )
                    self._seen_spells[sid] = name
        for gid_str, ab_info in summary_abilities.items():
            sid = int(gid_str)
            name = str(ab_info.get("name", ""))
            if name != self._seen_spells.get(sid):
                spell_batch.append(
                    (
                        sid,
                        name,
                        int(ab_info.get("type", 1) or 1),
                        str(ab_info.get("icon", "")),
                    )
                )
                self._seen_spells[sid] = name
        if spell_batch:
            self.master_db.upsert_spells_batch(spell_batch)

        rel_path = out_path.relative_to(self.data_dir).as_posix()
        self.index_db.mark_fight_packed(
            report_code=report_code,
            fight_id=fight_id,
            file_rel_path=rel_path,
            compressed_bytes=comp_bytes,
            event_count=event_count,
            duration_ms=duration_ms,
        )

    async def process_single_fight(self, fight_row: Dict[str, Any]) -> Path:
        try:
            return await self._process_single_fight(fight_row)
        except Exception as exc:
            self.index_db.mark_fight_failed(
                fight_row["report_code"], int(fight_row["fight_id"]), str(exc)
            )
            raise

    async def _process_single_fight(self, fight_row: Dict[str, Any]) -> Path:
        if isinstance(self.raw_archive, FightArchive):
            _, paths = await self._process_rows_pipeline([fight_row])
            if not paths:
                raise RuntimeError('Fight download or durable packing failed')
            return paths[0]
        report_code = fight_row["report_code"]
        fight_id = int(fight_row["fight_id"])

        master = await self._get_report_master(report_code)
        out_path, fight_meta, actors, abilities, start_time, end_time = self._prepare_fight_payload(
            fight_row, master
        )
        raw_events = await self.client.fetch_all_fight_events(
            report_code, fight_id, start_time, end_time
        )

        loop = asyncio.get_running_loop()
        comp_bytes, norm_count, summary_abilities = await _await_pack(loop.run_in_executor(
            self._get_pack_executor(),
            _analyze_and_pack_cpu,
            out_path,
            fight_meta,
            actors,
            abilities,
            raw_events,
            self.zstd_level,
            self.raw_archive,
        ))
        self._commit_fight_result(
            report_code=report_code,
            fight_id=fight_id,
            out_path=out_path,
            master_abilities=abilities,
            summary_abilities=summary_abilities,
            comp_bytes=comp_bytes,
            event_count=norm_count,
            duration_ms=end_time - start_time,
        )
        return out_path

    async def _process_rows_pipeline(
        self,
        pending: List[Dict[str, Any]],
        on_progress: Optional[Callable[[Dict[str, Any]], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> Tuple[int, List[Path]]:
        if not pending:
            return 0, []
        if isinstance(self.raw_archive, FightArchive):
            owns_workers = not self._background_pack_tasks
            interrupted = False
            try:
                await self._download_rows_to_staging(pending, on_progress, should_stop)
                paths = await self._wait_for_staged_rows(pending)
                return len(paths), paths
            except asyncio.CancelledError:
                interrupted = True
                raise
            finally:
                if owns_workers:
                    await self.finish_background_packing(cancel=interrupted or bool(should_stop and should_stop()))

        download_queue: asyncio.Queue[Optional[Dict[str, Any]]] = asyncio.Queue()
        for row in pending:
            download_queue.put_nowait(row)

        num_downloaders = max(1, min(self.download_concurrency, len(pending)))
        num_packers = max(1, min(self.pack_workers, len(pending)))
        for _ in range(num_downloaders):
            download_queue.put_nowait(None)

        pack_queue: asyncio.Queue[
            Optional[Tuple[Dict[str, Any], Path, Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]]
        ] = asyncio.Queue(maxsize=max(4, num_packers * 2))

        packed_paths: List[Path] = []
        packed_count = 0

        async def _downloader_worker() -> None:
            while True:
                row = await download_queue.get()
                if row is None:
                    return
                if should_stop and should_stop():
                    continue
                report_code = row["report_code"]
                fight_id = int(row["fight_id"])
                try:
                    download_started = time.perf_counter()
                    if on_progress:
                        on_progress(
                            {
                                "current_task": (
                                    f"Downloading {row['encounter_slug']} "
                                    f"(Report {report_code} #{fight_id})..."
                                )
                            }
                        )
                    master = await self._get_report_master(report_code)
                    out_path, fight_meta, actors, abilities, start_time, end_time = (
                        self._prepare_fight_payload(row, master)
                    )
                    raw_events = await self.client.fetch_all_fight_events(
                        report_code, fight_id, start_time, end_time
                    )
                    if should_stop and should_stop():
                        continue
                    if on_progress:
                        on_progress({'log':f"Downloaded {report_code}#{fight_id}: {len(raw_events)} events in {time.perf_counter()-download_started:.2f}s; ready to pack"})
                    await pack_queue.put(
                        (row, out_path, fight_meta, actors, abilities, raw_events)
                    )
                except Exception as exc:
                    self.index_db.mark_fight_failed(report_code, fight_id, str(exc))
                    if on_progress:
                        on_progress(
                            {
                                "log": f"Skipped fight {report_code}#{fight_id}: {exc}",
                            }
                        )

        async def _packer_worker() -> None:
            nonlocal packed_count
            loop = asyncio.get_running_loop()
            executor = self._get_pack_executor()
            while True:
                item = await pack_queue.get()
                if item is None:
                    return
                if should_stop and should_stop():
                    continue
                row, out_path, fight_meta, actors, abilities, raw_events = item
                report_code = row["report_code"]
                fight_id = int(row["fight_id"])
                try:
                    pack_started = time.perf_counter()
                    comp_bytes, norm_count, summary_abilities = await _await_pack(loop.run_in_executor(
                        executor,
                        _analyze_and_pack_cpu,
                        out_path,
                        fight_meta,
                        actors,
                        abilities,
                        raw_events,
                        self.zstd_level,
                        self.raw_archive,
                    ))
                    self._commit_fight_result(
                        report_code=report_code,
                        fight_id=fight_id,
                        out_path=out_path,
                        master_abilities=abilities,
                        summary_abilities=summary_abilities,
                        comp_bytes=comp_bytes,
                        event_count=norm_count,
                        duration_ms=fight_meta["end_time"] - fight_meta["start_time"],
                    )
                    packed_count += 1
                    packed_paths.append(out_path)
                    if on_progress:
                        kb = out_path.stat().st_size / 1024.0 if out_path.exists() else 0.0
                        on_progress(
                            {
                                "log": f"Saved {out_path.name} ({kb:.1f} KB; packed and verified in {time.perf_counter()-pack_started:.2f}s)",
                            }
                        )
                except Exception as exc:
                    self.index_db.mark_fight_failed(report_code, fight_id, str(exc))
                    if on_progress:
                        on_progress(
                            {
                                "log": f"Skipped fight {report_code}#{fight_id}: {exc}",
                            }
                        )

        packer_tasks = [asyncio.create_task(_packer_worker()) for _ in range(num_packers)]
        downloader_tasks = [asyncio.create_task(_downloader_worker()) for _ in range(num_downloaders)]
        try:
            await asyncio.gather(*downloader_tasks)
        finally:
            for _ in range(num_packers):
                await pack_queue.put(None)
            await asyncio.gather(*packer_tasks)

        return packed_count, packed_paths

    async def import_report_by_code(
        self,
        report_code: str,
        content_type: str = "mplus",
        zone_id: int = 9,
        partition: int = 5,
    ) -> List[Path]:
        self.index_db.retry_failed_work(report_code=report_code)
        try:
            if not await self.client.unarchive_report(report_code):
                raise RuntimeError("Unarchive request was not successful")
            master = await self._get_report_master(report_code)
        except Exception as exc:
            self.index_db.mark_report_failed(report_code, str(exc))
            raise
        target_fight_ids = set()
        for f in master.get("fights", []):
            boss_id = int(f.get("boss", 0) or 0)
            if boss_id <= 0:
                continue
            fight_id = int(f["id"])
            target_fight_ids.add(fight_id)
            slug = str(f.get("name", f"encounter-{boss_id}")).lower().replace(" ", "-")
            self.index_db.enqueue_fight(
                report_code=report_code,
                fight_id=fight_id,
                content_type="mplus" if f.get("keystoneLevel") else content_type,
                zone_id=zone_id,
                partition=partition,
                encounter_id=boss_id,
                encounter_slug=slug,
                keystone_level=int(f.get("keystoneLevel", 0) or 0),
                difficulty=int(f.get("difficulty", 0) or 0),
                affixes=f.get("affixes", []),
                duration_ms=max(1, int(f.get("end_time", 0)) - int(f.get("start_time", 0))),
            )
            self.index_db.mark_report_unarchived(report_code, "UNARCHIVED")

        if not target_fight_ids:
            return []

        if isinstance(self.raw_archive, FightArchive):
            owns_workers = not self._background_pack_tasks
            interrupted = False
            await self.start_background_packing()
            try:
                while True:
                    rows = await self._db_read(self.index_db.get_unarchived_pending_fights,
                                               limit=100, report_code=report_code)
                    if not rows:
                        break
                    await self._download_rows_to_staging(rows)
                rows = [row for row in await self._db_read(self.index_db.search_fights, report_code=report_code,
                            limit=max(100, len(target_fight_ids))) if int(row['fight_id']) in target_fight_ids]
                paths = await self._wait_for_staged_rows(rows)
                failed = [row for row in await self._db_read(self.index_db.search_fights, report_code=report_code,
                            limit=max(100, len(target_fight_ids)))
                          if int(row['fight_id']) in target_fight_ids and row['status'] != 'PACKED']
                if failed:
                    raise ImportIncompleteError(report_code, paths, failed)
                return paths
            except asyncio.CancelledError:
                interrupted = True
                raise
            finally:
                if owns_workers:
                    await self.finish_background_packing(cancel=interrupted)

        packed_paths: List[Path] = []
        while True:
            fight_rows = [
                row for row in await self._db_read(self.index_db.get_unarchived_pending_fights,
                                                   limit=100, report_code=report_code)
                if int(row["fight_id"]) in target_fight_ids
            ]
            if not fight_rows:
                break
            _, batch_paths = await self._process_rows_pipeline(fight_rows)
            packed_paths.extend(batch_paths)
        failed_rows = [
            row for row in await self._db_read(self.index_db.search_fights, report_code=report_code,
                                               limit=max(100, len(target_fight_ids)))
            if int(row["fight_id"]) in target_fight_ids and row["status"] != "PACKED"
        ]
        if failed_rows:
            raise ImportIncompleteError(report_code, packed_paths, failed_rows)
        return packed_paths

    async def _fetch_rankings_refs(
        self,
        zone_id: int,
        encounter_id: int,
        partition: int,
        class_name: str,
        spec_name: str,
        page: int,
        difficulty: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if difficulty and int(difficulty) > 0:
            try:
                return await self.client.fetch_rankings_fight_refs(
                    zone_id=zone_id,
                    encounter_id=encounter_id,
                    partition=partition,
                    class_name=class_name,
                    spec_name=spec_name,
                    page=page,
                    difficulty=int(difficulty),
                )
            except TypeError:
                pass
        return await self.client.fetch_rankings_fight_refs(
            zone_id=zone_id,
            encounter_id=encounter_id,
            partition=partition,
            class_name=class_name,
            spec_name=spec_name,
            page=page,
        )

    async def _run_discovery_pool(
        self,
        units: List[Dict[str, Any]],
        pages_per_spec: int = 0,
        on_progress: Optional[Callable[[Dict[str, Any]], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        pack_as_discovered: bool = False,
    ) -> int:
        if not units:
            return 0

        enqueued = 0
        specs_total = len(units)
        specs_done = 0
        pages_scanned = await self._db_read(self.index_db.get_scanned_pages_count)
        independent_packing = isinstance(self.raw_archive, FightArchive)
        inline_pack = pack_as_discovered and self.scan_concurrency <= 1 and not independent_packing
        concurrent_pack = pack_as_discovered and (self.scan_concurrency > 1 or independent_packing)

        if inline_pack:
            await self.drain_queue_batch(
                batch_size=max(12, self.download_concurrency * 3),
                on_progress=on_progress,
                should_stop=should_stop,
            )

        work_queue: asyncio.Queue[Optional[Dict[str, Any]]] = asyncio.Queue()
        for unit in units:
            work_queue.put_nowait(unit)

        num_workers = max(1, min(self.scan_concurrency, len(units)))
        for _ in range(num_workers):
            work_queue.put_nowait(None)

        scan_finished = asyncio.Event()

        async def _scan_worker() -> None:
            nonlocal enqueued, specs_done, pages_scanned
            while True:
                unit = await work_queue.get()
                if unit is None:
                    return
                if should_stop and should_stop():
                    continue

                zone_id = int(unit["zone_id"])
                encounter_id = int(unit["encounter_id"])
                encounter_slug = str(unit["encounter_slug"])
                partition = int(unit["partition"])
                difficulty = int(unit["difficulty"])
                content_type = str(unit["content_type"])
                class_name = str(unit["class_name"])
                spec_name = str(unit["spec_name"])
                role = str(unit["role"])
                label_prefix = str(unit["label_prefix"])
                title = str(unit["title"])
                log_empty_page1 = bool(unit.get("log_empty_page1", False))

                exhausted_at = self.index_db.get_exhausted_page(
                    zone_id=zone_id,
                    encounter_id=encounter_id,
                    partition=partition,
                    class_name=class_name,
                    spec_name=spec_name,
                    difficulty=difficulty,
                )

                page = 1
                while True:
                    if should_stop and should_stop():
                        return
                    if pages_per_spec > 0 and page > pages_per_spec:
                        break
                    if exhausted_at is not None and page >= exhausted_at:
                        break
                    if self.index_db.is_page_scanned(
                        zone_id=zone_id,
                        encounter_id=encounter_id,
                        partition=partition,
                        class_name=class_name,
                        spec_name=spec_name,
                        page=page,
                        difficulty=difficulty,
                    ):
                        page += 1
                        continue

                    if on_progress:
                        on_progress(
                            {
                                "current_task": (
                                    f"[{label_prefix}] {title} — {class_name} ({spec_name}) | "
                                    f"Page {page}{'/' + str(pages_per_spec) if pages_per_spec > 0 else ' (ALL)'}"
                                ),
                                "specs_done": specs_done,
                                "specs_total": specs_total,
                                "pages_scanned": pages_scanned,
                                "log": f"Scanning {label_prefix}: {title} | {class_name} {spec_name} (Page {page})...",
                            }
                        )

                    refs: Optional[List[Dict[str, Any]]] = None
                    last_scan_exc: Optional[Exception] = None
                    for scan_attempt in range(3):
                        if should_stop and should_stop():
                            return
                        try:
                            refs = await self._fetch_rankings_refs(
                                zone_id=zone_id,
                                encounter_id=encounter_id,
                                partition=partition,
                                class_name=class_name,
                                spec_name=spec_name,
                                page=page,
                                difficulty=difficulty if difficulty > 0 else None,
                            )
                            last_scan_exc = None
                            break
                        except Exception as exc:
                            last_scan_exc = exc
                            if should_stop and should_stop():
                                return
                            if scan_attempt < 2:
                                wait_s = self._scan_retry_base_delay * (scan_attempt + 1)
                                if on_progress:
                                    on_progress(
                                        {
                                            "log": (
                                                f"Scan retry {scan_attempt + 1}/2 for {title} | "
                                                f"{class_name} {spec_name} (Page {page}): {exc}"
                                            )
                                        }
                                    )
                                await asyncio.sleep(wait_s)

                    if last_scan_exc is not None or refs is None:
                        self.index_db.mark_page_failed(
                            zone_id=zone_id,
                            encounter_id=encounter_id,
                            partition=partition,
                            class_name=class_name,
                            spec_name=spec_name,
                            page=page,
                            difficulty=difficulty,
                            reason=str(last_scan_exc or "Rankings page returned no result"),
                            attempts=3,
                        )
                        if on_progress:
                            on_progress(
                                {
                                    "log": (
                                        f"WARNING: Scan failed on {title} | {class_name} {spec_name} "
                                        f"(Page {page}): {last_scan_exc}"
                                    )
                                }
                            )
                        # The exhausted page is durable and will make the job incomplete.
                        # Continue other scopes while unrelated downloads finish normally.
                        break

                    pages_scanned += 1
                    # Browser pages retain visible rows before report/fight deduplication.
                    # Plain protocol lists use their length when no page metadata exists.
                    row_count = int(getattr(refs, "row_count", len(refs)))
                    has_next = getattr(refs, "has_next", None)
                    terminal_page = not refs or row_count < 70 or has_next is False

                    if not refs:
                        self.index_db.mark_page_scanned(
                            zone_id=zone_id,
                            encounter_id=encounter_id,
                            partition=partition,
                            class_name=class_name,
                            spec_name=spec_name,
                            page=page,
                            refs_found=row_count,
                            is_exhausted=True,
                            difficulty=difficulty,
                        )
                        if on_progress and page == 1 and log_empty_page1:
                            on_progress(
                                {
                                    "pages_scanned": pages_scanned,
                                    "log": f"No rankings returned for {title} | {class_name} {spec_name}.",
                                }
                            )
                        break

                    page_enqueued = 0
                    for r in refs:
                        rep = r.get("report") or {}
                        code = rep.get("code") or r.get("reportID")
                        fid = rep.get("fightID") or r.get("fightID")
                        if not code or not fid:
                            continue
                        keystone_level = (
                            int(r.get("bracketData", 0) or r.get("keystoneLevel", 0) or 0)
                            if content_type == "mplus"
                            else 0
                        )
                        affixes = r.get("affixes", []) if content_type == "mplus" else None
                        inserted = self.index_db.enqueue_fight(
                            report_code=str(code),
                            fight_id=int(fid),
                            content_type=content_type,
                            zone_id=zone_id,
                            partition=partition,
                            encounter_id=encounter_id,
                            encounter_slug=encounter_slug,
                            keystone_level=keystone_level,
                            difficulty=difficulty,
                            affixes=affixes,
                            duration_ms=int(r.get("duration", 0) or 0),
                            participant={
                                "name": r.get("name", "Player"),
                                "class": class_name,
                                "spec": spec_name,
                                "role": role,
                            },
                        )
                        if inserted:
                            enqueued += 1
                            page_enqueued += 1

                    self.index_db.mark_page_scanned(
                        zone_id=zone_id,
                        encounter_id=encounter_id,
                        partition=partition,
                        class_name=class_name,
                        spec_name=spec_name,
                        page=page,
                        refs_found=row_count,
                        is_exhausted=terminal_page,
                        difficulty=difficulty,
                    )

                    if on_progress:
                        on_progress(
                            {
                                "pages_scanned": pages_scanned,
                                "log": (
                                    f"Found {row_count} ranking rows ({len(refs)} unique references) on page {page} "
                                    f"for {title} [{label_prefix}] ({class_name} {spec_name}) "
                                    f"-> +{page_enqueued} new fights queued."
                                ),
                            }
                        )

                    if inline_pack and page_enqueued > 0:
                        await self.drain_queue_batch(
                            batch_size=max(12, self.download_concurrency * 3),
                            on_progress=on_progress,
                            should_stop=should_stop,
                        )

                    if terminal_page:
                        break
                    page += 1

                specs_done += 1
                if on_progress:
                    on_progress({"specs_done": specs_done, "specs_total": specs_total})

        async def _concurrent_drain_loop() -> None:
            while True:
                if should_stop and should_stop():
                    return
                packed = await self.drain_queue_batch(
                    batch_size=max(25, self.download_concurrency * 6),
                    on_progress=on_progress,
                    should_stop=should_stop,
                    continuous=True,
                )
                if scan_finished.is_set() and packed == 0:
                    counts = await self._db_read(self.index_db.get_queue_counts)
                    if not any(counts[key] for key in (
                        "pending_reports", "queued_fights", "retryable_reports", "retryable_fights", "staged_fights"
                    )) and not (getattr(self.client, 'requires_browser_enrichment', False)
                                and counts['api_staged_fights']):
                        return
                if packed == 0:
                    await asyncio.sleep(0.5)

        async def _run_scans() -> None:
            workers = [asyncio.create_task(_scan_worker()) for _ in range(num_workers)]
            try:
                await asyncio.gather(*workers)
            finally:
                scan_finished.set()
                for worker in workers:
                    if not worker.done():
                        worker.cancel()
                await asyncio.gather(*workers, return_exceptions=True)

        if concurrent_pack:
            tasks = [asyncio.create_task(_run_scans()), asyncio.create_task(_concurrent_drain_loop())]
            try:
                await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        else:
            await _run_scans()

        return enqueued

    async def discover_and_enqueue_mplus_s5(
        self,
        pages_per_spec: int = 0,
        on_progress: Optional[Callable[[Dict[str, Any]], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        pack_as_discovered: bool = False,
    ) -> int:
        units: List[Dict[str, Any]] = []
        for enc_id, slug in LEGION_MPLUS_S5_DUNGEONS.items():
            dungeon_title = slug.replace("-", " ").title()
            for class_name, spec_name, role in LEGION_SPECS:
                units.append(
                    {
                        "zone_id": LEGION_MPLUS_ZONE_ID,
                        "encounter_id": enc_id,
                        "encounter_slug": slug,
                        "partition": LEGION_MPLUS_S5_PARTITION,
                        "difficulty": 0,
                        "content_type": "mplus",
                        "class_name": class_name,
                        "spec_name": spec_name,
                        "role": role,
                        "label_prefix": "M+ S5",
                        "title": dungeon_title,
                        "log_empty_page1": True,
                    }
                )
        return await self._run_discovery_pool(
            units=units,
            pages_per_spec=pages_per_spec,
            on_progress=on_progress,
            should_stop=should_stop,
            pack_as_discovered=pack_as_discovered,
        )

    async def discover_and_enqueue_raids(
        self,
        pages_per_spec: int = 0,
        difficulties: Optional[List[int]] = None,
        on_progress: Optional[Callable[[Dict[str, Any]], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        pack_as_discovered: bool = False,
    ) -> int:
        if should_stop and should_stop():
            return 0
        target_diffs = [
            int(d)
            for d in (difficulties or [5, 4, 3, 1])
            if int(d) in LEGION_RAID_DIFFICULTIES
        ]
        if not target_diffs:
            target_diffs = [5, 4, 3, 1]

        units: List[Dict[str, Any]] = []
        for zone_id, raid_info in LEGION_RAIDS.items():
            raid_name = raid_info.get("name", f"Zone {zone_id}")
            for partition in raid_info.get("partitions", [1]):
                for diff_id in target_diffs:
                    diff_name = LEGION_RAID_DIFFICULTIES.get(diff_id, f"Diff {diff_id}")
                    for boss_id, boss_slug in raid_info.get("bosses", {}).items():
                        boss_title = boss_slug.replace("-", " ").title()
                        for class_name, spec_name, role in LEGION_SPECS:
                            units.append(
                                {
                                    "zone_id": zone_id,
                                    "encounter_id": boss_id,
                                    "encounter_slug": boss_slug,
                                    "partition": partition,
                                    "difficulty": diff_id,
                                    "content_type": "raid",
                                    "class_name": class_name,
                                    "spec_name": spec_name,
                                    "role": role,
                                    "label_prefix": f"{raid_name} - {diff_name}",
                                    "title": boss_title,
                                    "log_empty_page1": False,
                                }
                            )
        return await self._run_discovery_pool(
            units=units,
            pages_per_spec=pages_per_spec,
            on_progress=on_progress,
            should_stop=should_stop,
            pack_as_discovered=pack_as_discovered,
        )

    async def drain_queue_batch(
        self,
        batch_size: int = 25,
        on_progress: Optional[Callable[[Dict[str, Any]], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        continuous: bool = False,
    ) -> int:
        self._raise_background_failure()
        if should_stop and should_stop():
            return 0
        if continuous and isinstance(self.raw_archive, FightArchive):
            return await self._continuous_download_step(on_progress, should_stop)
        await self.warm_unarchive_batch(limit=batch_size, should_stop=should_stop)
        if should_stop and should_stop():
            return 0
        pending = await self._db_read(self.index_db.get_unarchived_pending_fights, limit=batch_size)
        if should_stop and should_stop():
            return 0
        if not pending:
            return 0
        if isinstance(self.raw_archive, FightArchive):
            return await self._download_rows_to_staging(pending, on_progress, should_stop)
        packed, _ = await self._process_rows_pipeline(
            pending, on_progress=on_progress, should_stop=should_stop
        )
        return packed

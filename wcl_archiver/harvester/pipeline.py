import asyncio
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from wcl_archiver.codec.wclz_format import WCLZWriter
from wcl_archiver.codec.fight_archive import FightArchive
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
        await future
        raise


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
    ):
        self.data_dir = Path(data_dir)
        self.index_db = index_db
        self.master_db = master_db
        self.client = protocol_client
        self.raw_archive = raw_archive
        self.zstd_level = int(zstd_level)
        self.download_concurrency = max(1, int(download_concurrency))
        default_pack = max(2, min(6, os.cpu_count() or 4))
        self.pack_workers = max(1, int(pack_workers if pack_workers is not None else default_pack))
        self.scan_concurrency = max(1, int(scan_concurrency))
        self._master_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._master_cache_limit = 128
        self._master_inflight: Dict[str, asyncio.Task[Dict[str, Any]]] = {}
        self._seen_spells: Dict[int, str] = {}
        self._scan_retry_base_delay: float = 2.0
        self._pack_executor: Optional[ThreadPoolExecutor] = None

    def _get_pack_executor(self) -> ThreadPoolExecutor:
        if self._pack_executor is None:
            self._pack_executor = ThreadPoolExecutor(
                max_workers=self.pack_workers,
                thread_name_prefix="wclz-pack",
            )
        return self._pack_executor

    def close(self) -> None:
        if self._pack_executor is not None:
            self._pack_executor.shutdown(wait=True)
            self._pack_executor = None
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
        codes = self.index_db.get_pending_reports_for_unarchive(limit=limit)
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

        packed_paths: List[Path] = []
        while True:
            fight_rows = [
                row for row in self.index_db.get_unarchived_pending_fights(limit=100, report_code=report_code)
                if int(row["fight_id"]) in target_fight_ids
            ]
            if not fight_rows:
                break
            _, batch_paths = await self._process_rows_pipeline(fight_rows)
            packed_paths.extend(batch_paths)
        failed_rows = [
            row for row in self.index_db.search_fights(report_code=report_code, limit=max(100, len(target_fight_ids)))
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
        pages_scanned = self.index_db.get_scanned_pages_count()
        inline_pack = pack_as_discovered and self.scan_concurrency <= 1
        concurrent_pack = pack_as_discovered and self.scan_concurrency > 1

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
                )
                if scan_finished.is_set() and packed == 0:
                    counts = self.index_db.get_queue_counts()
                    if not any(counts[key] for key in (
                        "pending_reports", "queued_fights", "retryable_reports", "retryable_fights"
                    )):
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
    ) -> int:
        if should_stop and should_stop():
            return 0
        await self.warm_unarchive_batch(limit=batch_size, should_stop=should_stop)
        if should_stop and should_stop():
            return 0
        pending = self.index_db.get_unarchived_pending_fights(limit=batch_size)
        if not pending:
            return 0
        packed, _ = await self._process_rows_pipeline(
            pending, on_progress=on_progress, should_stop=should_stop
        )
        return packed

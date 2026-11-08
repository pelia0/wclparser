from pathlib import Path
from typing import Any, Dict, List
from wcl_archiver.codec.wclz_format import WCLZWriter
from wcl_archiver.config import (
    LEGION_MPLUS_S5_DUNGEONS,
    LEGION_MPLUS_S5_PARTITION,
    LEGION_MPLUS_ZONE_ID,
    LEGION_RAIDS,
    LEGION_SPECS,
)
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.parser.combat_state_machine import CombatStateMachine
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient


class HarvesterPipeline:
    def __init__(
        self,
        data_dir: Path,
        index_db: IndexDB,
        master_db: MasterDictDB,
        protocol_client: WCLProtocolClient,
    ):
        self.data_dir = Path(data_dir)
        self.index_db = index_db
        self.master_db = master_db
        self.client = protocol_client

    async def warm_unarchive_batch(self, limit: int = 25) -> int:
        codes = self.index_db.get_pending_reports_for_unarchive(limit=limit)
        warmed = 0
        for code in codes:
            ok = await self.client.unarchive_report(code)
            if ok:
                self.index_db.mark_report_unarchived(code, "UNARCHIVED")
                warmed += 1
        return warmed

    async def process_single_fight(self, fight_row: Dict[str, Any]) -> Path:
        report_code = fight_row["report_code"]
        fight_id = int(fight_row["fight_id"])
        encounter_id = int(fight_row["encounter_id"])
        encounter_slug = fight_row["encounter_slug"]
        content_type = fight_row["content_type"]

        master = await self.client.fetch_report_master_and_fights(report_code)
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
        for ab in abilities:
            if "gameID" in ab:
                self.master_db.upsert_spell(
                    int(ab["gameID"]),
                    str(ab.get("name", "")),
                    int(ab.get("type", 1) or 1),
                    str(ab.get("icon", "")),
                )

        actors: List[Dict[str, Any]] = []
        for group_key in ("friendlies", "enemies", "friendlyPets", "enemyPets"):
            for actor in master.get(group_key, []):
                actors.append(actor)

        raw_events = await self.client.fetch_all_fight_events(
            report_code, fight_id, start_time, end_time
        )

        maps = target_fight.get("maps") or [0]
        map_id = int(maps[0]["id"] if isinstance(maps[0], dict) else maps[0])
        fight_meta = {
            "report_code": report_code,
            "fight_id": fight_id,
            "start_time": start_time,
            "end_time": end_time,
            "encounter_id": encounter_id,
            "name": target_fight.get("name", encounter_slug),
            "keystone_level": int(target_fight.get("keystoneLevel", fight_row.get("keystone_level", 0)) or 0),
            "affixes": target_fight.get("affixes", []),
            "map_id": map_id,
            "pulls": target_fight.get("dungeonPulls", []),
        }

        summary, norm_events, replay = CombatStateMachine.analyze_fight(
            fight_meta, actors, abilities, raw_events
        )

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
        comp_bytes = WCLZWriter.write_fight(out_path, summary, norm_events, replay, zstd_level=22)

        rel_path = out_path.relative_to(self.data_dir).as_posix()
        self.index_db.mark_fight_packed(
            report_code=report_code,
            fight_id=fight_id,
            file_rel_path=rel_path,
            compressed_bytes=comp_bytes,
            event_count=len(norm_events),
        )
        return out_path

    async def import_report_by_code(
        self,
        report_code: str,
        content_type: str = "mplus",
        zone_id: int = 9,
        partition: int = 5,
    ) -> List[Path]:
        await self.client.unarchive_report(report_code)
        self.index_db.mark_report_unarchived(report_code, "UNARCHIVED")
        master = await self.client.fetch_report_master_and_fights(report_code)
        packed_paths: List[Path] = []
        for f in master.get("fights", []):
            boss_id = int(f.get("boss", 0) or 0)
            if boss_id <= 0:
                continue
            fight_id = int(f["id"])
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
            fight_rows = [
                r
                for r in self.index_db.get_unarchived_pending_fights(limit=100)
                if r["report_code"] == report_code and int(r["fight_id"]) == fight_id
            ]
            if fight_rows:
                p = await self.process_single_fight(fight_rows[0])
                packed_paths.append(p)
        return packed_paths

    async def discover_and_enqueue_mplus_s5(self, pages_per_spec: int = 1) -> int:
        enqueued = 0
        for enc_id, slug in LEGION_MPLUS_S5_DUNGEONS.items():
            for class_name, spec_name, role in LEGION_SPECS:
                for page in range(1, pages_per_spec + 1):
                    refs = await self.client.fetch_rankings_fight_refs(
                        zone_id=LEGION_MPLUS_ZONE_ID,
                        encounter_id=enc_id,
                        partition=LEGION_MPLUS_S5_PARTITION,
                        class_name=class_name,
                        spec_name=spec_name,
                        page=page,
                    )
                    for r in refs:
                        rep = r.get("report") or {}
                        code = rep.get("code") or r.get("reportID")
                        fid = rep.get("fightID") or r.get("fightID")
                        if not code or not fid:
                            continue
                        inserted = self.index_db.enqueue_fight(
                            report_code=str(code),
                            fight_id=int(fid),
                            content_type="mplus",
                            zone_id=LEGION_MPLUS_ZONE_ID,
                            partition=LEGION_MPLUS_S5_PARTITION,
                            encounter_id=enc_id,
                            encounter_slug=slug,
                            keystone_level=int(r.get("bracketData", 0) or r.get("keystoneLevel", 0) or 0),
                            affixes=r.get("affixes", []),
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
        return enqueued

    async def discover_and_enqueue_raids(self, pages_per_spec: int = 1) -> int:
        enqueued = 0
        for zone_id, raid_info in LEGION_RAIDS.items():
            for partition in raid_info.get("partitions", [1]):
                for boss_id, boss_slug in raid_info.get("bosses", {}).items():
                    for class_name, spec_name, role in LEGION_SPECS:
                        for page in range(1, pages_per_spec + 1):
                            refs = await self.client.fetch_rankings_fight_refs(
                                zone_id=zone_id,
                                encounter_id=boss_id,
                                partition=partition,
                                class_name=class_name,
                                spec_name=spec_name,
                                page=page,
                            )
                            for r in refs:
                                rep = r.get("report") or {}
                                code = rep.get("code") or r.get("reportID")
                                fid = rep.get("fightID") or r.get("fightID")
                                if not code or not fid:
                                    continue
                                inserted = self.index_db.enqueue_fight(
                                    report_code=str(code),
                                    fight_id=int(fid),
                                    content_type="raid",
                                    zone_id=zone_id,
                                    partition=partition,
                                    encounter_id=boss_id,
                                    encounter_slug=boss_slug,
                                    difficulty=5,
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
        return enqueued

    async def drain_queue_batch(self, batch_size: int = 25) -> int:
        await self.warm_unarchive_batch(limit=batch_size)
        pending = self.index_db.get_unarchived_pending_fights(limit=batch_size)
        packed = 0
        for row in pending:
            try:
                await self.process_single_fight(row)
                packed += 1
            except Exception:
                continue
        return packed

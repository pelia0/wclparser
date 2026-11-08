import asyncio
import re
from pathlib import Path
from typing import Any, Dict, Optional
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from wcl_archiver.codec.wclz_format import WCLZReader
from wcl_archiver.config import (
    BASE_DIR,
    DATA_DIR,
    LEGION_MPLUS_S5_DUNGEONS,
    LEGION_MPLUS_S5_PARTITION,
    LEGION_MPLUS_ZONE_ID,
    LEGION_RAIDS,
    LEGION_SPECS,
)
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.harvester.pipeline import HarvesterPipeline
from wcl_archiver.wcl_client.auth import WCLAuthManager
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient

REPORT_CODE_RE = re.compile(r"(?:reports/)?([A-Za-z0-9]{10,20})")


class LoginRequest(BaseModel):
    email: str = ""
    password: str = ""
    client_id: str = ""
    client_secret: str = ""
    wcl_session_cookie: str = ""
    xsrf_token: str = ""


class ImportReportRequest(BaseModel):
    report: str
    content_type: str = "mplus"


class StartHarvestRequest(BaseModel):
    mode: str = "mplus_s5"
    pages_per_spec: int = 1


def create_app(data_dir: Path = DATA_DIR) -> FastAPI:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    index_db = IndexDB(data_dir / "index.db")
    master_db = MasterDictDB(data_dir / "master_dict.db")
    auth_manager = WCLAuthManager(data_dir / "auth_session.json")
    protocol_client = WCLProtocolClient(auth_manager=auth_manager)
    harvester = HarvesterPipeline(
        data_dir=data_dir,
        index_db=index_db,
        master_db=master_db,
        protocol_client=protocol_client,
    )
    harvester_state: Dict[str, Any] = {"running": False, "current_task": "Idle", "last_error": ""}

    app = FastAPI(title="WCL Legion Archiver & Offline Viewer")

    @app.get("/api/meta/catalog")
    def get_catalog() -> Dict[str, Any]:
        return {
            "mplus_zone_id": LEGION_MPLUS_ZONE_ID,
            "mplus_partition": LEGION_MPLUS_S5_PARTITION,
            "mplus_dungeons": [
                {"id": k, "slug": v, "name": v.replace("-", " ").title()}
                for k, v in LEGION_MPLUS_S5_DUNGEONS.items()
            ],
            "raids": LEGION_RAIDS,
            "specs": [
                {"class": c, "spec": s, "role": r} for c, s, r in LEGION_SPECS
            ],
        }

    @app.get("/api/auth/status")
    def get_auth_status() -> Dict[str, Any]:
        return auth_manager.get_auth_status()

    @app.post("/api/auth/login")
    async def post_login(req: LoginRequest) -> Dict[str, Any]:
        if req.wcl_session_cookie.strip():
            cookies = auth_manager.parse_cookie_input(req.wcl_session_cookie, req.xsrf_token)
            if req.client_id and req.client_secret:
                await auth_manager.login_with_credentials(
                    email="",
                    password="",
                    client_id=req.client_id,
                    client_secret=req.client_secret,
                )
            return auth_manager.save_session_cookies(
                cookies=cookies,
                xsrf_token=req.xsrf_token,
                email=req.email.strip() or "Browser Session",
                client_id=req.client_id,
                client_secret=req.client_secret,
            )
        return await auth_manager.login_with_credentials(
            email=req.email.strip(),
            password=req.password,
            client_id=req.client_id.strip(),
            client_secret=req.client_secret.strip(),
        )

    @app.get("/api/fights")
    def list_fights(
        content_type: Optional[str] = None,
        encounter_id: Optional[int] = None,
        class_name: Optional[str] = None,
        spec: Optional[str] = None,
        min_key_level: Optional[int] = None,
        limit: int = 100,
    ) -> Any:
        return index_db.search_fights(
            content_type=content_type,
            encounter_id=encounter_id,
            class_name=class_name,
            spec=spec,
            min_key_level=min_key_level,
            limit=limit,
        )

    def _resolve_fight_file(fight_uid: str) -> Path:
        with index_db._connect() as conn:
            row = conn.execute(
                "SELECT file_rel_path FROM fights WHERE fight_uid = ?",
                (fight_uid,),
            ).fetchone()
        if not row or not row["file_rel_path"]:
            raise HTTPException(status_code=404, detail=f"Fight {fight_uid} is not packed on disk")
        file_path = data_dir / row["file_rel_path"]
        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"File {file_path} missing")
        return file_path

    @app.get("/api/fights/{fight_uid}/summary")
    def get_fight_summary(fight_uid: str) -> Dict[str, Any]:
        file_path = _resolve_fight_file(fight_uid)
        return WCLZReader.read_summary_only(file_path)

    @app.get("/api/fights/{fight_uid}/replay")
    def get_fight_replay(fight_uid: str) -> Dict[str, Any]:
        file_path = _resolve_fight_file(fight_uid)
        return WCLZReader.read_replay_only(file_path)

    @app.get("/api/fights/{fight_uid}/events")
    def get_fight_events(
        fight_uid: str,
        t_start: int = 0,
        t_end: Optional[int] = None,
        actor_id: Optional[int] = None,
    ) -> Any:
        file_path = _resolve_fight_file(fight_uid)
        events = WCLZReader.read_events_only(file_path)
        filtered = [
            e
            for e in events
            if e.get("timestamp", 0) >= t_start
            and (t_end is None or e.get("timestamp", 0) <= t_end)
            and (actor_id is None or e.get("sourceID") == actor_id or e.get("targetID") == actor_id)
        ]
        return {"count": len(filtered), "events": filtered[:5000]}

    @app.get("/api/harvester/status")
    def get_harvester_status() -> Dict[str, Any]:
        with index_db._connect() as conn:
            queued = conn.execute("SELECT COUNT(*) AS c FROM fights WHERE status = 'QUEUED'").fetchone()["c"]
            packed = conn.execute("SELECT COUNT(*) AS c FROM fights WHERE status = 'PACKED'").fetchone()["c"]
            total_bytes = conn.execute(
                "SELECT COALESCE(SUM(compressed_bytes), 0) AS b FROM fights WHERE status = 'PACKED'"
            ).fetchone()["b"]
        return {
            **harvester_state,
            "queued_fights": int(queued),
            "packed_fights": int(packed),
            "compressed_mb": round(int(total_bytes) / (1024 * 1024), 2),
        }

    @app.post("/api/harvester/import-report")
    async def post_import_report(req: ImportReportRequest) -> Dict[str, Any]:
        m = REPORT_CODE_RE.search(req.report.strip())
        if not m:
            raise HTTPException(status_code=400, detail="Invalid WarcraftLogs report code or URL")
        code = m.group(1)
        harvester_state["running"] = True
        harvester_state["current_task"] = f"Importing report {code}"
        try:
            paths = await harvester.import_report_by_code(code, content_type=req.content_type)
            harvester_state["current_task"] = f"Imported {len(paths)} fights from {code}"
            return {"report_code": code, "packed_count": len(paths), "files": [p.name for p in paths]}
        except Exception as exc:
            harvester_state["last_error"] = str(exc)
            raise HTTPException(status_code=500, detail=str(exc))
        finally:
            harvester_state["running"] = False

    async def _background_harvest_worker(mode: str, pages_per_spec: int) -> None:
        harvester_state["running"] = True
        harvester_state["last_error"] = ""
        try:
            if mode == "mplus_s5":
                harvester_state["current_task"] = "Scanning Mythic+ Season 5 (7.3.5) rankings..."
                await harvester.discover_and_enqueue_mplus_s5(pages_per_spec=pages_per_spec)
            elif mode == "raids":
                harvester_state["current_task"] = "Scanning Legion Raids (All 5) rankings..."
                await harvester.discover_and_enqueue_raids(pages_per_spec=pages_per_spec)

            while harvester_state["running"]:
                harvester_state["current_task"] = "Unarchiving & packing queued fights (.wclz Level 22)..."
                packed = await harvester.drain_queue_batch(batch_size=25)
                if packed == 0:
                    break
                await asyncio.sleep(0.5)
            harvester_state["current_task"] = "Harvest batch completed"
        except Exception as exc:
            harvester_state["last_error"] = str(exc)
            harvester_state["current_task"] = "Stopped due to error"
        finally:
            harvester_state["running"] = False

    @app.post("/api/harvester/start")
    async def post_start_harvest(req: StartHarvestRequest, bg: BackgroundTasks) -> Dict[str, Any]:
        if not harvester_state["running"]:
            bg.add_task(_background_harvest_worker, req.mode, req.pages_per_spec)
        return get_harvester_status()

    @app.post("/api/harvester/stop")
    def post_stop_harvest() -> Dict[str, Any]:
        harvester_state["running"] = False
        harvester_state["current_task"] = "Stopping..."
        return get_harvester_status()

    web_dir = BASE_DIR / "wcl_archiver" / "web"
    if web_dir.exists():
        app.mount("/", StaticFiles(directory=str(web_dir), html=True), name="web")

    return app

import asyncio
from datetime import datetime
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException
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
    pages_per_spec: int = 0


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
    harvester_logs: List[str] = []
    harvester_state: Dict[str, Any] = {
        "running": False,
        "current_task": "Idle",
        "last_error": "",
        "specs_done": 0,
        "specs_total": 0,
        "pages_scanned": 0,
        "progress_pct": 0.0,
    }

    def _append_log(msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        harvester_logs.append(f"[{ts}] {msg}")
        if len(harvester_logs) > 40:
            del harvester_logs[:-40]

    def _on_progress(update: Dict[str, Any]) -> None:
        if "current_task" in update:
            harvester_state["current_task"] = update["current_task"]
        if "specs_done" in update:
            harvester_state["specs_done"] = int(update["specs_done"])
        if "specs_total" in update:
            harvester_state["specs_total"] = int(update["specs_total"])
        if "pages_scanned" in update:
            harvester_state["pages_scanned"] = int(update["pages_scanned"])
        total = harvester_state.get("specs_total", 0)
        done = harvester_state.get("specs_done", 0)
        harvester_state["progress_pct"] = round(done * 100.0 / total, 1) if total > 0 else 0.0
        if "log" in update and update["log"]:
            _append_log(str(update["log"]))

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
            "recent_logs": list(harvester_logs),
        }

    @app.post("/api/harvester/import-report")
    async def post_import_report(req: ImportReportRequest) -> Dict[str, Any]:
        m = REPORT_CODE_RE.search(req.report.strip())
        if not m:
            raise HTTPException(status_code=400, detail="Invalid WarcraftLogs report code or URL")
        code = m.group(1)
        harvester_state["running"] = True
        harvester_state["last_error"] = ""
        harvester_state["current_task"] = f"Importing report {code}..."
        _append_log(f"Starting single report import for {code}...")
        try:
            paths = await harvester.import_report_by_code(code, content_type=req.content_type)
            harvester_state["current_task"] = f"Imported {len(paths)} fights from {code}"
            _append_log(f"Imported and packed {len(paths)} fights from report {code}.")
            return {"report_code": code, "packed_count": len(paths), "files": [p.name for p in paths]}
        except Exception as exc:
            harvester_state["last_error"] = str(exc)
            harvester_state["current_task"] = "Import failed"
            _append_log(f"ERROR importing {code}: {exc}")
            raise HTTPException(status_code=500, detail=str(exc))
        finally:
            harvester_state["running"] = False

    async def _background_harvest_worker(mode: str, pages_per_spec: int) -> None:
        should_stop = lambda: not harvester_state["running"]
        try:
            if mode == "mplus_s5":
                await harvester.discover_and_enqueue_mplus_s5(
                    pages_per_spec=pages_per_spec,
                    on_progress=_on_progress,
                    should_stop=should_stop,
                    pack_as_discovered=True,
                )
            elif mode == "raids":
                await harvester.discover_and_enqueue_raids(
                    pages_per_spec=pages_per_spec,
                    on_progress=_on_progress,
                    should_stop=should_stop,
                    pack_as_discovered=True,
                )

            while harvester_state["running"]:
                harvester_state["current_task"] = "Unarchiving & packing remaining queued fights (.wclz Level 22)..."
                packed = await harvester.drain_queue_batch(
                    batch_size=25, on_progress=_on_progress, should_stop=should_stop
                )
                if packed == 0:
                    break
                await asyncio.sleep(0.2)

            if harvester_state["running"]:
                harvester_state["current_task"] = "Harvest completed"
                harvester_state["progress_pct"] = 100.0
                _append_log("Harvest completed successfully.")
            else:
                harvester_state["current_task"] = "Stopped by user"
                _append_log("Harvester stopped by user.")
        except Exception as exc:
            harvester_state["last_error"] = str(exc)
            harvester_state["current_task"] = f"Error: {exc}"
            _append_log(f"ERROR: {exc}")
        finally:
            harvester_state["running"] = False

    @app.post("/api/harvester/start")
    async def post_start_harvest(req: StartHarvestRequest) -> Dict[str, Any]:
        if not harvester_state["running"]:
            pages_desc = "ALL pages (0)" if req.pages_per_spec <= 0 else f"{req.pages_per_spec} page(s)/spec"
            harvester_state["running"] = True
            harvester_state["last_error"] = ""
            harvester_state["specs_done"] = 0
            harvester_state["specs_total"] = 468 if req.mode == "mplus_s5" else 1440
            harvester_state["pages_scanned"] = 0
            harvester_state["progress_pct"] = 0.0
            harvester_state["current_task"] = f"Starting {req.mode} harvest ({pages_desc})..."
            _append_log(f"Started {req.mode} harvest ({pages_desc}).")
            asyncio.create_task(_background_harvest_worker(req.mode, req.pages_per_spec))
        return get_harvester_status()

    @app.post("/api/harvester/stop")
    def post_stop_harvest() -> Dict[str, Any]:
        harvester_state["running"] = False
        harvester_state["current_task"] = "Stopped"
        _append_log("Stop requested by user.")
        return get_harvester_status()

    web_dir = BASE_DIR / "wcl_archiver" / "web"
    if web_dir.exists():
        app.mount("/", StaticFiles(directory=str(web_dir), html=True), name="web")

    return app

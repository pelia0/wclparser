import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime
import re
import time
import json
import os
import tempfile
import secrets
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urlsplit
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import Response, FileResponse, HTMLResponse, RedirectResponse
from pydantic import BaseModel, conint

from wcl_archiver.codec.wclz_format import WCLZReader
from wcl_archiver.codec.raw_archive import RawArchive, SourceFightReader
from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader
from wcl_archiver.config import (
    AppConfig,
    BASE_DIR,
    DATA_DIR,
    LEGION_MPLUS_S5_DUNGEONS,
    LEGION_MPLUS_S5_PARTITION,
    LEGION_MPLUS_ZONE_ID,
    LEGION_RAID_DIFFICULTIES,
    LEGION_RAIDS,
    LEGION_SPECS,
)
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.harvester.pipeline import HarvesterPipeline
from wcl_archiver.wcl_client.auth import WCLAuthManager
from wcl_archiver.wcl_client.browser_client import BrowserClient
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient
from wcl_archiver.wcl_client.hybrid_client import HybridClient

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
    rankings_source: str = "browser"


DownloadConcurrency = conint(strict=True, ge=1)


class HarvesterSettingsRequest(BaseModel):
    download_concurrency: DownloadConcurrency


class StartHarvestRequest(BaseModel):
    mode: str = "mplus_s5"
    pages_per_spec: int = 2
    rankings_source: str = "browser"
    raid_difficulty: int = 0
    download_concurrency: Optional[DownloadConcurrency] = None


def create_app(
    data_dir: Path = DATA_DIR,
    rankings_source: str = "browser",
    error_restart_base_delay: float = 5.0,
) -> FastAPI:
    cfg = AppConfig()
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    settings_path = data_dir / 'harvester_settings.json'
    settings_error = ''
    try:
        saved_settings = json.loads(settings_path.read_text(encoding='utf-8'))
        cfg.download_concurrency = HarvesterSettingsRequest.model_validate(saved_settings).download_concurrency
    except FileNotFoundError:
        pass
    except (ValueError, TypeError, OSError) as exc:
        settings_error = f'Invalid harvester settings; using {cfg.download_concurrency} download workers: {exc}'
    index_db = IndexDB(data_dir / "index.db")
    master_db = MasterDictDB(data_dir / "master_dict.db")
    auth_manager = WCLAuthManager(data_dir / "auth_session.json")
    raw_archive = FightArchive(data_dir, compression=cfg.archive_compression)
    protocol_client = WCLProtocolClient(auth_manager=auth_manager,raw_archive=raw_archive)
    browser_client_ref: Dict[str, Optional[BrowserClient]] = {"instance": None}
    hybrid_browser_archive = FightArchive(data_dir, compression=cfg.archive_compression)
    hybrid_browser_ref: Dict[str, Optional[BrowserClient]] = {"instance": None}
    hybrid_client_ref = {"instance": None}
    harvester = HarvesterPipeline(
        data_dir=data_dir,
        index_db=index_db,
        master_db=master_db,
        protocol_client=protocol_client,
        zstd_level=cfg.zstd_level,
        download_concurrency=cfg.download_concurrency,
        pack_workers=cfg.pack_workers,
        scan_concurrency=cfg.scan_concurrency,
        raw_archive=raw_archive,
    )
    harvester_logs: List[str] = []
    harvester_state: Dict[str, Any] = {
        "running": False,
        "stopping": False,
        "current_task": "Idle",
        "last_error": "",
        "specs_done": 0,
        "specs_total": 0,
        "pages_scanned": 0,
        "progress_pct": 0.0,
        "rankings_source": rankings_source,
        "browser_state": "not_started",
        "browser_message": "Browser not started",
    }

    def _append_log(msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        harvester_logs.append(f"[{ts}] {msg}")
        if len(harvester_logs) > 40:
            del harvester_logs[:-40]

    if settings_error:
        _append_log(settings_error)

    settings_lock = asyncio.Lock()

    async def _apply_download_concurrency(value: int) -> None:
        async with settings_lock:
            temp_path = None
            try:
                with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=data_dir,
                                                 prefix='.harvester_settings-', suffix='.tmp', delete=False) as handle:
                    temp_path = Path(handle.name)
                    json.dump({'download_concurrency': value}, handle)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_path, settings_path)
            finally:
                if temp_path is not None:
                    temp_path.unlink(missing_ok=True)
            harvester.download_concurrency = value
            for reference in (browser_client_ref, hybrid_browser_ref):
                if reference['instance'] is not None:
                    await reference['instance'].set_request_concurrency(2 * value)
            _append_log(f'Download workers set to {value}; browser JSON request ceiling {2 * value}.')

    def _on_browser_status(b_state: str, b_msg: str) -> None:
        prev_state = harvester_state.get("browser_state")
        harvester_state["browser_state"] = b_state
        harvester_state["browser_message"] = b_msg
        if b_state in ("waiting_challenge", "waiting_login") and prev_state != b_state:
            harvester_state["current_task"] = b_msg
            _append_log(f"[Browser Action Required] {b_msg}")

    def _get_browser_client() -> BrowserClient:
        if browser_client_ref["instance"] is None:
            browser_client_ref["instance"] = BrowserClient(
                profile_dir=data_dir / "browser_profile",
                on_status=_on_browser_status,
                should_stop=lambda: not harvester_state["running"],
                max_concurrent_requests=2 * harvester.download_concurrency,
                scan_concurrency=cfg.scan_concurrency,
                raw_archive=raw_archive,
            )
        return browser_client_ref["instance"]

    def _get_hybrid_browser() -> BrowserClient:
        if hybrid_browser_ref['instance'] is None:
            hybrid_browser_ref['instance'] = BrowserClient(
                profile_dir=data_dir / 'browser_profile', on_status=_on_browser_status,
                should_stop=lambda: not harvester_state['running'],
                max_concurrent_requests=2 * harvester.download_concurrency,
                scan_concurrency=cfg.scan_concurrency, raw_archive=hybrid_browser_archive)
        return hybrid_browser_ref['instance']

    def _select_collection_client(source):
        if source == 'hybrid':
            if not auth_manager.get_auth_status().get('has_user_oauth_token'):
                raise HTTPException(status_code=409, detail='Connect your WCL account before using strict hybrid collection.')
            if hybrid_client_ref['instance'] is None:
                hybrid_client_ref['instance'] = HybridClient(protocol_client, _get_hybrid_browser(),
                    raw_archive, hybrid_browser_archive)
            return hybrid_client_ref['instance']
        return _get_browser_client() if source == 'browser' else protocol_client

    async def _close_collection_browsers():
        try:
            if browser_client_ref['instance'] is not None:
                await browser_client_ref['instance'].stop()
        finally:
            try:
                if hybrid_browser_ref['instance'] is not None:
                    await hybrid_browser_ref['instance'].stop()
            finally:
                browser_client_ref['instance'] = hybrid_browser_ref['instance'] = None
                hybrid_client_ref['instance'] = None
                harvester_state['browser_state'] = 'not_started'
                harvester_state['browser_message'] = 'Browser not started'

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

    job_task: Optional[asyncio.Task] = None
    stop_task: Optional[asyncio.Task] = None
    shutting_down = False
    starting_job = False

    def _require_idle() -> None:
        if shutting_down or starting_job or harvester_state["stopping"] or (job_task is not None and not job_task.done()):
            raise HTTPException(status_code=409, detail="A collection job is active or stopping")

    async def _run_job(work):
        cancelled = False
        try:
            await harvester.start_background_packing(on_progress=_on_progress)
            return await work
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            # A second stop/shutdown request cannot cut off atomic pack cleanup.
            cleanup = asyncio.create_task(harvester.finish_background_packing(cancel=cancelled))
            try:
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        cancelled = True
                await cleanup
            finally:
                if job_task is asyncio.current_task():
                    harvester_state["running"] = False
                    raw_archive.clear()
                    if hybrid_browser_ref['instance'] is not None:
                        hybrid_browser_archive.clear()
                if hasattr(work, 'close'):
                    work.close()

    async def _stop_job() -> None:
        nonlocal stop_task
        async def finish_stop():
            harvester_state["stopping"] = True
            harvester_state["running"] = False
            try:
                if job_task is not None and not job_task.done():
                    job_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await job_task
                harvester_state["current_task"] = "Stopped by user"
            finally:
                harvester_state["stopping"] = False
        if stop_task is None or stop_task.done():
            stop_task = asyncio.create_task(finish_stop())
        await asyncio.shield(stop_task)

    @asynccontextmanager
    async def lifespan(app):
        nonlocal shutting_down
        try:
            yield
        finally:
            shutting_down = True
            try:
                await _stop_job()
            finally:
                try:
                    await _close_collection_browsers()
                finally:
                    harvester.close()

    app = FastAPI(title="WCL Legion Archiver & Offline Viewer", lifespan=lifespan)

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
            "raid_difficulties": [
                {"id": k, "name": v} for k, v in LEGION_RAID_DIFFICULTIES.items()
            ],
            "specs": [
                {"class": c, "spec": s, "role": r} for c, s, r in LEGION_SPECS
            ],
        }

    @app.get("/api/auth/status")
    def get_auth_status() -> Dict[str, Any]:
        return auth_manager.get_auth_status()

    oauth_cookie = 'wcl_oauth_state'

    def _oauth_failure(message, status=400):
        response = HTMLResponse('<!doctype html><html lang="en"><meta charset="utf-8">'
            '<title>WCL authorization</title><h1>WCL authorization</h1><p>' + message +
            '</p><p><a href="/">Return to the archiver</a></p></html>', status_code=status,
            headers={'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'})
        response.delete_cookie(oauth_cookie, path='/', httponly=True, samesite='lax')
        return response

    @app.get('/api/auth/oauth/start')
    async def start_user_oauth(request: Request, redirect_uri: str = 'http://localhost:8080/'):
        try:
            target = urlsplit(redirect_uri)
            if (any(ord(c) <= 32 for c in redirect_uri) or '\\' in redirect_uri
                    or target.scheme != 'http' or target.hostname not in ('localhost', '127.0.0.1')
                    or target.username is not None or target.password is not None
                    or target.query or target.fragment
                    or target.path not in ('/', '/api/auth/oauth/callback')
                    or (target.port or 80) != (request.url.port or 80)):
                raise ValueError('Invalid loopback redirect')
            # Set the cookie on the callback's host even when the app was opened
            # using its other loopback alias. No authorization state exists yet.
            if target.hostname != request.url.hostname:
                return RedirectResponse(target._replace(path='/api/auth/oauth/start',
                    query=urlencode({'redirect_uri': redirect_uri})).geturl(), status_code=307)
            flow = auth_manager.begin_user_oauth(redirect_uri)
        except ValueError:
            return _oauth_failure('Save your Client ID and Secret, and use a loopback redirect URL on this server port.')
        response = RedirectResponse(flow['authorization_url'], status_code=303,
            headers={'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'})
        response.set_cookie(oauth_cookie, flow['state'], max_age=flow['expires_in'],
                            httponly=True, samesite='lax', path='/')
        return response

    async def _complete_user_oauth(request: Request):
        query = request.query_params
        state, cookie = query.get('state', ''), request.cookies.get(oauth_cookie, '')
        if (any(len(query.getlist(name)) > 1 for name in ('state', 'code', 'error'))
                or not state or not cookie or not secrets.compare_digest(state, cookie)):
            return _oauth_failure('Authorization state is missing or invalid. Start Connect WCL account again.')
        try:
            if query.get('error'):
                auth_manager.cancel_user_oauth(state)
                return _oauth_failure('WCL authorization was declined. Your existing login is unchanged.')
            if not query.get('code'):
                return _oauth_failure('WCL did not return an authorization code. Connect again.')
            await auth_manager.complete_user_oauth(query['code'], state)
        except ValueError:
            return _oauth_failure('This authorization has expired or was already used. Connect again.')
        except RuntimeError:
            return _oauth_failure('WCL could not complete the token exchange. Check the registered redirect URL and reconnect.', 502)
        response = RedirectResponse('/?oauth=connected', status_code=303,
            headers={'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'})
        response.delete_cookie(oauth_cookie, path='/', httponly=True, samesite='lax')
        return response

    @app.get('/api/auth/oauth/callback')
    async def user_oauth_callback(request: Request):
        return await _complete_user_oauth(request)

    @app.get('/')
    async def root_or_oauth_callback(request: Request):
        if any(name in request.query_params for name in ('code', 'state', 'error')):
            return await _complete_user_oauth(request)
        return FileResponse(BASE_DIR / 'wcl_archiver' / 'web' / 'index.html',
                            headers={'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'})

    @app.post('/api/auth/oauth/disconnect')
    async def disconnect_user_oauth():
        _require_idle()
        return auth_manager.disconnect_user_oauth()

    @app.post('/api/auth/oauth/check')
    async def check_user_oauth_access():
        if not auth_manager.get_auth_status().get('has_user_oauth_token'):
            raise HTTPException(status_code=409, detail='Connect your WCL account first.')
        with index_db._connect() as conn:
            sample = conn.execute("SELECT report_code FROM fights WHERE status = 'PACKED' ORDER BY rowid LIMIT 1").fetchone()
        rate = 'rateLimitData {limitPerHour pointsSpentThisHour pointsResetIn}'
        query = ('query($code:String!) {' + rate +
                 ' reportData {report(code:$code) {code archiveStatus {isArchived isAccessible}}}}') if sample else 'query {' + rate + '}'
        variables = {'code': sample['report_code']} if sample else {}
        try:
            endpoint, token = await auth_manager.get_api_auth()
            if not endpoint.endswith('/api/v2/user'):
                raise HTTPException(status_code=409, detail='Connect your WCL account first.')
            # Interactive check is bounded and does not enter a background quota wait.
            async with protocol_client._make_client() as client:
                response = await client.post(endpoint, json={'query': query, 'variables': variables},
                    headers={'Authorization': f'Bearer {token}'}, timeout=30)
            if response.status_code in (401, 403):
                raise HTTPException(status_code=401, detail='WCL rejected user authorization. Reconnect your WCL account.')
            if response.status_code == 429:
                raise HTTPException(status_code=429, detail='WCL API quota is exhausted. Try again after its reset.')
            if response.status_code != 200:
                raise RuntimeError('User API check failed')
            body = protocol_client._json_payload(response, 'user API')
            if body.get('errors') or not isinstance(body.get('data'), dict):
                raise RuntimeError('User API check failed')
            quota = body['data'].get('rateLimitData')
            if not isinstance(quota, dict):
                raise RuntimeError('User API quota response missing')
            report = None
            if sample:
                source = (body['data'].get('reportData') or {}).get('report') or {}
                archive_status = source.get('archiveStatus')
                if not isinstance(archive_status, dict) or type(archive_status.get('isAccessible')) is not bool:
                    raise RuntimeError('Report access response missing')
                report = {'report_code': sample['report_code'], **archive_status}
            return {'api_endpoint': '/api/v2/user', 'quota': quota, 'report': report}
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=502, detail='Could not verify WCL user API access. Reconnect or try again later.') from None

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
        difficulty: Optional[int] = None,
        status: str = "PACKED",
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> Any:
        status = status.upper()
        if status not in {"ALL", "PACKED", "QUEUED", "RETRY", "FAILED", "STAGED", "API_STAGED"}:
            raise HTTPException(status_code=400, detail="Invalid fight status")
        return index_db.search_fights(
            content_type=content_type,
            encounter_id=encounter_id,
            class_name=class_name,
            spec=spec,
            min_key_level=min_key_level,
            difficulty=difficulty,
            status=None if status == "ALL" else status,
            limit=limit,
            offset=offset,
        )

    def _resolve_fight_file(fight_uid: str) -> Path:
        with index_db._connect() as conn:
            row = conn.execute(
                "SELECT file_rel_path FROM fights WHERE fight_uid = ?",
                (fight_uid,),
            ).fetchone()
        if not row or not row["file_rel_path"]:
            raise HTTPException(status_code=404, detail=f"Fight {fight_uid} is not packed on disk")
        file_path = (data_dir / row["file_rel_path"]).resolve()
        if not file_path.is_relative_to(data_dir.resolve()):
            raise HTTPException(status_code=400, detail='Fight path is outside the archive')
        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"File {file_path} missing")
        return file_path

    def _reader(file_path):
        if file_path.suffix == '.wclf':
            return PackedFightReader if PackedFightReader.is_packed_fight(file_path) else SourceFightReader
        return WCLZReader

    def _legacy_source_archive(path):
        payload = SourceFightReader._read(path)
        root = (path.parent/payload['archive_root']).resolve()
        if not (root/'manifest.db').is_file():
            raise HTTPException(status_code=409,detail='Legacy fight source store is missing')
        return RawArchive(root)

    @app.get('/api/fights/{fight_uid}/sources')
    def get_fight_sources(fight_uid: str):
        path = _resolve_fight_file(fight_uid)
        if path.suffix != '.wclf':
            raise HTTPException(status_code=409,detail='Legacy analysis file has no complete source archive')
        if PackedFightReader.is_packed_fight(path):
            manifest = PackedFightReader.source_manifest(path)
            for capture in manifest['captures']:
                capture['download_url'] = f"/api/fights/{fight_uid}/sources/{capture['id']}"
            return manifest
        manifest = SourceFightReader.source_manifest(path)
        legacy = _legacy_source_archive(path)
        ids = [manifest['master_capture'],*manifest['event_captures'],*manifest.get('supplemental_captures',[])]
        manifest['captures'] = [{**legacy.capture_info(cid),
                                 'download_url':f'/api/archive/responses/{cid}'} for cid in ids]
        return manifest

    @app.get('/api/fights/{fight_uid}/sources/{capture_id}')
    def get_packed_source_response(fight_uid: str,capture_id: str):
        path = _resolve_fight_file(fight_uid)
        if not PackedFightReader.is_packed_fight(path):
            raise HTTPException(status_code=409,detail='Fight uses a legacy source format')
        try:
            body = PackedFightReader.read_source_response(path,capture_id)
        except KeyError:
            raise HTTPException(status_code=404,detail='Source record not found in fight')
        return Response(body,media_type='application/json',headers={
            'Content-Disposition':f'attachment; filename="{capture_id}.json"'})

    @app.get('/api/fights/{fight_uid}/download')
    def download_fight(fight_uid: str):
        path = _resolve_fight_file(fight_uid)
        return FileResponse(path,media_type='application/octet-stream',filename=path.name)

    @app.get('/api/archive/responses/{capture_id}')
    def get_source_response(capture_id: str):
        legacy_root = data_dir/'raw'
        if not (legacy_root/'manifest.db').is_file():
            raise HTTPException(status_code=404,detail='No legacy source store; download the standalone fight instead')
        try:
            body = RawArchive(legacy_root).read_body(capture_id)
        except KeyError:
            raise HTTPException(status_code=404,detail='Source response not found')
        return Response(body,media_type='application/octet-stream',headers={
            'Content-Disposition':f'attachment; filename="{capture_id}.source"'})

    @app.get('/api/fights/{fight_uid}/raw-events')
    @app.get('/api/fights/{fight_uid}/full-events')
    def get_raw_events(fight_uid: str):
        path = _resolve_fight_file(fight_uid)
        if path.suffix != '.wclf':
            raise HTTPException(status_code=409,detail='Legacy analysis file has no original events')
        reader = _reader(path)
        if reader is PackedFightReader:
            return Response(PackedFightReader.full_events_json(path),media_type='application/json')
        return reader.read_raw_events(path)

    @app.get("/api/fights/{fight_uid}/summary")
    def get_fight_summary(fight_uid: str) -> Dict[str, Any]:
        file_path = _resolve_fight_file(fight_uid)
        summary = _reader(file_path).read_summary_only(file_path)
        abilities = summary.get("abilities") or {}
        with master_db._connect() as conn:
            for row in conn.execute("SELECT spell_id, name, school, icon FROM spells").fetchall():
                sid = str(row["spell_id"])
                if sid not in abilities:
                    abilities[sid] = {
                        "gameID": int(row["spell_id"]),
                        "name": row["name"],
                        "type": int(row["school"] or 1),
                        "icon": row["icon"] or "",
                    }
        summary["abilities"] = abilities
        summary['source_available'] = file_path.suffix == '.wclf'
        return summary

    @app.get("/api/fights/{fight_uid}/replay")
    def get_fight_replay(fight_uid: str) -> Dict[str, Any]:
        file_path = _resolve_fight_file(fight_uid)
        return _reader(file_path).read_replay_only(file_path)

    def _build_pet_owner_map(actors: List[Dict[str, Any]], events: List[Dict[str, Any]]) -> Dict[int, int]:
        pet_map: Dict[int, int] = {}
        for a in actors:
            aid = int(a.get("id", 0) or 0)
            owner = int(a.get("petOwner") or 0)
            if aid and owner > 0:
                pet_map[aid] = owner
        for ev in events:
            if ev.get("type") == "summon":
                src = int(ev.get("sourceID", 0) or 0)
                tgt = int(ev.get("targetID", 0) or 0)
                if src and tgt:
                    pet_map[tgt] = pet_map.get(src, src)
        return pet_map

    @app.get("/api/fights/{fight_uid}/breakdown")
    def get_fight_breakdown(
        fight_uid: str,
        mode: str = "damage",
        source_id: Optional[int] = None,
        target_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        file_path = _resolve_fight_file(fight_uid)
        summary = _reader(file_path).read_summary_only(file_path)
        events = _reader(file_path).read_events_only(file_path)
        duration_ms = max(1, int(summary.get("duration_ms", 1) or 1))
        actors = summary.get("actors") or []
        actor_map = {int(a["id"]): a for a in actors if "id" in a}
        pet_map = _build_pet_owner_map(actors, events)

        is_heal = mode == "healing"
        valid_types = ("heal", "absorbed") if is_heal else ("damage",)

        by_source: Dict[int, Dict[str, int]] = {}
        by_target: Dict[int, Dict[str, int]] = {}
        by_ability: Dict[int, Dict[str, int]] = {}
        all_targets_totals: Dict[int, Dict[str, int]] = {}

        for ev in events:
            ev_type = ev.get("type")
            raw_src = int(ev.get("sourceID", 0) or 0)
            owner_id = pet_map.get(raw_src, raw_src)
            tgt_id = int(ev.get("targetID", 0) or 0)
            ab_id = int(ev.get("abilityGameID", 0) or 0)
            if not owner_id or not ab_id:
                continue

            if ev_type in ("damage", "heal", "absorbed") and tgt_id:
                t_entry = all_targets_totals.setdefault(tgt_id, {"dmg": 0, "heal": 0})
                amt0 = int(ev.get("amount", 0) or 0)
                if ev_type == "damage":
                    ovk0 = int(ev.get("overkill", 0) or 0)
                    t_entry["dmg"] += max(0, amt0 - ovk0)
                else:
                    t_entry["heal"] += amt0

            if ev_type not in valid_types:
                continue

            src_info = actor_map.get(owner_id, {})
            if src_info.get("type") in ("Boss", "NPC", "Enemy") or src_info.get("subType") in ("Boss", "NPC", "Enemy"):
                continue

            raw_amt = int(ev.get("amount", 0) or 0)
            overkill = int(ev.get("overkill", 0) or 0)
            eff_amt = raw_amt if is_heal else max(0, raw_amt - overkill)
            hit_type = int(ev.get("hitType", 1) or 1)
            is_crit = 1 if hit_type == 2 else 0

            if target_id is None or tgt_id == target_id:
                s_stat = by_source.setdefault(owner_id, {"total": 0, "hits": 0, "crits": 0})
                s_stat["total"] += eff_amt
                s_stat["hits"] += 1
                s_stat["crits"] += is_crit

            if source_id is None or owner_id == source_id or raw_src == source_id:
                if tgt_id:
                    t_stat = by_target.setdefault(tgt_id, {"total": 0, "hits": 0, "crits": 0})
                    t_stat["total"] += eff_amt
                    t_stat["hits"] += 1
                    t_stat["crits"] += is_crit

            if (source_id is None or owner_id == source_id or raw_src == source_id) and (
                target_id is None or tgt_id == target_id
            ):
                a_stat = by_ability.setdefault(ab_id, {"total": 0, "hits": 0, "crits": 0, "overkill": 0})
                a_stat["total"] += eff_amt
                a_stat["hits"] += 1
                a_stat["crits"] += is_crit
                a_stat["overkill"] += overkill

        source_rows = [
            {
                "actor_id": aid,
                "total": st["total"],
                "per_sec": round((st["total"] * 1000.0) / duration_ms, 1),
                "hits": st["hits"],
                "crits": st["crits"],
            }
            for aid, st in sorted(by_source.items(), key=lambda kv: kv[1]["total"], reverse=True)
        ]
        target_rows = [
            {
                "target_id": tid,
                "total": st["total"],
                "per_sec": round((st["total"] * 1000.0) / duration_ms, 1),
                "hits": st["hits"],
                "crits": st["crits"],
            }
            for tid, st in sorted(by_target.items(), key=lambda kv: kv[1]["total"], reverse=True)
        ]
        ability_rows = [
            {
                "ability_id": ab_id,
                "total": st["total"],
                "per_sec": round((st["total"] * 1000.0) / duration_ms, 1),
                "hits": st["hits"],
                "crits": st["crits"],
                "overkill": st["overkill"],
            }
            for ab_id, st in sorted(by_ability.items(), key=lambda kv: kv[1]["total"], reverse=True)
        ]
        active_targets = [
            {
                "id": tid,
                "name": actor_map.get(tid, {}).get("name", f"Actor #{tid}"),
                "type": actor_map.get(tid, {}).get("type", "NPC"),
                "dmg_taken": totals["dmg"],
                "heal_taken": totals["heal"],
            }
            for tid, totals in sorted(
                all_targets_totals.items(),
                key=lambda kv: kv[1]["dmg"] + kv[1]["heal"],
                reverse=True,
            )
        ]
        return {
            "mode": mode,
            "duration_ms": duration_ms,
            "by_source": source_rows,
            "by_target": target_rows,
            "by_ability": ability_rows,
            "active_targets": active_targets,
        }

    @app.get("/api/fights/{fight_uid}/events")
    def get_fight_events(
        fight_uid: str,
        t_start: int = 0,
        t_end: Optional[int] = None,
        actor_id: Optional[int] = None,
        source_id: Optional[int] = None,
        target_id: Optional[int] = None,
        event_type: Optional[str] = None,
        ability_id: Optional[int] = None,
        offset: int = 0,
        limit: int = 500,
    ) -> Any:
        file_path = _resolve_fight_file(fight_uid)
        summary = _reader(file_path).read_summary_only(file_path)
        events = _reader(file_path).read_events_only(file_path)
        pet_map = _build_pet_owner_map(summary.get("actors") or [], events)

        type_set = None
        if event_type and event_type != "all":
            if event_type == "damage":
                type_set = {"damage"}
            elif event_type in ("heal", "healing"):
                type_set = {"heal", "absorbed"}
            elif event_type == "cast":
                type_set = {"cast", "begincast"}
            elif event_type == "buff":
                type_set = {
                    "applybuff",
                    "removebuff",
                    "refreshbuff",
                    "applybuffstack",
                    "removebuffstack",
                    "applydebuff",
                    "removedebuff",
                    "refreshdebuff",
                    "applydebuffstack",
                    "removedebuffstack",
                }
            elif event_type == "death":
                type_set = {"death", "interrupt", "instakill"}
            else:
                type_set = {t.strip() for t in event_type.split(",") if t.strip()}

        filtered = []
        for e in events:
            ts = e.get("timestamp", 0)
            if ts < t_start:
                continue
            if t_end is not None and ts > t_end:
                continue
            et = e.get("type", "")
            if et == "combatantinfo":
                continue
            if type_set is not None and et not in type_set:
                continue
            raw_src = int(e.get("sourceID", 0) or 0)
            owner_src = pet_map.get(raw_src, raw_src)
            tgt = int(e.get("targetID", 0) or 0)
            if actor_id is not None and raw_src != actor_id and owner_src != actor_id and tgt != actor_id:
                continue
            if source_id is not None and raw_src != source_id and owner_src != source_id:
                continue
            if target_id is not None and tgt != target_id:
                continue
            if ability_id is not None and int(e.get("abilityGameID", 0) or 0) != ability_id:
                continue
            filtered.append(e)

        safe_limit = max(1, min(5000, limit))
        safe_offset = max(0, offset)
        return {
            "count": len(filtered),
            "offset": safe_offset,
            "limit": safe_limit,
            "events": filtered[safe_offset : safe_offset + safe_limit],
        }

    @app.get("/api/harvester/status")
    def get_harvester_status() -> Dict[str, Any]:
        with index_db._connect() as conn:
            queued = conn.execute("SELECT COUNT(*) AS c FROM fights WHERE status = 'QUEUED'").fetchone()["c"]
            packed = conn.execute("SELECT COUNT(*) AS c FROM fights WHERE status = 'PACKED'").fetchone()["c"]
            total_bytes = conn.execute(
                "SELECT COALESCE(SUM(compressed_bytes), 0) AS b FROM fights WHERE status = 'PACKED'"
            ).fetchone()["b"]
        active_browser = hybrid_browser_ref if harvester_state['rankings_source'] == 'hybrid' else browser_client_ref
        if active_browser['instance'] is not None:
            harvester_state['browser_state'] = active_browser['instance'].state
            harvester_state['browser_message'] = active_browser['instance'].status_message
        db_scanned = index_db.get_scanned_pages_count()
        if db_scanned > int(harvester_state.get("pages_scanned", 0)):
            harvester_state["pages_scanned"] = db_scanned
        source_stats = raw_archive.stats()
        if (data_dir/'raw'/'manifest.db').is_file():
            source_stats = {**source_stats,**RawArchive(data_dir/'raw').stats()}
        return {
            **harvester_state,
            **index_db.get_queue_counts(),
            **harvester.get_staging_stats(),
            **protocol_client.get_quota_status(),
            "download_concurrency": harvester.download_concurrency,
            "browser_request_concurrency": 2 * harvester.download_concurrency,
            "pack_workers": harvester.pack_workers,
            "hybrid_strict": harvester_state['rankings_source'] == 'hybrid',
            "queued_fights": int(queued),
            "packed_fights": int(packed),
            **source_stats,
            'archive_mode':'standalone',
            'archive_bytes':int(total_bytes),
            "source_mb": round(source_stats['source_bytes']/(1024*1024),2),
            "analysis_mb": round(int(total_bytes)/(1024*1024),2),
            "compressed_mb": round((int(total_bytes)+source_stats['source_bytes']) / (1024 * 1024), 2),
            "recent_logs": list(harvester_logs),
        }

    @app.post('/api/harvester/settings')
    async def post_harvester_settings(req: HarvesterSettingsRequest) -> Dict[str, Any]:
        await _apply_download_concurrency(req.download_concurrency)
        return get_harvester_status()

    @app.post("/api/browser/open")
    async def post_browser_open() -> Dict[str, Any]:
        b_client = _get_browser_client()
        try:
            harvester_state["last_error"] = ""
            await b_client.start()
            _append_log("Opened visible browser window.")
        except Exception as exc:
            harvester_state["last_error"] = f"Browser open failed: {exc}"
            _append_log(f"ERROR opening browser: {exc}")
            raise HTTPException(status_code=500, detail=str(exc))
        return get_harvester_status()

    @app.post("/api/browser/close")
    async def post_browser_close() -> Dict[str, Any]:
        _require_idle()
        await _close_collection_browsers()
        _append_log("Closed visible browser window.")
        return get_harvester_status()

    @app.post("/api/harvester/import-report")
    async def post_import_report(req: ImportReportRequest) -> Dict[str, Any]:
        nonlocal job_task
        _require_idle()
        m = REPORT_CODE_RE.search(req.report.strip())
        if not m:
            raise HTTPException(status_code=400, detail="Invalid WarcraftLogs report code or URL")
        code = m.group(1)
        source = req.rankings_source or harvester_state.get("rankings_source", "browser")
        client = _select_collection_client(source)
        harvester_state["rankings_source"] = source
        harvester.client = client
        harvester_state["running"] = True
        harvester_state["last_error"] = ""
        harvester_state["current_task"] = f"Importing report {code}..."
        _append_log(f"Starting single report import for {code} (source={source})...")
        async def import_work():
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
        job_task = asyncio.create_task(_run_job(import_work()))
        try:
            return await asyncio.shield(job_task)
        except asyncio.CancelledError:
            if job_task.cancelled():
                raise HTTPException(status_code=409, detail="Import stopped")
            raise

    async def _background_harvest_worker(
        mode: str, pages_per_spec: int, source: str, raid_difficulty: int = 0
    ) -> None:
        should_stop = lambda: not harvester_state["running"]
        restart_attempt = 0
        try:
            while harvester_state["running"]:
                attempt_started_at = time.monotonic()
                try:
                    if source == 'browser':
                        b_client = _get_browser_client()
                        await b_client.start()
                        harvester.client = _select_collection_client(source)
                    elif source == 'hybrid':
                        # Browser startup and enrichment run in their own pool;
                        # API admission must not wait for its browser session.
                        harvester.client = _select_collection_client(source)
                    else:
                        harvester.client = protocol_client

                    if mode == "mplus_s5":
                        await harvester.discover_and_enqueue_mplus_s5(
                            pages_per_spec=pages_per_spec,
                            on_progress=_on_progress,
                            should_stop=should_stop,
                            pack_as_discovered=True,
                        )
                    elif mode == "raids":
                        diffs = [raid_difficulty] if raid_difficulty > 0 else [5, 4, 3, 1]
                        await harvester.discover_and_enqueue_raids(
                            pages_per_spec=pages_per_spec,
                            difficulties=diffs,
                            on_progress=_on_progress,
                            should_stop=should_stop,
                            pack_as_discovered=True,
                        )

                    while harvester_state["running"]:
                        harvester_state["current_task"] = (
                            f"Unarchiving & packing queued fights ({harvester.download_concurrency}x DL, "
                            f"{harvester.pack_workers}x CPU)..."
                        )
                        packed = await harvester.drain_queue_batch(
                            batch_size=40, on_progress=_on_progress, should_stop=should_stop, continuous=True
                        )
                        counts = index_db.get_queue_counts()
                        pending = sum(counts.get(key, 0) for key in (
                            "pending_reports", "queued_fights", "retryable_reports", "retryable_fights", "staged_fights"
                        ))
                        if source == 'hybrid':
                            pending += counts.get('api_staged_fights', 0)
                        if packed == 0 and pending == 0:
                            break
                        await asyncio.sleep(0.2)

                    break
                except Exception as exc:
                    if not harvester_state["running"]:
                        break
                    if time.monotonic() - attempt_started_at >= 120.0:
                        restart_attempt = 0
                    restart_attempt += 1
                    base = max(0.05, float(error_restart_base_delay))
                    backoff_s = min(60.0, base * (2 ** min(restart_attempt - 1, 4)))
                    backoff_label = f"{backoff_s:.0f}s" if backoff_s >= 1.0 else f"{backoff_s:.2f}s"
                    harvester_state["last_error"] = str(exc)
                    harvester_state["current_task"] = (
                        f"Error: {exc} — restarting in {backoff_label} (attempt #{restart_attempt})..."
                    )
                    _append_log(f"ERROR: {exc}")
                    _append_log(
                        f"Auto-restarting harvest (scan + pack) in {backoff_label} (attempt #{restart_attempt})..."
                    )
                    # Finish capture cleanup before closing/replacing its browser.
                    with suppress(Exception):
                        await harvester.stop_background_downloading()
                    with suppress(Exception):
                        await harvester.stop_background_enrichment(cancel=True)
                    if source in ('browser', 'hybrid'):
                        try:
                            await _close_collection_browsers()
                        except Exception:
                            pass
                    deadline = time.monotonic() + backoff_s
                    while harvester_state["running"] and time.monotonic() < deadline:
                        await asyncio.sleep(min(0.5, max(0.02, deadline - time.monotonic())))

            if harvester_state["running"]:
                counts = index_db.get_queue_counts()
                failed_pages = counts.get('failed_scan_pages',0)
                failed = counts["failed_reports"] + counts["failed_fights"] + failed_pages
                waiting_browser = counts.get('api_staged_fights', 0)
                failed += waiting_browser
                if failed:
                    message = f"Harvest incomplete: {counts['failed_reports']} failed reports, {counts['failed_fights']} failed fights, {failed_pages} failed ranking pages"
                    if waiting_browser:
                        message += f", {waiting_browser} hybrid fights await browser enrichment"
                    harvester_state["current_task"] = message
                    harvester_state["last_error"] = message
                    harvester_state["progress_pct"] = min(99.9, harvester_state["progress_pct"])
                    _append_log(message)
                else:
                    harvester_state["current_task"] = "Harvest completed"
                    harvester_state["last_error"] = ""
                    harvester_state["progress_pct"] = 100.0
                    _append_log("Harvest completed successfully.")
            else:
                harvester_state["current_task"] = "Stopped by user"
                _append_log("Harvester stopped by user.")
        finally:
            harvester_state["running"] = False

    @app.post("/api/harvester/start")
    async def post_start_harvest(req: StartHarvestRequest) -> Dict[str, Any]:
        nonlocal starting_job
        _require_idle()
        starting_job = True
        try:
            return await _start_harvest(req)
        finally:
            starting_job = False

    async def _start_harvest(req: StartHarvestRequest) -> Dict[str, Any]:
        nonlocal job_task
        if req.mode not in {"mplus_s5", "raids"}:
            raise HTTPException(status_code=400, detail="Invalid harvest mode")
        source = req.rankings_source or harvester_state.get('rankings_source', 'browser')
        harvester.client = _select_collection_client(source)
        if req.download_concurrency is not None:
            await _apply_download_concurrency(req.download_concurrency)
        index_db.retry_failed_work()
        if not harvester_state["running"]:
            source = req.rankings_source or harvester_state.get("rankings_source", "browser")
            harvester_state["rankings_source"] = source
            pages_desc = "ALL pages (0)" if req.pages_per_spec <= 0 else f"{req.pages_per_spec} page(s)/spec"
            harvester_state["running"] = True
            harvester_state["last_error"] = ""
            harvester_state["specs_done"] = 0
            if req.mode == "mplus_s5":
                harvester_state["specs_total"] = 468
            else:
                diff_mult = 1 if req.raid_difficulty > 0 else 4
                harvester_state["specs_total"] = 2880 * diff_mult
            harvester_state["pages_scanned"] = index_db.get_scanned_pages_count()
            harvester_state["progress_pct"] = 0.0
            harvester_state["current_task"] = f"Starting {req.mode} harvest ({pages_desc}, source={source})..."
            _append_log(f"Started {req.mode} harvest ({pages_desc}, source={source}).")
            job_task = asyncio.create_task(
                _run_job(_background_harvest_worker(req.mode, req.pages_per_spec, source, req.raid_difficulty))
            )
        return get_harvester_status()

    @app.post("/api/harvester/stop")
    async def post_stop_harvest() -> Dict[str, Any]:
        _append_log("Stop requested by user.")
        await _stop_job()
        return get_harvester_status()

    web_dir = BASE_DIR / "wcl_archiver" / "web"
    if web_dir.exists():
        app.mount("/", StaticFiles(directory=str(web_dir), html=True), name="web")

    return app

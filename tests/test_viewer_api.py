from pathlib import Path
from unittest.mock import AsyncMock
from fastapi.testclient import TestClient
import wcl_archiver.api.app as app_module
from wcl_archiver.api.app import create_app
from wcl_archiver.codec.wclz_format import WCLZWriter
from wcl_archiver.db.schema import IndexDB


def test_explorer_ui_paginates_and_renders_untrusted_names_as_text():
    import shutil
    import subprocess
    from wcl_archiver.config import BASE_DIR

    script = r'''
const fs = require('fs');
const vm = require('vm');
const assert = require('assert');
class Element {
  constructor() { this.children = []; this.value = ''; this.textContent = ''; this.classList={add(){}}; }
  appendChild(child) { this.children.push(child); return child; }
  replaceChildren(...children) { this.children = children; }
  addEventListener() {}
  set innerHTML(value) { if (value.includes('<img src=x')) throw Error('unsafe HTML name'); this.html=value; this.children = []; }
}
const fields = {};
const document = {
  getElementById(id) { return fields[id] ||= new Element(); },
  createElement() { return new Element(); },
};
let urls = [];
const rows = [
  {fight_uid:'unsafe\'id', status:'QUEUED', encounter_slug:'<img src=x onerror=alert(1)>', participants:[]},
  {fight_uid:'safe_2', status:'PACKED', encounter_slug:'Boss', participants:[{player_name:'<img src=x>',spec_name:'Monk'}]},
];
const context = {document, window:{addEventListener(){}}, URLSearchParams, console,
  fetch:async url => { urls.push(url); return {ok:true,json:async()=>rows}; }};
vm.createContext(context);
vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'), context);
(async()=>{
  await vm.runInContext('loadFightsList()',context);
  assert(urls[0].includes('status=PACKED'));
  assert(urls[0].includes('offset=0'));
  let rendered = fields['fights-tbody'].children;
  assert.equal(rendered[0].children[0].textContent, '<img src=x onerror=alert(1)>');
  assert.equal(rendered[0].children[4].textContent, 'QUEUED');
  assert(!rendered[0].children.some(cell=>cell.children.some(child=>child.textContent==='Open Fight')));
  assert(rendered[1].children.some(cell=>cell.children.some(child=>child.textContent==='Open Fight')));
  await vm.runInContext('loadFightsList(100)',context);
  assert(urls[1].includes('offset=100'));
  assert.equal(fields['btn-fights-prev'].disabled,false);
  await vm.runInContext(`currentFightSummary = {
    actors:[{id:1,name:'<img src=x>',type:'Monk'}],
    abilities:{10:{name:'<img src=x>'}},
    cast_timeline:{1:[{ability_id:10,t_ms:1000}]}
  }; renderFightTab('casts')`,context);
  assert(fields['tab-content-area'].html.includes('&lt;img src=x&gt;'));
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    result = subprocess.run([shutil.which("node"), "-e", script, str(BASE_DIR / "wcl_archiver/web/app.js")],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_viewer_api_serves_catalog_search_and_isolated_single_fight(tmp_path: Path):
    index_db = IndexDB(tmp_path / "index.db")
    index_db.enqueue_fight(
        report_code="aBcDeFg123",
        fight_id=2,
        content_type="mplus",
        zone_id=9,
        partition=5,
        encounter_id=11492,
        encounter_slug="maw-of-souls",
        keystone_level=28,
        duration_ms=840000,
        participant={"name": "Pellia", "class": "Monk", "spec": "Windwalker", "role": "dps", "ilvl": 975},
    )

    rel_path = "mplus_s5/11492_maw-of-souls/k28_14m00s_aBcDeFg123_f2.wclz"
    full_path = tmp_path / rel_path
    summary = {
        "report_code": "aBcDeFg123",
        "fight_id": 2,
        "keystone_level": 28,
        "duration_ms": 840000,
        "actors": [{"id": 1, "name": "Pellia", "type": "Player"}],
        "damage_done": [{"actor_id": 1, "total": 150000000, "dps": 178571.4, "spells": []}],
    }
    events = [
        {"timestamp": 1000, "type": "cast", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "amount": 0, "x": -41000, "y": 28000},
        {"timestamp": 2000, "type": "cast", "sourceID": 2, "targetID": 99, "abilityGameID": 100780, "amount": 0, "x": -41000, "y": 28000},
        {"timestamp": 5000, "type": "damage", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "amount": 50000, "x": -41000, "y": 28000},
    ]
    replay = {"map_id": 1492, "frames": [{"t": 1000, "actor": 1, "x": -41000, "y": 28000, "hp_pct": 10000}]}

    comp_size = WCLZWriter.write_fight(full_path, summary, events, replay, zstd_level=22)
    index_db.mark_fight_packed("aBcDeFg123", 2, rel_path, comp_size, 1)

    app = create_app(data_dir=tmp_path)
    client = TestClient(app)

    # 1. Catalog
    cat = client.get("/api/meta/catalog").json()
    assert len(cat["mplus_dungeons"]) == 13
    assert len(cat["raids"]) == 5
    assert len(cat["specs"]) == 36

    # 2. Search fights
    fights = client.get("/api/fights?content_type=mplus&spec=Windwalker").json()
    assert len(fights) == 1
    assert fights[0]["fight_uid"] == "aBcDeFg123_2"

    # 3. Summary
    s_resp = client.get("/api/fights/aBcDeFg123_2/summary").json()
    assert s_resp["keystone_level"] == 28
    assert s_resp["damage_done"][0]["total"] == 150000000

    # 4. Replay
    r_resp = client.get("/api/fights/aBcDeFg123_2/replay").json()
    assert r_resp["map_id"] == 1492
    assert len(r_resp["frames"]) == 1

    # 5. Events & filtering
    e_resp = client.get("/api/fights/aBcDeFg123_2/events?t_start=1500&t_end=4000").json()
    assert e_resp["count"] == 1
    assert e_resp["events"][0]["timestamp"] == 2000

    e_actor = client.get("/api/fights/aBcDeFg123_2/events?actor_id=1").json()
    assert e_actor["count"] == 2
    assert {e["timestamp"] for e in e_actor["events"]} == {1000, 5000}

    e_dmg = client.get("/api/fights/aBcDeFg123_2/events?event_type=damage&source_id=1&target_id=99").json()
    assert e_dmg["count"] == 1
    assert e_dmg["events"][0]["amount"] == 50000

    b_resp = client.get("/api/fights/aBcDeFg123_2/breakdown?mode=damage&source_id=1&target_id=99").json()
    assert b_resp["by_source"][0]["actor_id"] == 1
    assert b_resp["by_source"][0]["total"] == 50000
    assert b_resp["by_target"][0]["target_id"] == 99
    assert b_resp["by_ability"][0]["ability_id"] == 113656

    # 6. Not found handling (record missing from DB)
    err404 = client.get("/api/fights/nonexistent_1/summary")
    assert err404.status_code == 404

    # 7. Auth status & login with session cookie
    auth_status = client.get("/api/auth/status").json()
    assert "authenticated" in auth_status

    login_resp = client.post(
        "/api/auth/login",
        json={"wcl_session_cookie": "test_cookie", "xsrf_token": "test_token"}
    ).json()
    assert login_resp["authenticated"] is True


def test_viewer_api_file_missing_on_disk(tmp_path: Path):
    index_db = IndexDB(tmp_path / "index.db")
    index_db.enqueue_fight(
        report_code="xyz999",
        fight_id=1,
        content_type="mplus",
        zone_id=9,
        partition=5,
        encounter_id=11492,
        encounter_slug="maw-of-souls",
        keystone_level=20,
        duration_ms=600000,
    )
    rel_path = "mplus_s5/11492_maw-of-souls/missing.wclz"
    index_db.mark_fight_packed("xyz999", 1, rel_path, 100, 1)

    app = create_app(data_dir=tmp_path)
    client = TestClient(app)

    # DB has record, but file is missing on disk -> 404 detail "File ... missing"
    resp = client.get("/api/fights/xyz999_1/summary")
    assert resp.status_code == 404
    assert "missing" in resp.json()["detail"]


def test_viewer_api_login_with_credentials(tmp_path: Path, monkeypatch):
    app = create_app(data_dir=tmp_path)
    client = TestClient(app)

    mock_login = AsyncMock(return_value={"authenticated": True, "email": "user@example.com"})
    monkeypatch.setattr(
        "wcl_archiver.wcl_client.auth.WCLAuthManager.login_with_credentials",
        mock_login,
    )

    resp = client.post(
        "/api/auth/login",
        json={"email": "user@example.com", "password": "secret_password"}
    ).json()
    assert resp["authenticated"] is True
    assert resp["email"] == "user@example.com"


def test_viewer_api_mounts_static_web_dir(tmp_path: Path, monkeypatch):
    web_dir = tmp_path / "fake_base" / "wcl_archiver" / "web"
    web_dir.mkdir(parents=True, exist_ok=True)
    (web_dir / "index.html").write_text("<h1>WCL Viewer</h1>", encoding="utf-8")

    monkeypatch.setattr(app_module, "BASE_DIR", tmp_path / "fake_base")

    app = create_app(data_dir=tmp_path)
    client = TestClient(app)

    resp = client.get("/")
    assert resp.status_code == 200
    assert "WCL Viewer" in resp.text


def test_web_ui_root_serves_english_html(tmp_path: Path):
    import re
    from wcl_archiver.config import BASE_DIR

    app = create_app(data_dir=tmp_path)
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    assert "WCL Legion 7.3.5 Archiver" in resp.text
    assert "Log Explorer" in resp.text

    # Verify all web UI files contain only English (no Cyrillic text)
    cyrillic_re = re.compile(r"[\u0400-\u04FF]")
    for fname in ("index.html", "app.js", "replay2d.js"):
        content = (BASE_DIR / "wcl_archiver" / "web" / fname).read_text(encoding="utf-8")
        assert not cyrillic_re.search(content), f"Found non-English Cyrillic text in {fname}"

    st = client.get("/api/harvester/status").json()
    assert st["queued_fights"] == 0
    assert st["packed_fights"] == 0
    stop_st = client.post("/api/harvester/stop").json()
    assert stop_st["running"] is False


def test_harvester_auto_restarts_scan_and_pack_on_error(tmp_path: Path, monkeypatch):
    import time
    from wcl_archiver.harvester.pipeline import HarvesterPipeline

    attempts = {"count": 0}
    packed_on_retry = {"done": False}

    async def fake_discover_mplus(
        self,
        pages_per_spec: int = 0,
        on_progress=None,
        should_stop=None,
        pack_as_discovered: bool = False,
    ) -> int:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise TimeoutError("Page.goto: Timeout 30000ms exceeded.")
        return 0

    async def fake_drain(self, batch_size: int = 25, on_progress=None, should_stop=None) -> int:
        if attempts["count"] >= 2 and not packed_on_retry["done"]:
            packed_on_retry["done"] = True
            return 1
        return 0

    monkeypatch.setattr(HarvesterPipeline, "discover_and_enqueue_mplus_s5", fake_discover_mplus)
    monkeypatch.setattr(HarvesterPipeline, "drain_queue_batch", fake_drain)

    app = create_app(
        data_dir=tmp_path,
        rankings_source="protocol",
        error_restart_base_delay=0.05,
    )
    with TestClient(app) as client:
        start_res = client.post(
            "/api/harvester/start",
            json={"mode": "mplus_s5", "pages_per_spec": 1, "rankings_source": "protocol"},
        )
        assert start_res.status_code == 200

        deadline = time.monotonic() + 5.0
        st = {}
        while time.monotonic() < deadline:
            st = client.get("/api/harvester/status").json()
            if not st["running"] and attempts["count"] >= 2:
                break
            time.sleep(0.05)

    assert attempts["count"] == 2
    assert packed_on_retry["done"] is True
    assert st["running"] is False
    assert st["progress_pct"] == 100.0
    assert any("Auto-restarting harvest (scan + pack)" in line for line in st["recent_logs"])
    assert any("Harvest completed successfully." in line for line in st["recent_logs"])

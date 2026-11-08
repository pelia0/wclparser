from pathlib import Path
from unittest.mock import AsyncMock
from fastapi.testclient import TestClient
import wcl_archiver.api.app as app_module
from wcl_archiver.api.app import create_app
from wcl_archiver.codec.wclz_format import WCLZWriter
from wcl_archiver.db.schema import IndexDB


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
    app = create_app(data_dir=tmp_path)
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    assert "WCL Legion 7.3.5 Archiver" in resp.text
    assert "Log Explorer" in resp.text

    st = client.get("/api/harvester/status").json()
    assert st["queued_fights"] == 0
    assert st["packed_fights"] == 0
    stop_st = client.post("/api/harvester/stop").json()
    assert stop_st["running"] is False


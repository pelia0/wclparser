from pathlib import Path
import httpx
import pytest
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.wcl_client.auth import WCLAuthManager
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient
from wcl_archiver.harvester.pipeline import HarvesterPipeline
from wcl_archiver.codec.wclz_format import WCLZReader


@pytest.mark.asyncio
async def test_harvester_pipeline_unarchives_paginates_and_writes_individual_wclz(tmp_path: Path):
    def mock_wcl(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/reports/unarchive/aBcDeFg123":
            return httpx.Response(200, json={"success": True, "status": "unarchived"})
        if path == "/reports/fights-and-participants/aBcDeFg123/0":
            return httpx.Response(
                200,
                json={
                    "fights": [
                        {
                            "id": 2,
                            "start_time": 1000,
                            "end_time": 5000,
                            "boss": 11492,
                            "name": "Maw of Souls",
                            "keystoneLevel": 28,
                            "affixes": [10, 6, 3, 13],
                            "maps": [1492],
                        }
                    ],
                    "friendlies": [
                        {"id": 1, "name": "Pellia", "type": "Monk", "icon": "Monk-Windwalker"}
                    ],
                    "enemies": [{"id": 99, "name": "Helya", "type": "Boss"}],
                    "abilities": [{"gameID": 113656, "name": "Fists of Fury", "type": 1}],
                },
            )
        if path.startswith("/reports/events/summary/aBcDeFg123/2/1000/5000"):
            return httpx.Response(
                200,
                json={
                    "events": [
                        {"timestamp": 1500, "type": "cast", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "x": -41000, "y": 28000},
                    ],
                    "nextPageTimestamp": 3000,
                },
            )
        if path.startswith("/reports/events/summary/aBcDeFg123/2/3000/5000"):
            return httpx.Response(
                200,
                json={
                    "events": [
                        {"timestamp": 3200, "type": "damage", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "amount": 950000, "hitType": 2, "x": -41020, "y": 28010},
                    ],
                    "nextPageTimestamp": None,
                },
            )
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_wcl)
    index_db = IndexDB(tmp_path / "index.db")
    master_db = MasterDictDB(tmp_path / "master_dict.db")
    auth = WCLAuthManager(tmp_path / "auth_session.json", transport=transport)
    auth.save_session_cookies({"wcl_session": "sub123", "XSRF-TOKEN": "csrf123"})

    client = WCLProtocolClient(auth_manager=auth, transport=transport)
    pipeline = HarvesterPipeline(
        data_dir=tmp_path,
        index_db=index_db,
        master_db=master_db,
        protocol_client=client,
    )

    index_db.enqueue_fight(
        report_code="aBcDeFg123",
        fight_id=2,
        content_type="mplus",
        zone_id=9,
        partition=5,
        encounter_id=11492,
        encounter_slug="maw-of-souls",
        keystone_level=28,
        duration_ms=4000,
        participant={"name": "Pellia", "class": "Monk", "spec": "Windwalker", "role": "dps", "ilvl": 975},
    )

    await pipeline.warm_unarchive_batch(limit=5)
    pending = index_db.get_unarchived_pending_fights(limit=5)
    assert len(pending) == 1

    wclz_file = await pipeline.process_single_fight(pending[0])
    assert wclz_file.exists()
    assert wclz_file.suffix == ".wclz"

    summary = WCLZReader.read_summary_only(wclz_file)
    assert summary["report_code"] == "aBcDeFg123"
    assert summary["damage_done"][0]["total"] == 950000
    assert master_db.get_spell(113656)["name"] == "Fists of Fury"


@pytest.mark.asyncio
async def test_harvester_pipeline_raid_fight_and_missing_fight_error(tmp_path: Path):
    def mock_wcl(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/reports/fights-and-participants/rAiD123/0":
            return httpx.Response(
                200,
                json={
                    "fights": [
                        {
                            "id": 10,
                            "start_time": 2000,
                            "end_time": 68000,
                            "boss": 2092,
                            "name": "Argus the Unmaker",
                            "maps": [{"id": 1188}],
                        }
                    ],
                    "friendlies": [
                        {"id": 1, "name": "Pellia", "type": "Monk", "icon": "Monk-Windwalker"}
                    ],
                    "enemies": [{"id": 50, "name": "Argus", "type": "Boss"}],
                    "abilities": [],
                },
            )
        if path.startswith("/reports/events/summary/rAiD123/10/2000/68000"):
            return httpx.Response(
                200,
                json={
                    "events": [
                        {"timestamp": 2500, "type": "cast", "sourceID": 1, "targetID": 50, "abilityGameID": 100784},
                    ],
                    "nextPageTimestamp": None,
                },
            )
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_wcl)
    index_db = IndexDB(tmp_path / "index.db")
    master_db = MasterDictDB(tmp_path / "master_dict.db")
    auth = WCLAuthManager(tmp_path / "auth_session.json", transport=transport)

    client = WCLProtocolClient(auth_manager=auth, transport=transport)
    pipeline = HarvesterPipeline(
        data_dir=tmp_path,
        index_db=index_db,
        master_db=master_db,
        protocol_client=client,
    )

    index_db.enqueue_fight(
        report_code="rAiD123",
        fight_id=10,
        content_type="raid",
        zone_id=17,
        partition=1,
        encounter_id=2092,
        encounter_slug="argus-the-unmaker",
        duration_ms=66000,
    )
    index_db.mark_report_unarchived("rAiD123", "UNARCHIVED")
    pending = index_db.get_unarchived_pending_fights(limit=5)
    assert len(pending) == 1

    wclz_file = await pipeline.process_single_fight(pending[0])
    assert wclz_file.exists()
    assert "raids/zone_17/2092_argus-the-unmaker" in str(wclz_file).replace("\\", "/")
    assert wclz_file.name == "1m06s_rAiD123_f10.wclz"

    # Missing fight test
    missing_row = dict(pending[0])
    missing_row["fight_id"] = 999
    with pytest.raises(ValueError, match="Fight 999 not found in report rAiD123"):
        await pipeline.process_single_fight(missing_row)


@pytest.mark.asyncio
async def test_protocol_client_rankings_and_unarchive_fail(tmp_path: Path):
    def mock_wcl(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/reports/unarchive/failCode":
            return httpx.Response(500, json={"error": "server error"})
        if path == "/api/v2/client":
            req_json = request.read().decode("utf-8")
            if "fail" in req_json:
                return httpx.Response(500)
            if "null_encounter" in req_json:
                return httpx.Response(200, json={"data": {"worldData": {"encounter": None}}})
            return httpx.Response(
                200,
                json={
                    "data": {
                        "worldData": {
                            "encounter": {
                                "fightRankings": {
                                    "rankings": [
                                        {"report": {"code": "rep123", "fightID": 5}, "rank": 1}
                                    ]
                                }
                            }
                        }
                    }
                },
            )
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_wcl)
    auth = WCLAuthManager(tmp_path / "auth_session.json", transport=transport)
    client = WCLProtocolClient(auth_manager=auth, transport=transport)

    # unarchive failure
    ok = await client.unarchive_report("failCode")
    assert ok is False

    # rankings success
    rankings = await client.fetch_rankings_fight_refs(
        zone_id=9, encounter_id=11492, partition=5, class_name="Monk", spec_name="Windwalker", page=1
    )
    assert len(rankings) == 1
    assert rankings[0]["report"]["code"] == "rep123"

    # rankings failure returns empty list
    rankings_fail = await client.fetch_rankings_fight_refs(
        zone_id=9, encounter_id=11492, partition=5, class_name="fail", spec_name="fail", page=1
    )
    assert rankings_fail == []

    # rankings with null encounter returns empty list without AttributeError
    rankings_null = await client.fetch_rankings_fight_refs(
        zone_id=9, encounter_id=11492, partition=5, class_name="null_encounter", spec_name="Any", page=1
    )
    assert rankings_null == []


@pytest.mark.asyncio
async def test_protocol_client_graphql_fallback_when_web_endpoints_blocked(tmp_path: Path):
    def mock_wcl(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/oauth/token":
            return httpx.Response(200, json={"access_token": "tok_gql"})
        if path.startswith("/reports/fights-and-participants") or path.startswith("/reports/events/summary"):
            return httpx.Response(403, text="Cloudflare block")
        if path == "/api/v2/client":
            body = request.read().decode("utf-8")
            if "masterData" in body:
                return httpx.Response(
                    200,
                    json={
                        "data": {
                            "reportData": {
                                "report": {
                                    "fights": [
                                        {
                                            "id": 3,
                                            "startTime": 1000,
                                            "endTime": 4000,
                                            "encounterID": 11492,
                                            "name": "Maw of Souls",
                                            "keystoneLevel": 25,
                                            "keystoneAffixes": [10, 6],
                                            "difficulty": 10,
                                            "maps": [{"id": 1492}],
                                        }
                                    ],
                                    "masterData": {
                                        "abilities": [{"gameID": 100780, "name": "Tiger Palm", "type": 1}],
                                        "actors": [
                                            {"id": 1, "name": "Pellia", "type": "Player", "subType": "Monk"},
                                            {"id": 99, "name": "Ymiron", "type": "NPC", "subType": "Boss"},
                                        ],
                                    },
                                }
                            }
                        }
                    },
                )
            if "events(" in body:
                return httpx.Response(
                    200,
                    json={
                        "data": {
                            "reportData": {
                                "report": {
                                    "events": {
                                        "data": [
                                            {"timestamp": 1200, "type": "cast", "sourceID": 1, "targetID": 99, "abilityGameID": 100780}
                                        ],
                                        "nextPageTimestamp": None,
                                    }
                                }
                            }
                        }
                    },
                )
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_wcl)
    auth = WCLAuthManager(tmp_path / "auth.json", transport=transport)
    await auth.login_with_credentials(email="", password="", client_id="cid", client_secret="csec")
    client = WCLProtocolClient(auth_manager=auth, transport=transport)

    master = await client.fetch_report_master_and_fights("gqlRep123")
    assert master["fights"][0]["id"] == 3
    assert master["friendlies"][0]["name"] == "Pellia"

    events = await client.fetch_all_fight_events("gqlRep123", 3, 1000, 4000)
    assert len(events) == 1
    assert events[0]["abilityGameID"] == 100780


@pytest.mark.asyncio
async def test_discover_mplus_s5_zero_pages_downloads_until_empty_and_reports_progress(tmp_path: Path):
    def mock_wcl(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/v2/client":
            return httpx.Response(401, text="No OAuth")
        if path.startswith("/zone/rankings-table/"):
            page = int(request.url.params.get("page", "1"))
            if "11458" in path and "DeathKnight/Blood" in path and page <= 2:
                html = f'<a href="/reports/repPage{page}Abc123#fight={page}">Run {page}</a>'
                return httpx.Response(200, text=html)
            return httpx.Response(200, text="<table></table>")
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_wcl)
    index_db = IndexDB(tmp_path / "index.db")
    master_db = MasterDictDB(tmp_path / "master.db")
    auth = WCLAuthManager(tmp_path / "auth.json", transport=transport)
    auth.save_session_cookies({"wcl_session": "cookie_sub_1"})
    client = WCLProtocolClient(auth_manager=auth, transport=transport)
    pipeline = HarvesterPipeline(tmp_path, index_db, master_db, client)

    progress_events = []
    stops = {"count": 0}

    def should_stop():
        # Stop after first spec (2 non-empty pages + 1 empty page) to keep test fast
        return stops["count"] >= 1

    def on_prog(upd):
        progress_events.append(upd)
        if upd.get("specs_done", 0) >= 1:
            stops["count"] = 1

    enqueued = await pipeline.discover_and_enqueue_mplus_s5(
        pages_per_spec=0, on_progress=on_prog, should_stop=should_stop
    )
    assert enqueued == 2
    assert any("specs_total" in e and e["specs_total"] == 468 for e in progress_events)

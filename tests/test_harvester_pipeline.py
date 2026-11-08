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

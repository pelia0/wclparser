from pathlib import Path
import httpx
import pytest
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.wcl_client.auth import WCLAuthManager
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient
from wcl_archiver.harvester.pipeline import HarvesterPipeline
from wcl_archiver.codec.wclz_format import WCLZReader


class RetryWorkClient:
    def __init__(self, report_failures=0, event_failures=0):
        self.report_failures = report_failures
        self.event_failures = event_failures
        self.report_calls = {}
        self.event_calls = {}

    async def unarchive_report(self, code):
        self.report_calls[code] = self.report_calls.get(code, 0) + 1
        if code == "bad" and self.report_calls[code] <= self.report_failures:
            raise OSError("unarchive offline")
        return True

    async def fetch_report_master_and_fights(self, code):
        return {"fights": [{"id": 1, "start_time": 123, "end_time": 12668}],
                "friendlies": [], "enemies": [], "abilities": []}

    async def fetch_all_fight_events(self, code, fid, start, end):
        self.event_calls[code] = self.event_calls.get(code, 0) + 1
        if code == "bad" and self.event_calls[code] <= self.event_failures:
            raise OSError("capture failed")
        return []


def _retry_pipeline(tmp_path, client):
    db = IndexDB(tmp_path / "index.db")
    return db, HarvesterPipeline(tmp_path, db, MasterDictDB(tmp_path / "master.db"), client)


@pytest.mark.asyncio
async def test_empty_failed_batch_preserves_later_pending_and_recovers(tmp_path):
    client = RetryWorkClient(report_failures=1)
    db, pipeline = _retry_pipeline(tmp_path, client)
    for code in ("bad", "fresh"):
        db.enqueue_fight(code, 1, "raid", 17, 1, 2092, "argus", duration_ms=999999)
    try:
        assert await pipeline.drain_queue_batch(batch_size=1) == 0
        assert db.get_queue_counts()["pending_reports"] == 1
        assert await pipeline.drain_queue_batch(batch_size=1) == 1
        assert client.report_calls["fresh"] == 1
        assert await pipeline.drain_queue_batch(batch_size=1) == 1
        assert {r["duration_ms"] for r in db.search_fights(status="PACKED")} == {12545}
    finally:
        pipeline.close()


@pytest.mark.asyncio
async def test_transient_capture_failure_retries_and_preserves_exact_duration(tmp_path):
    client = RetryWorkClient(event_failures=1)
    db, pipeline = _retry_pipeline(tmp_path, client)
    db.enqueue_fight("bad", 1, "raid", 17, 1, 2092, "argus", duration_ms=1000)
    try:
        assert await pipeline.drain_queue_batch(batch_size=1) == 0
        assert db.search_fights()[0]["last_error"] == "capture failed"
        assert await pipeline.drain_queue_batch(batch_size=1) == 1
        assert db.search_fights()[0]["duration_ms"] == 12545
        assert client.event_calls["bad"] == 2
    finally:
        pipeline.close()


@pytest.mark.asyncio
async def test_single_fight_processing_updates_duration_from_actual_boundaries(tmp_path):
    db, pipeline = _retry_pipeline(tmp_path, RetryWorkClient())
    db.enqueue_fight("single", 1, "raid", 17, 1, 2092, "argus", duration_ms=1000)
    try:
        await pipeline.process_single_fight(db.search_fights()[0])
        assert db.search_fights()[0]["duration_ms"] == 12545
    finally:
        pipeline.close()


@pytest.mark.asyncio
async def test_single_fight_capture_error_remains_durable_failure(tmp_path):
    db, pipeline = _retry_pipeline(tmp_path, RetryWorkClient(event_failures=1))
    db.enqueue_fight("bad", 1, "raid", 17, 1, 2092, "argus")
    try:
        with pytest.raises(OSError, match="capture failed"):
            await pipeline.process_single_fight(db.search_fights()[0])
        row = db.search_fights()[0]
        assert row["status"] == "RETRY"
        assert row["last_error"] == "capture failed"
    finally:
        pipeline.close()


@pytest.mark.asyncio
async def test_import_unarchive_failure_is_durable_and_not_marked_success(tmp_path):
    db, pipeline = _retry_pipeline(tmp_path, RetryWorkClient(report_failures=1))
    try:
        with pytest.raises(OSError, match="unarchive offline"):
            await pipeline.import_report_by_code("bad")
        assert db.get_queue_counts()["retryable_reports"] == 1
    finally:
        pipeline.close()


class ImportWorkClient(RetryWorkClient):
    def __init__(self, count=2, failures=0):
        super().__init__()
        self.count, self.failures = count, failures

    async def fetch_report_master_and_fights(self, code):
        return {"fights": [{"id": fid, "boss": 2092, "name": "Argus", "start_time": 0,
                            "end_time": 100} for fid in range(1, self.count + 1)],
                "friendlies": [], "enemies": [], "abilities": []}

    async def fetch_all_fight_events(self, code, fid, start, end):
        self.event_calls[fid] = self.event_calls.get(fid, 0) + 1
        if fid == 2 and self.event_calls[fid] <= self.failures:
            raise OSError("canonical capture failed")
        return []


@pytest.mark.asyncio
async def test_import_retries_transient_capture_failure_before_success(tmp_path):
    client = ImportWorkClient(failures=1)
    db, pipeline = _retry_pipeline(tmp_path, client)
    try:
        paths = await pipeline.import_report_by_code("target")
        assert len(paths) == 2
        assert client.event_calls[2] == 2
        assert db.get_queue_counts()["retryable_fights"] == 0
    finally:
        pipeline.close()


@pytest.mark.asyncio
async def test_import_terminal_failure_raises_incomplete_retaining_successful_paths(tmp_path):
    client = ImportWorkClient(failures=20)
    db, pipeline = _retry_pipeline(tmp_path, client)
    try:
        with pytest.raises(RuntimeError, match="(?i)incomplete.*target") as error:
            await pipeline.import_report_by_code("target")
        assert client.event_calls[2] == 3
        assert len(error.value.successful_paths) == 1
        assert error.value.successful_paths[0].exists()
        assert db.get_queue_counts()["failed_fights"] == 1
        assert len(db.search_fights(status="PACKED")) == 1
        db.enqueue_fight("unrelated", 1, "raid", 17, 1, 2092, "argus")
        for _ in range(3):
            db.mark_fight_failed("unrelated", 1, "unrelated failure")
        client.failures = 0
        assert len(await pipeline.import_report_by_code("target")) == 1
        assert db.get_queue_counts()["failed_fights"] == 1  # Other report's budget is untouched.
    finally:
        pipeline.close()


@pytest.mark.asyncio
async def test_import_report_is_not_starved_by_unrelated_queue_limit(tmp_path):
    client = ImportWorkClient(count=1)
    db, pipeline = _retry_pipeline(tmp_path, client)
    for fid in range(1, 105):
        db.enqueue_fight("ahead", fid, "raid", 17, 1, 2092, "argus", keystone_level=40)
    db.mark_report_unarchived("ahead")
    try:
        paths = await pipeline.import_report_by_code("target")
        assert len(paths) == 1
        assert db.get_queue_counts()["queued_fights"] == 104
    finally:
        pipeline.close()


@pytest.mark.asyncio
async def test_import_processes_more_than_one_report_batch(tmp_path):
    client = ImportWorkClient(count=105)
    db, pipeline = _retry_pipeline(tmp_path, client)
    try:
        paths = await pipeline.import_report_by_code("target")
        assert len(paths) == 105
        assert db.get_queue_counts()["queued_fights"] == 0
    finally:
        pipeline.close()


@pytest.mark.asyncio
async def test_discovery_error_cancels_and_awaits_background_drain(tmp_path):
    import asyncio
    db, pipeline = _retry_pipeline(tmp_path, RetryWorkClient())
    pipeline.scan_concurrency = 2
    pipeline._scan_retry_base_delay = 0
    active = False
    release = asyncio.Event()

    async def fail_scan(**kwargs):
        raise OSError("scan offline")

    async def blocked_drain(**kwargs):
        nonlocal active
        active = True
        try:
            await release.wait()
        finally:
            active = False
        return 0

    pipeline._fetch_rankings_refs = fail_scan
    pipeline.drain_queue_batch = blocked_drain
    unit = dict(zone_id=17, encounter_id=2092, encounter_slug="argus", partition=1,
                difficulty=5, content_type="raid", class_name="Monk", spec_name="Windwalker",
                role="dps", label_prefix="Mythic", title="Argus")
    try:
        with pytest.raises(RuntimeError, match="Discovery failed"):
            await pipeline._run_discovery_pool([dict(unit) for _ in range(8)],
                                                pages_per_spec=1, pack_as_discovered=True)
        assert not active
    finally:
        release.set()
        await asyncio.sleep(0.01)
        pipeline.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["report", "events"])
async def test_permanent_failures_terminate_with_durable_reason_and_retry_next_run(tmp_path, stage):
    client = RetryWorkClient(report_failures=10 if stage == "report" else 0,
                             event_failures=10 if stage == "events" else 0)
    db, pipeline = _retry_pipeline(tmp_path, client)
    db.enqueue_fight("bad", 1, "raid", 17, 1, 2092, "argus")
    try:
        for _ in range(6):
            assert await pipeline.drain_queue_batch(batch_size=1) == 0
        calls = client.report_calls if stage == "report" else client.event_calls
        assert calls["bad"] == 3
        counts = db.get_queue_counts()
        assert counts["failed_fights"] == 1
        assert sum(counts[k] for k in ("pending_reports", "queued_fights", "retryable_reports", "retryable_fights")) == 0
        assert db.search_fights()[0]["last_error"]
        db.retry_failed_work()
        client.report_failures = client.event_failures = 0
        assert await pipeline.drain_queue_batch(batch_size=1) == 1
        assert db.search_fights()[0]["status"] == "PACKED"
    finally:
        pipeline.close()


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

    # An unavailable rankings page must not be certified as an empty page.
    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_rankings_fight_refs(
            zone_id=9, encounter_id=11492, partition=5, class_name="fail", spec_name="fail", page=1
        )

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
            if "11501" in path and "DeathKnight/Blood" in path and page <= 2:
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

    # Re-running the same spec should skip already scanned pages from scan_history
    stops["count"] = 0
    enqueued_again = await pipeline.discover_and_enqueue_mplus_s5(
        pages_per_spec=0, on_progress=on_prog, should_stop=should_stop
    )
    assert enqueued_again == 0


@pytest.mark.asyncio
async def test_drain_queue_batch_concurrent_downloads_master_cache_and_thread_packing(tmp_path: Path):
    import asyncio
    from collections import Counter

    class ConcurrentFakeClient:
        def __init__(self):
            self.master_calls = Counter()
            self.unarchive_calls = Counter()
            self.active_event_fetches = 0
            self.max_active_event_fetches = 0

        async def unarchive_report(self, report_code: str) -> bool:
            self.unarchive_calls[report_code] += 1
            await asyncio.sleep(0.01)
            return True

        async def fetch_report_master_and_fights(self, report_code: str):
            self.master_calls[report_code] += 1
            await asyncio.sleep(0.02)
            fights = [
                {
                    "id": fid,
                    "start_time": 1000,
                    "end_time": 5000,
                    "boss": 11492,
                    "name": "Maw of Souls",
                    "keystoneLevel": 25,
                    "affixes": [10, 6, 3, 13],
                    "maps": [1492],
                }
                for fid in range(1, 6)
            ]
            return {
                "fights": fights,
                "friendlies": [{"id": 1, "name": "Pellia", "type": "Monk", "icon": "Monk-Windwalker"}],
                "enemies": [{"id": 99, "name": "Helya", "type": "Boss"}],
                "abilities": [{"gameID": 113656, "name": "Fists of Fury", "type": 1}],
            }

        async def fetch_all_fight_events(
            self, report_code: str, fight_id: int, start_time: int, end_time: int
        ):
            self.active_event_fetches += 1
            if self.active_event_fetches > self.max_active_event_fetches:
                self.max_active_event_fetches = self.active_event_fetches
            try:
                await asyncio.sleep(0.05)
                return [
                    {
                        "timestamp": start_time + 500,
                        "type": "damage",
                        "sourceID": 1,
                        "targetID": 99,
                        "abilityGameID": 113656,
                        "amount": 500000 + fight_id,
                        "hitType": 2,
                    }
                ]
            finally:
                self.active_event_fetches -= 1

    index_db = IndexDB(tmp_path / "index.db")
    master_db = MasterDictDB(tmp_path / "master_dict.db")
    client = ConcurrentFakeClient()
    pipeline = HarvesterPipeline(
        data_dir=tmp_path,
        index_db=index_db,
        master_db=master_db,
        protocol_client=client,  # type: ignore[arg-type]
        zstd_level=22,
        download_concurrency=4,
        pack_workers=3,
    )
    try:
        for fid in range(1, 5):
            index_db.enqueue_fight("repShared001", fid, "mplus", 9, 5, 11492, "maw-of-souls", keystone_level=25)
        for fid in range(1, 3):
            index_db.enqueue_fight("repOther0002", fid, "mplus", 9, 5, 11492, "maw-of-souls", keystone_level=26)

        packed = await pipeline.drain_queue_batch(batch_size=10)
        assert packed == 6
        assert client.master_calls["repShared001"] == 1
        assert client.master_calls["repOther0002"] == 1
        assert client.max_active_event_fetches >= 2

        fights = index_db.search_fights(content_type="mplus", status="PACKED")
        assert len(fights) == 6
        for f in fights:
            wclz_path = tmp_path / f["file_rel_path"]
            assert wclz_path.exists()
            summary = WCLZReader.read_summary_only(wclz_path)
            assert summary["damage_done"][0]["total"] == 500000 + f["fight_id"]
    finally:
        pipeline.close()


@pytest.mark.asyncio
async def test_harvester_pipeline_concurrent_scan_and_pack(tmp_path: Path):
    import asyncio

    class ConcurrentScanAndPackClient:
        def __init__(self):
            self.active_scans = 0
            self.max_active_scans = 0
            self.idx = 0

        async def fetch_rankings_fight_refs(
            self,
            zone_id: int,
            encounter_id: int,
            partition: int,
            class_name: str = "Any",
            spec_name: str = "Any",
            page: int = 1,
            difficulty=None,
        ):
            self.active_scans += 1
            if self.active_scans > self.max_active_scans:
                self.max_active_scans = self.active_scans
            try:
                await asyncio.sleep(0.03)
                if encounter_id == 11492 and class_name == "Monk":
                    return [
                        {
                            "report": {"code": f"repScan{spec_name[:3]}1234", "fightID": 1},
                            "name": "Player",
                            "bracketData": 25,
                            "duration": 10000,
                        }
                    ]
                return []
            finally:
                self.active_scans -= 1

        async def unarchive_report(self, report_code: str) -> bool:
            return True

        async def fetch_report_master_and_fights(self, report_code: str):
            return {
                "fights": [
                    {
                        "id": 1,
                        "start_time": 1000,
                        "end_time": 5000,
                        "boss": 11492,
                        "name": "Maw of Souls",
                        "keystoneLevel": 25,
                        "affixes": [10, 6],
                        "maps": [1492],
                    }
                ],
                "friendlies": [{"id": 1, "name": "Pellia", "type": "Monk", "icon": "Monk-Windwalker"}],
                "enemies": [{"id": 99, "name": "Helya", "type": "Boss"}],
                "abilities": [{"gameID": 113656, "name": "Fists of Fury", "type": 1}],
            }

        async def fetch_all_fight_events(self, report_code: str, fight_id: int, start_time: int, end_time: int):
            await asyncio.sleep(0.01)
            return [
                {
                    "timestamp": 1500,
                    "type": "damage",
                    "sourceID": 1,
                    "targetID": 99,
                    "abilityGameID": 113656,
                    "amount": 100000,
                    "hitType": 2,
                }
            ]

    index_db = IndexDB(tmp_path / "index.db")
    master_db = MasterDictDB(tmp_path / "master_dict.db")
    client = ConcurrentScanAndPackClient()
    pipeline = HarvesterPipeline(
        data_dir=tmp_path,
        index_db=index_db,
        master_db=master_db,
        protocol_client=client,  # type: ignore[arg-type]
        zstd_level=22,
        download_concurrency=4,
        pack_workers=3,
        scan_concurrency=3,
    )
    try:
        units = [
            {
                "zone_id": 9,
                "encounter_id": 11492,
                "encounter_slug": "maw-of-souls",
                "partition": 5,
                "difficulty": 0,
                "content_type": "mplus",
                "class_name": "Monk",
                "spec_name": spec,
                "role": "dps",
                "label_prefix": "M+ S5",
                "title": "Maw Of Souls",
                "log_empty_page1": False,
            }
            for spec in ("Windwalker", "Brewmaster", "Mistweaver")
        ]
        enqueued = await pipeline._run_discovery_pool(
            units=units,
            pages_per_spec=1,
            pack_as_discovered=True,
        )
        assert enqueued == 3
        assert client.max_active_scans >= 2
        packed = index_db.search_fights(content_type="mplus", status="PACKED")
        assert len(packed) == 3
    finally:
        pipeline.close()


@pytest.mark.asyncio
async def test_discovery_pool_survives_single_spec_scan_error_and_continues_packing(tmp_path: Path):
    index_db = IndexDB(tmp_path / "index.db")
    master_db = MasterDictDB(tmp_path / "master.db")

    class FlakyScanClient:
        async def fetch_rankings_fight_refs(
            self,
            zone_id: int,
            encounter_id: int,
            partition: int,
            class_name: str = "Any",
            spec_name: str = "Any",
            page: int = 1,
            difficulty: int = 0,
        ):
            if spec_name == "Guardian":
                raise TimeoutError("Page.goto: Timeout 30000ms exceeded.")
            return [
                {
                    "name": "Player",
                    "bracketData": 20,
                    "duration": 1000000,
                    "report": {"code": f"REP{spec_name[:4].upper()}1", "fightID": 1},
                }
            ]

        async def unarchive_report(self, report_code: str) -> bool:
            return True

        async def fetch_report_master_and_fights(self, report_code: str):
            return {
                "fights": [{"id": 1, "boss": 1962, "name": "Guarm", "start_time": 0, "end_time": 50000, "difficulty": 5}],
                "friendlies": [{"id": 1, "name": "Player", "type": "Druid"}],
                "enemies": [],
                "abilities": [{"gameID": 100780, "name": "Rejuvenation"}],
            }

        async def fetch_all_fight_events(self, report_code: str, fight_id: int, start_time: int, end_time: int):
            return [{"timestamp": 1000, "type": "cast", "sourceID": 1, "abilityGameID": 100780}]

    pipeline = HarvesterPipeline(
        data_dir=tmp_path,
        index_db=index_db,
        master_db=master_db,
        protocol_client=FlakyScanClient(),  # type: ignore[arg-type]
        zstd_level=22,
        download_concurrency=2,
        pack_workers=2,
        scan_concurrency=2,
    )
    pipeline._scan_retry_base_delay = 0.01
    logs = []
    try:
        units = [
            {
                "zone_id": 12,
                "encounter_id": 1962,
                "encounter_slug": "guarm",
                "partition": 1,
                "difficulty": 5,
                "content_type": "raids",
                "class_name": "Druid",
                "spec_name": spec,
                "role": "tank",
                "label_prefix": "Mythic",
                "title": "Trial of Valor - Mythic: Guarm",
                "log_empty_page1": False,
            }
            for spec in ("Guardian", "Restoration")
        ]
        enqueued = await pipeline._run_discovery_pool(
            units=units,
            pages_per_spec=1,
            on_progress=lambda u: logs.append(u["log"]) if "log" in u else None,
            pack_as_discovered=True,
        )
        assert enqueued == 1
        assert any("WARNING: Scan failed" in msg for msg in logs)
        packed = index_db.search_fights(content_type="raids", status="PACKED")
        assert len(packed) == 1
        assert index_db.is_page_scanned(12, 1962, 1, "Druid", "Guardian", 1, difficulty=5) is False
        # Successful later specs must not hide an exhausted page's failure.
        assert index_db.get_queue_counts()["failed_scan_pages"] == 1
        with index_db._connect() as conn:
            failure = dict(conn.execute("SELECT * FROM scan_failures").fetchone())
        assert failure["failure_count"] == 3
        assert "Timeout" in failure["last_error"]
        # A new run retries the unscanned page and clears only its matching failure.
        async def recovered_rankings(**kwargs):
            return []
        pipeline.client.fetch_rankings_fight_refs = recovered_rankings
        await pipeline._run_discovery_pool(units=units, pages_per_spec=1)
        assert index_db.get_queue_counts()["failed_scan_pages"] == 0
        assert index_db.is_page_scanned(12, 1962, 1, "Druid", "Guardian", 1, difficulty=5)
    finally:
        pipeline.close()

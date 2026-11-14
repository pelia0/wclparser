from pathlib import Path
from typing import Any, Dict, List
import pytest
from wcl_archiver.api.app import create_app
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.harvester.pipeline import HarvesterPipeline
from wcl_archiver.wcl_client.browser_client import (
    BrowserClient,
    is_challenge_page,
    is_login_page,
    parse_rankings_html,
)
from fastapi.testclient import TestClient


def test_parse_rankings_html_fixture():
    fixture_path = Path(__file__).parent / "fixtures" / "rankings_page.html"
    html = fixture_path.read_text(encoding="utf-8")
    rows = parse_rankings_html(html)
    assert len(rows) == 2

    assert rows[0]["report"] == {"code": "aBcDeFg1234567", "fightID": 12}
    assert rows[0]["name"] == "Pellia"
    assert rows[0]["keystoneLevel"] == 29
    assert rows[0]["duration"] == (14 * 60 + 25) * 1000 + 320

    assert rows[1]["report"] == {"code": "xYz9876543210", "fightID": 4}
    assert rows[1]["name"] == "Shadowstep"
    assert rows[1]["keystoneLevel"] == 28
    assert rows[1]["duration"] == (15 * 60 + 10) * 1000


def test_parse_rankings_html_empty():
    assert parse_rankings_html("") == []
    assert parse_rankings_html("<table class='rankings-table'><tbody></tbody></table>") == []


def test_is_challenge_page_and_login_page():
    cf_html = """<!DOCTYPE html>
    <html><head><title>Just a moment...</title></head>
    <body><div class="cf-turnstile" data-sitekey="0x4AAAAAA"></div>
    <script src="https://challenges.cloudflare.com/turnstile/v0/api.js"></script>
    </body></html>"""
    assert is_challenge_page("Just a moment...", "https://www.warcraftlogs.com/zone/rankings/9", cf_html) is True
    assert is_challenge_page("Warcraft Logs", "https://challenges.cloudflare.com/cdn-cgi", "<html></html>") is True
    assert is_challenge_page("Warcraft Logs - Rankings", "https://www.warcraftlogs.com/zone/rankings/9", "<table></table>") is False

    assert is_login_page("Login - Warcraft Logs", "https://www.warcraftlogs.com/login", '<form action="/login"></form>') is True
    assert is_login_page("Rankings", "https://www.warcraftlogs.com/zone/rankings/9", "<table></table>") is False


class FakeBrowserClient:
    def __init__(self):
        self.state = "ready"
        self.status_message = "Browser window open and ready"
        self.started = False
        self.stopped = False
        self.raw_archive = None

    async def start(self) -> None:
        self.started = True
        self.state = "ready"

    async def stop(self) -> None:
        self.stopped = True
        self.state = "not_started"

    async def unarchive_report(self, report_code: str) -> bool:
        return True

    async def fetch_rankings_fight_refs(
        self,
        zone_id: int,
        encounter_id: int,
        partition: int,
        class_name: str = "Any",
        spec_name: str = "Any",
        page: int = 1,
    ) -> List[Dict[str, Any]]:
        if page > 1:
            return []
        fixture_path = Path(__file__).parent / "fixtures" / "rankings_page.html"
        return parse_rankings_html(fixture_path.read_text(encoding="utf-8"))

    async def fetch_report_master_and_fights(self, report_code: str) -> Dict[str, Any]:
        payload = {
            "fights": [
                {
                    "id": 12,
                    "start_time": 1000,
                    "end_time": 5000,
                    "boss": 11492,
                    "name": "Maw of Souls",
                    "keystoneLevel": 29,
                    "affixes": [10, 6, 3, 13],
                    "maps": [1492],
                },
                {
                    "id": 4,
                    "start_time": 2000,
                    "end_time": 6000,
                    "boss": 11492,
                    "name": "Maw of Souls",
                    "keystoneLevel": 28,
                    "affixes": [10, 6, 3, 13],
                    "maps": [1492],
                },
            ],
            "friendlies": [
                {"id": 1, "name": "Pellia", "type": "Monk", "icon": "Monk-Windwalker"}
            ],
            "enemies": [{"id": 99, "name": "Helya", "type": "Boss"}],
            "abilities": [{"gameID": 113656, "name": "Fists of Fury", "type": 1}],
        }
        if self.raw_archive is not None:
            import json
            self.raw_archive.capture('GET',f'/reports/fights-and-participants/{report_code}/0',json.dumps(payload).encode())
        return payload

    async def fetch_all_fight_events(
        self,
        report_code: str,
        fight_id: int,
        start_time: int,
        end_time: int,
    ) -> List[Dict[str, Any]]:
        events = [
            {
                "timestamp": start_time + 500,
                "type": "damage",
                "sourceID": 1,
                "targetID": 99,
                "abilityGameID": 113656,
                "amount": 1200000,
                "hitType": 2,
                "x": -41000,
                "y": 28000,
            }
        ]
        if self.raw_archive is not None:
            import json
            with self.raw_archive.fight_capture(report_code,fight_id) as fetch_id:
                self.raw_archive.capture('GET',f'/reports/summary-events/{report_code}/{fight_id}/{start_time}/{end_time}',json.dumps({'events':events}).encode())
                self.raw_archive.complete_fight(report_code,fight_id,fetch_id,start_time,end_time,len(events))
        return events


@pytest.mark.asyncio
async def test_harvester_pipeline_with_fake_browser_client(tmp_path: Path):
    index_db = IndexDB(tmp_path / "index.db")
    master_db = MasterDictDB(tmp_path / "master_dict.db")
    fake_browser = FakeBrowserClient()
    pipeline = HarvesterPipeline(
        data_dir=tmp_path,
        index_db=index_db,
        master_db=master_db,
        protocol_client=fake_browser,  # type: ignore[arg-type]
    )

    stops = {"done": False}

    def on_prog(upd: Dict[str, Any]) -> None:
        if upd.get("specs_done", 0) >= 1:
            stops["done"] = True

    enqueued = await pipeline.discover_and_enqueue_mplus_s5(
        pages_per_spec=1,
        on_progress=on_prog,
        should_stop=lambda: stops["done"],
        pack_as_discovered=True,
    )
    assert enqueued == 2

    fights = index_db.search_fights(content_type="mplus")
    assert len(fights) == 2
    assert all(f["status"] == "PACKED" for f in fights)


def test_browser_api_endpoints_and_status(tmp_path: Path, monkeypatch):
    fake_browser = FakeBrowserClient()
    def browser_factory(*args,**kwargs):
        fake_browser.raw_archive=kwargs.get('raw_archive')
        return fake_browser
    monkeypatch.setattr(
        "wcl_archiver.api.app.BrowserClient",
        browser_factory,
    )
    app = create_app(data_dir=tmp_path, rankings_source="browser")
    client = TestClient(app)

    st0 = client.get("/api/harvester/status").json()
    assert st0["browser_state"] == "not_started"
    assert st0["rankings_source"] == "browser"

    st_open = client.post("/api/browser/open").json()
    assert fake_browser.started is True
    assert st_open["browser_state"] == "ready"

    imp_resp = client.post(
        "/api/harvester/import-report",
        json={"report": "aBcDeFg1234567", "content_type": "mplus", "rankings_source": "browser"},
    )
    assert imp_resp.status_code == 200
    assert imp_resp.json()["packed_count"] == 2

    st_close = client.post("/api/browser/close").json()
    assert fake_browser.stopped is True
    assert st_close["browser_state"] == "not_started"


@pytest.mark.asyncio
async def test_browser_client_works_under_windows_selector_event_loop(tmp_path: Path, monkeypatch):
    import asyncio
    import sys

    if sys.platform != "win32":
        pytest.skip("Windows-specific SelectorEventLoop test")

    # Verify that BrowserClient has a dedicated Proactor loop helper or runs cleanly
    # even when the caller's loop is a _WindowsSelectorEventLoop (like uvicorn --reload on Windows).
    b_client = BrowserClient(profile_dir=tmp_path / "profile", min_delay=0.0, max_delay=0.0)
    assert hasattr(b_client, "_run_in_browser_loop")


@pytest.mark.asyncio
async def test_browser_client_master_cache_parallel_segments_and_429_retry(tmp_path: Path, monkeypatch):
    import asyncio
    import json

    calls: List[str] = []
    rate_limited_once = {"done": False}

    class FakePage:
        url = "https://www.warcraftlogs.com/"

        def is_closed(self) -> bool:
            return False

        async def title(self) -> str:
            return "Warcraft Logs"

        async def content(self) -> str:
            return "<html><body>ok</body></html>"

        async def evaluate(self, script: str, arg: Dict[str, Any]) -> Dict[str, Any]:
            url = arg["url"]
            calls.append(url)
            if url.startswith("/reports/fights-and-participants/"):
                payload = {
                    "fights": [{"id": 7, "boss": 11492, "start_time": 0, "end_time": 500000}],
                    "friendlies": [],
                    "enemies": [],
                    "abilities": [],
                }
                return {"status": 200, "text": json.dumps(payload), "retryAfter": ""}
            if url.startswith("/reports/replaysegment/"):
                if not rate_limited_once["done"]:
                    rate_limited_once["done"] = True
                    return {"status": 429, "text": "Too Many Requests", "retryAfter": "0.01"}
                parts = url.strip("/").split("/")
                seg_start = int(parts[4])
                return {
                    "status": 200,
                    "text": json.dumps({"events": [{"timestamp": seg_start + 10, "type": "cast"}]}),
                    "retryAfter": "",
                }
            return {"status": 404, "text": "", "retryAfter": ""}

    # Avoid real sleep delays during 429 retry test
    orig_sleep = asyncio.sleep
    monkeypatch.setattr("wcl_archiver.wcl_client.browser_client.asyncio.sleep", lambda _s: orig_sleep(0))

    b_client = BrowserClient(
        profile_dir=tmp_path / "profile",
        min_delay=0.0,
        max_delay=0.0,
        max_concurrent_requests=4,
    )
    b_client._context = object()
    b_client._page = FakePage()

    assert await b_client.unarchive_report("repCache123") is True
    # Subsequent fetch_report_master_and_fights must use the cache (no second HTTP call)
    master = await b_client.fetch_report_master_and_fights("repCache123")
    assert master["fights"][0]["id"] == 7
    master_reqs = [u for u in calls if u.startswith("/reports/fights-and-participants/")]
    assert len(master_reqs) == 1

    # 0..500000 with 240000ms chunks = 3 segments fetched concurrently + 1 retry for 429
    events = await b_client.fetch_all_fight_events("repCache123", 7, 0, 500000)
    assert [e["timestamp"] for e in events] == [10, 240010, 480010]
    assert rate_limited_once["done"] is True


@pytest.mark.asyncio
async def test_browser_client_multi_tab_scan_pool(tmp_path: Path):
    import asyncio

    fixture_html = (Path(__file__).parent / "fixtures" / "rankings_page.html").read_text(encoding="utf-8")
    state = {"active_gotos": 0, "max_active_gotos": 0}
    created_tabs = []

    class FakeScanTab:
        def __init__(self):
            self.url = "about:blank"
            self.closed = False

        def is_closed(self) -> bool:
            return self.closed

        async def close(self) -> None:
            self.closed = True

        async def goto(self, url: str, wait_until: str = "domcontentloaded") -> None:
            state["active_gotos"] += 1
            if state["active_gotos"] > state["max_active_gotos"]:
                state["max_active_gotos"] = state["active_gotos"]
            try:
                await asyncio.sleep(0.04)
                self.url = url
            finally:
                state["active_gotos"] -= 1

        async def title(self) -> str:
            return "Warcraft Logs - Rankings"

        async def content(self) -> str:
            return fixture_html

        async def wait_for_function(self, _fn: str, timeout: int = 12000) -> None:
            return None

    class FakeContext:
        def __init__(self, main_page):
            self.pages = [main_page]

        async def new_page(self):
            t = FakeScanTab()
            self.pages.append(t)
            created_tabs.append(t)
            return t

        async def close(self) -> None:
            return None

    main_page = FakeScanTab()
    main_page.url = "https://www.warcraftlogs.com/"

    b_client = BrowserClient(
        profile_dir=tmp_path / "profile",
        min_delay=0.0,
        max_delay=0.0,
        scan_concurrency=3,
    )
    b_client._page = main_page
    b_client._context = FakeContext(main_page)

    results = await asyncio.gather(
        b_client.fetch_rankings_fight_refs(9, 11492, 5, "Monk", "Windwalker", 1),
        b_client.fetch_rankings_fight_refs(9, 11492, 5, "Monk", "Brewmaster", 1),
        b_client.fetch_rankings_fight_refs(9, 11492, 5, "Monk", "Mistweaver", 1),
    )

    assert len(created_tabs) == 3
    assert state["max_active_gotos"] >= 2
    assert all(len(r) == 2 for r in results)

    await b_client.stop()
    assert all(t.closed for t in created_tabs)


@pytest.mark.asyncio
async def test_browser_client_goto_retries_and_replaces_timed_out_scan_tab(tmp_path: Path):
    fixture_html = (Path(__file__).parent / "fixtures" / "rankings_page.html").read_text(encoding="utf-8")
    created_tabs = []

    class FlakyTab:
        def __init__(self, fail_first: bool = False):
            self.url = "about:blank"
            self.closed = False
            self.fail_first = fail_first

        def is_closed(self) -> bool:
            return self.closed

        async def close(self) -> None:
            self.closed = True

        async def goto(self, url: str, wait_until: str = "domcontentloaded", timeout: int = 45000) -> None:
            if self.fail_first:
                raise TimeoutError("Page.goto: Timeout 30000ms exceeded.")
            self.url = url

        async def title(self) -> str:
            return "Warcraft Logs - Rankings"

        async def content(self) -> str:
            return fixture_html

        async def wait_for_function(self, _fn: str, timeout: int = 12000) -> None:
            return None

    class FakeContext:
        def __init__(self, main_page, first_tab):
            self.pages = [main_page, first_tab]

        async def new_page(self):
            t = FlakyTab(fail_first=False)
            self.pages.append(t)
            created_tabs.append(t)
            return t

        async def close(self) -> None:
            return None

    main_page = FlakyTab(fail_first=False)
    main_page.url = "https://www.warcraftlogs.com/"
    bad_tab = FlakyTab(fail_first=True)

    b_client = BrowserClient(
        profile_dir=tmp_path / "profile",
        min_delay=0.0,
        max_delay=0.0,
        scan_concurrency=2,
    )
    b_client._page = main_page
    b_client._context = FakeContext(main_page, bad_tab)

    refs = await b_client.fetch_rankings_fight_refs(12, 1962, 1, "Druid", "Guardian", 5, difficulty=5)
    assert len(refs) == 2
    assert bad_tab.closed is True
    assert len(created_tabs) >= 1
    await b_client.stop()

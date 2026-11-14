"""Full canonical acquisition overlaps independent inclusive ranges and replay."""

import asyncio
from copy import deepcopy
from decimal import Decimal
import re

import pytest

from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader, json_bytes
from wcl_archiver.wcl_client.browser_client import BrowserClient, RateLimitError


CODE = "abcdefghij"


def events(count, *, start=0):
    result = [{"timestamp": start + i, "type": "damage", "amount": i,
               "ability": {"guid": 42, "name": "Hit"},
               "exact": Decimal("0.12345678901234567890123456789"),
               "unknown": {"nullable": None, "values": [{"enabled": True}]}}
              for i in range(count)]
    if count > 350:
        result[350].update(type="death", killerID=7, killingAbility={"guid": 123})
    return result


def enrich(canonical):
    result = deepcopy(canonical)
    for event in result:
        event.update(x=1234, hitPoints=9999, classResources=[{"type": 1, "amount": 77}])
        event.setdefault("ability", {})["icon"] = "spell_icon"
    return result


class Server:
    def __init__(self, canonical, replay, page_size=300):
        self.canonical, self.replay, self.page_size = canonical, replay, page_size
        self.calls = []

    async def reply(self, path):
        self.calls.append(path)
        match = re.search(r"/replaysegment/[^/]+/\d+/(-?\d+)/(-?\d+)/", path)
        if match:
            start, end = map(int, match.groups())
            return 200, {"events": [event for event in self.replay if start <= event["timestamp"] <= end]}
        match = re.search(r"/summary-events/[^/]+/\d+/(-?\d+)/(-?\d+)/", path)
        assert match, path
        start, end = map(int, match.groups())
        remaining = [event for event in self.canonical if start <= event["timestamp"] <= end]
        count = min(self.page_size, len(remaining))
        while count < len(remaining) and count and remaining[count]["timestamp"] == remaining[count - 1]["timestamp"]:
            count += 1
        return 200, {"events": remaining[:count], "nextPageTimestamp": remaining[count]["timestamp"] if count < len(remaining) else None}


def client_for(tmp_path, server, end, *, max_requests=4):
    archive = FightArchive(tmp_path / "unused")
    archive.capture("GET", f"https://www.warcraftlogs.com/reports/fights-and-participants/{CODE}/0",
                    json_bytes({"fights": [{"id": i, "boss": 42, "start_time": 0, "end_time": end} for i in range(1, 5)]}))
    class Page:
        url = BrowserClient.BASE_URL
        def is_closed(self): return False
        async def evaluate(self, script, arg):
            status, body = await server.reply(arg["url"])
            return {"status": status, "text": json_bytes(body).decode(), "retryAfter": "0.001"}
    client = BrowserClient(tmp_path / "profile", raw_archive=archive, min_delay=0, max_delay=0,
                           max_concurrent_requests=max_requests)
    client._page, client._context = Page(), object()
    client._fight_boss_cache.update({(CODE, i): 42 for i in range(1, 5)})
    return client, archive


async def collect(client, end, fight_id=1):
    return await client._fetch_with_source_archive(CODE, fight_id, 0, end)


async def test_canonical_starts_while_replay_is_blocked(tmp_path):
    canonical = events(650)
    server = Server(canonical, enrich(canonical))
    original = server.reply
    replay_started, canonical_started, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def reply(path):
        if "/replaysegment/" in path:
            replay_started.set()
            await release.wait()
        else:
            canonical_started.set()
        return await original(path)
    server.reply = reply
    client, archive = client_for(tmp_path, server, 1000)
    task = asyncio.create_task(collect(client, 1000))
    try:
        await asyncio.wait_for(replay_started.wait(), 1)
        await asyncio.sleep(0)
        assert canonical_started.is_set(), "canonical acquisition waited for the entire replay download"
    finally:
        release.set()
        await task


async def test_two_windows_preserve_full_fields_order_boundaries_and_timestamp_groups(tmp_path):
    canonical = events(700)
    canonical.extend([{"timestamp": 500, "type": "cast", "unknownNull": None, "large": 10**80} for _ in range(350)])
    canonical.sort(key=lambda event: event["timestamp"])
    server = Server(canonical, enrich(canonical))
    original = server.reply
    async def reply(path):
        if f"/1/0/500/" in path:
            await asyncio.sleep(0.01)  # Later range finishes first; archive order must remain chronological.
        return await original(path)
    server.reply = reply
    client, archive = client_for(tmp_path, server, 1000)
    received = await collect(client, 1000)
    assert received == canonical
    assert received[350]["killerID"] == 7
    assert any("/1/0/500/" in path for path in server.calls)
    assert any("/1/501/1000/" in path for path in server.calls)
    path = tmp_path / "full.wclf"
    archive.write_fight(path, {}, CODE, 1)
    assert PackedFightReader.read_raw_events(path) == canonical
    assert PackedFightReader._read_replay_events(path) == enrich(canonical)


@pytest.mark.parametrize("canonical,end", [(events(1), 1000), (events(1), 0), ([], 1000)])
async def test_empty_window_and_single_timestamp_range_are_complete(tmp_path, canonical, end):
    server = Server(canonical, enrich(canonical))
    client, archive = client_for(tmp_path, server, end)
    assert await collect(client, end) == canonical
    assert archive.fight_manifest(CODE, 1)["event_count"] == len(canonical)


@pytest.mark.parametrize("cursor", [0, -1, 1001, True, Decimal("0.5")])
async def test_invalid_window_cursor_raises_without_silent_truncation(tmp_path, cursor):
    server = Server(events(650), events(650))
    original = server.reply
    async def reply(path):
        status, body = await original(path)
        if "/summary-events/" in path:
            body["nextPageTimestamp"] = cursor
        return status, body
    server.reply = reply
    client, archive = client_for(tmp_path, server, 1000)
    with pytest.raises(RuntimeError, match="cursor|pagination"):
        await collect(client, 1000)
    assert archive.stats()["buffered_event_captures"] == 0


@pytest.mark.parametrize("failure_side", ["replay", "canonical"])
async def test_failed_acquisition_cancels_and_awaits_other_streams(tmp_path, failure_side):
    server = Server(events(650), events(650))
    active, entered = set(), asyncio.Event()
    async def reply(path):
        is_replay = "/replaysegment/" in path
        should_fail = is_replay if failure_side == "replay" else not is_replay
        if should_fail:
            await entered.wait()
            return 503, {"error": "offline"}
        active.add(path)
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            active.remove(path)
    server.reply = reply
    client, archive = client_for(tmp_path, server, 1000)
    with pytest.raises(RuntimeError, match="503"):
        await asyncio.wait_for(collect(client, 1000), 2)
    assert not active
    assert archive.stats()["buffered_event_captures"] == 0


async def test_exhausted_429_propagates_and_releases_all_capture_buffers(tmp_path):
    server = Server(events(650), events(650))
    original = server.reply
    async def reply(path):
        if "/summary-events/" in path:
            return 429, {"error": "Too Many Requests"}
        return await original(path)
    server.reply = reply
    client, archive = client_for(tmp_path, server, 1000)
    with pytest.raises(RateLimitError):
        await collect(client, 1000)
    assert archive.stats()["buffered_event_captures"] == 0


async def test_cancelled_acquisition_awaits_siblings_and_releases_buffers(tmp_path):
    server = Server(events(650), events(650))
    active, both_started = set(), asyncio.Event()
    async def reply(path):
        active.add(path)
        if any("/replaysegment/" in item for item in active) and any("/summary-events/" in item for item in active):
            both_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            active.remove(path)
    server.reply = reply
    client, archive = client_for(tmp_path, server, 1000)
    task = asyncio.create_task(collect(client, 1000))
    try:
        await asyncio.wait_for(both_started.wait(), 1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not active
    assert archive.stats()["buffered_event_captures"] == 0


@pytest.mark.parametrize("limit", [2, 8])
async def test_four_fights_share_one_bounded_request_semaphore(tmp_path, limit):
    server = Server(events(650), events(650))
    original = server.reply
    active, maximum = 0, 0
    async def reply(path):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        try:
            await asyncio.sleep(0.002)
            return await original(path)
        finally:
            active -= 1
    server.reply = reply
    client, archive = client_for(tmp_path, server, 1000, max_requests=limit)
    results = await asyncio.gather(*(collect(client, 1000, fight_id=i) for i in range(1, 5)))
    assert all(result == server.canonical for result in results)
    assert maximum == limit
    assert active == 0


async def test_request_reuses_decimal_capture_body_without_float_conversion(tmp_path):
    canonical = events(1)
    server = Server(canonical, canonical)
    client, archive = client_for(tmp_path, server, 1000)
    with archive.fight_capture(CODE, 1) as fetch_id:
        status, body = await client._request_json_impl("GET", f"/reports/summary-events/{CODE}/1/0/1000/0/0/Any/0/-1.0.-1.-1/0")
        assert status == 200
        assert type(body["events"][0]["exact"]) is Decimal
        assert body["events"][0]["exact"] == Decimal("0.12345678901234567890123456789")
        assert body is archive.get_capture_body(archive._attempts[fetch_id][-1])

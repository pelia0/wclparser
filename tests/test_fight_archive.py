import asyncio
from decimal import Decimal
import json
from pathlib import Path
import shutil

import pytest

from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader


MASTER = b'{"fights":[{"id":1,"start_time":0,"end_time":100}],"unknown":{"null":null}}'


def collect(archive, events_body, code="Report1", replay=None):
    master = archive.capture("GET", f"/reports/fights-and-participants/{code}/0", MASTER)
    with archive.fight_capture(code, 1) as fetch_id:
        event = archive.capture("GET", f"/reports/summary-events/{code}/1/0/100", events_body)
        supplemental = archive.capture("GET", f"/reports/replaysegment/{code}/99/0/100", replay) if replay else None
        archive.complete_fight(code, 1, fetch_id, 0, 100, len(json.loads(events_body)["events"]))
    return master, event, supplemental


def test_standalone_copy_preserves_every_field_and_exact_logical_numbers(tmp_path):
    root = tmp_path / "must-not-exist"
    archive = FightArchive(root)
    body = b'{"events":[{"timestamp":1,"unknown":184467440737095516160,"null":null,"bool":true,"nested":{"n":0.12345678901234567890123456789},"arr":[false,null,-184467440737095516160]},{"timestamp":2,"unknown":null}],"nextPageTimestamp":null,"pageExtra":{"number":1.00}}'
    master, event, _ = collect(archive, body)
    output = tmp_path / "original.wclf"
    assert archive.write_fight(output, {"report_code":"Report1","fight_id":1,"actors":[]}, "Report1", 1) == output.stat().st_size
    assert not root.exists()
    assert output.read_bytes().startswith(b"WCLF\x02")
    assert archive.stats()["source_bytes"] == archive.stats()["source_responses"] == 0
    assert archive.stats()["buffered_event_captures"] == 0
    copied = tmp_path / "copied.wclf"
    shutil.copyfile(output, copied)
    del archive
    output.unlink()
    expected = json.loads(body, parse_float=Decimal)
    assert PackedFightReader.read_raw_events(copied) == expected["events"]
    regenerated = PackedFightReader.read_source_response(copied, event)
    assert json.loads(regenerated, parse_float=Decimal) == expected
    assert json.loads(PackedFightReader.read_source_response(copied, master)) == json.loads(MASTER)
    manifest = PackedFightReader.source_manifest(copied)
    info = next(row for row in manifest["captures"] if row["id"] == event)
    assert info["kind"] == "events" and info["fetch_id"] == manifest["fetch_id"]
    assert info["url"].endswith("/1/0/100")
    assert PackedFightReader.is_packed_fight(copied)


def test_columnar_metadata_uses_slices_without_duplicate_event_arrays(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    collect(archive, b'{"events":[{"timestamp":1,"privateUnknown":"onlyhere"}],"other":true}')
    output = tmp_path / "fight.wclf"
    archive.write_fight(output, {}, "Report1", 1)
    metadata = PackedFightReader._read_section(output, "metadata")
    assert "onlyhere" not in repr(metadata)
    assert any(row.get("slice") == [0, 1] for row in metadata["captures"])


def test_graphql_master_and_events_keep_full_original_objects(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    master = {"data":{"reportData":{"report":{"fights":[{"id":1}],"masterData":{"actors":[]},"newKey":None}}},"extensions":{"cost":2}}
    mid = archive.capture("POST", "/api/v2/client", json.dumps(master).encode(), request_json={"variables":{"code":"Graph1"}})
    body = {"data":{"reportData":{"report":{"events":{"data":[{"timestamp":2,"new":True}],"nextPageTimestamp":None},"other":4}}},"extensions":{"cost":1}}
    with archive.fight_capture("Graph1", 1) as fetch:
        eid = archive.capture("POST", "/api/v2/client", json.dumps(body).encode(), request_json={"variables":{"code":"Graph1","fightId":1}})
        archive.complete_fight("Graph1", 1, fetch, 0, 10, 1)
    output = tmp_path / "graph.wclf"
    archive.write_fight(output, {}, "Graph1", 1)
    assert json.loads(PackedFightReader.read_source_response(output, mid)) == master
    assert json.loads(PackedFightReader.read_source_response(output, eid)) == body


def test_master_unknown_events_field_does_not_change_response_kind(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    master = {"fights":[{"id":1}], "events":[{"unknownMetadataEvent":True}]}
    mid = archive.capture("GET", "/reports/fights-and-participants/Master1/0", json.dumps(master).encode())
    assert mid is not None
    with archive.fight_capture("Master1", 1) as fetch:
        archive.capture("GET", "/reports/summary-events/Master1/1/0/100", b'{"events":[]}')
        archive.complete_fight("Master1", 1, fetch, 0, 100, 0)
    path = tmp_path / "master.wclf"
    archive.write_fight(path, {}, "Master1", 1)
    assert json.loads(PackedFightReader.read_source_response(path, mid)) == master


def test_corrupt_sections_are_rejected(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    collect(archive, b'{"events":[{"timestamp":1}]}')
    output = tmp_path / "fight.wclf"
    archive.write_fight(output, {}, "Report1", 1)
    data = bytearray(output.read_bytes())
    data[-1] ^= 1
    output.write_bytes(data)
    with pytest.raises(ValueError, match="(?i)corrupt|checksum"):
        PackedFightReader.read_raw_events(output)


@pytest.mark.asyncio
async def test_concurrent_contexts_failed_attempts_and_verified_write_release_buffers(tmp_path):
    archive = FightArchive(tmp_path / "unused")

    async def run(code):
        archive.capture("GET", f"/reports/fights-and-participants/{code}/0", MASTER)
        with archive.fight_capture(code, 1) as fetch:
            archive.capture("GET", f"/reports/summary-events/{code}/1/0/100", b'{"events":[{"timestamp":1}]}')
            await asyncio.sleep(0)
            archive.complete_fight(code, 1, fetch, 0, 100, 1)
        path = tmp_path / (code + ".wclf")
        archive.write_fight(path, {}, code, 1)
        assert PackedFightReader.source_manifest(path)["report_code"] == code

    await asyncio.gather(run("Concurrent1"), run("Concurrent2"))
    with pytest.raises(RuntimeError):
        with archive.fight_capture("Failed", 1):
            archive.capture("GET", "/reports/summary-events/Failed/1/0/100", b'{"events":[{"timestamp":1}]}')
            raise RuntimeError("download failed")
    assert archive.stats()["buffered_event_captures"] == 0
    assert not (tmp_path / "unused").exists()


def test_ignored_content_bounded_master_cache_and_retry_replaces_old_buffers(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    assert archive.capture("GET", "/rankings", b"<html>ranking</html>", source="rendered_html") is None
    assert archive.capture("GET", "/reports/summary-events/R/1/0/1", b'{"error":"rate limit"}', status=429) is None
    assert archive.capture("GET", "/other", b'{"unrelated":true}') is None
    for n in range(130):
        archive.capture("GET", f"/reports/fights-and-participants/R{n}/0", MASTER)
    assert archive.stats()["buffered_master_captures"] == 128
    collect(archive, b'{"events":[{"timestamp":1,"old":true}]}')
    collect(archive, b'{"events":[{"timestamp":2,"new":true}]}')
    assert archive.stats()["buffered_event_captures"] == 1
    path = tmp_path / "retry.wclf"
    archive.write_fight(path, {}, "Report1", 1)
    assert PackedFightReader.read_raw_events(path) == [{"timestamp":2,"new":True}]


def test_non_json_game_response_is_ignored_for_client_challenge_recovery(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    assert archive.capture("GET", "/reports/fights-and-participants/Challenge1/0", b"<html>challenge</html>") is None
    with archive.fight_capture("Challenge1", 1):
        assert archive.capture("GET", "/reports/summary-events/Challenge1/1/0/100", b"not JSON") is None
    assert archive.stats()["buffered_event_captures"] == 0


def test_concurrently_completed_replay_segments_are_sorted_by_requested_start(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    archive.capture("GET", "/reports/fights-and-participants/Replay1/0", MASTER)
    with archive.fight_capture("Replay1", 1) as fetch:
        archive.capture("GET", "/reports/summary-events/Replay1/1/0/100", b'{"events":[]}')
        late = archive.capture("GET", "/reports/replaysegment/Replay1/99/50/100/",
            b'{"events":[{"timestamp":60,"type":"damage","sourceID":1,"x":102,"y":202}]}')
        early = archive.capture("GET", "/reports/replaysegment/Replay1/99/0/49/",
            b'{"events":[{"timestamp":10,"type":"damage","sourceID":1,"x":100,"y":200}]}')
        archive.complete_fight("Replay1", 1, fetch, 0, 100, 0)
    path = tmp_path / "replay_order.wclf"
    archive.write_fight(path, {"actors":[{"id":1}]}, "Replay1", 1)
    frames = PackedFightReader.read_replay_only(path)["frames"]
    assert [frame["t"] for frame in frames] == [10, 60]
    assert PackedFightReader.source_manifest(path)["supplemental_captures"] == [early, late]


def test_replay_supplement_and_derived_viewer_survive_buffer_destruction(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    replay = b'{"events":[{"timestamp":10,"type":"damage","sourceID":1,"targetID":2,"abilityGameID":3,"amount":8,"x":100,"y":200,"unknownReplay":false}]}'
    _, _, replay_id = collect(archive, b'{"events":[{"timestamp":1,"type":"damage","sourceID":1,"targetID":2,"abilityGameID":3,"amount":7}]}', replay=replay)
    path = tmp_path / "replay.wclf"
    archive.write_fight(path, {"actors":[{"id":1,"name":"Player","type":"Monk"}]}, "Report1", 1)
    del archive
    assert PackedFightReader.read_events_only(path)[0]["amount"] == 7
    assert json.loads(PackedFightReader.read_source_response(path, replay_id)) == json.loads(replay)
    assert isinstance(PackedFightReader.read_replay_only(path)["frames"], list)


def test_failed_verification_keeps_old_file_and_releases_attempt_buffers(tmp_path, monkeypatch):
    archive = FightArchive(tmp_path / "unused")
    collect(archive, b'{"events":[{"timestamp":1}]}')
    path = tmp_path / "fight.wclf"
    path.write_bytes(b"previous committed file")
    monkeypatch.setattr(PackedFightReader, "_read_section", lambda *args: {"incorrect": True})
    with pytest.raises(ValueError, match="verification"):
        archive.write_fight(path, {}, "Report1", 1)
    assert path.read_bytes() == b"previous committed file"
    assert archive.stats()["buffered_event_captures"] == 0
    assert archive.stats()["buffered_complete_fights"] == 0
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.asyncio
async def test_cancelled_or_incomplete_capture_scope_drops_transient_pages(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    with archive.fight_capture("Incomplete", 1):
        archive.capture("GET", "/reports/summary-events/Incomplete/1/0/100", b'{"events":[]}')
    assert archive.stats()["buffered_event_captures"] == 0
    with pytest.raises(asyncio.CancelledError):
        with archive.fight_capture("Cancelled", 1):
            archive.capture("GET", "/reports/summary-events/Cancelled/1/0/100", b'{"events":[{}]}')
            raise asyncio.CancelledError()
    assert archive.stats()["buffered_event_captures"] == 0


@pytest.mark.asyncio
async def test_active_download_pins_metadata_across_concurrent_cache_eviction(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    mid = archive.capture("GET", "/reports/fights-and-participants/Active1/0", MASTER)
    started, evicted = asyncio.Event(), asyncio.Event()

    async def download():
        with archive.fight_capture("Active1", 1) as fetch:
            started.set()
            await evicted.wait()
            archive.capture("GET", "/reports/summary-events/Active1/1/0/100", b'{"events":[]}')
            archive.complete_fight("Active1", 1, fetch, 0, 100, 0)

    async def replace_cache():
        await started.wait()
        for n in range(130):
            archive.capture("GET", f"/reports/fights-and-participants/Evict{n}/0", MASTER)
        evicted.set()

    await asyncio.gather(download(), replace_cache())
    path = tmp_path / "pinned.wclf"
    archive.write_fight(path, {}, "Active1", 1)
    assert json.loads(PackedFightReader.read_source_response(path, mid)) == json.loads(MASTER)
    assert archive.stats()["buffered_event_captures"] == 0


def test_metadata_presence_probe_refreshes_cache_recency(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    for n in range(128):
        archive.capture("GET", f"/reports/fights-and-participants/Lru{n}/0", MASTER)
    assert archive.has_report_metadata("Lru0")
    archive.capture("GET", "/reports/fights-and-participants/Latest1/0", MASTER)
    assert archive.has_report_metadata("Lru0")
    assert not archive.has_report_metadata("Lru1")


def test_empty_records_and_multiple_pages_reconstruct_source_boundaries(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    archive.capture("GET", "/reports/fights-and-participants/Pages1/0", MASTER)
    bodies = [b'{"events":[{}, {"x":null}],"nextPageTimestamp":50,"custom":true}',
              b'{"events":[],"nextPageTimestamp":null,"custom":false}']
    ids = []
    with archive.fight_capture("Pages1", 1) as fetch:
        for cursor, body in zip((0,50),bodies):
            ids.append(archive.capture("GET", f"/reports/summary-events/Pages1/1/{cursor}/100", body))
        archive.complete_fight("Pages1", 1, fetch, 0, 100, 2)
    path = tmp_path / "pages.wclf"
    archive.write_fight(path, {}, "Pages1", 1)
    assert PackedFightReader.read_raw_events(path) == [{}, {"x":None}]
    assert [json.loads(PackedFightReader.read_source_response(path, cid)) for cid in ids] == [json.loads(b) for b in bodies]


def test_sparse_columns_bitmap_boundary_preserves_unknown_null_and_missing():
    from wcl_archiver.codec.fight_archive import _columns, _records
    events = [{"always":i} for i in range(9)]
    events[0]["unknown"] = None
    events[8]["unknown"] = {"nested":[False, None, 2**80]}
    events[7]["late"] = True
    table = _columns(events)
    assert table["columns"]["unknown"]["presence"] == b"\x01\x01"
    assert table["columns"]["unknown"]["values"] == [None, {"nested":[False, None, 2**80]}]
    assert "presence" not in table["columns"]["always"]
    assert _records(table) == events


@pytest.mark.parametrize("presence, values", [
    (b"\x01", [None]),  # Too short for nine records.
    (b"\x01\x00\x00", [None]),  # Too long.
    (b"\x01\x80", [None, True]),  # Bits past the final record.
    (b"\x01\x01", [None]),  # Presence/value count mismatch.
])
def test_corrupt_sparse_presence_bitmaps_are_rejected(presence, values):
    from wcl_archiver.codec.fight_archive import _records
    with pytest.raises(ValueError, match="(?i)corrupt|bitmap|presence|column"):
        _records({"count":9, "columns":{"field":{"presence":presence,"values":values}}})


def test_parsed_capture_body_reuses_precise_values_without_bulk_promotion(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    assert archive.supports_parallel_event_pages is True
    assert not getattr(archive,"supports_bulk_events",False)
    archive.capture("GET", "/reports/fights-and-participants/Precise1/0", MASTER)
    with archive.fight_capture("Precise1",1) as fetch:
        cid = archive.capture("GET", "/reports/summary-events/Precise1/1/0/100", b'{"events":[{"value":0.123456789012345678901,"unknown":[null,true,184467440737095516160]}]}')
        body = archive.get_capture_body(cid)
        assert body is archive.get_capture_body(cid)
        assert body["events"][0]["value"] == Decimal("0.123456789012345678901")
        archive.complete_fight("Precise1",1,fetch,0,100,1)
    path = tmp_path / "precise.wclf"
    archive.write_fight(path, {}, "Precise1",1)
    assert PackedFightReader.read_raw_events(path) == body["events"]


@pytest.mark.parametrize("grammar", ["summary-events","events/summary"])
def test_parallel_canonical_pages_are_ordered_by_requested_cursor_and_fully_reconstruct(tmp_path,grammar):
    archive = FightArchive(tmp_path / "unused")
    archive.capture("GET", "/reports/fights-and-participants/Parallel1/0", MASTER)
    pages = {
        (0,49): {"events":[{"timestamp":10,"id":"z","unknown":None},{"timestamp":10,"id":"a","bool":True}],"nextPageTimestamp":30,"extra":{"x":None}},
        (30,49): {"events":[{"timestamp":30,"id":"middle"}],"nextPageTimestamp":None,"extra":False},
        (50,100): {"events":[{"timestamp":60,"id":"late-first"}],"nextPageTimestamp":80},
        (80,100): {"events":[{"timestamp":80,"id":"late-last"}],"nextPageTimestamp":None},
    }
    ids = {}
    with archive.fight_capture("Parallel1",1) as fetch:
        for cursor,end in [(80,100),(30,49),(50,100),(0,49)]:
            body = pages[(cursor,end)]
            ids[(cursor,end)] = archive.capture("GET",f"/reports/{grammar}/Parallel1/1/{cursor}/{end}",json.dumps(body).encode())
        archive.complete_fight("Parallel1",1,fetch,0,100,5)
    path = tmp_path / "parallel.wclf"
    archive.write_fight(path,{},"Parallel1",1)
    expected = [event for body in pages.values() for event in body["events"]]
    assert PackedFightReader.read_raw_events(path) == expected
    assert PackedFightReader.source_manifest(path)["event_captures"] == list(ids[key] for key in pages)
    for key,cid in ids.items():
        assert json.loads(PackedFightReader.read_source_response(path,cid)) == pages[key]


def test_parallel_empty_window_retains_source_metadata_and_coverage(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    archive.capture("GET", "/reports/fights-and-participants/EmptyWindow1/0",MASTER)
    with archive.fight_capture("EmptyWindow1",1) as fetch:
        late = archive.capture("GET","/reports/summary-events/EmptyWindow1/1/50/100",b'{"events":[{"timestamp":60}],"nextPageTimestamp":null}')
        early = archive.capture("GET","/reports/summary-events/EmptyWindow1/1/0/49",b'{"events":[],"nextPageTimestamp":null,"unknown":{"empty":true}}')
        archive.complete_fight("EmptyWindow1",1,fetch,0,100,1)
    path = tmp_path / "empty_window.wclf"
    archive.write_fight(path,{},"EmptyWindow1",1)
    assert PackedFightReader.source_manifest(path)["event_captures"] == [early,late]
    assert json.loads(PackedFightReader.read_source_response(path,early))["unknown"] == {"empty":True}


@pytest.mark.parametrize("ranges,next_values", [
    ([(0,49),(51,100)],[None,None]),
    ([(0,99)],[None]),
    ([(0,100)],[50]),
    ([(0,100),(31,100)],[30,None]),
    ([(-1,100)],[None]),
    ([(0,101)],[None]),
    ([(0,100)],[0]),
])
def test_complete_rejects_gaps_truncation_and_invalid_canonical_page_bounds(tmp_path,ranges,next_values):
    archive = FightArchive(tmp_path / "unused")
    archive.capture("GET","/reports/fights-and-participants/InvalidPages1/0",MASTER)
    with archive.fight_capture("InvalidPages1",1) as fetch:
        for (cursor,end),next_value in zip(ranges,next_values):
            archive.capture("GET",f"/reports/summary-events/InvalidPages1/1/{cursor}/{end}",json.dumps({"events":[],"nextPageTimestamp":next_value}).encode())
        with pytest.raises(ValueError,match="(?i)page|window|cursor|coverage|bound"):
            archive.complete_fight("InvalidPages1",1,fetch,0,100,0)
    assert archive.stats()["buffered_complete_fights"] == 0
    assert archive.stats()["buffered_event_captures"] == 0


@pytest.mark.parametrize("wrong_url", ["/reports/summary-events/Wrong/1/0/100","/reports/summary-events/BoundPages1/2/0/100"])
def test_complete_rejects_canonical_url_report_or_fight_mismatch(tmp_path,wrong_url):
    archive = FightArchive(tmp_path / "unused")
    archive.capture("GET","/reports/fights-and-participants/BoundPages1/0",MASTER)
    with archive.fight_capture("BoundPages1",1) as fetch:
        cid = archive.capture("GET","/reports/summary-events/BoundPages1/1/0/100",b'{"events":[]}')
        archive._captures[cid]["info"]["url"] = wrong_url
        with pytest.raises(ValueError,match="(?i)report|fight|binding"):
            archive.complete_fight("BoundPages1",1,fetch,0,100,0)


def test_canonical_page_integral_decimal_cursor_matches_integer_request(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    archive.capture("GET","/reports/fights-and-participants/DecimalCursor1/0",MASTER)
    with archive.fight_capture("DecimalCursor1",1) as fetch:
        archive.capture("GET","/reports/summary-events/DecimalCursor1/1/0/100",b'{"events":[{"timestamp":10}],"nextPageTimestamp":50.0}')
        archive.capture("GET","/reports/summary-events/DecimalCursor1/1/50/100",b'{"events":[],"nextPageTimestamp":null}')
        archive.complete_fight("DecimalCursor1",1,fetch,0,100,1)
    path = tmp_path / "decimal_cursor.wclf"
    archive.write_fight(path,{},"DecimalCursor1",1)
    assert PackedFightReader.read_raw_events(path) == [{"timestamp":10}]


@pytest.mark.parametrize("bad_cursor", [True,Decimal("50.5"),Decimal("NaN"),Decimal("Infinity"),Decimal("-Infinity")])
def test_canonical_page_rejects_boolean_nonintegral_and_nonfinite_cursors(tmp_path,bad_cursor):
    archive = FightArchive(tmp_path / "unused")
    cap = {"info":{"url":"/reports/summary-events/BadCursor1/1/0/100"},"body":{"events":[],"nextPageTimestamp":bad_cursor}}
    with pytest.raises(ValueError,match="(?i)cursor"):
        archive._ordered_canonical_pages([cap],"BadCursor1",1,0,100)


def test_graphql_event_capture_order_remains_unchanged(tmp_path):
    archive = FightArchive(tmp_path / "unused")
    archive.capture("GET","/reports/fights-and-participants/GraphOrder1/0",MASTER)
    with archive.fight_capture("GraphOrder1",1) as fetch:
        ids = []
        for timestamp in (60,10):
            body = {"data":{"reportData":{"report":{"events":{"data":[{"timestamp":timestamp}],"nextPageTimestamp":None}}}}}
            ids.append(archive.capture("POST","/api/v2/client",json.dumps(body).encode(),request_json={"variables":{"code":"GraphOrder1","fightId":1}}))
        archive.complete_fight("GraphOrder1",1,fetch,0,100,2)
    path = tmp_path / "graphql_order.wclf"
    archive.write_fight(path,{},"GraphOrder1",1)
    assert PackedFightReader.read_raw_events(path) == [{"timestamp":60},{"timestamp":10}]
    assert PackedFightReader.source_manifest(path)["event_captures"] == ids

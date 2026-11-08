import json
from pathlib import Path
import pytest
from wcl_archiver.codec.varint_delta import encode_delta_ints, decode_delta_ints
from wcl_archiver.codec.wclz_format import WCLZWriter, WCLZReader


def test_delta_varint_roundtrip_handles_negative_and_monotonic_series():
    values = [0, 15, 15, 48, 120, -41289, -41289, -41280, 9999999]
    encoded = encode_delta_ints(values)
    decoded = decode_delta_ints(encoded, len(values))
    assert decoded == values


def test_delta_varint_empty_and_single():
    assert decode_delta_ints(encode_delta_ints([]), 0) == []
    assert decode_delta_ints(encode_delta_ints([42]), 1) == [42]
    assert decode_delta_ints(encode_delta_ints([-42]), 1) == [-42]


def test_wclz_single_fight_sections_and_high_compression(tmp_path: Path):
    summary_section = {
        "report_code": "aBcDeFg123",
        "fight_id": 2,
        "encounter_id": 11492,
        "keystone_level": 28,
        "duration_ms": 840000,
        "actors": [
            {"id": 1, "name": "Pellia", "type": "Player", "subType": "Monk-Windwalker"},
            {"id": 99, "name": "Helya", "type": "Boss", "subType": "Boss"},
        ],
        "damage_table": [{"actor_id": 1, "total": 185000000, "dps": 2202380.9}],
    }

    events = []
    t = 1000
    x, y = -41200, 28100
    for i in range(5000):
        t += 20 if (i % 3) else 0
        x += (i % 5) - 2
        y += (i % 3) - 1
        events.append(
            {
                "timestamp": t,
                "type": "damage" if (i % 2 == 0) else "cast",
                "sourceID": 1,
                "targetID": 99,
                "targetInstance": 0,
                "abilityGameID": 113656 if (i % 4 == 0) else 100780,
                "hitType": 2 if (i % 3 == 0) else 1,
                "amount": 485000 + (i % 100) * 10,
                "unmitigatedAmount": 500000,
                "absorbed": 0,
                "overkill": 0,
                "hitPoints": 95000000 - i * 1000,
                "maxHitPoints": 100000000,
                "resourceType": 12,
                "resourceAmount": 3,
                "resourceMax": 5,
                "x": x,
                "y": y,
                "facing": 180,
            }
        )

    replay_stream = {
        "map_id": 1492,
        "frames": [
            {"t": e["timestamp"], "actor": 1, "x": e["x"], "y": e["y"], "hp_pct": 9500}
            for e in events[::5]
        ],
    }

    wclz_path = tmp_path / "k28_maw_aBcDeFg123_f2.wclz"
    WCLZWriter.write_fight(wclz_path, summary_section, events, replay_stream, zstd_level=22)

    raw_json_bytes = len(json.dumps({"s": summary_section, "e": events, "r": replay_stream}).encode("utf-8"))
    compressed_bytes = wclz_path.stat().st_size
    assert compressed_bytes * 20 < raw_json_bytes

    s_only = WCLZReader.read_summary_only(wclz_path)
    assert s_only["keystone_level"] == 28
    assert s_only["actors"][0]["name"] == "Pellia"

    r_only = WCLZReader.read_replay_only(wclz_path)
    assert r_only["map_id"] == 1492
    assert len(r_only["frames"]) == len(replay_stream["frames"])
    assert r_only["frames"][17] == replay_stream["frames"][17]

    e_only = WCLZReader.read_events_only(wclz_path)
    assert len(e_only) == 5000
    assert e_only[0] == events[0]
    assert e_only[2499] == events[2499]
    assert e_only[-1] == events[-1]

    # Test read_full_fight and read_header
    hdr = WCLZReader.read_header(wclz_path)
    assert hdr["version"] == 1
    assert hdr["event_count"] == 5000
    assert hdr["duration_ms"] == 840000
    assert hdr["checksum64"] != 0

    full = WCLZReader.read_full_fight(wclz_path)
    assert full["summary"]["report_code"] == "aBcDeFg123"
    assert len(full["events"]) == 5000
    assert full["replay"]["map_id"] == 1492


def test_wclz_invalid_magic(tmp_path: Path):
    bad_file = tmp_path / "corrupt.wclz"
    bad_file.write_bytes(b"NOPE" + b"\x00" * 72)
    with pytest.raises(ValueError, match="Invalid WCLZ magic"):
        WCLZReader.read_header(bad_file)


def test_wclz_truncated_header_and_null_replay_fields(tmp_path: Path):
    trunc_file = tmp_path / "truncated.wclz"
    trunc_file.write_bytes(b"WCLZ\x01\x00")
    with pytest.raises(ValueError, match="Truncated WCLZ header"):
        WCLZReader.read_header(trunc_file)

    # Replay frame with 0 hp_pct and null/none coordinates
    wclz_path = tmp_path / "null_replay.wclz"
    summary = {"duration_ms": 1000}
    events = []
    replay = {
        "map_id": 100,
        "frames": [
            {"t": None, "actor": None, "x": None, "y": None, "hp_pct": 0},
            {"t": 500, "actor": 2, "x": 10, "y": 20, "hp_pct": None},
        ],
    }
    WCLZWriter.write_fight(wclz_path, summary, events, replay, zstd_level=1)
    read_rep = WCLZReader.read_replay_only(wclz_path)
    assert len(read_rep["frames"]) == 2
    assert read_rep["frames"][0] == {"t": 0, "actor": 0, "x": 0, "y": 0, "hp_pct": 0}
    assert read_rep["frames"][1] == {"t": 500, "actor": 2, "x": 10, "y": 20, "hp_pct": 10000}

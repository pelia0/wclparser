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
    assert hdr["version"] == 2
    assert hdr["flags"] == 1
    assert hdr["event_count"] == 5000
    assert hdr["duration_ms"] == 840000
    assert hdr["checksum64"] != 0

    full = WCLZReader.read_full_fight(wclz_path)
    assert full["summary"]["report_code"] == "aBcDeFg123"
    assert len(full["events"]) == 5000
    assert full["replay"]["map_id"] == 1492
    assert full["replay"]["frames"] == replay_stream["frames"]


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


def test_wclz_v2_derived_replay_cast_timeline_and_v1_compat(tmp_path: Path):
    import struct
    import msgpack
    import xxhash
    import zstandard as zstd
    from wcl_archiver.codec.wclz_format import HEADER_STRUCT, HEADER_SIZE, MAGIC, INT_COLUMNS
    from wcl_archiver.parser.combat_state_machine import CombatStateMachine

    raw_events = [
        {
            "timestamp": 1000,
            "type": "cast",
            "sourceID": 1,
            "targetID": 99,
            "abilityGameID": 100780,
            "hitPoints": 5000000,
            "maxHitPoints": 10000000,
            "x": -41000,
            "y": 28000,
            "classResources": [{"type": 12, "amount": 3, "max": 5}],
        },
        {
            "timestamp": 1250,
            "type": "applybuff",
            "sourceID": 1,
            "targetID": 1,
            "abilityGameID": 137639,
            "hitPoints": 5000000,
            "maxHitPoints": 10000000,
            "x": -41010,
            "y": 28010,
        },
        {
            "timestamp": 1500,
            "type": "cast",
            "sourceID": 1,
            "targetID": 99,
            "abilityGameID": 113656,
            "hitPoints": 4900000,
            "maxHitPoints": 10000000,
            "x": -41010,
            "y": 28010,
            "classResources": [{"type": 12, "amount": 2, "max": 5}],
        },
    ]
    fight_meta = {
        "report_code": "testV2",
        "fight_id": 1,
        "start_time": 1000,
        "end_time": 6000,
        "encounter_id": 11492,
        "name": "Maw of Souls",
        "keystone_level": 28,
        "map_id": 1492,
        "pulls": [],
    }
    actors = [{"id": 1, "name": "Pellia", "type": "Monk"}, {"id": 99, "name": "Helya", "type": "Boss"}]
    abilities = [{"gameID": 100780, "name": "Tiger Palm", "type": 1}]
    summary, norm_events, replay = CombatStateMachine.analyze_fight(fight_meta, actors, abilities, raw_events)

    v2_path = tmp_path / "fight_v2.wclz"
    WCLZWriter.write_fight(v2_path, summary, norm_events, replay)
    hdr = WCLZReader.read_header(v2_path)
    assert hdr["version"] == 2
    assert hdr["sec3_size"] < 200  # derived replay takes minimal space

    full_v2 = WCLZReader.read_full_fight(v2_path)
    assert full_v2["summary"] == msgpack.unpackb(msgpack.packb(summary, use_bin_type=True), raw=False)
    assert full_v2["events"] == norm_events
    assert full_v2["replay"] == replay

    # Synthesize a legacy v1 Zstd file and verify WCLZReader reads it cleanly
    v1_path = tmp_path / "legacy_v1.wclz"
    type_vocab, type_list, type_indices = {}, [], []
    cols = {c: [] for c in INT_COLUMNS}
    for ev in norm_events:
        ts = ev["type"]
        if ts not in type_vocab:
            type_vocab[ts] = len(type_list)
            type_list.append(ts)
        type_indices.append(type_vocab[ts])
        for c in INT_COLUMNS:
            cols[c].append(ev[c])
    s1 = zstd.ZstdCompressor(level=1).compress(msgpack.packb(summary, use_bin_type=True))
    s2 = zstd.ZstdCompressor(level=1).compress(
        msgpack.packb(
            {
                "count": len(norm_events),
                "type_list": type_list,
                "type_indices": encode_delta_ints(type_indices),
                "cols": {c: encode_delta_ints(v) for c, v in cols.items()},
            },
            use_bin_type=True,
        )
    )
    frames = replay["frames"]
    s3 = zstd.ZstdCompressor(level=1).compress(
        msgpack.packb(
            {
                "map_id": replay["map_id"],
                "pulls": replay["pulls"],
                "count": len(frames),
                "t": encode_delta_ints([f["t"] for f in frames]),
                "actor": encode_delta_ints([f["actor"] for f in frames]),
                "x": encode_delta_ints([f["x"] for f in frames]),
                "y": encode_delta_ints([f["y"] for f in frames]),
                "hp_pct": encode_delta_ints([f["hp_pct"] for f in frames]),
            },
            use_bin_type=True,
        )
    )
    o1 = HEADER_SIZE
    o2 = o1 + len(s1)
    o3 = o2 + len(s2)
    chk = xxhash.xxh64(s1 + s2 + s3).intdigest()
    v1_hdr = HEADER_STRUCT.pack(MAGIC, 1, 0, 0, len(norm_events), 5000, o1, len(s1), o2, len(s2), o3, len(s3), chk)
    v1_path.write_bytes(v1_hdr + s1 + s2 + s3)

    full_v1 = WCLZReader.read_full_fight(v1_path)
    assert full_v1 == full_v2

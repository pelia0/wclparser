"""Lossless compact storage, independently constructed legacy/corrupt fixtures."""
from copy import deepcopy
from decimal import Decimal
import hashlib
import lzma
import struct
import zlib

import pytest
import zstandard as zstd

from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader, _columns, _pack, _unpack


def sections():
    return {"summary": {"unknown": [True, 1, Decimal("1.00"), 2**100]},
            "events": _columns([{"timestamp":10,"same":None,"typed":True},
                                {"timestamp":5,"same":None,"typed":False}, {}]),
            "replay": _columns([{"typed":1,"timestamp":10,"same":None,"new":False},
                                {"typed":0,"timestamp":5,"same":None}, {}]),
            "metadata": {"manifest":{"event_count":3}, "captures":[]}}


def fixture(path, values, magic=b"WCLF\x02", codecs=None, encodings=None):
    index, chunks, offset = {}, [], 0
    for name, value in values.items():
        raw = _pack(value)
        codec = (codecs or {}).get(name, "zstd")
        chunk = lzma.compress(raw, preset=6) if codec == "lzma" else zstd.ZstdCompressor(level=9).compress(raw)
        index[name] = {"offset":offset,"length":len(chunk),"size":len(raw),"sha256":hashlib.sha256(raw).digest()}
        if codecs:
            index[name]["compression"] = codec
        if encodings and name in encodings:
            index[name]["encoding"] = encodings[name]
        chunks.append(chunk)
        offset += len(chunk)
    header = _pack(index)
    path.write_bytes(magic + struct.pack("<I",len(header)) + header + b"".join(chunks))


def physical(path, name):
    data = path.read_bytes()
    length = struct.unpack("<I", data[5:9])[0]
    index = _unpack(data[9:9+length])
    section = index[name]
    raw = data[9+length+section["offset"]:9+length+section["offset"]+section["length"]]
    return index, _unpack(lzma.decompress(raw) if section.get("compression") == "lzma" else zstd.ZstdDecompressor().decompress(raw))


@pytest.mark.parametrize("profile", ["fast", "compact"])
def test_compact_roundtrip_preserves_typed_sections_and_replay_column_order(tmp_path, profile):
    source = sections()
    before = _pack(source)
    path = tmp_path / "fight.wclf"
    assert FightArchive.write_sections(path, source, compression=profile) == path.stat().st_size
    assert path.read_bytes().startswith(b"WCLF\x03")
    assert PackedFightReader.is_packed_fight(path)
    assert _pack(PackedFightReader.read_sections(path)) == before
    assert _pack(source) == before
    index, replay = physical(path,"replay")
    assert replay["shared_columns"] == {"timestamp":"timestamp", "same":"same"}
    assert replay["column_order"] == ["typed","timestamp","same","new"]
    assert "typed" in replay["table"]["columns"]  # bool and int must never share.
    assert index["summary"]["compression"] == "zstd"  # Smaller than XZ for tiny sections.


def test_old_v2_fixture_remains_readable_without_any_transform(tmp_path):
    path = tmp_path / "old.wclf"
    source = sections()
    source["metadata"]["captures"] = [{"id":"old-page", "kind":"events", "slice":[0,3],
        "event_path":["events"], "template":{"events":None,"unknown":{"decimal":Decimal("1.00")}}}]
    fixture(path, source)
    assert PackedFightReader.is_packed_fight(path)
    assert _pack(PackedFightReader.read_sections(path)) == _pack(source)
    assert PackedFightReader.read_raw_events(path) == [{"timestamp":10,"same":None,"typed":True}, {"timestamp":5,"same":None,"typed":False}, {}]
    assert PackedFightReader.read_source_response(path,"old-page") == b'{"events":[{"timestamp":10,"same":null,"typed":true},{"timestamp":5,"same":null,"typed":false},{}],"unknown":{"decimal":1.00}}'


def test_count_mismatch_keeps_full_replay_and_unknown_table_fields(tmp_path):
    source = sections()
    source["events"]["future"] = {"precision":Decimal("2.000")}
    source["replay"] = {"future":False, **_columns([{"timestamp":10}])}
    path = tmp_path / "mismatch.wclf"
    FightArchive.write_sections(path,source)
    assert _pack(PackedFightReader.read_sections(path)) == _pack(source)
    assert physical(path,"replay")[1]["shared_columns"] == {}


def test_integer_delta_is_exact_and_only_used_when_compression_saves_bytes(tmp_path):
    source = sections()
    source["events"] = _columns([{"timestamp":2**90 + i*7, "bool":True, "decimal":Decimal("1.00")} for i in range(3000)])
    path = tmp_path / "delta.wclf"
    FightArchive.write_sections(path,source,compression="fast")
    encoded = physical(path,"events")[1]
    assert encoded["delta_columns"] == ["timestamp"]
    assert _pack(PackedFightReader.read_sections(path)) == _pack(source)


@pytest.mark.parametrize("mutation", ["missing", "cycle", "count", "order", "delta", "codec", "encoding", "negative_offset", "compression_missing", "encoding_missing", "overlap"])
def test_invalid_transforms_and_section_descriptors_are_rejected(tmp_path, mutation):
    source = sections()
    replay = {"table":deepcopy(source["replay"]), "shared_columns":{"timestamp":"timestamp"},
              "column_order":list(source["replay"]["columns"]),"delta_columns":[]}
    del replay["table"]["columns"]["timestamp"]
    source["replay"] = replay
    source["events"] = {"table":source["events"], "shared_columns":{},
                         "column_order":list(source["events"]["columns"]),"delta_columns":[]}
    if mutation == "missing": replay["shared_columns"]["timestamp"] = "missing"
    if mutation == "cycle": replay["shared_columns"]["timestamp"] = {"section":"replay","column":"timestamp"}
    if mutation == "count": replay["table"]["count"] = 4
    if mutation == "order": replay["column_order"].append("timestamp")
    if mutation == "delta": replay["delta_columns"] = ["typed"]  # ints only: fixture contains [1,0].
    if mutation == "delta": replay["table"]["columns"]["typed"]["values"] = [True, False]
    codecs = {"replay":"unknown"} if mutation == "codec" else {"replay":"zstd"}
    encodings = {"events":"columns-v1", "replay":"unknown" if mutation == "encoding" else "columns-v1"}
    path = tmp_path / "corrupt.wclf"
    fixture(path,source,magic=b"WCLF\x03",codecs=codecs,encodings=encodings)
    if mutation in ("negative_offset", "compression_missing", "encoding_missing", "overlap"):
        data = path.read_bytes()
        length = struct.unpack("<I",data[5:9])[0]
        index = _unpack(data[9:9+length])
        if mutation == "negative_offset": index["replay"]["offset"] = -1
        if mutation == "compression_missing": del index["replay"]["compression"]
        if mutation == "encoding_missing": del index["replay"]["encoding"]
        if mutation == "overlap": index["replay"] = dict(index["events"])
        header = _pack(index)
        path.write_bytes(data[:5]+struct.pack("<I",len(header))+header+data[9+length:])
    with pytest.raises(ValueError,match="(?i)corrupt|codec|encoding|reference|column|count"):
        PackedFightReader._read_section(path,"replay")


def test_compression_failure_preserves_old_destination_and_cleans_temp(tmp_path, monkeypatch):
    path = tmp_path / "fight.wclf"
    path.write_bytes(b"committed")
    def fail(*args, **kwargs): raise lzma.LZMAError("failed compression")
    monkeypatch.setattr(lzma,"compress",fail)
    with pytest.raises(lzma.LZMAError):
        FightArchive.write_sections(path,sections(),compression="compact")
    assert path.read_bytes() == b"committed"
    assert list(tmp_path.glob("*.tmp")) == []


def test_write_verification_distinguishes_boolean_from_integer(tmp_path, monkeypatch):
    path = tmp_path / "fight.wclf"
    path.write_bytes(b"committed")
    source = sections()
    original = PackedFightReader._read_section
    def read(path, name):
        value = original(path,name)
        if name == "summary": value["unknown"][0] = 1
        return value
    monkeypatch.setattr(PackedFightReader,"_read_section",read)
    with pytest.raises(ValueError,match="verification"):
        FightArchive.write_sections(path,source)
    assert path.read_bytes() == b"committed"
    assert list(tmp_path.glob("*.tmp")) == []


def test_lzma_section_rejects_excessive_dictionary_memory(tmp_path):
    path = tmp_path / "dictionary.wclf"
    fixture(path, sections(), magic=b"WCLF\x03", codecs={"summary":"lzma"})
    data = bytearray(path.read_bytes())
    header_size = struct.unpack("<I",data[5:9])[0]
    start = 9 + header_size  # Summary is the first section.
    # Raise the LZMA2 declared dictionary from preset 6's 8 MiB to 256 MiB.
    # The tiny valid payload needs no such history; the stream otherwise decodes.
    data[start+16] = 32
    data[start+20:start+24] = struct.pack("<I", zlib.crc32(data[start+12:start+20]))
    path.write_bytes(data)
    with pytest.raises(ValueError,match="(?i)corrupt|memory"):
        PackedFightReader.read_summary_only(path)

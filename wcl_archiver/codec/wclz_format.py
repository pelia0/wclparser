import struct
from pathlib import Path
from typing import Any, Dict, List, Optional
import msgpack
import xxhash
import zstandard as zstd

from wcl_archiver.codec.varint_delta import encode_delta_ints, decode_delta_ints

HEADER_STRUCT = struct.Struct("<4sHHIIIQQQQQQQ")
HEADER_SIZE = HEADER_STRUCT.size
MAGIC = b"WCLZ"
VERSION = 1

INT_COLUMNS = (
    "timestamp",
    "sourceID",
    "targetID",
    "targetInstance",
    "abilityGameID",
    "hitType",
    "amount",
    "unmitigatedAmount",
    "absorbed",
    "overkill",
    "hitPoints",
    "maxHitPoints",
    "resourceType",
    "resourceAmount",
    "resourceMax",
    "x",
    "y",
    "facing",
)


class WCLZWriter:
    @staticmethod
    def _pack_events_columnar(events: List[Dict[str, Any]]) -> bytes:
        type_vocab: Dict[str, int] = {}
        type_list: List[str] = []
        type_indices: List[int] = []

        columns: Dict[str, List[int]] = {col: [] for col in INT_COLUMNS}

        for ev in events:
            t_str = str(ev.get("type", "unknown"))
            if t_str not in type_vocab:
                type_vocab[t_str] = len(type_list)
                type_list.append(t_str)
            type_indices.append(type_vocab[t_str])

            for col in INT_COLUMNS:
                columns[col].append(int(ev.get(col, 0) or 0))

        packed_cols = {col: encode_delta_ints(vals) for col, vals in columns.items()}
        payload = {
            "count": len(events),
            "type_list": type_list,
            "type_indices": encode_delta_ints(type_indices),
            "cols": packed_cols,
        }
        return msgpack.packb(payload, use_bin_type=True)

    @staticmethod
    def _pack_replay_columnar(replay_stream: Dict[str, Any]) -> bytes:
        frames = replay_stream.get("frames", [])
        payload = {
            "map_id": int(replay_stream.get("map_id", 0)),
            "pulls": replay_stream.get("pulls", []),
            "count": len(frames),
            "t": encode_delta_ints([int(f.get("t", 0) or 0) for f in frames]),
            "actor": encode_delta_ints([int(f.get("actor", 0) or 0) for f in frames]),
            "x": encode_delta_ints([int(f.get("x", 0) or 0) for f in frames]),
            "y": encode_delta_ints([int(f.get("y", 0) or 0) for f in frames]),
            "hp_pct": encode_delta_ints(
                [int(f.get("hp_pct", 0) if f.get("hp_pct") is not None else 10000) for f in frames]
            ),
        }
        return msgpack.packb(payload, use_bin_type=True)

    @classmethod
    def write_fight(
        cls,
        file_path: Path,
        summary_section: Dict[str, Any],
        events: List[Dict[str, Any]],
        replay_stream: Dict[str, Any],
        zstd_level: int = 22,
        zstd_dict_bytes: Optional[bytes] = None,
    ) -> int:
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")

        dict_data = zstd.ZstdCompressionDict(zstd_dict_bytes) if zstd_dict_bytes else None
        dict_id = dict_data.dict_id() if dict_data else 0
        cctx = zstd.ZstdCompressor(level=zstd_level, dict_data=dict_data)

        sec1_raw = msgpack.packb(summary_section, use_bin_type=True)
        sec2_raw = cls._pack_events_columnar(events)
        sec3_raw = cls._pack_replay_columnar(replay_stream)

        sec1_comp = cctx.compress(sec1_raw)
        sec2_comp = cctx.compress(sec2_raw)
        sec3_comp = cctx.compress(sec3_raw)

        sec1_offset = HEADER_SIZE
        sec1_size = len(sec1_comp)
        sec2_offset = sec1_offset + sec1_size
        sec2_size = len(sec2_comp)
        sec3_offset = sec2_offset + sec2_size
        sec3_size = len(sec3_comp)

        hasher = xxhash.xxh64()
        hasher.update(sec1_comp)
        hasher.update(sec2_comp)
        hasher.update(sec3_comp)
        checksum64 = hasher.intdigest()

        header = HEADER_STRUCT.pack(
            MAGIC,
            VERSION,
            0,
            dict_id,
            len(events),
            int(summary_section.get("duration_ms", 0)),
            sec1_offset,
            sec1_size,
            sec2_offset,
            sec2_size,
            sec3_offset,
            sec3_size,
            checksum64,
        )

        with open(tmp_path, "wb") as f:
            f.write(header)
            f.write(sec1_comp)
            f.write(sec2_comp)
            f.write(sec3_comp)

        tmp_path.replace(file_path)
        return file_path.stat().st_size


class WCLZReader:
    @staticmethod
    def read_header(file_path: Path) -> Dict[str, int]:
        with open(file_path, "rb") as f:
            buf = f.read(HEADER_SIZE)
        if len(buf) < HEADER_SIZE:
            raise ValueError(f"Truncated WCLZ header in {file_path}")
        (
            magic,
            version,
            flags,
            dict_id,
            event_count,
            duration_ms,
            sec1_offset,
            sec1_size,
            sec2_offset,
            sec2_size,
            sec3_offset,
            sec3_size,
            checksum64,
        ) = HEADER_STRUCT.unpack(buf)
        if magic != MAGIC:
            raise ValueError(f"Invalid WCLZ magic in {file_path}")
        return {
            "version": version,
            "flags": flags,
            "dict_id": dict_id,
            "event_count": event_count,
            "duration_ms": duration_ms,
            "sec1_offset": sec1_offset,
            "sec1_size": sec1_size,
            "sec2_offset": sec2_offset,
            "sec2_size": sec2_size,
            "sec3_offset": sec3_offset,
            "sec3_size": sec3_size,
            "checksum64": checksum64,
        }

    @staticmethod
    def _read_decompressed_slice(
        file_path: Path, offset: int, size: int, zstd_dict_bytes: Optional[bytes] = None
    ) -> bytes:
        with open(file_path, "rb") as f:
            f.seek(offset)
            comp = f.read(size)
        dict_data = zstd.ZstdCompressionDict(zstd_dict_bytes) if zstd_dict_bytes else None
        dctx = zstd.ZstdDecompressor(dict_data=dict_data)
        return dctx.decompress(comp)

    @classmethod
    def read_summary_only(
        cls, file_path: Path, zstd_dict_bytes: Optional[bytes] = None
    ) -> Dict[str, Any]:
        hdr = cls.read_header(file_path)
        raw = cls._read_decompressed_slice(
            file_path, hdr["sec1_offset"], hdr["sec1_size"], zstd_dict_bytes
        )
        return msgpack.unpackb(raw, raw=False)

    @classmethod
    def read_events_only(
        cls, file_path: Path, zstd_dict_bytes: Optional[bytes] = None
    ) -> List[Dict[str, Any]]:
        hdr = cls.read_header(file_path)
        raw = cls._read_decompressed_slice(
            file_path, hdr["sec2_offset"], hdr["sec2_size"], zstd_dict_bytes
        )
        payload = msgpack.unpackb(raw, raw=False)
        count = int(payload["count"])
        type_list = payload["type_list"]
        type_indices = decode_delta_ints(payload["type_indices"], count)
        decoded_cols = {
            col: decode_delta_ints(payload["cols"][col], count) for col in INT_COLUMNS
        }

        events: List[Dict[str, Any]] = []
        for i in range(count):
            ev: Dict[str, Any] = {"type": type_list[type_indices[i]]}
            for col in INT_COLUMNS:
                ev[col] = decoded_cols[col][i]
            events.append(ev)
        return events

    @classmethod
    def read_replay_only(
        cls, file_path: Path, zstd_dict_bytes: Optional[bytes] = None
    ) -> Dict[str, Any]:
        hdr = cls.read_header(file_path)
        raw = cls._read_decompressed_slice(
            file_path, hdr["sec3_offset"], hdr["sec3_size"], zstd_dict_bytes
        )
        payload = msgpack.unpackb(raw, raw=False)
        count = int(payload["count"])
        t_col = decode_delta_ints(payload["t"], count)
        actor_col = decode_delta_ints(payload["actor"], count)
        x_col = decode_delta_ints(payload["x"], count)
        y_col = decode_delta_ints(payload["y"], count)
        hp_col = decode_delta_ints(payload["hp_pct"], count)

        frames = [
            {"t": t_col[i], "actor": actor_col[i], "x": x_col[i], "y": y_col[i], "hp_pct": hp_col[i]}
            for i in range(count)
        ]
        return {
            "map_id": payload["map_id"],
            "pulls": payload.get("pulls", []),
            "frames": frames,
        }

    @classmethod
    def read_full_fight(
        cls, file_path: Path, zstd_dict_bytes: Optional[bytes] = None
    ) -> Dict[str, Any]:
        return {
            "summary": cls.read_summary_only(file_path, zstd_dict_bytes),
            "events": cls.read_events_only(file_path, zstd_dict_bytes),
            "replay": cls.read_replay_only(file_path, zstd_dict_bytes),
        }

from collections import Counter
import lzma
import struct
from pathlib import Path
from typing import Any, Dict, List, Optional
import msgpack
import xxhash
import zstandard as zstd

from wcl_archiver.codec.varint_delta import (
    encode_delta_ints,
    decode_delta_ints,
    encode_uvarints,
    decode_uvarints,
    encode_zvarints,
    decode_zvarints,
)

HEADER_STRUCT = struct.Struct("<4sHHIIIQQQQQQQ")
HEADER_SIZE = HEADER_STRUCT.size
MAGIC = b"WCLZ"
VERSION = 2
FLAG_LZMA = 1
_EXPECTED_CAST_KEYS = frozenset(
    {"t_ms", "ability_id", "target_id", "active_buffs", "resource_type", "resource_amount"}
)

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


def _derive_replay_frames_from_events(events: List[Dict[str, Any]]) -> List[Dict[str, int]]:
    last_pos: Dict[int, tuple[int, int, int]] = {}
    frames: List[Dict[str, int]] = []
    for ev in events:
        src_id = int(ev.get("sourceID", 0) or 0)
        x_val = int(ev.get("x", 0) or 0)
        y_val = int(ev.get("y", 0) or 0)
        if (x_val != 0 or y_val != 0) and src_id:
            raw_hp = int(ev.get("hitPoints", 0) or 0)
            raw_max_hp = int(ev.get("maxHitPoints", 0) or 0)
            hp = raw_hp if raw_max_hp > 0 else 100
            max_hp = raw_max_hp if raw_max_hp > 0 else 100
            hp_pct = int((hp * 10000) // max(1, max_hp))
            prev = last_pos.get(src_id)
            if prev != (x_val, y_val, hp_pct):
                last_pos[src_id] = (x_val, y_val, hp_pct)
                frames.append(
                    {
                        "t": int(ev.get("timestamp", 0) or 0),
                        "actor": src_id,
                        "x": x_val,
                        "y": y_val,
                        "hp_pct": hp_pct,
                    }
                )
    return frames


class WCLZWriter:
    @staticmethod
    def _pack_summary_v2(summary_section: Dict[str, Any]) -> bytes:
        s = dict(summary_section)
        ct = s.get("cast_timeline")
        if isinstance(ct, dict) and ct:
            can_compact = all(
                isinstance(c, dict) and c.keys() == _EXPECTED_CAST_KEYS
                for casts in ct.values()
                for c in casts
            )
            if can_compact:
                buff_set_vocab: Dict[tuple[int, ...], int] = {}
                buff_sets: List[List[int]] = []
                new_ct: Dict[str, List[List[int]]] = {}
                for aid, casts in ct.items():
                    new_casts: List[List[int]] = []
                    for c in casts:
                        ab_tuple = tuple(int(b) for b in (c.get("active_buffs") or []))
                        idx = buff_set_vocab.get(ab_tuple)
                        if idx is None:
                            idx = len(buff_sets)
                            buff_set_vocab[ab_tuple] = idx
                            buff_sets.append(list(ab_tuple))
                        new_casts.append(
                            [
                                int(c["t_ms"]),
                                int(c["ability_id"]),
                                int(c["target_id"]),
                                idx,
                                int(c["resource_type"]),
                                int(c["resource_amount"]),
                            ]
                        )
                    new_ct[aid] = new_casts
                s["cast_timeline_v2"] = {"buff_sets": buff_sets, "actors": new_ct}
                s.pop("cast_timeline", None)
        return msgpack.packb(s, use_bin_type=True)

    @staticmethod
    def _pack_events_columnar(events: List[Dict[str, Any]]) -> bytes:
        n = len(events)
        if n == 0:
            return msgpack.packb({"count": 0}, use_bin_type=True)

        sig_vocab: Dict[tuple[Any, ...], int] = {}
        sig_list: List[tuple[Any, ...]] = []
        sig_indices: List[int] = []

        ts_d: List[int] = []
        amt: List[int] = []
        unmit: List[int] = []
        abso: List[int] = []
        ovk: List[int] = []
        hp_d: List[int] = []
        ra: List[int] = []
        x_d: List[int] = []
        y_d: List[int] = []
        fc_d: List[int] = []

        last_ts = 0
        last_hp: Dict[int, int] = {}
        last_x: Dict[int, int] = {}
        last_y: Dict[int, int] = {}
        last_fc: Dict[int, int] = {}

        for ev in events:
            sid = int(ev.get("sourceID", 0) or 0)
            sig = (
                str(ev.get("type", "unknown")),
                sid,
                int(ev.get("targetID", 0) or 0),
                int(ev.get("targetInstance", 0) or 0),
                int(ev.get("abilityGameID", 0) or 0),
                int(ev.get("hitType", 0) or 0),
                int(ev.get("resourceType", 0) or 0),
                int(ev.get("resourceMax", 0) or 0),
                int(ev.get("maxHitPoints", 0) or 0),
            )
            si = sig_vocab.get(sig)
            if si is None:
                si = len(sig_list)
                sig_vocab[sig] = si
                sig_list.append(sig)
            sig_indices.append(si)

            ts = int(ev.get("timestamp", 0) or 0)
            ts_d.append(ts - last_ts)
            last_ts = ts

            amt.append(int(ev.get("amount", 0) or 0))
            unmit.append(int(ev.get("unmitigatedAmount", 0) or 0))
            abso.append(int(ev.get("absorbed", 0) or 0))
            ovk.append(int(ev.get("overkill", 0) or 0))

            hp = int(ev.get("hitPoints", 0) or 0)
            hp_d.append(hp - last_hp.get(sid, 0))
            last_hp[sid] = hp

            ra.append(int(ev.get("resourceAmount", 0) or 0))

            xv = int(ev.get("x", 0) or 0)
            x_d.append(xv - last_x.get(sid, 0))
            last_x[sid] = xv

            yv = int(ev.get("y", 0) or 0)
            y_d.append(yv - last_y.get(sid, 0))
            last_y[sid] = yv

            fc = int(ev.get("facing", 0) or 0)
            fc_d.append(fc - last_fc.get(sid, 0))
            last_fc[sid] = fc

        freq = Counter(sig_indices)
        old_to_new = [0] * len(sig_list)
        sorted_sigs: List[tuple[Any, ...]] = []
        for new_i, (old_i, _) in enumerate(freq.most_common()):
            old_to_new[old_i] = new_i
            sorted_sigs.append(sig_list[old_i])
        remapped_idx = [old_to_new[i] for i in sig_indices]

        def pack_z(vals: List[int]) -> bytes:
            return b"" if not any(vals) else encode_zvarints(vals)

        payload = {
            "count": n,
            "sigs": sorted_sigs,
            "sig_idx": encode_uvarints(remapped_idx),
            "ts": pack_z(ts_d),
            "amt": pack_z(amt),
            "unmit": pack_z(unmit),
            "abs": pack_z(abso),
            "ovk": pack_z(ovk),
            "hp": pack_z(hp_d),
            "ra": pack_z(ra),
            "x": pack_z(x_d),
            "y": pack_z(y_d),
            "fc": pack_z(fc_d),
        }
        return msgpack.packb(payload, use_bin_type=True)

    @staticmethod
    def _pack_replay_columnar(
        replay_stream: Dict[str, Any], events: Optional[List[Dict[str, Any]]] = None
    ) -> bytes:
        norm_frames = [
            {
                "t": int(f.get("t", 0) or 0),
                "actor": int(f.get("actor", 0) or 0),
                "x": int(f.get("x", 0) or 0),
                "y": int(f.get("y", 0) or 0),
                "hp_pct": int(f.get("hp_pct", 0) if f.get("hp_pct") is not None else 10000),
            }
            for f in replay_stream.get("frames", [])
        ]
        map_id = int(replay_stream.get("map_id", 0))
        pulls = replay_stream.get("pulls", [])
        if events is not None and norm_frames == _derive_replay_frames_from_events(events):
            return msgpack.packb(
                {"mode": "derived", "map_id": map_id, "pulls": pulls},
                use_bin_type=True,
            )
        payload = {
            "mode": "explicit",
            "map_id": map_id,
            "pulls": pulls,
            "count": len(norm_frames),
            "t": encode_delta_ints([f["t"] for f in norm_frames]),
            "actor": encode_delta_ints([f["actor"] for f in norm_frames]),
            "x": encode_delta_ints([f["x"] for f in norm_frames]),
            "y": encode_delta_ints([f["y"] for f in norm_frames]),
            "hp_pct": encode_delta_ints([f["hp_pct"] for f in norm_frames]),
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
        codec: str = "lzma",
    ) -> int:
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")

        sec1_raw = cls._pack_summary_v2(summary_section)
        sec2_raw = cls._pack_events_columnar(events)
        sec3_raw = cls._pack_replay_columnar(replay_stream, events=events)

        if codec == "lzma":
            flags = FLAG_LZMA
            dict_id = 0
            sec1_comp = lzma.compress(sec1_raw, preset=6)
            sec2_comp = lzma.compress(sec2_raw, preset=6)
            sec3_comp = lzma.compress(sec3_raw, preset=6)
        else:
            flags = 0
            dict_data = zstd.ZstdCompressionDict(zstd_dict_bytes) if zstd_dict_bytes else None
            dict_id = dict_data.dict_id() if dict_data else 0
            cctx = zstd.ZstdCompressor(level=zstd_level, dict_data=dict_data)
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
            flags,
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
        file_path: Path,
        offset: int,
        size: int,
        flags: int = 0,
        zstd_dict_bytes: Optional[bytes] = None,
    ) -> bytes:
        with open(file_path, "rb") as f:
            f.seek(offset)
            comp = f.read(size)
        if flags & FLAG_LZMA:
            return lzma.decompress(comp)
        dict_data = zstd.ZstdCompressionDict(zstd_dict_bytes) if zstd_dict_bytes else None
        dctx = zstd.ZstdDecompressor(dict_data=dict_data)
        return dctx.decompress(comp)

    @classmethod
    def read_summary_only(
        cls, file_path: Path, zstd_dict_bytes: Optional[bytes] = None
    ) -> Dict[str, Any]:
        hdr = cls.read_header(file_path)
        raw = cls._read_decompressed_slice(
            file_path,
            hdr["sec1_offset"],
            hdr["sec1_size"],
            flags=hdr["flags"],
            zstd_dict_bytes=zstd_dict_bytes,
        )
        summary = msgpack.unpackb(raw, raw=False)
        ct_v2 = summary.pop("cast_timeline_v2", None)
        if isinstance(ct_v2, dict):
            buff_sets = ct_v2.get("buff_sets", [])
            actors_ct = ct_v2.get("actors", {})
            summary["cast_timeline"] = {
                aid: [
                    {
                        "t_ms": c[0],
                        "ability_id": c[1],
                        "target_id": c[2],
                        "active_buffs": list(buff_sets[c[3]]),
                        "resource_type": c[4],
                        "resource_amount": c[5],
                    }
                    for c in casts
                ]
                for aid, casts in actors_ct.items()
            }
        return summary

    @classmethod
    def read_events_only(
        cls, file_path: Path, zstd_dict_bytes: Optional[bytes] = None
    ) -> List[Dict[str, Any]]:
        hdr = cls.read_header(file_path)
        raw = cls._read_decompressed_slice(
            file_path,
            hdr["sec2_offset"],
            hdr["sec2_size"],
            flags=hdr["flags"],
            zstd_dict_bytes=zstd_dict_bytes,
        )
        payload = msgpack.unpackb(raw, raw=False)
        count = int(payload["count"])
        if count == 0:
            return []

        if hdr["version"] == 1:
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

        sigs = payload["sigs"]
        sig_idx = decode_uvarints(payload["sig_idx"], count)
        ts_d = decode_zvarints(payload["ts"], count)
        amt = decode_zvarints(payload["amt"], count)
        unmit = decode_zvarints(payload["unmit"], count)
        abso = decode_zvarints(payload["abs"], count)
        ovk = decode_zvarints(payload["ovk"], count)
        hp_d = decode_zvarints(payload["hp"], count)
        ra = decode_zvarints(payload["ra"], count)
        x_d = decode_zvarints(payload["x"], count)
        y_d = decode_zvarints(payload["y"], count)
        fc_d = decode_zvarints(payload["fc"], count)

        last_ts = 0
        last_hp: Dict[int, int] = {}
        last_x: Dict[int, int] = {}
        last_y: Dict[int, int] = {}
        last_fc: Dict[int, int] = {}

        events = []
        for i in range(count):
            t_str, sid, tid, tinst, abid, ht, rt, rmax, mhp = sigs[sig_idx[i]]
            last_ts += ts_d[i]
            hp = last_hp.get(sid, 0) + hp_d[i]
            last_hp[sid] = hp
            xv = last_x.get(sid, 0) + x_d[i]
            last_x[sid] = xv
            yv = last_y.get(sid, 0) + y_d[i]
            last_y[sid] = yv
            fc = last_fc.get(sid, 0) + fc_d[i]
            last_fc[sid] = fc
            events.append(
                {
                    "type": t_str,
                    "timestamp": last_ts,
                    "sourceID": sid,
                    "targetID": tid,
                    "targetInstance": tinst,
                    "abilityGameID": abid,
                    "hitType": ht,
                    "amount": amt[i],
                    "unmitigatedAmount": unmit[i],
                    "absorbed": abso[i],
                    "overkill": ovk[i],
                    "hitPoints": hp,
                    "maxHitPoints": mhp,
                    "resourceType": rt,
                    "resourceAmount": ra[i],
                    "resourceMax": rmax,
                    "x": xv,
                    "y": yv,
                    "facing": fc,
                }
            )
        return events

    @classmethod
    def read_replay_only(
        cls,
        file_path: Path,
        zstd_dict_bytes: Optional[bytes] = None,
        events: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        hdr = cls.read_header(file_path)
        raw = cls._read_decompressed_slice(
            file_path,
            hdr["sec3_offset"],
            hdr["sec3_size"],
            flags=hdr["flags"],
            zstd_dict_bytes=zstd_dict_bytes,
        )
        payload = msgpack.unpackb(raw, raw=False)
        mode = payload.get("mode", "explicit")
        if mode == "derived":
            ev_list = (
                events
                if events is not None
                else cls.read_events_only(file_path, zstd_dict_bytes=zstd_dict_bytes)
            )
            return {
                "map_id": payload["map_id"],
                "pulls": payload.get("pulls", []),
                "frames": _derive_replay_frames_from_events(ev_list),
            }

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
        events = cls.read_events_only(file_path, zstd_dict_bytes)
        return {
            "summary": cls.read_summary_only(file_path, zstd_dict_bytes),
            "events": events,
            "replay": cls.read_replay_only(file_path, zstd_dict_bytes, events=events),
        }

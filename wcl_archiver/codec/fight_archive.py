"""Self-contained, full-field WCLF2 archives with transient capture buffers.

Only write_fight touches the filesystem. Original JSON values are authoritative;
wire whitespace is intentionally discarded. Each independent Zstd section has a
SHA-256 checksum over its MessagePack representation.

Event tables contain count and dynamic columns. Each column stores only its
present values, plus an optional LSB-first presence bitmap (ceil(count/8) bytes).
An omitted bitmap means every record has the field. Explicit null remains a
normal value; padding bits must be zero. Readers also accept earlier dense
columns with the reserved missing-value extension.
"""
from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import struct
import uuid

import msgpack
import zstandard as zstd


MAGIC = b"WCLF\x02"
_MISSING = msgpack.ExtType(1, b"")
_BYTE_SET_BITS = tuple(tuple(bit for bit in range(8) if byte & (1 << bit)) for byte in range(256))


def _default(value):
    if isinstance(value, Decimal):
        return msgpack.ExtType(2, str(value).encode("ascii"))
    if isinstance(value, int):
        return msgpack.ExtType(3, str(value).encode("ascii"))
    raise TypeError(f"Unsupported archive value: {type(value).__name__}")


def _extension(code, data):
    if code == 1 and data == b"":
        return _MISSING
    if code == 2:
        return Decimal(data.decode("ascii"))
    if code == 3:
        return int(data)
    raise ValueError("Unknown archive value extension")


def _pack(value):
    return msgpack.packb(value, use_bin_type=True, default=_default)


def _unpack(value):
    return msgpack.unpackb(value, raw=False, ext_hook=_extension, strict_map_key=False)


def _json_bytes(value):
    """Encode Decimal as a JSON number, without passing through binary float."""
    def encode(item):
        if isinstance(item, Decimal):
            if not item.is_finite():
                raise ValueError("Nonfinite JSON number")
            return str(item)
        if isinstance(item, dict):
            return "{" + ",".join(json.dumps(key, ensure_ascii=False) + ":" + encode(val)
                                  for key, val in item.items()) + "}"
        if isinstance(item, list):
            return "[" + ",".join(map(encode, item)) + "]"
        return json.dumps(item, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    return encode(value).encode("utf-8")


# Public export encoder: JSON numbers keep their decimal precision.
json_bytes = _json_bytes


def _columns(events):
    count, states = len(events), {}
    bitmap_size = (count + 7) >> 3
    for index, event in enumerate(events):
        byte_index, bit = index >> 3, 1 << (index & 7)
        for key, value in event.items():
            state = states.get(key)
            if state is None:
                state = states[key] = [[], None]
            values, presence = state
            if presence is None and len(values) != index:
                # The final bitmap size is known. Allocate once instead of extending
                # or looking up per-value metadata millions of times per fight.
                presence = state[1] = _prefix_bitmap(len(values), bitmap_size)
            if presence is not None:
                presence[byte_index] |= bit
            values.append(value)
    columns = {}
    for key, (values, presence) in states.items():
        column = {"values": values}
        if len(values) < count:
            if presence is None:
                presence = _prefix_bitmap(len(values), bitmap_size)
            column["presence"] = bytes(presence)
        columns[key] = column
    return {"count": count, "columns": columns}


def _prefix_bitmap(present_count, byte_count):
    full, tail = divmod(present_count, 8)
    result = bytearray(b"\xff" * full)
    if tail:
        result.append((1 << tail) - 1)
    result.extend(b"\0" * (byte_count - len(result)))
    return result


def _records(table):
    count, columns = table["count"], table["columns"]
    if not isinstance(count, int) or count < 0 or not isinstance(columns, dict):
        raise ValueError("Corrupt archive column table")
    rows = [{} for _ in range(count)]
    for key, column in columns.items():
        if isinstance(column, list):  # Earlier WCLF2 dense columns.
            if len(column) != count:
                raise ValueError("Corrupt archive column lengths")
            for row, value in zip(rows, column):
                if value is not _MISSING:
                    row[key] = value
            continue
        if not isinstance(column, dict) or not isinstance(column.get("values"), list):
            raise ValueError("Corrupt archive column values")
        values = column["values"]
        presence = column.get("presence")
        if presence is None:
            if len(values) != count:
                raise ValueError("Corrupt archive column lengths")
            for row, value in zip(rows, values):
                row[key] = value
            continue
        if not isinstance(presence, bytes) or len(presence) != (count + 7) >> 3:
            raise ValueError("Corrupt archive presence bitmap length")
        if count & 7 and presence[-1] >> (count & 7):
            raise ValueError("Corrupt archive presence bitmap padding")
        if int.from_bytes(presence, "little").bit_count() != len(values):
            raise ValueError("Corrupt archive presence/value count")
        value_index = 0
        for byte_index, byte in enumerate(presence):
            base = byte_index << 3
            for bit in _BYTE_SET_BITS[byte]:
                rows[base + bit][key] = values[value_index]
                value_index += 1
    return rows


def _report(body):
    if "fights" in body:
        return body
    return ((body.get("data") or {}).get("reportData") or {}).get("report")


def _events_path(body):
    if isinstance(body.get("events"), list):
        return ["events"]
    report = _report(body)
    if isinstance(report, dict) and isinstance(report.get("events"), dict):
        if isinstance(report["events"].get("data"), list):
            return ["data", "reportData", "report", "events", "data"]
    return None


def _at(body, path):
    for key in path:
        body = body[key]
    return body


def _set(body, path, value):
    for key in path[:-1]:
        body = body[key]
    body[path[-1]] = value


def _page_template(body, path):
    """Clone metadata without ever cloning the large event payload itself."""
    if not path:
        return None
    key = path[0]
    result = {name: deepcopy(value) for name, value in body.items() if name != key}
    result[key] = _page_template(body[key], path[1:])
    return result


class FightArchive:
    supports_parallel_event_pages = True

    def __init__(self, root):
        self.root = Path(root)  # Compatibility only; never created or accessed.
        self._scope = ContextVar("fight_capture_" + uuid.uuid4().hex, default=None)
        self._captures = {}
        self._masters = OrderedDict()
        self._attempts = {}
        self._pinned_masters = {}
        self._completed = {}

    def _drop_attempt(self, fetch_id):
        self._pinned_masters.pop(fetch_id, None)
        for capture_id in self._attempts.pop(fetch_id, []):
            self._captures.pop(capture_id, None)

    @contextmanager
    def fight_capture(self, report_code, fight_id):
        fetch_id = uuid.uuid4().hex
        key = (report_code, int(fight_id))
        token = self._scope.set((*key, fetch_id))
        self._attempts[fetch_id] = []
        if self.has_report_metadata(report_code):
            self._pinned_masters[fetch_id] = self._masters[report_code]
        succeeded = False
        try:
            yield fetch_id
            succeeded = self._completed.get(key, {}).get("manifest", {}).get("fetch_id") == fetch_id
        finally:
            self._scope.reset(token)
            if not succeeded:
                current = self._completed.get(key)
                if current and current["manifest"]["fetch_id"] == fetch_id:
                    self._completed.pop(key)
                self._drop_attempt(fetch_id)

    def capture(self, method, url, body, status=200, request_json=None, source="http"):
        if int(status) != 200 or source == "rendered_html":
            return None
        if not isinstance(body, bytes):
            raise TypeError("Capture body must be bytes")
        match = re.search(r"/reports/(fights-and-participants|summary-events|events/summary|replaysegment)/([A-Za-z0-9]+)/(\d+)", url)
        variables = (request_json or {}).get("variables") or {}
        if not match and not variables.get("code"):
            return None
        try:
            obj = json.loads(body, parse_float=Decimal)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None  # The network client owns challenge recovery/JSON errors.
        if not isinstance(obj, dict) or obj.get("errors"):
            return None
        endpoint, report_code, number = match.groups() if match else (None, variables.get("code"), None)
        report_code = variables.get("code", report_code)
        event_path = _events_path(obj)
        report_obj = _report(obj)
        is_master_request = endpoint == "fights-and-participants" or (endpoint is None and variables.get("fightId") is None)
        if is_master_request and isinstance(report_obj, dict) and isinstance(report_obj.get("fights"), list):
            kind = "master"
        elif endpoint == "replaysegment" and event_path:
            kind = "replay"
        elif event_path:
            kind = "events"
        elif isinstance(report_obj, dict) and isinstance(report_obj.get("fights"), list):
            kind = "master"
        else:
            return None
        scope = self._scope.get()
        if kind != "master" and scope is None:
            return None  # Event buffers must have an explicit attempt lifetime.
        fight_id, fetch_id = None, None
        if scope:
            scoped_report, fight_id, fetch_id = scope
            if scoped_report != report_code:
                raise ValueError("Capture report does not match active attempt")
        cid = uuid.uuid4().hex
        info = {"id": cid, "method": method.upper(), "url": url, "status": int(status),
                "request_json": deepcopy(request_json), "source": source,
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "report_code": report_code, "fight_id": fight_id, "fetch_id": fetch_id,
                "kind": kind}
        capture = {"info": info, "body": obj, "event_path": event_path}
        self._captures[cid] = capture
        if kind == "master":
            previous = self._masters.pop(report_code, None)
            if previous:
                self._captures.pop(previous["info"]["id"], None)
            self._masters[report_code] = capture
            while len(self._masters) > 128:
                _, old = self._masters.popitem(last=False)
                self._captures.pop(old["info"]["id"], None)
        else:
            self._attempts[fetch_id].append(cid)
        return cid

    def capture_info(self, capture_id):
        return deepcopy(self._captures[capture_id]["info"])

    def get_capture_body(self, capture_id):
        """Reuse the parsed, precision-preserving object; callers must not mutate it."""
        return self._captures[capture_id]["body"]

    @staticmethod
    def _ordered_canonical_pages(pages, report_code, fight_id, start, end):
        windows = {}
        has_graphql = False
        for cap in pages:
            match = re.search(r"/reports/(?:summary-events|events/summary)/([A-Za-z0-9]+)/(\d+)/(-?\d+)/(-?\d+)(?:/|\?|$)",cap["info"]["url"])
            if match is None:
                variables = (cap["info"].get("request_json") or {}).get("variables") or {}
                if variables.get("code") == report_code and variables.get("fightId") == int(fight_id):
                    has_graphql = True
                    continue
                raise ValueError("Invalid canonical page request URL")
            code, fid, cursor, window_end = match.groups()
            cursor, window_end = int(cursor), int(window_end)
            if code != report_code or int(fid) != int(fight_id):
                raise ValueError("Canonical page URL report/fight binding mismatch")
            if not int(start) <= cursor <= window_end <= int(end):
                raise ValueError("Canonical page request bounds exceed fight boundaries")
            windows.setdefault(window_end, []).append((cursor, cap))
        # GraphQL can be a sequential fallback for the remaining web tail.
        # Its capture order remains authoritative for that legacy path.
        if has_graphql:
            return pages
        ordered = []
        window_start = int(start)
        for window_end, captures in sorted(windows.items()):
            expected_cursor = window_start
            captures.sort(key=lambda item: item[0])
            for index, (cursor, cap) in enumerate(captures):
                if cursor != expected_cursor:
                    raise ValueError("Canonical page cursor chain has a gap or overlap")
                next_cursor = cap["body"].get("nextPageTimestamp")
                if next_cursor is not None:
                    is_integer = type(next_cursor) is int or (
                        isinstance(next_cursor, Decimal) and next_cursor.is_finite()
                        and next_cursor == next_cursor.to_integral_value()
                    )
                    if not is_integer or not cursor < next_cursor <= window_end:
                        raise ValueError("Invalid canonical next page cursor")
                    expected_cursor = int(next_cursor)
                    if index == len(captures) - 1:
                        raise ValueError("Canonical page chain is incomplete")
                elif index != len(captures) - 1:
                    raise ValueError("Canonical page exists after a terminal response")
                ordered.append(cap)
            window_start = window_end + 1
        if window_start != int(end) + 1:
            raise ValueError("Canonical window coverage does not match fight boundaries")
        return ordered

    def has_report_metadata(self, report_code):
        if report_code not in self._masters:
            return False
        self._masters.move_to_end(report_code)
        return True

    def read_body(self, capture_id):
        return _json_bytes(self._captures[capture_id]["body"])

    def complete_fight(self, report_code, fight_id, fetch_id, start, end, event_count):
        master = self._pinned_masters.get(fetch_id) or self._masters.get(report_code)
        report_obj = _report(master["body"]) if master else None
        if not master or not any(int(f.get("id", -1)) == int(fight_id) for f in report_obj["fights"]):
            raise ValueError("Complete source report metadata is missing")
        captures = [self._captures[cid] for cid in self._attempts.get(fetch_id, [])]
        pages = [cap for cap in captures if cap["info"]["kind"] == "events"]
        pages = self._ordered_canonical_pages(pages, report_code, fight_id, start, end)
        supplemental = [cap for cap in captures if cap["info"]["kind"] == "replay"]
        def segment_start(cap):
            match = re.search(r"/replaysegment/[^/]+/\d+/(-?\d+)/", cap["info"]["url"])
            return int(match[1]) if match else 0
        supplemental.sort(key=segment_start)
        if not pages or sum(len(_at(cap["body"], cap["event_path"])) for cap in pages) != int(event_count):
            raise ValueError("Complete canonical event source pages are missing or count differs")
        if any(cap["info"]["report_code"] != report_code or cap["info"]["fight_id"] != int(fight_id) for cap in captures):
            raise ValueError("Capture attempt belongs to a different fight")
        manifest = {"report_code": report_code, "fight_id": int(fight_id), "fetch_id": fetch_id,
                    "start_time": int(start), "end_time": int(end), "event_count": int(event_count),
                    "master_capture": master["info"]["id"],
                    "event_captures": [cap["info"]["id"] for cap in pages],
                    "supplemental_captures": [cap["info"]["id"] for cap in supplemental], "complete": True}
        key = (report_code, int(fight_id))
        old = self._completed.get(key)
        if old and old["manifest"]["fetch_id"] != fetch_id:
            self._drop_attempt(old["manifest"]["fetch_id"])
        self._completed[key] = {"manifest": manifest, "master": master, "pages": pages, "replay": supplemental}

    def fight_manifest(self, report_code, fight_id):
        try:
            return deepcopy(self._completed[(report_code, int(fight_id))]["manifest"])
        except KeyError as exc:
            raise ValueError("No complete source archive for this fight") from exc

    def stats(self):
        return {"source_bytes": 0, "source_responses": 0,
                "buffered_master_captures": len(self._masters),
                "buffered_event_captures": sum(len(ids) for ids in self._attempts.copy().values()),
                "buffered_complete_fights": len(self._completed)}

    def clear(self):
        self._captures.clear()
        self._masters.clear()
        self._attempts.clear()
        self._pinned_masters.clear()
        self._completed.clear()

    def write_fight(self, path, summary, report_code, fight_id):
        key = (report_code, int(fight_id))
        try:
            completed = self._completed[key]
        except KeyError as exc:
            raise ValueError("No complete source archive for this fight") from exc
        path = Path(path)
        tmp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            events, replay, capture_rows = [], [], []
            for cap in [completed["master"], *completed["pages"], *completed["replay"]]:
                record = {**cap["info"], "template": (
                    deepcopy(cap["body"]) if cap["info"]["kind"] == "master"
                    else _page_template(cap["body"], cap["event_path"]))}
                if cap["info"]["kind"] != "master":
                    target = events if cap["info"]["kind"] == "events" else replay
                    page_events = _at(cap["body"], cap["event_path"])
                    record["slice"] = [len(target), len(target) + len(page_events)]
                    record["event_path"] = cap["event_path"]
                    target.extend(page_events)
                capture_rows.append(record)
            metadata = {"manifest": completed["manifest"], "captures": capture_rows}
            sections = {"summary": summary, "events": _columns(events), "replay": _columns(replay), "metadata": metadata}
            index, chunks, offset = {}, [], 0
            compressor = zstd.ZstdCompressor(level=9)
            for name, value in sections.items():
                raw = _pack(value)
                compressed = compressor.compress(raw)
                index[name] = {"offset": offset, "length": len(compressed), "size": len(raw),
                               "sha256": hashlib.sha256(raw).digest()}
                chunks.append(compressed)
                offset += len(compressed)
            header = _pack(index)
            path.parent.mkdir(parents=True, exist_ok=True)
            with tmp.open("xb") as stream:
                stream.write(MAGIC + struct.pack("<I", len(header)) + header)
                for chunk in chunks:
                    stream.write(chunk)
                stream.flush()
                os.fsync(stream.fileno())
            # Check every section and all full-field event records before publication.
            for name in ("summary", "metadata"):
                if _pack(PackedFightReader._read_section(tmp, name)) != _pack(sections[name]):
                    raise ValueError("Fight section verification failed")
            if PackedFightReader.read_raw_events(tmp) != events or PackedFightReader._read_replay_events(tmp) != replay:
                raise ValueError("Fight full-field event verification failed")
            os.replace(tmp, path)
            return path.stat().st_size
        finally:
            tmp.unlink(missing_ok=True)
            self._completed.pop(key, None)
            self._drop_attempt(completed["manifest"]["fetch_id"])


class PackedFightReader:
    @staticmethod
    def is_packed_fight(path):
        with Path(path).open("rb") as stream:
            return stream.read(len(MAGIC)) == MAGIC

    @staticmethod
    def _read_section(path, name):
        try:
            with Path(path).open("rb") as stream:
                if stream.read(5) != MAGIC:
                    raise ValueError("Invalid WCLF2 archive")
                header_size = struct.unpack("<I", stream.read(4))[0]
                if header_size > 1024 * 1024:
                    raise ValueError("Corrupt archive header")
                index = _unpack(stream.read(header_size))
                section = index[name]
                stream.seek(9 + header_size + section["offset"])
                compressed = stream.read(section["length"])
                if len(compressed) != section["length"]:
                    raise ValueError("Corrupt truncated archive section")
            raw = zstd.ZstdDecompressor().decompress(compressed, max_output_size=section["size"])
            if len(raw) != section["size"] or hashlib.sha256(raw).digest() != section["sha256"]:
                raise ValueError("Corrupt archive section checksum")
            return _unpack(raw)
        except (OSError, KeyError, TypeError, struct.error, msgpack.UnpackException, zstd.ZstdError) as exc:
            raise ValueError("Corrupt or missing WCLF2 archive section") from exc

    @classmethod
    def read_summary_only(cls, path):
        return cls._read_section(path, "summary")

    @classmethod
    def source_manifest(cls, path):
        metadata = cls._read_section(path, "metadata")
        return {**metadata["manifest"], "captures": metadata["captures"]}

    @classmethod
    def read_raw_events(cls, path):
        events = _records(cls._read_section(path, "events"))
        if len(events) != cls.source_manifest(path)["event_count"]:
            raise ValueError("Corrupt archive event count")
        return events

    @classmethod
    def full_events_json(cls, path):
        return _json_bytes(cls.read_raw_events(path))

    @classmethod
    def _read_replay_events(cls, path):
        return _records(cls._read_section(path, "replay"))

    @classmethod
    def read_source_response(cls, path, capture_id):
        captures = cls._read_section(path, "metadata")["captures"]
        cap = next((row for row in captures if row["id"] == capture_id), None)
        if cap is None:
            raise KeyError(capture_id)
        body = cap["template"]
        if "slice" in cap:
            events = cls.read_raw_events(path) if cap["kind"] == "events" else cls._read_replay_events(path)
            first, last = cap["slice"]
            if not 0 <= first <= last <= len(events):
                raise ValueError("Corrupt source response slice")
            _set(body, cap["event_path"], events[first:last])
        return _json_bytes(body)

    @classmethod
    def read_events_only(cls, path):
        from wcl_archiver.parser.combat_state_machine import CombatStateMachine
        summary = cls.read_summary_only(path)
        meta = {**summary, **cls.source_manifest(path)}
        return CombatStateMachine.analyze_fight(meta, summary.get("actors", []), [], cls.read_raw_events(path))[1]

    @classmethod
    def read_replay_only(cls, path):
        from wcl_archiver.parser.combat_state_machine import CombatStateMachine
        from wcl_archiver.codec.wclz_format import _derive_replay_frames_from_events
        summary = cls.read_summary_only(path)
        supplemental = cls._read_replay_events(path)
        if supplemental:
            meta = {**summary, **cls.source_manifest(path)}
            frames = CombatStateMachine.analyze_fight(meta, summary.get("actors", []), [], supplemental)[2]["frames"]
        else:
            frames = _derive_replay_frames_from_events(cls.read_events_only(path))
        return {"map_id": summary.get("map_id", 0), "pulls": summary.get("pulls", []), "frames": frames}

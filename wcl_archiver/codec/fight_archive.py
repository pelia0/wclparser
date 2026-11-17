"""Self-contained, full-field WCLF3 archives with transient capture buffers.

Writers touch the filesystem only during publication. JSON values are authoritative;
wire whitespace is intentionally discarded. Each independent compressed section has a
SHA-256 checksum over its MessagePack representation.

Event tables contain count and dynamic columns. Each column stores only its
present values, plus an optional LSB-first presence bitmap (ceil(count/8) bytes).
An omitted bitmap means every record has the field. Explicit null remains a
normal value; padding bits must be zero. Readers also accept earlier dense
columns with the reserved missing-value extension.
"""
from collections import Counter, OrderedDict, defaultdict, deque
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import lzma
import os
from pathlib import Path
import re
import struct
import uuid

import msgpack
import zstandard as zstd


MAGIC = b"WCLF\x03"
LEGACY_MAGIC = b"WCLF\x02"
SECTION_NAMES = ("summary", "events", "replay", "metadata")
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


def _validate_table(table):
    """Validate lengths without allocating an event dictionary per row."""
    count, columns = table["count"], table["columns"]
    if type(count) is not int or count < 0 or not isinstance(columns, dict):
        raise ValueError("Corrupt archive column table")
    for column in columns.values():
        values = column if isinstance(column, list) else column["values"]
        presence = None if isinstance(column, list) else column.get("presence")
        if not isinstance(values, list):
            raise ValueError("Corrupt archive column values")
        if presence is None:
            valid = len(values) == count
        else:
            valid = (isinstance(presence, bytes) and len(presence) == (count + 7) >> 3
                     and (not count & 7 or not presence[-1] >> (count & 7))
                     and int.from_bytes(presence, "little").bit_count() == len(values))
        if not valid:
            raise ValueError("Corrupt archive column lengths or presence bitmap")


def _encode_table(table, canonical=None):
    _validate_table(table)
    columns, shared, deltas = {}, {}, []
    compressor = zstd.ZstdCompressor(level=9)
    for key, column in table["columns"].items():
        if (canonical is not None and table["count"] == canonical["count"]
                and key in canonical["columns"] and _pack(column) == _pack(canonical["columns"][key])):
            shared[key] = key
            continue
        if isinstance(column, dict) and column["values"] and all(type(v) is int for v in column["values"]):
            previous, differences = 0, []
            for value in column["values"]:
                differences.append(value - previous)
                previous = value
            candidate = {**column, "values": differences}
            if len(compressor.compress(_pack([candidate, key]))) < len(compressor.compress(_pack([column]))):
                column = candidate
                deltas.append(key)
        columns[key] = column
    return {"table": {**table, "columns": columns}, "shared_columns": shared,
            "column_order": list(table["columns"]), "delta_columns": deltas}


def _decode_table(encoded, canonical=None):
    table = encoded["table"]
    columns, shared = table["columns"], encoded["shared_columns"]
    order, deltas = encoded["column_order"], encoded["delta_columns"]
    if (not isinstance(columns, dict) or not isinstance(shared, dict) or not isinstance(order, list)
            or not isinstance(deltas, list) or len(set(order)) != len(order)
            or len(set(deltas)) != len(deltas) or set(columns) & set(shared)
            or set(order) != set(columns) | set(shared) or not set(deltas) <= set(columns)):
        raise ValueError("Corrupt archive column encoding")
    if shared and (canonical is None or type(table["count"]) is not int or table["count"] != canonical["count"]):
        raise ValueError("Corrupt archive reference count")
    for key, reference in shared.items():
        if reference != key or reference not in canonical["columns"]:
            raise ValueError("Corrupt archive column reference")
        columns[key] = canonical["columns"][reference]
    for key in deltas:
        column = columns[key]
        values = column["values"]
        if not isinstance(values, list) or not all(type(v) is int for v in values):
            raise ValueError("Corrupt archive integer delta column")
        previous, restored = 0, []
        for value in values:
            previous += value
            restored.append(previous)
        columns[key] = {**column, "values": restored}
    table["columns"] = {key: columns[key] for key in order}
    _validate_table(table)
    return table


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


def _event_identity(event):
    """Normalize only alignment keys; the original objects remain authoritative."""
    if not isinstance(event, dict) or 'timestamp' not in event or 'type' not in event:
        raise ValueError('Hybrid event is missing timestamp or type identity')
    values = [event['timestamp'], event['type']]
    for flat, inline, nested in (('sourceID', 'source', 'id'), ('targetID', 'target', 'id'),
                                 ('abilityGameID', 'ability', 'guid')):
        obj = event.get(inline)
        alternate = obj.get(nested) if isinstance(obj, dict) else None
        value = event.get(flat, alternate)
        if flat in event and isinstance(obj, dict) and nested in obj and value != alternate:
            raise ValueError('Hybrid event has conflicting identity aliases')
        values.append(value)
    try:
        identity = tuple(values)
        hash(identity)
        return identity
    except TypeError as exc:
        raise ValueError('Invalid hybrid event identity') from exc


def _capture_events(captures):
    return [event for cap in captures for event in _at(cap['body'], cap['event_path'])]


def _integral(value):
    return type(value) is int or (isinstance(value, Decimal) and value.is_finite()
                                 and value == value.to_integral_value())


def _validate_event_bounds(events, start=None, end=None):
    for event in events:
        _event_identity(event)
        timestamp = event['timestamp']
        if not _integral(timestamp) or (start is not None and not start <= timestamp <= end):
            raise ValueError('Selective source event timestamp is invalid or outside fight bounds')


def supplement_windows(api_events, replay_events):
    """Select all deaths and API occurrences absent from replay, without deduping rows."""
    _validate_event_bounds(api_events)
    _validate_event_bounds(replay_events)
    available = Counter(_event_identity(event) for event in replay_events)
    timestamps = set()
    for event in api_events:
        identity = _event_identity(event)
        represented = available[identity] > 0
        if represented:
            available[identity] -= 1
        if str(event['type']).lower() == 'death' or not represented:
            timestamps.add(int(event['timestamp']))
    return [(timestamp, timestamp) for timestamp in sorted(timestamps)]


def _merge_missing(target, source, *, index, capture_id, conflicts, path=()):
    if isinstance(target, dict) and isinstance(source, dict):
        for key, value in source.items():
            if key not in target:
                target[key] = deepcopy(value)
            else:
                _merge_missing(target[key], value, index=index, capture_id=capture_id,
                               conflicts=conflicts, path=(*path, key))
    elif isinstance(target, list) and isinstance(source, list):
        for key, value in enumerate(source):
            if key >= len(target):
                target.append(deepcopy(value))
            else:
                _merge_missing(target[key], value, index=index, capture_id=capture_id,
                               conflicts=conflicts, path=(*path, key))
    elif _pack(target) != _pack(source) and conflicts is not None:
        conflicts.append({'index': index, 'path': list(path), 'source_capture': capture_id})


def _full_event_view(primary, canonical_captures, replay_captures, conflicts=None, *, selective=False):
    canonical = [(event, cap['info']['id']) for cap in canonical_captures
                 for event in _at(cap['body'], cap['event_path'])]
    if not selective and len(primary) != len(canonical):
        raise ValueError('Hybrid canonical event count mismatch')
    result = deepcopy(primary)
    canonical_indices = defaultdict(deque)
    if selective:
        for index, event in enumerate(primary):
            canonical_indices[_event_identity(event)].append(index)
    for offset, (other, cid) in enumerate(canonical):
        if selective:
            queue = canonical_indices.get(_event_identity(other))
            if not queue:
                raise ValueError('Selective canonical event is absent from API primary')
            index = queue.popleft()
        else:
            index = offset
            if _event_identity(result[index]) != _event_identity(other):
                raise ValueError(f'Hybrid canonical event identity/order mismatch at index {index}')
        _merge_missing(result[index], other, index=index, capture_id=cid, conflicts=conflicts)
    # Preserve repeated identical event identities by matching each occurrence once.
    indices = defaultdict(deque)
    for index, event in enumerate(primary):
        indices[_event_identity(event)].append(index)
    for cap in replay_captures:
        for event in _at(cap['body'], cap['event_path']):
            queue = indices.get(_event_identity(event))
            if queue:
                index = queue.popleft()
                _merge_missing(result[index], event, index=index, capture_id=cap['info']['id'], conflicts=conflicts)
    return result


class FightArchive:
    supports_parallel_event_pages = True

    def __init__(self, root, compression="compact"):
        if compression not in ("fast", "compact"):
            raise ValueError("Unknown archive compression profile")
        self.compression = compression
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

    def supplement_replay_events(self, report_code, fight_id, fetch_id, start, end):
        """Return only this attempt's replay records, on the capture-owning loop."""
        if not all(type(value) is int for value in (start, end)) or start > end:
            raise ValueError('Invalid selective replay fight bounds')
        if fetch_id not in self._attempts:
            raise ValueError('Unknown selective capture attempt')
        replay = []
        for cid in self._attempts[fetch_id]:
            cap = self._captures[cid]
            info = cap['info']
            if (info['report_code'], info['fight_id'], info['fetch_id']) != (report_code, int(fight_id), fetch_id):
                raise ValueError('Selective replay attempt binding mismatch')
            if info['kind'] == 'replay':
                replay.append(cap)
        replay.sort(key=lambda cap: int(re.search(r'/replaysegment/[^/]+/\d+/(-?\d+)/', cap['info']['url'])[1]))
        events = _capture_events(replay)
        _validate_event_bounds(events, start, end)
        return deepcopy(events)

    def complete_supplement(self, report_code, fight_id, fetch_id, start, end, api_events):
        """Complete received metadata/replay plus explicitly selected canonical windows."""
        if not all(type(value) is int for value in (start, end)) or start > end:
            raise ValueError('Invalid selective source fight bounds')
        _validate_event_bounds(api_events, start, end)
        master = self._pinned_masters.get(fetch_id) or self._masters.get(report_code)
        if master is None:
            raise ValueError('Selective source report metadata is missing')
        replay_events = self.supplement_replay_events(report_code, fight_id, fetch_id, start, end)
        windows = supplement_windows(api_events, replay_events)
        captures = [self._captures[cid] for cid in self._attempts[fetch_id]]
        pages = [cap for cap in captures if cap['info']['kind'] == 'events']
        replay = [cap for cap in captures if cap['info']['kind'] == 'replay']
        pages.sort(key=lambda cap: int(re.search(r'/(?:summary-events|events/summary)/[^/]+/\d+/(-?\d+)/', cap['info']['url'])[1]))
        replay.sort(key=lambda cap: int(re.search(r'/replaysegment/[^/]+/\d+/(-?\d+)/', cap['info']['url'])[1]))
        manifest = {'report_code': report_code, 'fight_id': int(fight_id), 'fetch_id': fetch_id,
                    'start_time': start, 'end_time': end, 'event_count': len(api_events),
                    'master_capture': master['info']['id'], 'event_captures': [cap['info']['id'] for cap in pages],
                    'supplemental_captures': [cap['info']['id'] for cap in replay], 'complete': True,
                    'source_mode': 'browser_supplement_v1', 'canonical_windows': [list(window) for window in windows]}
        completed = {'manifest': manifest, 'master': master, 'pages': pages, 'replay': replay}
        self._validate_provider_capture(completed, report_code, int(fight_id), api_events=api_events)
        key = (report_code, int(fight_id))
        old = self._completed.get(key)
        if old and old['manifest']['fetch_id'] != fetch_id:
            self._drop_attempt(old['manifest']['fetch_id'])
        self._completed[key] = completed

    def fight_manifest(self, report_code, fight_id):
        try:
            return deepcopy(self._completed[(report_code, int(fight_id))]["manifest"])
        except KeyError as exc:
            raise ValueError("No complete source archive for this fight") from exc

    @staticmethod
    def _validate_provider_capture(completed, report_code, fight_id, *, api_events=None):
        """Certify complete source payloads, rather than trusting a flag alone."""
        try:
            manifest = completed['manifest']
            mode = manifest.get('source_mode')
            if mode not in (None, 'browser_supplement_v1'):
                raise ValueError('Unknown hybrid source mode')
            selective = mode == 'browser_supplement_v1'
            if (manifest['report_code'] != report_code or manifest['fight_id'] != fight_id
                    or manifest['complete'] is not True or not manifest['fetch_id']):
                raise ValueError('Hybrid source identity or completeness mismatch')
            start, end, count = (manifest[key] for key in ('start_time', 'end_time', 'event_count'))
            if any(type(v) is not int for v in (start, end, count)) or start > end or count < 0:
                raise ValueError('Hybrid source boundaries or count invalid')
            master, pages, replay = completed['master'], completed['pages'], completed['replay']
            if not isinstance(pages, list) or (not pages and not selective) or not isinstance(replay, list):
                raise ValueError('Hybrid source canonical pages missing')
            captures = [master, *pages, *replay]
            ids = [cap['info']['id'] for cap in captures]
            if (len(ids) != len(set(ids)) or manifest['master_capture'] != ids[0]
                    or manifest['event_captures'] != ids[1:1 + len(pages)]
                    or manifest['supplemental_captures'] != ids[1 + len(pages):]):
                raise ValueError('Hybrid source captures differ from manifest')
            for kind, caps in (('master', [master]), ('events', pages), ('replay', replay)):
                for cap in caps:
                    info = cap['info']
                    if info['kind'] != kind or info['report_code'] != report_code or info['status'] != 200:
                        raise ValueError('Hybrid source capture identity mismatch')
                    variables = (info.get('request_json') or {}).get('variables') or {}
                    match = re.search(r'/reports/(?:fights-and-participants|summary-events|events/summary|replaysegment)/([^/]+)(?:/|$)', info['url'])
                    if (match and match[1] != report_code or 'code' in variables and variables['code'] != report_code
                            or kind != 'master' and 'fightId' in variables and variables['fightId'] != fight_id):
                        raise ValueError('Hybrid source request binding mismatch')
                    if kind != 'master':
                        if (info['fight_id'] != fight_id or info['fetch_id'] != manifest['fetch_id']
                                or cap['event_path'] != _events_path(cap['body'])):
                            raise ValueError('Hybrid source attempt binding mismatch')
                        events = _at(cap['body'], cap['event_path'])
                        if not isinstance(events, list) or any(not isinstance(e, dict) for e in events):
                            raise ValueError('Hybrid source events invalid')
            fights = _report(master['body'])['fights']
            fight = next((row for row in fights if row['id'] == fight_id), None)
            if fight is None:
                raise ValueError('Hybrid source master does not bind fight')
            for names, expected in ((('start_time', 'startTime'), start), (('end_time', 'endTime'), end)):
                if any(name in fight and fight[name] != expected for name in names):
                    raise ValueError('Hybrid source master fight boundaries mismatch')
            if selective:
                FightArchive._validate_supplement_sources(completed, fight, api_events)
                return captures
            ordered = FightArchive._ordered_canonical_pages(pages, report_code, fight_id, start, end)
            if [cap['info']['id'] for cap in ordered] != manifest['event_captures'] or len(_capture_events(pages)) != count:
                raise ValueError('Hybrid source canonical count/order mismatch')
            # GraphQL completeness needs its own full cursor-chain proof. The legacy
            # browser fallback intentionally remains accepted by the plain codec.
            graphql = [cap for cap in pages if not re.search(r'/reports/(?:summary-events|events/summary)/', cap['info']['url'])]
            if graphql:
                if len(graphql) != len(pages):
                    raise ValueError('Hybrid mixed canonical transports cannot prove complete coverage')
                cursor = start
                for index, cap in enumerate(graphql):
                    variables = (cap['info'].get('request_json') or {}).get('variables') or {}
                    if variables.get('startTime') != cursor or variables.get('endTime') != end:
                        raise ValueError('Hybrid GraphQL canonical request bounds/cursor mismatch')
                    page = _report(cap['body'])['events']
                    following = page.get('nextPageTimestamp')
                    if following is None:
                        if index != len(graphql) - 1:
                            raise ValueError('Hybrid canonical page exists after terminal response')
                    elif (index == len(graphql) - 1 or isinstance(following, bool)
                          or not isinstance(following, (int, Decimal)) or not cursor < following <= end):
                        raise ValueError('Hybrid canonical page chain is incomplete')
                    cursor = following
            # Browser replay may have fewer events than canonical; completeness is
            # established from requested segment ranges, including empty responses.
            browser_master = re.search(r'/reports/fights-and-participants/', master['info']['url'])
            if browser_master:
                boss = fight.get('boss', fight.get('encounterID', 0)) or 0
                if boss > 0 and not replay:
                    raise ValueError('Hybrid browser replay source missing')
                if replay:
                    cursor = start
                    for cap in replay:
                        match = re.search(r'/replaysegment/[^/]+/(\d+)/(-?\d+)/(-?\d+)(?:/|\?|$)', cap['info']['url'])
                        if not match:
                            raise ValueError('Hybrid replay request bounds missing')
                        replay_boss, left, right = map(int, match.groups())
                        if left != cursor or not left <= right <= end or boss > 0 and replay_boss != boss:
                            raise ValueError('Hybrid replay source coverage mismatch')
                        cursor = right + 1
                    if cursor != end + 1:
                        raise ValueError('Hybrid replay source coverage incomplete')
            return captures
        except (KeyError, TypeError, IndexError, AttributeError, StopIteration) as exc:
            raise ValueError('Invalid hybrid source capture structure') from exc

    @staticmethod
    def _validate_supplement_sources(completed, fight, api_events):
        manifest = completed['manifest']
        start, end = manifest['start_time'], manifest['end_time']
        code, fid = manifest['report_code'], manifest['fight_id']
        if api_events is None or len(api_events) != manifest['event_count']:
            raise ValueError('Selective source requires its complete API primary event count')
        _validate_event_bounds(api_events, start, end)
        master_url = completed['master']['info']['url']
        boss = fight.get('boss', fight.get('encounterID', 0))
        if not re.search(r'/reports/fights-and-participants/', master_url) or not _integral(boss) or boss <= 0:
            raise ValueError('Selective source requires browser metadata with a valid replay boss')
        cursor = start
        for cap in completed['replay']:
            match = re.search(r'/replaysegment/[^/]+/(\d+)/(-?\d+)/(-?\d+)(?:/|\?|$)', cap['info']['url'])
            if not match:
                raise ValueError('Selective replay request bounds missing')
            replay_boss, left, right = map(int, match.groups())
            if replay_boss != boss or left != cursor or not left <= right <= end:
                raise ValueError('Selective replay source coverage mismatch')
            _validate_event_bounds(_at(cap['body'], cap['event_path']), left, right)
            cursor = right + 1
        if cursor != end + 1:
            raise ValueError('Selective replay source coverage incomplete')
        replay_events = _capture_events(completed['replay'])
        windows = [list(window) for window in supplement_windows(api_events, replay_events)]
        declared = manifest['canonical_windows']
        if (not isinstance(declared, list)
                or any(not isinstance(window, list) or len(window) != 2
                       or any(type(value) is not int for value in window) for window in declared)
                or declared != windows):
            raise ValueError('Selective canonical windows differ from required API occurrences')
        pages = completed['pages']
        if len(pages) != len(windows):
            raise ValueError('Selective canonical required windows are missing or extra')
        primary_counts = Counter()
        expected_windows = {left: Counter() for left, _ in windows}
        for row in api_events:
            identity = _event_identity(row)
            primary_counts[identity] += 1
            if row['timestamp'] in expected_windows:
                expected_windows[row['timestamp']][identity] += 1
        replay_counts = Counter(_event_identity(row) for row in replay_events)
        if replay_counts - primary_counts:
            raise ValueError('Selective replay contains occurrences absent from API primary')
        for cap, (left, right) in zip(pages, windows):
            # Every selected timestamp is a complete canonical request. A cursor
            # cannot advance inside this single-timestamp window.
            match = re.search(r'/reports/(?:summary-events|events/summary)/([^/]+)/(\d+)/(-?\d+)/(-?\d+)(?:/|\?|$)', cap['info']['url'])
            if not match or (match[1], int(match[2]), int(match[3]), int(match[4])) != (code, fid, left, right):
                raise ValueError('Selective canonical request does not bind its required window')
            ordered = FightArchive._ordered_canonical_pages([cap], code, fid, left, right)
            if ordered != [cap]:
                raise ValueError('Selective canonical page order mismatch')
            records = _at(cap['body'], cap['event_path'])
            _validate_event_bounds(records, left, right)
            if Counter(_event_identity(row) for row in records) != expected_windows[left]:
                raise ValueError('Selective canonical window does not match all API occurrences')
        # Canonical and replay are overlapping views, not additive occurrences.
        represented = replay_counts | Counter(_event_identity(row) for row in _capture_events(pages))
        if primary_counts - represented:
            raise ValueError('Selective browser sources leave API occurrence coverage gaps')

    @staticmethod
    def _validate_hybrid_capture(completed):
        manifest = completed['manifest']
        hybrid = manifest.get('hybrid')
        if 'hybrid' in manifest and not isinstance(hybrid, dict):
            raise ValueError('Invalid hybrid phase manifest')
        if 'hybrid_pending' in manifest:
            if manifest['hybrid_pending'] is not True or hybrid is not None:
                raise ValueError('Invalid or conflicting hybrid pending phase markers')
            FightArchive._validate_provider_capture(completed, manifest['report_code'], manifest['fight_id'])
        if hybrid is None:
            if completed.get('auxiliary'):
                raise ValueError('Auxiliary sources require hybrid provenance')
            return
        try:
            manifest = completed['manifest']
            code, fid = manifest['report_code'], manifest['fight_id']
            api_manifest, browser_manifest = hybrid['api_manifest'], hybrid['browser_manifest']
            auxiliary = completed['auxiliary']
            if (not isinstance(auxiliary, list) or not auxiliary
                    or hybrid['auxiliary_captures'] != [cap['info']['id'] for cap in auxiliary]):
                raise ValueError('Hybrid auxiliary capture IDs mismatch')
            by_id = {cap['info']['id']: cap for cap in [completed['master'], *completed['pages'], *completed['replay'], *auxiliary]}
            if len(by_id) != 1 + len(completed['pages']) + len(completed['replay']) + len(auxiliary):
                raise ValueError('Duplicate hybrid capture IDs')
            expected_aux = [browser_manifest['master_capture'], *browser_manifest['event_captures']]
            if hybrid['auxiliary_captures'] != expected_aux:
                raise ValueError('Hybrid auxiliary sources differ from original browser manifest')
            if (any(manifest[key] != api_manifest[key] or api_manifest[key] != browser_manifest[key]
                    for key in ('report_code', 'fight_id', 'start_time', 'end_time', 'event_count', 'complete'))
                    or manifest['fetch_id'] != api_manifest['fetch_id']
                    or manifest['master_capture'] != api_manifest['master_capture']
                    or manifest['event_captures'] != api_manifest['event_captures']
                    or manifest['supplemental_captures'] != [*api_manifest['supplemental_captures'], *browser_manifest['supplemental_captures']]):
                raise ValueError('Hybrid provider manifests differ')
            def original(provider_manifest, rebind):
                caps = []
                for cid in [provider_manifest['master_capture'], *provider_manifest['event_captures'], *provider_manifest['supplemental_captures']]:
                    cap = by_id[cid]
                    if rebind:
                        info = cap['info']
                        if (info['fetch_id'] != manifest['fetch_id'] or info['fight_id'] != fid
                                or info['original_kind'] != info['kind']
                                or info['original_fetch_id'] != (None if info['kind'] == 'master' else provider_manifest['fetch_id'])):
                            raise ValueError('Hybrid auxiliary provenance mismatch')
                        cap = {**cap, 'info': {**info, 'fetch_id': info['original_fetch_id']}}
                    caps.append(cap)
                return {'manifest': provider_manifest, 'master': caps[0],
                        'pages': caps[1:1 + len(provider_manifest['event_captures'])],
                        'replay': caps[1 + len(provider_manifest['event_captures']):]}
            api, browser = original(api_manifest, False), original(browser_manifest, True)
            FightArchive._validate_provider_capture(api, code, fid)
            primary_events = _capture_events(api['pages'])
            FightArchive._validate_provider_capture(browser, code, fid, api_events=primary_events)
            conflicts = []
            _full_event_view(primary_events, browser['pages'], browser['replay'], conflicts,
                             selective=browser_manifest.get('source_mode') == 'browser_supplement_v1')
            if conflicts != hybrid['conflicts']:
                raise ValueError('Hybrid conflicts differ from authoritative source payloads')
        except (KeyError, TypeError, IndexError, AttributeError) as exc:
            raise ValueError('Invalid hybrid auxiliary structure') from exc

    def merge_hybrid_fight(self, browser_archive, report_code, fight_id):
        key = (report_code, int(fight_id))
        try:
            primary, browser = self._completed[key], browser_archive._completed[key]
        except KeyError as exc:
            raise ValueError('Both complete hybrid source captures are required') from exc
        if browser_archive is self or 'hybrid' in primary['manifest']:
            raise ValueError('Hybrid merge requires independent unmerged provider archives')
        self._validate_provider_capture(primary, *key)
        primary_events = _capture_events(primary['pages'])
        self._validate_provider_capture(browser, *key, api_events=primary_events)
        api_manifest, browser_manifest = primary['manifest'], browser['manifest']
        if any(api_manifest[name] != browser_manifest[name] for name in
               ('report_code', 'fight_id', 'start_time', 'end_time', 'event_count')):
            raise ValueError('Hybrid provider fight boundaries or event count mismatch')
        conflicts = []
        _full_event_view(primary_events, browser['pages'], browser['replay'], conflicts,
                         selective=browser_manifest.get('source_mode') == 'browser_supplement_v1')
        def rebind(cap):
            info = cap['info']
            return {**cap, 'info': {**deepcopy(info), 'original_fetch_id': info['fetch_id'],
                    'original_kind': info['kind'], 'original_fight_id': info['fight_id'],
                    'fetch_id': api_manifest['fetch_id'], 'fight_id': key[1]}}
        auxiliary = [rebind(cap) for cap in [browser['master'], *browser['pages']]]
        replay = [*primary['replay'], *(rebind(cap) for cap in browser['replay'])]
        manifest = {**api_manifest,
                    'supplemental_captures': [cap['info']['id'] for cap in replay],
                    'hybrid': {'api_manifest': deepcopy(api_manifest), 'browser_manifest': deepcopy(browser_manifest),
                               'auxiliary_captures': [cap['info']['id'] for cap in auxiliary], 'conflicts': conflicts}}
        # Only a successfully verified combined capture advances the outer phase.
        # The original API manifest above still records its durable pending state.
        manifest.pop('hybrid_pending', None)
        merged = {**primary, 'manifest': manifest, 'replay': replay, 'auxiliary': auxiliary}
        self._validate_hybrid_capture(merged)
        self._completed[key] = merged
        browser_archive.discard_fight(*key)
        return deepcopy(manifest)

    def discard_fight(self, report_code, fight_id):
        key = (report_code, int(fight_id))
        completed = self._completed.pop(key, None)
        if completed:
            self._drop_attempt(completed['manifest']['fetch_id'])
        # Cancellation can occur before complete_fight; release only matching scopes.
        for fetch, ids in list(self._attempts.items()):
            if any((self._captures.get(cid, {}).get('info', {}).get('report_code'),
                    self._captures.get(cid, {}).get('info', {}).get('fight_id')) == key for cid in ids):
                self._drop_attempt(fetch)

    def full_fight_events(self, report_code, fight_id):
        try:
            completed = self._completed[(report_code, int(fight_id))]
        except KeyError as exc:
            raise ValueError('No complete source archive for this fight') from exc
        if 'hybrid' not in completed['manifest']:
            if completed['manifest'].get('source_mode') == 'browser_supplement_v1':
                raise ValueError('Browser supplement requires its API primary for a full event view')
            return deepcopy(_capture_events(completed['pages']))
        self._validate_hybrid_capture(completed)
        hybrid = completed['manifest']['hybrid']
        canonical = [cap for cap in completed['auxiliary'] if cap['info']['id'] in hybrid['browser_manifest']['event_captures']]
        replay = [cap for cap in completed['replay'] if cap['info']['id'] in hybrid['browser_manifest']['supplemental_captures']]
        return _full_event_view(_capture_events(completed['pages']), canonical, replay,
                                selective=hybrid['browser_manifest'].get('source_mode') == 'browser_supplement_v1')

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
        if completed['manifest'].get('hybrid_pending') is True:
            raise ValueError('Hybrid source is pending browser enrichment and cannot be packed')
        if completed['manifest'].get('source_mode') == 'browser_supplement_v1':
            raise ValueError('Browser supplement requires its API primary before publication')
        try:
            self._validate_hybrid_capture(completed)
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
            for cap in completed.get('auxiliary', []):
                capture_rows.append({**cap['info'], 'template': deepcopy(cap['body']),
                                     'event_path': cap['event_path']})
            metadata = {"manifest": completed["manifest"], "captures": capture_rows}
            sections = {"summary": summary, "events": _columns(events), "replay": _columns(replay), "metadata": metadata}
            return self.write_sections(path, sections, compression=self.compression)
        finally:
            self._completed.pop(key, None)
            self._drop_attempt(completed["manifest"]["fetch_id"])

    @staticmethod
    def write_sections(path, sections, compression="compact"):
        """Verify all logical sections before atomically publishing a standalone file."""
        if compression not in ("fast", "compact") or set(sections) != set(SECTION_NAMES):
            raise ValueError("Unknown compression profile or archive sections")
        path = Path(path)
        tmp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            index, chunks, offset = {}, [], 0
            compressor = zstd.ZstdCompressor(level=9)
            for name in SECTION_NAMES:
                value = sections[name]
                if name in ("events", "replay"):
                    value = _encode_table(value, sections["events"] if name == "replay" else None)
                raw = _pack(value)
                compressed = compressor.compress(raw)
                codec = "zstd"
                if compression == "compact":
                    candidate = lzma.compress(raw, preset=6)
                    if len(candidate) < len(compressed):
                        compressed, codec = candidate, "lzma"
                index[name] = {"offset": offset, "length": len(compressed), "size": len(raw),
                               "sha256": hashlib.sha256(raw).digest(), "compression": codec}
                if name in ("events", "replay"):
                    index[name]["encoding"] = "columns-v1"
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
            for name in SECTION_NAMES:
                if _pack(PackedFightReader._read_section(tmp, name)) != _pack(sections[name]):
                    raise ValueError("Fight section verification failed")
            os.replace(tmp, path)
            return path.stat().st_size
        finally:
            tmp.unlink(missing_ok=True)


class PackedFightReader:
    @staticmethod
    def is_packed_fight(path):
        with Path(path).open("rb") as stream:
            return stream.read(len(MAGIC)) in (MAGIC, LEGACY_MAGIC)

    @staticmethod
    def _read_section(path, name):
        try:
            with Path(path).open("rb") as stream:
                magic = stream.read(5)
                if magic not in (MAGIC, LEGACY_MAGIC):
                    raise ValueError("Corrupt or invalid WCLF archive")
                header_size = struct.unpack("<I", stream.read(4))[0]
                if header_size > 1024 * 1024:
                    raise ValueError("Corrupt archive header")
                index = _unpack(stream.read(header_size))
                section = index[name]
                ranges = []
                for section_name in SECTION_NAMES:
                    entry = index[section_name]
                    if any(type(entry[field]) is not int or entry[field] < 0 for field in ("offset", "length", "size")):
                        raise ValueError("Corrupt archive section range")
                    end = entry["offset"] + entry["length"]
                    if not entry["length"] or not entry["size"] or 9 + header_size + end > os.fstat(stream.fileno()).st_size:
                        raise ValueError("Corrupt archive section range")
                    ranges.append((entry["offset"], end))
                ranges.sort()
                if any(left[1] > right[0] for left, right in zip(ranges, ranges[1:])):
                    raise ValueError("Corrupt overlapping archive sections")
                stream.seek(9 + header_size + section["offset"])
                compressed = stream.read(section["length"])
                if len(compressed) != section["length"]:
                    raise ValueError("Corrupt truncated archive section")
            codec = section.get("compression", "zstd" if magic == LEGACY_MAGIC else None)
            if magic == LEGACY_MAGIC and (codec != "zstd" or section.get("encoding") is not None):
                raise ValueError("Corrupt legacy archive encoding")
            if codec == "zstd":
                raw = zstd.ZstdDecompressor().decompress(compressed, max_output_size=section["size"])
            elif codec == "lzma":
                decoder = lzma.LZMADecompressor(memlimit=128 * 1024 * 1024)
                raw = decoder.decompress(compressed, max_length=section["size"] + 1)
                if not decoder.eof or decoder.unused_data:
                    raise ValueError("Corrupt archive LZMA section")
            else:
                raise ValueError("Unknown archive compression codec")
            if len(raw) != section["size"] or hashlib.sha256(raw).digest() != section["sha256"]:
                raise ValueError("Corrupt archive section checksum")
            value = _unpack(raw)
            encoding = section.get("encoding")
            if encoding is None:
                if magic == MAGIC and name in ("events", "replay"):
                    raise ValueError("Corrupt missing archive column encoding")
                return value
            if magic != MAGIC or encoding != "columns-v1" or name not in ("events", "replay"):
                raise ValueError("Unknown archive section encoding")
            canonical = PackedFightReader._read_section(path, "events") if name == "replay" and value["shared_columns"] else None
            return _decode_table(value, canonical)
        except (OSError, KeyError, TypeError, IndexError, AttributeError, struct.error, msgpack.UnpackException, zstd.ZstdError, lzma.LZMAError) as exc:
            raise ValueError("Corrupt or missing WCLF archive section") from exc

    @classmethod
    def read_sections(cls, path):
        return {name: cls._read_section(path, name) for name in SECTION_NAMES}

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
        return _json_bytes(cls.read_full_events(path))

    @classmethod
    def read_full_events(cls, path):
        metadata = cls._read_section(path, 'metadata')
        if 'hybrid' not in metadata['manifest']:
            return cls.read_raw_events(path)
        # Rebuild lightweight capture wrappers. Auxiliary templates already contain
        # their complete bodies; primary event/replay payloads remain columnar.
        raw, replay = cls.read_raw_events(path), cls._read_replay_events(path)
        captures = {}
        for record in metadata['captures']:
            body = deepcopy(record['template'])
            if 'slice' in record:
                events = raw if record['kind'] == 'events' else replay
                first, last = record['slice']
                if not 0 <= first <= last <= len(events):
                    raise ValueError('Corrupt source response slice')
                _set(body, record['event_path'], events[first:last])
            captures[record['id']] = {'info': {key: value for key, value in record.items()
                                                if key not in ('template', 'slice', 'event_path')},
                                      'body': body, 'event_path': record.get('event_path')}
        try:
            manifest = metadata['manifest']
            completed = {'manifest': manifest, 'master': captures[manifest['master_capture']],
                         'pages': [captures[cid] for cid in manifest['event_captures']],
                         'replay': [captures[cid] for cid in manifest['supplemental_captures']],
                         'auxiliary': [captures[cid] for cid in manifest['hybrid']['auxiliary_captures']]}
            archive = FightArchive('.')
            archive._completed[(manifest['report_code'], manifest['fight_id'])] = completed
            return archive.full_fight_events(manifest['report_code'], manifest['fight_id'])
        except (KeyError, TypeError, IndexError) as exc:
            raise ValueError('Corrupt hybrid source metadata') from exc

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
        return CombatStateMachine.analyze_fight(meta, summary.get("actors", []), [], cls.read_full_events(path))[1]

    @classmethod
    def read_replay_only(cls, path):
        from wcl_archiver.parser.combat_state_machine import CombatStateMachine
        from wcl_archiver.codec.wclz_format import _derive_replay_frames_from_events
        summary = cls.read_summary_only(path)
        manifest = cls.source_manifest(path)
        supplemental = cls._read_replay_events(path)
        if 'hybrid' in manifest:
            full = cls.read_full_events(path)
            remaining = defaultdict(int)
            for event in full:
                remaining[_event_identity(event)] += 1
            unmatched = []
            for event in supplemental:
                identity = _event_identity(event)
                if remaining[identity]:
                    remaining[identity] -= 1
                else:
                    unmatched.append(event)
            supplemental = sorted([*full, *unmatched], key=lambda event: event['timestamp'])
        if supplemental:
            meta = {**summary, **cls.source_manifest(path)}
            frames = CombatStateMachine.analyze_fight(meta, summary.get("actors", []), [], supplemental)[2]["frames"]
        else:
            frames = _derive_replay_frames_from_events(cls.read_events_only(path))
        return {"map_id": summary.get("map_id", 0), "pulls": summary.get("pulls", []), "frames": frames}

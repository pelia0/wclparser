"""Immutable per-response staging for one pending API snapshot."""
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

from .fight_archive import FightArchive, _pack, _unpack, supplement_windows
from .staging import _confined, _plain_path


class BrowserWork:
    def __init__(self, data_dir, filename, digest):
        self.data_dir = Path(data_dir).absolute()
        self.parent = [filename, digest]
        key = hashlib.sha256(_pack(self.parent)).hexdigest()
        self.root = _confined(self.data_dir / "browser-staging" / key, self.data_dir)

    def file(self, name):
        return _confined(self.root / name, self.root)

    def read(self, name):
        blob = self.file(name).read_bytes()
        if len(blob) < 32 or hashlib.sha256(blob[32:]).digest() != blob[:32]:
            raise ValueError("Browser staging checksum mismatch")
        parent, label, value = _unpack(blob[32:])
        if parent != self.parent or label != name:
            raise ValueError("Browser staging binding mismatch")
        return value

    def write(self, name, value):
        path = self.file(name)
        self.root.mkdir(parents=True, exist_ok=True)
        _plain_path(self.root)
        if path.exists():
            if _pack(self.read(name)) != _pack(value):
                raise ValueError("Conflicting immutable browser staging record")
            return
        raw = _pack([self.parent, name, value])
        tmp = self.file(uuid.uuid4().hex + ".tmp")
        try:
            with tmp.open("xb") as output:
                output.write(hashlib.sha256(raw).digest())
                output.write(raw)
                output.flush()
                os.fsync(output.fileno())
            try:
                os.link(tmp, path)
            except FileExistsError:
                if _pack(self.read(name)) != _pack(value):
                    raise ValueError("Conflicting browser staging publication")
        finally:
            tmp.unlink(missing_ok=True)

    def initialize(self, code, fid, start, end, fetch):
        if not re.fullmatch(r"[A-Za-z0-9]+", code) or start > end:
            raise ValueError("Invalid browser staging descriptor")
        self.write("descriptor", [code, fid, start, end, fetch])

    @property
    def master_path(self):
        code = self.read("descriptor")[0]
        return f"/reports/fights-and-participants/{code}/0?lang=en&"

    def phase(self):
        for marker, phase in (("canonical", "merge"), ("windows", "canonical"),
                              ("replay", "plan"), ("descriptor", "replay")):
            if self.file(marker).exists():
                self.read(marker)
                return phase
        return "initialize"

    def response_file(self, path):
        return self.file(hashlib.sha256(path.encode()).hexdigest() + ".part")

    def record(self, path):
        record = self.read(self.response_file(path).name)
        if record["path"] != path:
            raise ValueError("Browser response request binding mismatch")
        return record

    def response(self, path):
        try:
            record = self.record(path)
        except FileNotFoundError:
            return None
        return json.loads(record["body"], parse_float=Decimal)

    def save_response(self, path, raw):
        name = self.response_file(path).name
        if self.file(name).exists():
            if self.record(path)["body"] != raw:
                raise ValueError("Conflicting immutable browser response")
            return
        self.write(name, {"path": path, "body": raw,
                          "captured_at": datetime.now(timezone.utc).isoformat()})

    def replay_paths(self, master):
        code, fid, start, end, _ = self.read("descriptor")
        fights = [fight for fight in master["fights"] if fight.get("id") == fid]
        if len(fights) != 1 or (fights[0].get("start_time"), fights[0].get("end_time")) != (start, end):
            raise ValueError("Browser metadata boundaries differ from API snapshot")
        boss = fights[0].get("boss")
        if type(boss) is not int or boss <= 0:
            raise ValueError("Browser metadata lacks valid boss identity")
        return [f"/reports/replaysegment/{code}/{boss}/{left}/{min(left+239999,end)}/"
                for left in range(start, end+1, 240000)]

    def process(self, api_job, api_archive, *, merge):
        code, fid, start, end, fetch = self.read("descriptor")
        m = api_job["manifest"]
        if (code, fid, start, end, fetch) != tuple(m[k] for k in
                ("report_code", "fight_id", "start_time", "end_time", "fetch_id")) or not m.get("hybrid_pending"):
            raise ValueError("Browser staging API parent mismatch")
        browser = FightArchive(self.data_dir)
        def restore(path):
            item = self.record(path)
            cid = browser.capture("GET", "https://www.warcraftlogs.com"+path, item["body"],
                                  source="browser_fetch_text")
            if cid is None:
                raise ValueError("Invalid staged browser response")
            browser._captures[cid]["info"]["captured_at"] = item["captured_at"]
            return browser.get_capture_body(cid)
        master = restore(self.master_path)
        replay = self.replay_paths(master)
        if self.read("replay") != replay:
            raise ValueError("Invalid staged replay coverage")
        with browser.fight_capture(code, fid) as attempt:
            for path in replay:
                restore(path)
            if not merge:
                events = browser.supplement_replay_events(code, fid, attempt, start, end)
                self.write("windows", [list(w) for w in supplement_windows(api_job["raw_events"], events)])
                return
            canonical = self.read("canonical")
            paths = [self.master_path, *replay, *canonical]
            if len(set(paths)) != len(paths) or set(self.root.glob("*.part")) != {self.response_file(p) for p in paths}:
                raise ValueError("Unexpected or missing browser response")
            ordered = iter(canonical)
            for left, right in self.read("windows"):
                cursor = left
                while True:
                    path = (f"/reports/summary-events/{code}/{fid}/{cursor}/{right}"
                            "/0/0/Any/0/-1.0.-1.-1/0")
                    if next(ordered, None) != path:
                        raise ValueError("Incomplete or reordered canonical browser coverage")
                    page = restore(path)
                    cursor = page.get("nextPageTimestamp")
                    if cursor is None:
                        break
                    cursor = int(cursor)
            if next(ordered, None) is not None:
                raise ValueError("Unexpected canonical browser response")
            browser.complete_supplement(code, fid, attempt, start, end, api_job["raw_events"])
            if browser.fight_manifest(code, fid)["canonical_windows"] != self.read("windows"):
                raise ValueError("Canonical window plan mismatch")
            api_archive.merge_hybrid_fight(browser, code, fid)
        return master

    def cleanup(self):
        if not any(self.root.iterdir()):
            self.root.rmdir()
            return
        receipt = self.file("cleanup")
        if not receipt.exists():
            names = [self.response_file(p).name for p in
                     [self.master_path, *self.read("replay"), *self.read("canonical")]]
            names.extend(["descriptor", "windows", "canonical", "replay"])
            if set(self.root.iterdir()) != {self.file(name) for name in names}:
                raise ValueError("Unexpected browser staging files retained")
            for name in names:
                self.read(name)
            self.write("cleanup", names)
        names = self.read("cleanup")
        allowed = {self.file(name) for name in names}
        if any(path.parent != self.root for path in allowed) or set(self.root.iterdir()) - allowed - {receipt}:
            raise ValueError("Unexpected files in interrupted browser cleanup")
        for path in allowed:
            if path.exists():
                self.read(path.name)
        for path in allowed:
            path.unlink(missing_ok=True)
        receipt.unlink()
        self.root.rmdir()

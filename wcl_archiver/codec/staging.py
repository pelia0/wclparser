"""Durable, lossless source snapshots for the independent fight pack queue.

Snapshots deliberately have no compression. Event arrays live only in the captured
responses; the analyzer's flattened list is reconstructed when a packer loads one.
"""
import hashlib
import os
from pathlib import Path
import re
import stat
import struct
import uuid

import msgpack

from wcl_archiver.codec.fight_archive import (
    FightArchive, _at, _events_path, _pack, _report, _unpack,
)


MAGIC = b"WCLS\x01"
_HEADER_SIZE = len(MAGIC) + 8 + 32


def _plain_path(path):
    """Reject links/junctions in every existing component, before resolving it."""
    for item in reversed((path, *path.parents)):
        try:
            entry = item.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(entry.st_mode) or (
            getattr(entry, "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        ):
            raise ValueError("Staging path crosses a symlink or reparse point")


def _confined(path, root):
    path = Path(path)
    if ".." in path.parts:
        raise ValueError("Staging path traversal is forbidden")
    candidate = path if path.is_absolute() else root / path
    candidate = Path(os.path.abspath(candidate))
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("Staging path escapes its root") from exc
    if not relative.parts or any(":" in part for part in relative.parts):
        raise ValueError("Invalid confined staging path")
    _plain_path(candidate)
    return candidate


def _filename(uid, fetch_id, *, hybrid=False):
    # The fetch identifier is itself hashed so untrusted metadata cannot supply
    # path separators, alternate streams, or reserved device names.
    return hashlib.sha256(uid.encode("utf-8")).hexdigest() + "-" + hashlib.sha256(
        fetch_id.encode("utf-8")
    ).hexdigest() + ("-hybrid.wcls" if hybrid else ".wcls")


class StagingStore:
    def __init__(self, data_dir):
        self.data_dir = Path(os.path.abspath(data_dir))
        self.root = self.data_dir / "staging"
        _plain_path(self.root)

    def _path(self, path):
        path = Path(path)
        if not path.is_absolute() and path.parts and path.parts[0] == "staging":
            path = self.data_dir / path
        path = _confined(path, self.root)
        if path.parent != self.root or path.suffix != ".wcls":
            raise ValueError("Invalid staging snapshot path")
        return path

    def _validate(self, payload):
        """Validate bindings independently of a potentially stale master cache."""
        try:
            job, capture = payload["job"], payload["capture"]
            manifest = capture["manifest"]
            row, meta = job["fight_row"], job["fight_meta"]
            code, fid, fetch = manifest["report_code"], manifest["fight_id"], manifest["fetch_id"]
            if (not isinstance(code, str) or not code or type(fid) is not int or fid < 0
                    or not isinstance(fetch, str) or not fetch or manifest["complete"] is not True):
                raise ValueError("Invalid staging capture identity")
            uid = f"{code}_{fid}"
            if (row["fight_uid"] != uid or row["report_code"] != code or row["fight_id"] != fid
                    or meta["report_code"] != code or meta["fight_id"] != fid
                    or job["fetch_id"] != fetch or job["manifest"] != manifest):
                raise ValueError("Staging job/capture identity mismatch")
            start, end, count = (manifest[name] for name in ("start_time", "end_time", "event_count"))
            if (any(type(value) is not int for value in (start, end, count))
                    or start > end or count < 0
                    or meta["start_time"] != start or meta["end_time"] != end):
                raise ValueError("Staging fight boundaries or event count mismatch")
            if not isinstance(job["actors"], list) or not isinstance(job["abilities"], list):
                raise ValueError("Invalid staging actors or abilities")
            if not isinstance(job["out_rel_path"], str) or Path(job["out_rel_path"]).is_absolute():
                raise ValueError("Invalid staging output path")
            output = _confined(job["out_rel_path"], self.data_dir)
            if output == self.root or self.root in output.parents:
                raise ValueError("Archive output cannot replace a staging snapshot")
            master, pages, replay = capture["master"], capture["pages"], capture["replay"]
            auxiliary = capture.get('auxiliary', [])
            if not isinstance(auxiliary, list):
                raise ValueError('Invalid staging auxiliary sources')
            if not isinstance(pages, list) or not pages or not isinstance(replay, list):
                raise ValueError("Staging source pages are missing")
            sources = [master, *pages, *replay, *auxiliary]
            for cap in sources:
                info = cap['info']
                request_report = re.search(
                    r'/reports/(?:fights-and-participants|summary-events|events/summary|replaysegment)/([A-Za-z0-9]+)(?:/|$)',
                    info['url'],
                )
                variables = (info.get('request_json') or {}).get('variables') or {}
                if (info['status'] != 200
                        or request_report and request_report[1] != code
                        or 'code' in variables and variables['code'] != code
                        or info['kind'] != 'master' and 'fightId' in variables
                        and variables['fightId'] != fid):
                    raise ValueError('Staging source request binding mismatch')
            ids = [cap["info"]["id"] for cap in sources]
            if (any(not isinstance(cid, str) or not cid for cid in ids)
                    or len(set(ids)) != len(ids)
                    or manifest["master_capture"] != ids[0]
                    or manifest["event_captures"] != ids[1:1 + len(pages)]
                    or manifest["supplemental_captures"] != ids[1 + len(pages):1 + len(pages) + len(replay)]):
                raise ValueError("Staging source capture IDs differ from manifest")
            report = _report(master["body"])
            if (master["info"]["kind"] != "master" or master["info"]["report_code"] != code
                    or not any(f["id"] == fid for f in report["fights"])):
                raise ValueError("Staging report metadata does not bind this fight")
            observed_count = 0
            for kind, captures in (("events", pages), ("replay", replay)):
                for cap in captures:
                    info = cap["info"]
                    if (info["kind"] != kind or info["report_code"] != code
                            or info["fight_id"] != fid or info["fetch_id"] != fetch
                            or info["status"] != 200
                            or cap["event_path"] != _events_path(cap["body"])):
                        raise ValueError("Staging source attempt binding mismatch")
                    records = _at(cap["body"], cap["event_path"])
                    if not isinstance(records, list) or any(not isinstance(event, dict) for event in records):
                        raise ValueError("Invalid staging event records")
                    if kind == "events":
                        observed_count += len(records)
            if observed_count != count:
                raise ValueError("Staging source event count differs from manifest")
            ordered = FightArchive._ordered_canonical_pages(pages, code, fid, start, end)
            if [cap["info"]["id"] for cap in ordered] != manifest["event_captures"]:
                raise ValueError("Staging source page order differs from manifest")
            FightArchive._validate_hybrid_capture(capture)
            return uid, fetch
        except (KeyError, TypeError, IndexError, AttributeError) as exc:
            raise ValueError("Invalid staging snapshot structure") from exc

    def browser_work(self, path):
        from .browser_staging import BrowserWork
        path = self._path(path)
        with path.open("rb") as stream:
            header = stream.read(_HEADER_SIZE)
            if (len(header) != _HEADER_SIZE or not header.startswith(MAGIC)
                    or struct.unpack("<Q", header[len(MAGIC):-32])[0] != os.fstat(stream.fileno()).st_size - _HEADER_SIZE):
                raise ValueError("Invalid browser staging parent header")
        return BrowserWork(self.data_dir, path.name, header[-32:].hex())

    def _read(self, path, *, check_filename=True):
        try:
            _plain_path(path)
            with path.open("rb") as stream:
                header = stream.read(_HEADER_SIZE)
                if len(header) != _HEADER_SIZE or header[:len(MAGIC)] != MAGIC:
                    raise ValueError("Corrupt or unsupported staging snapshot header")
                size = struct.unpack("<Q", header[len(MAGIC):len(MAGIC) + 8])[0]
                if size != os.fstat(stream.fileno()).st_size - _HEADER_SIZE:
                    raise ValueError("Corrupt staging snapshot size")
                raw = stream.read()
            if hashlib.sha256(raw).digest() != header[-32:]:
                raise ValueError("Corrupt staging snapshot checksum")
            payload = _unpack(raw)
            uid, fetch = self._validate(payload)
            if check_filename and path.name != _filename(uid, fetch, hybrid='hybrid' in payload['capture']['manifest']):
                raise ValueError("Staging filename identity mismatch")
            return payload
        except (msgpack.UnpackException, UnicodeError, struct.error, OverflowError) as exc:
            raise ValueError("Corrupt staging snapshot encoding") from exc

    def stage_fight(self, archive, fight_row, fight_meta, actors, abilities, out_path):
        code, fid = fight_row["report_code"], int(fight_row["fight_id"])
        try:
            completed = archive._completed[(code, fid)]
        except KeyError as exc:
            raise ValueError("No completed source capture to stage") from exc
        output = _confined(out_path, self.data_dir)
        manifest = completed["manifest"]
        payload = {"job": {"fight_row": dict(fight_row), "fight_meta": fight_meta,
                           "actors": actors, "abilities": abilities,
                           "out_rel_path": output.relative_to(self.data_dir).as_posix(),
                           "fetch_id": manifest["fetch_id"], "manifest": manifest},
                   "capture": completed}
        uid, fetch = self._validate(payload)
        # Advancing enrichment is a new durable publication. The API snapshot stays
        # intact until its caller has durably relinked the job to this verified file.
        path = self._path(self.root / _filename(uid, fetch, hybrid='hybrid' in manifest))
        self.root.mkdir(parents=True, exist_ok=True)
        _plain_path(self.root)
        raw = _pack(payload)
        digest = hashlib.sha256(raw).digest()
        tmp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            with tmp.open("xb") as stream:
                stream.write(MAGIC + struct.pack("<Q", len(raw)) + digest)
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            verified = self._read(tmp, check_filename=False)
            if hashlib.sha256(_pack(verified)).digest() != digest:
                raise ValueError("Staging snapshot verification failed")
            del verified
            if path.exists():
                # Repeating publication of the same capture is harmless, but a
                # previously durable, different payload must never be replaced.
                previous = self._read(path)
                if hashlib.sha256(_pack(previous)).digest() != digest:
                    raise ValueError("A different staging snapshot already exists")
            else:
                _plain_path(path)
                os.replace(tmp, path)
            # Release only the same completed attempt; another capture can have
            # finished while the staging thread was writing this one.
            if archive._completed.get((code, fid)) is completed:
                archive._completed.pop((code, fid))
            archive._drop_attempt(fetch)
            return path
        finally:
            tmp.unlink(missing_ok=True)

    def load(self, path, compression="compact", *, include_events=True):
        path = self._path(path)
        payload = self._read(path)
        job, completed = payload["job"], payload["capture"]
        archive = FightArchive(self.data_dir, compression=compression)
        manifest = completed["manifest"]
        archive._completed[(manifest["report_code"], manifest["fight_id"])] = completed
        if include_events:
            job["raw_events"] = archive.full_fight_events(manifest['report_code'], manifest['fight_id'])
        return job, archive

    def list_jobs(self):
        _plain_path(self.root)
        if not self.root.exists():
            return []
        return sorted(self._path(path) for path in self.root.glob("*.wcls"))

    def stats(self):
        sizes = []
        for path in self.list_jobs():
            try:
                sizes.append(path.stat().st_size)
            except FileNotFoundError:
                # A verified packer may remove a snapshot after the directory
                # listing. That normal race must not break the status endpoint.
                continue
        spool = self.data_dir / "browser-staging"
        _plain_path(spool)
        files, total_bytes = len(sizes), sum(sizes)
        if spool.exists():
            for directory in spool.iterdir():
                _plain_path(directory)
                try:
                    for part in directory.iterdir():
                        _plain_path(part)
                        try:
                            total_bytes += part.stat().st_size
                            files += 1
                        except FileNotFoundError:
                            continue
                except FileNotFoundError:
                    continue
        return {"staging_files": files, "staging_bytes": total_bytes}

    def delete(self, path):
        path = self._path(path)
        self._read(path)  # Corrupt or mismatched snapshots remain for recovery.
        _plain_path(path)
        path.unlink()

"""Repackage standalone fight archives, with a read-only inventory by default.

Apply only while the server and other collection writers are stopped. Port
checks are a guard, not a substitute for keeping the server stopped throughout.
Only smaller, fully verified replacements are published. No raw sidecars or
backups are retained. A failed catalogue update can leave a stale size; rerunning
repairs it from the archive's actual size without changing logical fight data.
"""
import argparse
from contextlib import closing, contextmanager
import errno
import json
import os
from pathlib import Path, PureWindowsPath
import socket
import sqlite3
import stat
import uuid

from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader, _pack
from wcl_archiver.config import DATA_DIR as DEFAULT_DIR
from wcl_archiver.reset_data import _checked_entry, _checked_root


_SECTIONS = ("summary", "events", "replay", "metadata")


def _signature(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def _regular(path, root):
    info = _checked_entry(path, root)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("Collection target must be a regular file")
    if info.st_nlink != 1:
        raise ValueError("Refusing multiply linked collection file")
    return info


def _catalogue(root):
    path = root / "index.db"
    info = _regular(path, root)
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = root / ("index.db" + suffix)
        if sidecar.exists() or sidecar.is_symlink():
            _regular(sidecar, root)
    return path, info


def _archive_path(root, relative):
    if not isinstance(relative, str) or not relative or "\0" in relative:
        raise ValueError("Malformed packed catalogue path")
    windows = PureWindowsPath(relative)
    parts = relative.replace("\\", "/").split("/")
    if (windows.drive or windows.root or relative.startswith("/") or
            len(parts) < 2 or parts[0] not in ("mplus_s5", "raids") or
            any(part in ("", ".", "..") or ":" in part for part in parts) or
            not parts[-1].endswith(".wclf")):
        raise ValueError("Unsafe packed catalogue path")
    path = root.joinpath(*parts)
    return path, _regular(path, root)


def _preflight(data_dir, limit):
    if limit is not None and (type(limit) is not int or limit < 1):
        raise ValueError("Limit must be a positive integer")
    root = _checked_root(data_dir)
    database, identity = _catalogue(root)
    # mode=ro never changes catalogue rows. SQLite owns WAL consistency and may
    # maintain its shared-memory cache while reading a live catalogue.
    try:
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as conn:
            conn.execute("PRAGMA query_only=ON")
            rows = conn.execute("SELECT fight_uid, file_rel_path, compressed_bytes FROM fights "
                                "WHERE status='PACKED' ORDER BY fight_uid").fetchall()
    except sqlite3.Error as exc:
        raise ValueError("Cannot read packed catalogue") from exc
    current = _regular(database, root)
    if (identity.st_dev, identity.st_ino) != (current.st_dev, current.st_ino):
        raise ValueError("Catalogue changed during inventory")
    entries, seen = [], set()
    for uid, relative, recorded_size in rows:
        if (not isinstance(uid, str) or not uid or
                type(recorded_size) is not int or recorded_size < 0):
            raise ValueError("Malformed packed catalogue row")
        path, info = _archive_path(root, relative)
        key = str(path).casefold()
        if key in seen:
            raise ValueError("Multiple packed catalogue rows refer to one file")
        seen.add(key)
        entries.append((uid, relative, recorded_size, path, info))
    return root, database, identity, entries[:limit] if limit else entries


def _report(entries, dry_run):
    total = sum(entry[4].st_size for entry in entries)
    return dict(dry_run=dry_run, packed_files=len(entries), old_bytes=total,
                new_bytes=total, saved_bytes=0, repacked_files=0,
                skipped_files=0, corrected_sizes=0, errors=[])


def plan_repack(data_dir, limit=None):
    """Validate recognized PACKED paths and inventory bytes without conversion.

    A live WAL catalogue can be read safely; SQLite may update its SHM cache.
    No archive or catalogue content, queue, history, or authentication is changed.
    """
    return _report(_preflight(data_dir, limit)[3], True)


def _require_server_stopped():
    for family, address in ((socket.AF_INET, ("127.0.0.1", 8080)),
                            (socket.AF_INET6, ("::1", 8080))):
        try:
            with socket.socket(family, socket.SOCK_STREAM) as probe:
                probe.settimeout(0.3)
                result = probe.connect_ex(address)
        except OSError as exc:
            if family == socket.AF_INET6 and exc.errno in (errno.EAFNOSUPPORT, errno.EPROTONOSUPPORT):
                continue
            raise ValueError("Cannot establish that server port 8080 is stopped") from exc
        if result == 0:
            raise ValueError("Stop the local server on port 8080 before applying")
        if result not in (errno.ECONNREFUSED, 10061):
            raise ValueError("Cannot establish that server port 8080 is stopped")


@contextmanager
def _repack_lock(root):
    path = root / ".repack.lock"
    if path.exists() or path.is_symlink():
        _regular(path, root)
    if _checked_root(root) != root:
        raise ValueError("Collection root changed")
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    locked = False
    try:
        opened = os.fstat(descriptor)
        current = _regular(path, root)
        if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
            raise ValueError("Repack lock changed")
        if os.name == "nt":
            import msvcrt
            if opened.st_size == 0:
                os.write(descriptor, b"\0")
            os.lseek(descriptor, 0, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locked = True
        yield
    except OSError as exc:
        if not locked:
            raise ValueError("Another repacker holds the collection lock") from exc
        raise
    finally:
        # Keep the lock inode: unlinking it could let a third process bypass a
        # second process that has already opened the same lock file.
        if locked:
            if os.name == "nt":
                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _unchanged(path, root, original):
    if _signature(_regular(path, root)) != _signature(original):
        raise ValueError("Original archive changed during repacking")


def _update_size(root, database, database_identity, entry, candidate, size):
    uid, relative, _, path, original = entry
    current_db, identity = _catalogue(root)
    if current_db != database or (identity.st_dev, identity.st_ino) != (database_identity.st_dev, database_identity.st_ino):
        raise ValueError("Catalogue changed during repacking")
    # BEGIN IMMEDIATE protects the row's path/status through publication and
    # update. A failed commit after replace leaves valid data and a repairable
    # stale byte count; the next apply reconciles that count from disk.
    with closing(sqlite3.connect(database.as_uri() + "?mode=rw", uri=True, timeout=0)) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT status, file_rel_path, compressed_bytes FROM fights WHERE fight_uid=?", (uid,)).fetchone()
        if row is None or row[:2] != ("PACKED", relative):
            raise ValueError("Catalogue row changed during repacking")
        _unchanged(path, root, original)
        _require_server_stopped()
        if candidate is not None:
            _regular(candidate, root)
            os.replace(candidate, path)
        conn.execute("UPDATE fights SET compressed_bytes=? WHERE fight_uid=? AND status='PACKED' AND file_rel_path=?",
                     (size, uid, relative))
        return row[2] != size


def repack_collection(data_dir, compression="compact", limit=None):
    """Apply smaller verified standalone replacements while all writers are off."""
    if compression not in ("compact", "fast"):
        raise ValueError("Unknown compression profile")
    _require_server_stopped()
    root, database, identity, entries = _preflight(data_dir, limit)
    result = _report(entries, False)
    result["compression"] = compression
    with _repack_lock(root):
        for entry in entries:
            _, _, recorded_size, path, original = entry
            candidate = path.with_name("." + path.name + "." + uuid.uuid4().hex + ".repack.tmp")
            stage = "archive_conversion_failed"
            try:
                _unchanged(path, root, original)
                sections = {name:PackedFightReader._read_section(path, name) for name in _SECTIONS}
                _unchanged(path, root, original)
                FightArchive.write_sections(candidate, sections, compression=compression)
                size = _regular(candidate, root).st_size
                for name, value in sections.items():
                    if _pack(PackedFightReader._read_section(candidate, name)) != _pack(value):
                        raise ValueError("Candidate logical section verification failed")
                _unchanged(path, root, original)
                smaller = size < original.st_size
                final_size = size if smaller else original.st_size
                stage = "publication_or_catalogue_update_failed_rerun_to_reconcile_size"
                corrected = _update_size(root, database, identity, entry,
                                         candidate if smaller else None, final_size)
                result["corrected_sizes"] += int(not smaller and corrected)
                result["repacked_files" if smaller else "skipped_files"] += 1
                result["new_bytes"] -= original.st_size - final_size
                result["saved_bytes"] += original.st_size - final_size
            except Exception:
                # A codec failure is local to this file. Preserve the original,
                # clean its candidate and report no private exception contents.
                # Never include exception text, capture URLs or credentials.
                # Reconcile report bytes if replacement succeeded but DB failed.
                try:
                    actual_size = _regular(path, root).st_size
                    result["new_bytes"] += actual_size - original.st_size
                    result["saved_bytes"] += original.st_size - actual_size
                except (OSError, ValueError):
                    pass
                result["errors"].append({"file":path.name, "error":stage})
            finally:
                try:
                    if candidate.exists() or candidate.is_symlink():
                        _regular(candidate, root)
                        candidate.unlink()
                except (OSError, ValueError):
                    result["errors"].append({"file":path.name, "error":"candidate_cleanup_failed"})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DIR)
    parser.add_argument("--compression", choices=("compact", "fast"), default="compact")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--apply", action="store_true", help="Replace smaller verified files; server must remain stopped")
    args = parser.parse_args()
    try:
        result = (repack_collection(args.data_dir, args.compression, args.limit)
                  if args.apply else plan_repack(args.data_dir, args.limit))
    except (OSError, ValueError, sqlite3.Error):
        result = dict(dry_run=not args.apply, packed_files=0, old_bytes=0, new_bytes=0,
                      saved_bytes=0, repacked_files=0, skipped_files=0, corrected_sizes=0,
                      errors=[{"file":"index.db", "error":"preflight_or_offline_guard_failed"}])
    print(json.dumps(result))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

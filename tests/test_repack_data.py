"""Offline repacking must preserve source data and unrelated catalogue state."""
from decimal import Decimal
import hashlib
from contextlib import closing
import json
from pathlib import Path
import socket
import sqlite3
import struct
import subprocess
import sys

import pytest
import zstandard as zstd

from wcl_archiver.codec.fight_archive import FightArchive, PackedFightReader, _columns, _pack
from wcl_archiver.db.schema import IndexDB
from wcl_archiver import repack_data


def write_v2(path, sections, padding=0):
    chunks, index, offset = [], {}, 0
    for name, value in sections.items():
        raw = _pack(value)
        chunk = zstd.ZstdCompressor(level=1).compress(raw)
        index[name] = dict(offset=offset, length=len(chunk), size=len(raw),
                           sha256=hashlib.sha256(raw).digest())
        chunks.append(chunk)
        offset += len(chunk)
    header = _pack(index)
    path.write_bytes(b"WCLF\x02" + struct.pack("<I", len(header)) + header + b"".join(chunks) + b"\0" * padding)


def collection(tmp_path, padding=16000):
    root = tmp_path / "data"
    root.mkdir()
    db = IndexDB(root / "index.db")
    db.enqueue_fight("Report", 1, "raid", 17, 1, 2092, "argus")
    db.enqueue_fight("Queued", 2, "raid", 17, 1, 2092, "argus")
    db.mark_page_scanned(17, 2092, 1, "Monk", "Windwalker", 1, 1)
    path = root / "raids" / "argus" / "Report_1.wclf"
    path.parent.mkdir(parents=True)
    events = [{"timestamp":i, "bool":True, "int":1, "decimal":Decimal("1.00"),
               "unknown":{"keep":[None, False, 2**100]}} for i in range(100)]
    sections = {"summary":{"actors":[], "unknown":None}, "events":_columns(events),
                "replay":_columns(events), "metadata":{"manifest":{"event_count":100},
                "captures":[{"id":"master", "kind":"master", "template":{"fights":[{"id":1}], "unknown":True}},
                            {"id":"page", "kind":"events", "template":{"events":None,"next":None}, "event_path":["events"], "slice":[0,100]}]}}
    write_v2(path, sections, padding)
    db.mark_fight_packed("Report", 1, path.relative_to(root).as_posix(), path.stat().st_size, 100)
    (root / "auth_session.json").write_bytes(b"private exact preserved bytes")
    return root, path, sections


def snapshot(root):
    # SQLite may maintain its SHM read cache even for mode=ro. Content files,
    # main catalogue bytes and logical rows must remain unchanged.
    return {p.relative_to(root).as_posix():p.read_bytes() for p in root.rglob("*")
            if p.is_file() and p.name not in ("index.db-shm", "index.db-wal")}


def rows(root):
    with sqlite3.connect(root / "index.db") as conn:
        return {table:conn.execute(f"SELECT * FROM {table}").fetchall()
                for table in ("fights", "reports", "scan_history", "fight_participants")}


def test_dry_run_and_cli_preserve_archive_and_catalogue_content(tmp_path):
    root, path, _ = collection(tmp_path)
    before = snapshot(root)
    logical = rows(root)
    plan = repack_data.plan_repack(root)
    assert plan["packed_files"] == 1
    assert plan["old_bytes"] == path.stat().st_size
    assert snapshot(root) == before
    assert rows(root) == logical
    result = subprocess.run([sys.executable, "-m", "wcl_archiver.repack_data", "--data-dir", str(root)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["dry_run"] is True
    assert snapshot(root) == before
    assert rows(root) == logical


def test_apply_preserves_all_sections_sources_and_other_catalogue_rows(tmp_path, monkeypatch):
    root, path, sections = collection(tmp_path)
    monkeypatch.setattr(repack_data, "_require_server_stopped", lambda: None)
    before = rows(root)
    sources = {name:PackedFightReader.read_source_response(path, name) for name in ("master", "page")}
    old_size = path.stat().st_size
    result = repack_data.repack_collection(root)
    assert result["repacked_files"] == 1
    assert result["saved_bytes"] == old_size - path.stat().st_size > 0
    for name, value in sections.items():
        assert _pack(PackedFightReader._read_section(path, name)) == _pack(value)
    assert sources == {name:PackedFightReader.read_source_response(path, name) for name in sources}
    after = rows(root)
    assert after["reports"] == before["reports"]
    assert after["scan_history"] == before["scan_history"]
    assert after["fight_participants"] == before["fight_participants"]
    with sqlite3.connect(root / "index.db") as conn:
        assert conn.execute("SELECT compressed_bytes FROM fights WHERE fight_uid='Report_1'").fetchone()[0] == path.stat().st_size
        assert conn.execute("SELECT status FROM fights WHERE fight_uid='Queued_2'").fetchone()[0] == "QUEUED"
    assert (root / "auth_session.json").read_bytes() == b"private exact preserved bytes"
    second = repack_data.repack_collection(root)
    assert second["repacked_files"] == second["saved_bytes"] == 0


def test_no_savings_keeps_original_and_repairs_stale_catalogue_size(tmp_path, monkeypatch):
    root, path, _ = collection(tmp_path)
    monkeypatch.setattr(repack_data, "_require_server_stopped", lambda: None)
    repack_data.repack_collection(root)
    original = path.read_bytes()
    with sqlite3.connect(root / "index.db") as conn:
        conn.execute("UPDATE fights SET compressed_bytes=1 WHERE fight_uid='Report_1'")
    result = repack_data.repack_collection(root)
    assert result["skipped_files"] == result["corrected_sizes"] == 1
    assert path.read_bytes() == original
    with sqlite3.connect(root / "index.db") as conn:
        assert conn.execute("SELECT compressed_bytes FROM fights WHERE fight_uid='Report_1'").fetchone()[0] == len(original)


def test_failed_verification_keeps_original_and_cleans_candidate(tmp_path, monkeypatch):
    root, path, _ = collection(tmp_path)
    monkeypatch.setattr(repack_data, "_require_server_stopped", lambda: None)
    original = path.read_bytes()
    def corrupt(path, sections, compression="compact"):
        altered = dict(sections, summary={"corrupt":True})
        write_v2(path, altered)
        return path.stat().st_size
    monkeypatch.setattr(FightArchive, "write_sections", staticmethod(corrupt))
    result = repack_data.repack_collection(root)
    assert len(result["errors"]) == 1
    assert result["errors"][0]["file"] == path.name
    assert path.read_bytes() == original
    assert list(path.parent.iterdir()) == [path]


@pytest.mark.parametrize("relative", ["../escape.wclf", "/escape.wclf", "C:\\escape.wclf", "raw/a.wclf", "raids/../escape.wclf", "raids/a.wclz", None])
def test_unsafe_catalogue_path_refused_before_mutation(tmp_path, relative):
    root, _, _ = collection(tmp_path)
    with sqlite3.connect(root / "index.db") as conn:
        conn.execute("UPDATE fights SET file_rel_path=? WHERE fight_uid='Report_1'", (relative,))
    before = snapshot(root)
    with pytest.raises(ValueError):
        repack_data.plan_repack(root)
    assert snapshot(root) == before


def test_link_ancestor_refused(tmp_path):
    root, path, _ = collection(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    link = root / "mplus_s5"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("OS does not permit symlink creation")
    (outside / "x.wclf").write_bytes(path.read_bytes())
    with sqlite3.connect(root / "index.db") as conn:
        conn.execute("UPDATE fights SET file_rel_path='mplus_s5/x.wclf' WHERE fight_uid='Report_1'")
    with pytest.raises(ValueError, match="(?i)link|reparse"):
        repack_data.plan_repack(root)


def test_running_server_refused_before_any_write(tmp_path):
    root, _, _ = collection(tmp_path)
    with socket.socket() as server:
        try:
            server.bind(("127.0.0.1", 8080))
        except OSError:
            with socket.socket() as probe:
                if probe.connect_ex(("127.0.0.1", 8080)) != 0:
                    pytest.skip("8080 unavailable for listener test")
        else:
            server.listen()
        before = snapshot(root)
        with pytest.raises(ValueError, match="(?i)server|8080"):
            repack_data.repack_collection(root)
        assert snapshot(root) == before


def test_live_wal_inventory_reads_committed_rows_without_content_mutation(tmp_path):
    root, _, _ = collection(tmp_path)
    with closing(sqlite3.connect(root / "index.db")) as writer:
        writer.execute("UPDATE fights SET status='QUEUED' WHERE fight_uid='Report_1'")
        writer.commit()
        assert (root / "index.db-wal").stat().st_size > 0
        before, logical = snapshot(root), rows(root)
        assert repack_data.plan_repack(root)["packed_files"] == 0
        assert snapshot(root) == before
        assert rows(root) == logical


def test_unexpected_writer_failure_is_reported_without_losing_original(tmp_path, monkeypatch):
    root, path, _ = collection(tmp_path)
    monkeypatch.setattr(repack_data, "_require_server_stopped", lambda: None)
    original = path.read_bytes()
    def fail(candidate, sections, compression="compact"):
        candidate.write_bytes(b"partial")
        raise RuntimeError("private URL or credential must not appear in report")
    monkeypatch.setattr(FightArchive, "write_sections", staticmethod(fail))
    result = repack_data.repack_collection(root)
    assert len(result["errors"]) == 1
    assert "private" not in json.dumps(result)
    assert path.read_bytes() == original
    assert list(path.parent.iterdir()) == [path]


def test_catalogue_failure_after_replace_keeps_verified_file_and_rerun_repairs_size(tmp_path, monkeypatch):
    root, path, sections = collection(tmp_path)
    monkeypatch.setattr(repack_data, "_require_server_stopped", lambda: None)
    old_size = path.stat().st_size
    with closing(sqlite3.connect(root / "index.db")) as conn, conn:
        conn.execute("CREATE TRIGGER block_size BEFORE UPDATE OF compressed_bytes ON fights BEGIN SELECT RAISE(ABORT, 'blocked'); END")
    result = repack_data.repack_collection(root)
    assert len(result["errors"]) == 1
    assert path.stat().st_size < old_size
    assert result["new_bytes"] == path.stat().st_size
    assert result["saved_bytes"] == old_size - path.stat().st_size
    for name, expected in sections.items():
        assert _pack(PackedFightReader._read_section(path, name)) == _pack(expected)
    with closing(sqlite3.connect(root / "index.db")) as conn, conn:
        assert conn.execute("SELECT compressed_bytes FROM fights WHERE fight_uid='Report_1'").fetchone()[0] == old_size
        conn.execute("DROP TRIGGER block_size")
    retry = repack_data.repack_collection(root)
    assert retry["corrected_sizes"] == retry["skipped_files"] == 1
    assert retry["errors"] == []


def test_archive_changed_during_compression_is_never_replaced(tmp_path, monkeypatch):
    root, path, _ = collection(tmp_path)
    monkeypatch.setattr(repack_data, "_require_server_stopped", lambda: None)
    writer = FightArchive.write_sections
    concurrent_bytes = b"concurrent writer owns this version"
    def replace_source(candidate, sections, compression="compact"):
        result = writer(candidate, sections, compression=compression)
        path.write_bytes(concurrent_bytes)
        return result
    monkeypatch.setattr(FightArchive, "write_sections", staticmethod(replace_source))
    result = repack_data.repack_collection(root)
    assert len(result["errors"]) == 1
    assert path.read_bytes() == concurrent_bytes
    assert list(path.parent.iterdir()) == [path]


def test_second_repacker_cannot_enter_held_lock(tmp_path, monkeypatch):
    root, path, _ = collection(tmp_path)
    monkeypatch.setattr(repack_data, "_require_server_stopped", lambda: None)
    original = path.read_bytes()
    with repack_data._repack_lock(root):
        with pytest.raises(ValueError, match="(?i)lock|repacker"):
            repack_data.repack_collection(root)
    assert path.read_bytes() == original


def test_server_guard_is_rechecked_before_publication(tmp_path, monkeypatch):
    root, path, _ = collection(tmp_path)
    original = path.read_bytes()
    checks = []
    def server_started():
        checks.append(1)
        if len(checks) > 1:
            raise ValueError("server started during compression")
    monkeypatch.setattr(repack_data, "_require_server_stopped", server_started)
    result = repack_data.repack_collection(root)
    assert len(result["errors"]) == 1
    assert path.read_bytes() == original


def test_corrupt_archive_apply_cli_returns_nonzero_and_safe_error(tmp_path, monkeypatch):
    root, path, _ = collection(tmp_path)
    path.write_bytes(b"corrupt private source text")
    monkeypatch.setattr(repack_data, "_require_server_stopped", lambda: None)
    monkeypatch.setattr(sys, "argv", ["repack_data", "--data-dir", str(root), "--apply"])
    assert repack_data.main() == 1
    assert path.read_bytes() == b"corrupt private source text"


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_invalid_limit_is_refused(tmp_path, limit):
    root, _, _ = collection(tmp_path)
    with pytest.raises(ValueError, match="(?i)limit"):
        repack_data.plan_repack(root, limit=limit)

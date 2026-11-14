import json
from pathlib import Path
import stat
import subprocess
import sys
from types import SimpleNamespace

import pytest

from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.reset_data import plan_collection_reset, reset_collection


def collection_fixture(root):
    root.mkdir()
    db = IndexDB(root / "index.db")
    db.enqueue_fight("report", 1, "raid", 17, 1, 2092, "argus")
    db.mark_page_scanned(17, 2092, 1, "Monk", "Windwalker", 1, 1)
    MasterDictDB(root / "master_dict.db").upsert_spell(1, "old spell")
    for name in ("mplus_s5/a/old.wclz", "raids/b/old.wclz", "raw/blobs/source.zst"):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"saved source or cache")
    preserved = {}
    for name in ("auth_session.json", "browser_profile/Default/Cookies", "test_profile/Local State",
                 "notes.txt", "unknown/archive.bin"):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"preserve exact bytes: " + name.encode())
        preserved[path] = path.read_bytes()
    return preserved


def test_reset_only_recognized_collection_and_recreates_empty_catalogues(tmp_path):
    root = tmp_path / "data"
    preserved = collection_fixture(root)
    (root / "index.db-wal").write_bytes(b"stale wal")
    (root / "master_dict.db-shm").write_bytes(b"stale shm")
    removed_paths = [p for p in root.rglob("*") if p.is_file() and p not in preserved]
    expected_bytes = sum(p.stat().st_size for p in removed_paths)
    planned = plan_collection_reset(root)
    assert planned["removed_files"] == len(removed_paths)
    assert planned["removed_bytes"] == expected_bytes
    assert all(p.exists() for p in removed_paths)
    result = reset_collection(root)
    assert result == {"removed_files": len(removed_paths), "removed_bytes": expected_bytes}
    assert all(p.read_bytes() == body for p, body in preserved.items())
    assert all(not (root / name).exists() for name in ("mplus_s5", "raids", "raw", "index.db-wal", "master_dict.db-shm"))
    db = IndexDB(root / "index.db")
    assert db.search_fights() == []
    assert db.get_scanned_pages_count() == 0
    assert not any(db.get_queue_counts().values())
    assert MasterDictDB(root / "master_dict.db").get_spell(1) is None


def test_reset_refuses_nested_symlink_before_any_deletion(tmp_path):
    root = tmp_path / "data"
    collection_fixture(root)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_bytes(b"keep")
    link = root / "raids" / "escape"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("OS does not permit symlink creation")
    with pytest.raises(ValueError, match="(?i)link|reparse|confine"):
        reset_collection(root)
    assert (root / "mplus_s5/a/old.wclz").exists()
    assert (root / "index.db").exists()
    assert (outside / "keep").read_bytes() == b"keep"


def test_reset_refuses_reparse_attribute_before_any_deletion(tmp_path, monkeypatch):
    root = tmp_path / "data"
    collection_fixture(root)
    original_lstat = Path.lstat
    bad = root / "raw" / "blobs"

    def lstat(path):
        value = original_lstat(path)
        if path == bad:
            return SimpleNamespace(st_mode=value.st_mode,
                                   st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT)
        return value

    monkeypatch.setattr(Path, "lstat", lstat)
    with pytest.raises(ValueError, match="(?i)reparse|link"):
        reset_collection(root)
    assert (root / "mplus_s5/a/old.wclz").exists()
    assert (root / "raids/b/old.wclz").exists()


def test_reset_refuses_resolved_escape_before_any_deletion(tmp_path, monkeypatch):
    root = tmp_path / "data"
    collection_fixture(root)
    original_resolve = Path.resolve
    bad = root / "raw" / "blobs" / "source.zst"

    def resolve(path, *args, **kwargs):
        if path == bad:
            return tmp_path / "outside.zst"
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    with pytest.raises(ValueError, match="(?i)confine|outside|escape"):
        reset_collection(root)
    assert bad.exists()
    assert (root / "index.db").exists()


def test_cli_defaults_to_dry_run_and_requires_confirmation_for_mutation(tmp_path):
    root = tmp_path / "data"
    preserved = collection_fixture(root)
    command = [sys.executable, "-m", "wcl_archiver.reset_data", "--data-dir", str(root)]
    dry = subprocess.run(command, capture_output=True, text=True, check=True)
    assert json.loads(dry.stdout)["dry_run"] is True
    assert (root / "raids/b/old.wclz").exists()
    confirmed = subprocess.run(command + ["--confirm-reset"], capture_output=True, text=True, check=True)
    assert json.loads(confirmed.stdout)["dry_run"] is False
    assert not (root / "raids").exists()
    assert all(p.read_bytes() == body for p, body in preserved.items())

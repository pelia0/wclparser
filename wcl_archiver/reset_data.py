"""Reset recognized collection data after the local server has been stopped.

Authentication, browser profiles, and unrecognized top-level paths are untouched.
All deletion targets are checked before the first deletion. Use only while no
other process is modifying the data directory.
"""

import argparse
import json
import os
from pathlib import Path
import stat
from typing import Dict, List, Tuple

from wcl_archiver.config import DATA_DIR as DEFAULT_DIR
from wcl_archiver.db.schema import IndexDB, MasterDictDB


COLLECTION_DIRS = ("mplus_s5", "raids", "raw", "staging")
COLLECTION_FILES = tuple(
    name + suffix
    for name in ("index.db", "master_dict.db")
    for suffix in ("", "-wal", "-shm")
)


def _reject_link(path: Path, info: os.stat_result) -> None:
    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
        raise ValueError(f"Refusing link or reparse point: {path}")


def _checked_root(data_dir: Path) -> Path:
    root = Path(os.path.abspath(data_dir))
    if root == root.parent:
        raise ValueError("Refusing filesystem root as collection directory")
    # Check lexical ancestors before resolving so a junction cannot conceal an escape.
    for path in (root, *root.parents):
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        _reject_link(path, info)
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"Collection ancestor is not a directory: {path}")
    return root.resolve()


def _checked_entry(path: Path, root: Path) -> os.stat_result:
    if path == root or not path.is_relative_to(root):
        raise ValueError(f"Target is outside collection confinement: {path}")
    # Recheck parents too, including immediately before every mutation.
    for ancestor in (path.parent, *path.parent.parents):
        _reject_link(ancestor, ancestor.lstat())
        if ancestor == root:
            break
    info = path.lstat()
    _reject_link(path, info)
    resolved = path.resolve()
    if resolved == root or not resolved.is_relative_to(root):
        raise ValueError(f"Resolved target escapes collection confinement: {path}")
    if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
        raise ValueError(f"Refusing nonregular collection target: {path}")
    return info


def _preflight(data_dir: Path) -> Tuple[Path, List[Tuple[Path, os.stat_result]], Dict[str, int]]:
    root = _checked_root(data_dir)
    entries: List[Tuple[Path, os.stat_result]] = []
    counts = {"removed_files": 0, "removed_bytes": 0}

    def visit(path: Path, expected_dir=None) -> None:
        info = _checked_entry(path, root)
        is_dir = stat.S_ISDIR(info.st_mode)
        if expected_dir is not None and is_dir != expected_dir:
            raise ValueError(f"Unexpected collection target type: {path}")
        if is_dir:
            for child in sorted(path.iterdir()):
                visit(child)
        else:
            counts["removed_files"] += 1
            counts["removed_bytes"] += info.st_size
        entries.append((path, info))

    for name in (*COLLECTION_DIRS, *COLLECTION_FILES):
        path = root / name
        try:
            path.lstat()
        except FileNotFoundError:
            continue
        visit(path, expected_dir=name in COLLECTION_DIRS)
    return root, entries, counts


def plan_collection_reset(data_dir: Path) -> Dict[str, int]:
    """Validate all recognized targets and count files/bytes without writing."""
    return _preflight(data_dir)[2]


def reset_collection(data_dir: Path) -> Dict[str, int]:
    """Remove only validated collection targets and create empty catalogues."""
    root, entries, counts = _preflight(data_dir)
    for path, original in entries:
        current = _checked_entry(path, root)
        if (current.st_dev, current.st_ino, current.st_mode) != (original.st_dev, original.st_ino, original.st_mode):
            raise ValueError(f"Collection target changed after preflight: {path}")
        if stat.S_ISDIR(current.st_mode):
            path.rmdir()
        else:
            path.unlink()
    if _checked_root(root) != root:
        raise ValueError("Collection root changed during reset")
    root.mkdir(parents=True, exist_ok=True)
    IndexDB(root / "index.db")
    MasterDictDB(root / "master_dict.db")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DIR)
    parser.add_argument("--confirm-reset", action="store_true",
                        help="Delete recognized collection data; default only validates and counts")
    args = parser.parse_args()
    operation = reset_collection if args.confirm_reset else plan_collection_reset
    result = operation(args.data_dir)
    print(json.dumps({"dry_run": not args.confirm_reset, **result}))


if __name__ == "__main__":
    main()

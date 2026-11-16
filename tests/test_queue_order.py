"""Scheduling must preserve records while prioritizing Mythic+ in insertion order."""
import sqlite3
import pytest

from wcl_archiver.db.schema import IndexDB


def enqueue(db, code, fid=1, kind="mplus", **kwargs):
    return db.enqueue_fight(code, fid, kind, 9 if kind == "mplus" else 17,
                            5 if kind == "mplus" else 1, 11492 if kind == "mplus" else 2092,
                            "maw-of-souls" if kind == "mplus" else "argus", **kwargs)


def test_reports_and_ready_fights_follow_insertion_not_codes_keys_or_durations(tmp_path):
    db = IndexDB(tmp_path / "index.db")
    enqueue(db, "Zold", keystone_level=2, duration_ms=999999)
    enqueue(db, "Anew", keystone_level=40, duration_ms=1)
    assert db.get_pending_reports_for_unarchive() == ["Zold", "Anew"]
    for code in ("Anew", "Zold"):
        db.mark_report_unarchived(code)
    assert [r["report_code"] for r in db.get_unarchived_pending_fights()] == ["Zold", "Anew"]


def test_pending_mplus_blocks_both_new_and_already_ready_raids(tmp_path):
    db = IndexDB(tmp_path / "index.db")
    enqueue(db, "AreadyRaid", kind="raid")
    enqueue(db, "BpendingRaid", kind="raid")
    db.mark_report_unarchived("AreadyRaid")
    enqueue(db, "Zkey")
    assert db.get_pending_reports_for_unarchive() == ["Zkey"]
    assert db.get_unarchived_pending_fights() == []
    db.mark_report_unarchived("Zkey")
    assert db.get_pending_reports_for_unarchive() == []
    assert [r["report_code"] for r in db.get_unarchived_pending_fights()] == ["Zkey"]
    db.mark_fight_packed("Zkey", 1, "key.wclf", 10, 1)
    assert db.get_pending_reports_for_unarchive() == ["BpendingRaid"]
    assert [r["report_code"] for r in db.get_unarchived_pending_fights()] == ["AreadyRaid"]


def test_report_priority_comes_from_earliest_remaining_fight_not_report_age(tmp_path):
    db = IndexDB(tmp_path / "index.db")
    enqueue(db, "AoldReport")
    db.mark_fight_packed("AoldReport", 1, "old.wclf", 10, 1)
    enqueue(db, "Zmiddle")
    enqueue(db, "AoldReport", 2)
    assert db.get_pending_reports_for_unarchive() == ["Zmiddle", "AoldReport"]


def test_duplicates_reopen_and_status_updates_do_not_move_queue_or_change_rows(tmp_path):
    path = tmp_path / "index.db"
    db = IndexDB(path)
    enqueue(db, "Zold")
    enqueue(db, "Anew")
    assert not enqueue(db, "Zold", keystone_level=50)
    db.mark_report_failed("Zold", "temporary")
    db = IndexDB(path)
    with sqlite3.connect(path) as conn:
        before = {table: conn.execute(f"SELECT rowid,* FROM {table} ORDER BY rowid").fetchall()
                  for table in ("reports", "fights")}
    assert db.get_pending_reports_for_unarchive(limit=1) == ["Zold"]
    with sqlite3.connect(path) as conn:
        after = {table: conn.execute(f"SELECT rowid,* FROM {table} ORDER BY rowid").fetchall()
                 for table in ("reports", "fights")}
    assert after == before


def test_report_deduplication_and_explicit_import_scope(tmp_path):
    db = IndexDB(tmp_path / "index.db")
    enqueue(db, "Zkey", 9)
    enqueue(db, "Zkey", 1)
    enqueue(db, "Akey", 2)
    enqueue(db, "Raid", kind="raid")
    assert db.get_pending_reports_for_unarchive(limit=2) == ["Zkey", "Akey"]
    for code in ("Zkey", "Akey", "Raid"):
        db.mark_report_unarchived(code)
    assert [r["fight_uid"] for r in db.get_unarchived_pending_fights()] == ["Zkey_9", "Zkey_1", "Akey_2"]
    assert [r["fight_uid"] for r in db.get_unarchived_pending_fights(report_code="Raid")] == ["Raid_1"]


def test_bounded_failure_eventually_releases_raid_work_and_reset_restores_priority(tmp_path):
    db = IndexDB(tmp_path / "index.db")
    enqueue(db, "Raid", kind="raid")
    enqueue(db, "Key")
    for _ in range(3):
        assert db.get_pending_reports_for_unarchive(limit=1) == ["Key"]
        db.mark_report_failed("Key", "unavailable")
    assert db.get_pending_reports_for_unarchive() == ["Raid"]
    assert db.search_fights(status="FAILED")[0]["last_error"].endswith("unavailable")
    db.retry_failed_work()
    assert db.get_pending_reports_for_unarchive(limit=1) == ["Key"]


def test_orphan_report_can_finish_after_real_work_without_blocking_completion(tmp_path):
    db = IndexDB(tmp_path / "index.db")
    with db._connect() as conn:
        conn.execute("INSERT INTO reports(report_code) VALUES ('Orphan')")
    enqueue(db, "Key")
    assert db.get_pending_reports_for_unarchive() == ["Key"]
    db.mark_report_unarchived("Key")
    db.mark_fight_packed("Key", 1, "key.wclf", 10, 1)
    assert db.get_pending_reports_for_unarchive() == ["Orphan"]


@pytest.mark.parametrize("kind", ["mplus", "raid"])
def test_younger_ready_fight_waits_for_oldest_unprepared_report(tmp_path, kind):
    db = IndexDB(tmp_path / "index.db")
    enqueue(db, "Old", kind=kind)
    enqueue(db, "Young", kind=kind)
    db.mark_report_failed("Old", "temporary")
    db.mark_report_unarchived("Young")
    assert db.get_unarchived_pending_fights() == []
    # A direct import is an explicit request for that report only.
    assert [r["report_code"] for r in db.get_unarchived_pending_fights(report_code="Young")] == ["Young"]
    db.mark_report_unarchived("Old")
    assert [r["report_code"] for r in db.get_unarchived_pending_fights()] == ["Old", "Young"]

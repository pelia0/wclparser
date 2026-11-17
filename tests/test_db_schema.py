from pathlib import Path
from wcl_archiver.config import (
    LEGION_MPLUS_ZONE_ID,
    LEGION_MPLUS_S5_PARTITION,
    LEGION_MPLUS_S5_DUNGEONS,
    LEGION_RAIDS,
    LEGION_SPECS,
)
from wcl_archiver.db.schema import IndexDB, MasterDictDB


def test_legion_constants_cover_season5_and_all_5_raids():
    assert LEGION_MPLUS_ZONE_ID == 9
    assert LEGION_MPLUS_S5_PARTITION == 5
    assert len(LEGION_MPLUS_S5_DUNGEONS) == 13
    assert set(LEGION_RAIDS.keys()) == {10, 11, 12, 13, 17}
    assert len(LEGION_SPECS) == 36


def test_index_db_deduplicates_report_fight_and_tracks_participants(tmp_path: Path):
    db = IndexDB(tmp_path / "index.db")
    inserted1 = db.enqueue_fight(
        report_code="aBcDeFg123",
        fight_id=4,
        content_type="mplus",
        zone_id=9,
        partition=5,
        encounter_id=11492,
        encounter_slug="maw-of-souls",
        keystone_level=27,
        difficulty=10,
        affixes=[10, 6, 3, 13],
        duration_ms=852000,
        participant={"name": "Pellia", "class": "Monk", "spec": "Windwalker", "role": "dps", "ilvl": 972},
    )
    inserted2 = db.enqueue_fight(
        report_code="aBcDeFg123",
        fight_id=4,
        content_type="mplus",
        zone_id=9,
        partition=5,
        encounter_id=11492,
        encounter_slug="maw-of-souls",
        keystone_level=27,
        difficulty=10,
        affixes=[10, 6, 3, 13],
        duration_ms=852000,
        participant={"name": "Tankbro", "class": "DeathKnight", "spec": "Blood", "role": "tank", "ilvl": 975},
    )
    assert inserted1 is True
    assert inserted2 is False

    fights = db.search_fights(content_type="mplus", encounter_id=11492, spec="Windwalker")
    assert len(fights) == 1
    assert fights[0]["report_code"] == "aBcDeFg123"
    assert fights[0]["fight_id"] == 4
    assert len(fights[0]["participants"]) == 2
    assert fights[0]["affixes"] == [10, 6, 3, 13]

    # Test fight with NULL affixes_json
    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO fights (
                fight_uid, report_code, fight_id, content_type, zone_id, partition,
                encounter_id, encounter_slug, keystone_level, difficulty, affixes_json
            ) VALUES ('null_affix_1', 'nullRep', 1, 'raid', 10, 1, 1853, 'nythendra', 0, 15, NULL)
            """
        )
    null_affix_fights = db.search_fights(encounter_id=1853)
    assert len(null_affix_fights) == 1
    assert null_affix_fights[0]["affixes"] == []


def test_master_dict_db_stores_spells(tmp_path: Path):
    md = MasterDictDB(tmp_path / "master_dict.db")
    md.upsert_spell(113656, "Fists of Fury", 4, "monk_fistsoffury.jpg")
    spell = md.get_spell(113656)
    assert spell["name"] == "Fists of Fury"
    assert spell["school"] == 4


def test_index_db_scan_history_and_difficulty_filter(tmp_path: Path):
    db = IndexDB(tmp_path / "index.db")
    assert db.get_scanned_pages_count() == 0
    assert db.is_page_scanned(9, 11501, 5, "Monk", "Windwalker", 1, difficulty=0) is False
    assert db.get_exhausted_page(9, 11501, 5, "Monk", "Windwalker", difficulty=0) is None

    db.mark_page_scanned(9, 11501, 5, "Monk", "Windwalker", 1, refs_found=25, is_exhausted=False, difficulty=0)
    db.mark_page_scanned(9, 11501, 5, "Monk", "Windwalker", 2, refs_found=0, is_exhausted=True, difficulty=0)
    assert db.is_page_scanned(9, 11501, 5, "Monk", "Windwalker", 1, difficulty=0) is True
    assert db.get_exhausted_page(9, 11501, 5, "Monk", "Windwalker", difficulty=0) == 2
    assert db.get_scanned_pages_count() == 2

    # Raid difficulty filtering
    db.enqueue_fight("rHeroic", 1, "raid", 17, 1, 2092, "argus-the-unmaker", difficulty=4)
    db.enqueue_fight("rMythic", 2, "raid", 17, 1, 2092, "argus-the-unmaker", difficulty=5)
    heroic_only = db.search_fights(content_type="raid", difficulty=4)
    assert len(heroic_only) == 1
    assert heroic_only[0]["report_code"] == "rHeroic"


def _enqueue(db, code="report", fid=1, **kwargs):
    db.enqueue_fight(code, fid, "raid", 17, 1, 2092, "argus", **kwargs)


def test_exact_duration_and_stable_offset_pagination(tmp_path):
    db = IndexDB(tmp_path / "index.db")
    for code in ("z", "a", "m"):
        _enqueue(db, code, duration_ms=999999)
        db.mark_fight_packed(code, 1, f"{code}.wclz", 10, 1, duration_ms=12345)
    assert [r["fight_uid"] for r in db.search_fights(limit=1, offset=1)] == ["m_1"]
    assert all(r["duration_ms"] == 12345 for r in db.search_fights())
    db.mark_fight_packed("m", 1, "m.wclz", 10, 1)
    assert db.search_fights(limit=1, offset=1)[0]["duration_ms"] == 12345


def test_persistent_bounded_retries_and_terminal_report_dependencies(tmp_path):
    path = tmp_path / "index.db"
    db = IndexDB(path)
    _enqueue(db, "bad")
    db.mark_report_failed("bad", "upstream unavailable")
    _enqueue(db, "fresh")
    assert db.get_pending_reports_for_unarchive(limit=1) == ["bad"]
    assert db.get_queue_counts()["retryable_reports"] == 1
    db = IndexDB(path)
    for _ in range(2):
        db.mark_report_failed("bad", "upstream unavailable")
    counts = db.get_queue_counts()
    assert counts == dict(pending_reports=1, queued_fights=1, retryable_reports=0,
                          retryable_fights=0, failed_reports=1, failed_fights=1, failed_scan_pages=0, staged_fights=0,
                          api_staged_fights=0)
    row = db.search_fights(status="FAILED")[0]
    assert "upstream unavailable" in row["last_error"]
    assert db.get_pending_reports_for_unarchive() == ["fresh"]
    db.retry_failed_work()
    assert db.get_queue_counts()["failed_reports"] == 0
    assert db.get_queue_counts()["queued_fights"] == 2
    assert set(db.get_pending_reports_for_unarchive()) == {"bad", "fresh"}


def test_fight_retry_retains_fifo_position_saved_reasons_and_budget_reset(tmp_path):
    db = IndexDB(tmp_path / "index.db")
    _enqueue(db, "r", 1, keystone_level=40)
    _enqueue(db, "r", 2, keystone_level=1)
    db.mark_report_unarchived("r")
    db.mark_fight_failed("r", 1, "disk failure")
    assert db.get_unarchived_pending_fights(limit=1)[0]["fight_id"] == 1
    assert db.get_queue_counts()["retryable_fights"] == 1
    for _ in range(2):
        db.mark_fight_failed("r", 1, "disk failure")
    assert db.get_queue_counts()["failed_fights"] == 1
    failed = db.search_fights(status="FAILED")[0]
    assert failed["failure_count"] == 3
    assert failed["last_error"] == "disk failure"
    db.retry_failed_work()
    row = next(r for r in db.search_fights() if r["fight_id"] == 1)
    assert row["failure_count"] == 0
    assert row["status"] == "QUEUED"


def test_old_schema_migrates_retry_columns_without_losing_fights(tmp_path):
    import sqlite3
    path = tmp_path / "old.db"
    db = IndexDB(path)
    _enqueue(db)
    with sqlite3.connect(path) as conn:
        for table in ("reports", "fights"):
            for column in ("failure_count", "last_error"):
                if column in {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}:
                    conn.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
    db = IndexDB(path)
    assert db.search_fights()[0]["failure_count"] == 0
    db.mark_report_failed("report", "migrated")
    assert db.get_queue_counts()["retryable_reports"] == 1


def test_later_discovered_fights_under_terminal_report_do_not_remain_queued(tmp_path):
    db = IndexDB(tmp_path / "index.db")
    _enqueue(db, "bad")
    for _ in range(3):
        db.mark_report_failed("bad", "unavailable")
    _enqueue(db, "bad", 2)
    counts = db.get_queue_counts()
    assert counts["queued_fights"] == 0
    assert counts["failed_fights"] == 2


def test_discovery_failures_persist_until_matching_page_succeeds(tmp_path):
    path = tmp_path / "index.db"
    db = IndexDB(path)
    key = dict(zone_id=17, encounter_id=2092, partition=1, class_name="Monk",
               spec_name="Windwalker", page=2, difficulty=5)
    db.mark_page_failed(**key, reason="ranking page unavailable", attempts=3)
    db = IndexDB(path)
    db.retry_failed_work()
    assert db.get_queue_counts()["failed_scan_pages"] == 1
    assert not db.is_page_scanned(**key)
    with db._connect() as conn:
        row = dict(conn.execute("SELECT * FROM scan_failures").fetchone())
    assert row["last_error"] == "ranking page unavailable"
    assert row["failure_count"] == 3
    db.mark_page_failed(**key, reason="still unavailable", attempts=3)
    db.mark_page_scanned(**{**key, "difficulty": 4}, refs_found=0)
    assert db.get_queue_counts()["failed_scan_pages"] == 1
    with db._connect() as conn:
        assert conn.execute("SELECT failure_count FROM scan_failures").fetchone()[0] == 6
    db.mark_page_scanned(**key, refs_found=0, is_exhausted=True)
    assert db.get_queue_counts()["failed_scan_pages"] == 0


def test_terminal_scan_clears_only_superseded_failures_and_preserves_history(tmp_path):
    db = IndexDB(tmp_path / "index.db")
    scope = dict(zone_id=17, encounter_id=2092, partition=1, class_name="Monk",
                 spec_name="Windwalker", difficulty=5)
    for page in (1, 2, 3):
        db.mark_page_failed(**scope, page=page, reason="old scan failure")
    db.mark_page_failed(**{**scope, "difficulty":4}, page=3, reason="other scope")
    db.mark_page_scanned(**scope, page=4, refs_found=70)
    _enqueue(db)
    db.mark_page_scanned(**scope, page=2, refs_found=12, is_exhausted=True)
    assert db.get_queue_counts()["failed_scan_pages"] == 2
    assert db.is_page_scanned(**scope, page=4)
    assert len(db.search_fights()) == 1
    assert db.get_exhausted_page(**scope) == 2
    with db._connect() as conn:
        failures = {(row["difficulty"],row["page"]) for row in conn.execute("SELECT * FROM scan_failures")}
    assert failures == {(5,1),(4,3)}


def test_queue_counts_empty_unknown_and_three_scans(tmp_path, monkeypatch):
    from wcl_archiver.db.schema import IndexDB
    db = IndexDB(tmp_path / 'queue.db')
    keys = ('pending_reports', 'queued_fights', 'retryable_reports', 'retryable_fights',
            'failed_reports', 'failed_fights', 'staged_fights', 'api_staged_fights', 'failed_scan_pages')
    assert db.get_queue_counts() == dict.fromkeys(keys, 0)
    db.enqueue_fight('abcdefghij', 1, 'raid', 10, 1, 1853, 'nythendra')
    with db._connect() as conn:
        conn.execute("UPDATE fights SET status = 'UNKNOWN'")
        conn.execute("UPDATE reports SET archive_status = 'UNKNOWN'")
    statements = []
    original = db._connect
    from contextlib import contextmanager
    @contextmanager
    def traced():
        with original() as conn:
            conn.set_trace_callback(statements.append)
            yield conn
    monkeypatch.setattr(db, '_connect', traced)
    assert db.get_queue_counts() == dict.fromkeys(keys, 0)
    assert len([sql for sql in statements if sql.lstrip().upper().startswith('SELECT')]) == 3

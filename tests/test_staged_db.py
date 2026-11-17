from wcl_archiver.db.schema import IndexDB
from contextlib import contextmanager


def enqueue(db, code, kind='mplus'):
    db.enqueue_fight(code, 1, kind, 9, 5, 42, 'boss', duration_ms=456)


def test_staged_queue_is_independent_of_report_and_download_priority(tmp_path):
    db = IndexDB(tmp_path / 'index.db')
    enqueue(db, 'raid', 'raid')
    enqueue(db, 'key')
    db.mark_fight_staged('raid', 1, 'staging/raid.wcls')
    db.mark_fight_staged('key', 1, 'staging/key.wcls')
    assert [r['report_code'] for r in db.get_staged_fights()] == ['key', 'raid']
    assert not db.has_pending_mplus()
    assert db.get_queue_counts()['staged_fights'] == 2
    assert db.get_unarchived_pending_fights() == []
    assert db.get_staged_fights(limit=0) == []


def test_stage_failure_is_bounded_and_new_start_reuses_raw(tmp_path):
    db = IndexDB(tmp_path / 'index.db')
    enqueue(db, 'saved')
    enqueue(db, 'network')
    db.mark_fight_staged('saved', 1, 'staging/saved.wcls')
    db.mark_fight_failed('saved', 1, 'packing failed')
    db.mark_fight_failed('network', 1, 'network failed')
    assert db.get_fight('saved', 1)['status'] == 'FAILED'
    assert db.get_staged_fights() == []
    db.retry_failed_work()
    saved = db.get_fight('saved', 1)
    assert saved['status'] == 'STAGED' and saved['staging_rel_path'] == 'staging/saved.wcls'
    assert saved['duration_ms'] == 456 and saved['failure_count'] == 0
    assert db.get_fight('network', 1)['status'] == 'QUEUED'


def test_packed_commit_does_not_clear_raw_until_explicit_cleanup(tmp_path):
    path = tmp_path / 'index.db'
    db = IndexDB(path)
    enqueue(db, 'saved')
    db.mark_fight_staged('saved', 1, 'staging/saved.wcls')
    db.mark_fight_packed('saved', 1, 'fight.wclf', 100, 3)
    db = IndexDB(path)
    assert db.get_fight('saved', 1)['staging_rel_path'] == 'staging/saved.wcls'
    db.clear_fight_staging('saved', 1, 'staging/saved.wcls')
    assert db.get_fight('saved', 1)['staging_rel_path'] is None


def test_packing_queue_lookup_does_not_scan_the_download_backlog(tmp_path, monkeypatch):
    db = IndexDB(tmp_path / 'index.db')
    with db._connect() as conn:
        conn.executemany('''INSERT INTO fights
            (fight_uid, report_code, fight_id, content_type, zone_id, partition, encounter_id, encounter_slug)
            VALUES (?, ?, 1, 'raid', 1, 1, 1, 'boss')''',
            [(f'queued_{i}', f'queued{i}') for i in range(5000)])
    enqueue(db, 'ready')
    db.mark_fight_staged('ready', 1, 'staging/ready.wcls')
    connect = db._connect
    steps = []

    @contextmanager
    def bounded_connect():
        with connect() as conn:
            def budget():
                steps.append(1)
                return len(steps) > 20
            conn.set_progress_handler(budget, 100)
            yield conn

    monkeypatch.setattr(db, '_connect', bounded_connect)
    assert [row['report_code'] for row in db.get_staged_fights(limit=5)] == ['ready']


def test_key_priority_probe_skips_raid_backlog_and_completed_key_history(tmp_path, monkeypatch):
    db = IndexDB(tmp_path / 'index.db')
    with db._connect() as conn:
        conn.executemany('''INSERT INTO fights
            (fight_uid, report_code, fight_id, content_type, zone_id, partition, encounter_id, encounter_slug)
            VALUES (?, ?, 1, 'raid', 1, 1, 1, 'boss')''',
            [(f'queued_{i}', f'queued{i}') for i in range(5000)])
        conn.executemany('''INSERT INTO fights
            (fight_uid, report_code, fight_id, content_type, zone_id, partition, encounter_id, encounter_slug, status)
            VALUES (?, ?, 1, 'mplus', 9, 5, 42, 'boss', 'PACKED')''',
            [(f'packed_{i}', f'packed{i}') for i in range(5000)])
    connect = db._connect
    steps = []

    @contextmanager
    def bounded_connect():
        with connect() as conn:
            def budget():
                steps.append(1)
                return len(steps) > 20
            conn.set_progress_handler(budget, 100)
            yield conn

    monkeypatch.setattr(db, '_connect', bounded_connect)
    assert not db.has_pending_mplus()

import asyncio

import httpx

from test_disk_pipeline import CapturingClient
from wcl_archiver.api import app as api_module
from wcl_archiver.db.schema import IndexDB
from wcl_archiver.db.selection import QueueSelection


def enqueue(db, code, encounter_id, *, kind='raid', zone_id=17, spec='Fire', difficulty=5):
    db.enqueue_fight(code, 1, kind, zone_id, 1, encounter_id, 'encounter',
                     difficulty=difficulty, participant={'name': 'Player', 'class': 'Mage', 'spec': spec})


def test_targeted_fifo_uses_existing_spec_matches_and_ignores_outside_blockers(tmp_path):
    db = IndexDB(tmp_path / 'index.db')
    enqueue(db, 'older-key', 1, kind='mplus', zone_id=9)
    enqueue(db, 'other-spec', 2072, spec='Frost')
    enqueue(db, 'older-match', 2072)
    enqueue(db, 'younger-match', 2072)
    # One fight can occur in several ranking lists, including duplicate discoveries.
    enqueue(db, 'younger-match', 2072, spec='Frost')
    enqueue(db, 'other-boss', 2069)
    enqueue(db, 'other-difficulty', 2072, difficulty=4)
    db.mark_report_unarchived('younger-match')
    target = QueueSelection('raid', zone_id=17, encounter_id=2072, difficulty=5,
                            specialization=('Mage', 'Fire'))
    assert db.get_pending_reports_for_unarchive(selection=target) == ['older-match']
    assert db.get_unarchived_pending_fights(selection=target) == []
    db.mark_report_unarchived('older-match')
    rows = db.get_unarchived_pending_fights(selection=target)
    assert [row['report_code'] for row in rows] == ['older-match', 'younger-match']
    db.mark_fight_api_staged('other-boss', 1, 'staging/other.wcls')
    db.mark_fight_api_staged('older-match', 1, 'staging/older.wcls')
    db.mark_fight_staged('other-spec', 1, 'staging/frost.wcls')
    db.mark_fight_staged('younger-match', 1, 'staging/younger.wcls')
    assert [row['report_code'] for row in db.get_browser_dispatch(3, 1, selection=target)] == ['older-match']
    assert [row['report_code'] for row in db.get_staged_fights(selection=target)] == ['younger-match']
    assert db.has_selected_work(target, include_browser=True)
    db.mark_fight_packed('younger-match', 1, 'raid/packed.wclf', 1, 1)
    assert not db.has_selected_work(target, include_browser=False)
    assert db.get_fight('other-spec', 1)['status'] == 'STAGED'


def test_random_wraps_and_takes_prepared_fights_despite_pending_report(tmp_path, monkeypatch):
    db = IndexDB(tmp_path / 'index.db')
    for code in ('pending', 'first', 'second', 'third'):
        enqueue(db, code, 2072)
    for code in ('first', 'second', 'third'):
        db.mark_report_unarchived(code)
    pivots = iter([(2, 5, 4), (1, 2, 1)])
    def choose(start, stop):
        low, high, chosen = next(pivots)
        assert (start, stop) == (low, high)
        return chosen
    monkeypatch.setattr('wcl_archiver.db.schema.random.randrange', choose)
    monkeypatch.setattr('wcl_archiver.db.schema.random.shuffle', lambda rows: None)
    target = QueueSelection('raid', order='random', encounter_id=2072)
    rows = db.get_unarchived_pending_fights(limit=3, selection=target)
    assert [row['report_code'] for row in rows] == ['third', 'first', 'second']
    assert db.get_pending_reports_for_unarchive(selection=target) == ['pending']
    base, params = db._selected_pending_sql(target)
    with db._connect() as conn:
        plan = conn.execute('EXPLAIN QUERY PLAN SELECT f.* ' + base +
            ' AND f.rowid >= ? ORDER BY f.rowid LIMIT ?', (db.MAX_WORK_ATTEMPTS, *params, 1, 32)).fetchall()
    assert any('idx_fights_pending_boss' in row['detail'] for row in plan)
    assert not any('TEMP B-TREE' in row['detail'] for row in plan)


async def test_raid_start_reuses_scan_history_and_finishes_only_matching_queue(tmp_path, monkeypatch):
    captures = []

    def provider(*, auth_manager, raw_archive):
        client = CapturingClient(raw_archive)
        client.get_quota_status = lambda: {}
        captures.append(client)
        return client

    monkeypatch.setattr(api_module, 'WCLProtocolClient', provider)
    db = IndexDB(tmp_path / 'index.db')
    enqueue(db, 'OlderKey', 1, kind='mplus', zone_id=9)
    enqueue(db, 'FrostRaid', 2092, spec='Frost')
    enqueue(db, 'FireRaid', 2092)
    db.mark_fight_staged('FrostRaid', 1, 'staging/keep-this.wcls')
    for partition in (1, 2):
        db.mark_page_scanned(17, 2092, partition, 'Mage', 'Fire', 1, refs_found=1, difficulty=5)
    db.mark_page_failed(17, 2092, 1, 'Mage', 'Frost', 1, difficulty=5, reason='Unrelated scan')
    app = api_module.create_app(tmp_path)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as http:
            start = {'mode': 'raids', 'rankings_source': 'api', 'queue_order': 'fifo',
                     'raid_zone_id': 17, 'raid_boss_id': 2092, 'raid_difficulty': 5,
                     'raid_specialization': ['Mage', 'Fire'], 'pages_per_spec': 1}
            invalid = await http.post('/api/harvester/start', json={**start, 'raid_zone_id': 10})
            assert invalid.status_code == 422
            started = await http.post('/api/harvester/start', json=start)
            assert started.status_code == 200
            assert started.json()['specs_total'] == 2
            assert started.json()['queue_selection']['specialization'] == ['Mage', 'Fire']

            async def finish():
                while True:
                    status = (await http.get('/api/harvester/status')).json()
                    if not status['running']:
                        return status
                    await asyncio.sleep(.03)

            status = await asyncio.wait_for(finish(), 10)
            assert status['last_error'] == ''
            assert status['progress_pct'] == 100
    assert captures[0].downloads == [('FireRaid', 1)]
    assert db.get_fight('FireRaid', 1)['status'] == 'PACKED'
    assert db.get_fight('OlderKey', 1)['status'] == 'QUEUED'
    assert db.get_fight('FrostRaid', 1)['status'] == 'STAGED'

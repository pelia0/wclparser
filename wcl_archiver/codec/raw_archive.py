"""Authoritative, content-addressed source bodies; derived fight summaries.

HTTP content is captured before parsing. WCLF stores references, never a second
copy of the event stream. Legacy WCLZ is readable through its existing reader.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import uuid

import msgpack
import zstandard as zstd


def _atomic_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with tmp.open('xb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


class RawArchive:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._scope = ContextVar('wcl_capture_' + uuid.uuid4().hex, default=None)
        with self._connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS captures (
                    id TEXT PRIMARY KEY, sha256 TEXT NOT NULL, method TEXT NOT NULL,
                    url TEXT NOT NULL, status INTEGER NOT NULL, request_json TEXT,
                    captured_at TEXT NOT NULL, source TEXT NOT NULL,
                    report_code TEXT, fight_id INTEGER, kind TEXT NOT NULL, fetch_id TEXT
                );
                CREATE INDEX IF NOT EXISTS capture_report ON captures(report_code,kind);
                CREATE INDEX IF NOT EXISTS capture_fetch ON captures(fetch_id,kind);
                CREATE TABLE IF NOT EXISTS objects (sha256 TEXT PRIMARY KEY, compressed_bytes INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS complete_fights (
                    report_code TEXT NOT NULL, fight_id INTEGER NOT NULL,
                    manifest_json TEXT NOT NULL, PRIMARY KEY(report_code,fight_id)
                );
            ''')

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(str(self.root / 'manifest.db'), timeout=30)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                yield db
        finally:
            db.close()

    @contextmanager
    def fight_capture(self, report_code, fight_id):
        fetch_id = uuid.uuid4().hex
        token = self._scope.set((report_code, int(fight_id), fetch_id))
        try:
            yield fetch_id
        finally:
            self._scope.reset(token)

    def capture(self, method, url, body, status=200, request_json=None, source='http'):
        if not isinstance(body, bytes):
            raise TypeError('Source body must be bytes, before JSON normalization')
        digest = hashlib.sha256(body).hexdigest()
        blob = self.root / 'objects' / digest[:2] / (digest + '.zst')
        if not blob.exists():
            compressed = zstd.ZstdCompressor(level=9).compress(body)
            if zstd.ZstdDecompressor().decompress(compressed) != body:
                raise ValueError('Source compression verification failed')
            _atomic_write(blob, compressed)
        else:
            self._read_blob(digest)
        report = None
        fight = None
        fetch_id = None
        kind = 'other'
        match = re.search(r'/reports/(fights-and-participants|summary-events|events/summary|replaysegment)/([A-Za-z0-9]+)/(\d+)', url)
        if match:
            endpoint, report, number = match.groups()
            kind = 'master' if endpoint == 'fights-and-participants' else ('replay' if endpoint == 'replaysegment' else 'events')
            if kind == 'events':
                fight = int(number)
        if request_json:
            variables = request_json.get('variables') or {}
            report = variables.get('code', report)
            fight = variables.get('fightId', fight)
            if report:
                kind = 'events' if fight is not None else 'master'
        scope = self._scope.get()
        if scope:
            report, fight, fetch_id = scope
        capture_id = uuid.uuid4().hex
        with self._connect() as db:
            db.execute('INSERT OR IGNORE INTO objects VALUES (?,?)',(digest,blob.stat().st_size))
            db.execute('INSERT INTO captures VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', (
                capture_id, digest, method.upper(), url, int(status),
                json.dumps(request_json, ensure_ascii=False) if request_json is not None else None,
                datetime.now(timezone.utc).isoformat(), source, report, fight, kind, fetch_id))
        return capture_id

    def _read_blob(self, digest):
        if not re.fullmatch(r'[a-f0-9]{64}', digest):
            raise ValueError('Invalid source digest')
        try:
            raw = zstd.ZstdDecompressor().decompress((self.root/'objects'/digest[:2]/(digest+'.zst')).read_bytes())
        except (OSError, zstd.ZstdError) as exc:
            raise ValueError('Corrupt or missing source blob') from exc
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError('Corrupt source checksum')
        return raw

    def capture_info(self, capture_id):
        with self._connect() as db:
            row = db.execute('SELECT * FROM captures WHERE id=?', (capture_id,)).fetchone()
        if row is None:
            raise KeyError(capture_id)
        result = dict(row)
        if result['request_json'] is not None:
            result['request_json'] = json.loads(result['request_json'])
        return result

    def read_body(self, capture_id):
        return self._read_blob(self.capture_info(capture_id)['sha256'])

    def complete_fight(self, report_code, fight_id, fetch_id, start, end, event_count):
        with self._connect() as db:
            master = db.execute("SELECT id FROM captures WHERE report_code=? AND kind='master' AND status=200 ORDER BY rowid DESC LIMIT 1", (report_code,)).fetchone()
            captures = db.execute("SELECT id FROM captures WHERE fetch_id=? AND kind='events' AND status=200 ORDER BY rowid", (fetch_id,)).fetchall()
            supplemental = db.execute("SELECT id FROM captures WHERE fetch_id=? AND kind='replay' AND status=200 ORDER BY rowid", (fetch_id,)).fetchall()
        if master is None:
            raise ValueError('Source report metadata is missing')
        master_body = json.loads(self.read_body(master['id']))
        master_report = master_body if 'fights' in master_body else ((master_body.get('data') or {}).get('reportData') or {}).get('report')
        if (master_body.get('errors') or not isinstance(master_report,dict)
                or not isinstance(master_report.get('fights'),list)
                or not any(int(f.get('id',-1))==int(fight_id) for f in master_report['fights'])):
            raise ValueError('Incomplete source report metadata')
        if not captures:
            raise ValueError('No complete event source pages')
        # Count independently from preserved responses, including GraphQL pages.
        count = 0
        ids = [row['id'] for row in captures]
        for capture_id in ids:
            obj = json.loads(self.read_body(capture_id))
            if 'errors' in obj:
                raise ValueError('GraphQL source contains errors')
            if 'events' in obj:
                page = obj['events']
            else:
                page = obj['data']['reportData']['report']['events']['data']
            count += len(page)
        if count != event_count:
            raise ValueError('Source event count does not match fetched events')
        manifest = {'report_code':report_code, 'fight_id':int(fight_id),
                    'start_time':int(start), 'end_time':int(end), 'event_count':int(event_count),
                    'fetch_id':fetch_id, 'master_capture':master['id'], 'event_captures':ids,
                    'supplemental_captures':[row['id'] for row in supplemental],
                    'complete':True}
        with self._connect() as db:
            db.execute('INSERT OR REPLACE INTO complete_fights VALUES (?,?,?)',
                       (report_code, int(fight_id), json.dumps(manifest)))

    def fight_manifest(self, report_code, fight_id):
        with self._connect() as db:
            row = db.execute('SELECT manifest_json FROM complete_fights WHERE report_code=? AND fight_id=?', (report_code,int(fight_id))).fetchone()
        if row is None:
            raise ValueError('No complete source archive for this fight')
        return json.loads(row[0])

    def stats(self):
        with self._connect() as db:
            size = db.execute('SELECT COALESCE(sum(compressed_bytes),0) FROM objects').fetchone()[0]
            count = db.execute('SELECT count(*) FROM captures').fetchone()[0]
        return {'source_bytes':size,'source_responses':count}

    def write_fight(self, path, summary, report_code, fight_id):
        path = Path(path)
        manifest = self.fight_manifest(report_code, fight_id)
        payload = {'version':1, 'summary':summary, 'source':manifest,
                   'archive_root':os.path.relpath(self.root, path.parent)}
        raw = msgpack.packb(payload, use_bin_type=True)
        data = b'WCLF\x01' + hashlib.sha256(raw).digest() + zstd.ZstdCompressor(level=9).compress(raw)
        _atomic_write(path, data)
        if SourceFightReader.read_summary_only(path) != summary:
            # msgpack represents tuples as lists; compare through serialization.
            if msgpack.packb(SourceFightReader.read_summary_only(path), use_bin_type=True) != msgpack.packb(summary,use_bin_type=True):
                raise ValueError('Fight summary verification failed')
        return len(data)


class SourceFightReader:
    @staticmethod
    def _read(path):
        data = Path(path).read_bytes()
        if data[:5] != b'WCLF\x01':
            raise ValueError('Invalid WCLF source-reference file')
        try:
            raw = zstd.ZstdDecompressor().decompress(data[37:])
        except zstd.ZstdError as exc:
            raise ValueError('Corrupt WCLF file') from exc
        if hashlib.sha256(raw).digest() != data[5:37]:
            raise ValueError('Corrupt WCLF checksum')
        return msgpack.unpackb(raw,raw=False)

    @classmethod
    def read_summary_only(cls,path):
        return cls._read(path)['summary']

    @classmethod
    def source_manifest(cls,path):
        return cls._read(path)['source']

    @classmethod
    def read_raw_events(cls,path):
        payload = cls._read(path)
        archive = RawArchive(Path(path).parent/payload['archive_root'])
        events = []
        for capture_id in payload['source']['event_captures']:
            obj = json.loads(archive.read_body(capture_id))
            events.extend(obj['events'] if 'events' in obj else obj['data']['reportData']['report']['events']['data'])
        if len(events) != payload['source']['event_count']:
            raise ValueError('Source event count mismatch')
        return events

    @classmethod
    def read_events_only(cls,path):
        # Derived viewer representation. Exported source bodies remain untouched.
        from wcl_archiver.parser.combat_state_machine import CombatStateMachine
        payload = cls._read(path)
        summary = payload['summary']
        meta = {**summary, **payload['source']}
        return CombatStateMachine.analyze_fight(meta,summary.get('actors',[]),[],cls.read_raw_events(path))[1]

    @classmethod
    def read_replay_only(cls,path):
        from wcl_archiver.codec.wclz_format import _derive_replay_frames_from_events
        summary = cls.read_summary_only(path)
        payload = cls._read(path)
        supplemental = payload['source'].get('supplemental_captures',[])
        if supplemental:
            from wcl_archiver.parser.combat_state_machine import CombatStateMachine
            archive = RawArchive(Path(path).parent/payload['archive_root'])
            replay_events = []
            for capture_id in supplemental:
                replay_events.extend(json.loads(archive.read_body(capture_id)).get('events',[]))
            frames = CombatStateMachine.analyze_fight({**summary,**payload['source']},summary.get('actors',[]),[],replay_events)[2]['frames']
        else:
            frames = _derive_replay_frames_from_events(cls.read_events_only(path))
        return {'map_id':summary.get('map_id',0),'pulls':summary.get('pulls',[]),'frames':frames}

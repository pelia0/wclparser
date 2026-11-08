import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional


class IndexDB:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS reports (
                    report_code TEXT PRIMARY KEY,
                    archive_status TEXT NOT NULL DEFAULT 'PENDING',
                    updated_at INTEGER DEFAULT (strftime('%s','now'))
                );

                CREATE TABLE IF NOT EXISTS fights (
                    fight_uid TEXT PRIMARY KEY,
                    report_code TEXT NOT NULL,
                    fight_id INTEGER NOT NULL,
                    content_type TEXT NOT NULL,
                    zone_id INTEGER NOT NULL,
                    partition INTEGER NOT NULL,
                    encounter_id INTEGER NOT NULL,
                    encounter_slug TEXT NOT NULL,
                    keystone_level INTEGER DEFAULT 0,
                    difficulty INTEGER DEFAULT 0,
                    affixes_json TEXT DEFAULT '[]',
                    duration_ms INTEGER DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'QUEUED',
                    file_rel_path TEXT DEFAULT NULL,
                    compressed_bytes INTEGER DEFAULT 0,
                    event_count INTEGER DEFAULT 0,
                    UNIQUE(report_code, fight_id)
                );

                CREATE TABLE IF NOT EXISTS fight_participants (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fight_uid TEXT NOT NULL,
                    player_name TEXT NOT NULL,
                    class_name TEXT NOT NULL,
                    spec_name TEXT NOT NULL,
                    role TEXT NOT NULL,
                    ilvl INTEGER DEFAULT 0,
                    talents_json TEXT DEFAULT '[]',
                    legendaries_json TEXT DEFAULT '[]',
                    dps REAL DEFAULT 0,
                    hps REAL DEFAULT 0,
                    UNIQUE(fight_uid, player_name, class_name, spec_name)
                );

                CREATE INDEX IF NOT EXISTS idx_fights_lookup
                    ON fights(content_type, zone_id, encounter_id, status);
                CREATE INDEX IF NOT EXISTS idx_participants_spec
                    ON fight_participants(class_name, spec_name, fight_uid);
                """
            )

    @staticmethod
    def make_fight_uid(report_code: str, fight_id: int) -> str:
        return f"{report_code}_{fight_id}"

    def enqueue_fight(
        self,
        report_code: str,
        fight_id: int,
        content_type: str,
        zone_id: int,
        partition: int,
        encounter_id: int,
        encounter_slug: str,
        keystone_level: int = 0,
        difficulty: int = 0,
        affixes: Optional[List[int]] = None,
        duration_ms: int = 0,
        participant: Optional[Dict[str, Any]] = None,
    ) -> bool:
        fight_uid = self.make_fight_uid(report_code, fight_id)
        affixes_str = json.dumps(affixes or [])
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO reports (report_code, archive_status) VALUES (?, 'PENDING')",
                (report_code,),
            )
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO fights (
                    fight_uid, report_code, fight_id, content_type, zone_id, partition,
                    encounter_id, encounter_slug, keystone_level, difficulty, affixes_json, duration_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fight_uid,
                    report_code,
                    fight_id,
                    content_type,
                    zone_id,
                    partition,
                    encounter_id,
                    encounter_slug,
                    keystone_level,
                    difficulty,
                    affixes_str,
                    duration_ms,
                ),
            )
            inserted = cur.rowcount > 0
            if participant:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO fight_participants (
                        fight_uid, player_name, class_name, spec_name, role, ilvl,
                        talents_json, legendaries_json, dps, hps
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fight_uid,
                        participant.get("name", "Unknown"),
                        participant.get("class", ""),
                        participant.get("spec", ""),
                        participant.get("role", "dps"),
                        int(participant.get("ilvl", 0)),
                        json.dumps(participant.get("talents", [])),
                        json.dumps(participant.get("legendaries", [])),
                        float(participant.get("dps", 0.0)),
                        float(participant.get("hps", 0.0)),
                    ),
                )
            return inserted

    def mark_report_unarchived(self, report_code: str, status: str = "UNARCHIVED") -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE reports SET archive_status = ?, updated_at = strftime('%s','now') WHERE report_code = ?",
                (status, report_code),
            )

    def mark_fight_packed(
        self,
        report_code: str,
        fight_id: int,
        file_rel_path: str,
        compressed_bytes: int,
        event_count: int,
    ) -> None:
        fight_uid = self.make_fight_uid(report_code, fight_id)
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE fights
                SET status = 'PACKED',
                    file_rel_path = ?,
                    compressed_bytes = ?,
                    event_count = ?
                WHERE fight_uid = ?
                """,
                (file_rel_path, compressed_bytes, event_count, fight_uid),
            )

    def get_pending_reports_for_unarchive(self, limit: int = 30) -> List[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT report_code FROM reports WHERE archive_status = 'PENDING' LIMIT ?",
                (limit,),
            ).fetchall()
            return [r["report_code"] for r in rows]

    def get_unarchived_pending_fights(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT f.*
                FROM fights f
                JOIN reports r ON r.report_code = f.report_code
                WHERE r.archive_status = 'UNARCHIVED' AND f.status = 'QUEUED'
                ORDER BY f.keystone_level DESC, f.duration_ms ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def search_fights(
        self,
        content_type: Optional[str] = None,
        encounter_id: Optional[int] = None,
        class_name: Optional[str] = None,
        spec: Optional[str] = None,
        min_key_level: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        clauses = ["1=1"]
        params: List[Any] = []
        if content_type:
            clauses.append("f.content_type = ?")
            params.append(content_type)
        if encounter_id is not None:
            clauses.append("f.encounter_id = ?")
            params.append(encounter_id)
        if min_key_level is not None:
            clauses.append("f.keystone_level >= ?")
            params.append(min_key_level)
        if status:
            clauses.append("f.status = ?")
            params.append(status)
        if class_name or spec:
            sub_clauses = []
            if class_name:
                sub_clauses.append("fp.class_name = ?")
                params.append(class_name)
            if spec:
                sub_clauses.append("fp.spec_name = ?")
                params.append(spec)
            clauses.append(
                f"EXISTS (SELECT 1 FROM fight_participants fp WHERE fp.fight_uid = f.fight_uid AND {' AND '.join(sub_clauses)})"
            )

        params.append(limit)
        sql = f"""
            SELECT f.*
            FROM fights f
            WHERE {' AND '.join(clauses)}
            ORDER BY f.keystone_level DESC, f.duration_ms ASC
            LIMIT ?
        """
        with self._connect() as conn:
            fight_rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
            for f in fight_rows:
                f["affixes"] = json.loads(f.pop("affixes_json", "[]"))
                p_rows = conn.execute(
                    "SELECT * FROM fight_participants WHERE fight_uid = ?",
                    (f["fight_uid"],),
                ).fetchall()
                f["participants"] = [dict(p) for p in p_rows]
            return fight_rows


class MasterDictDB:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS spells (
                    spell_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    school INTEGER DEFAULT 1,
                    icon TEXT DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS items (
                    item_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    icon TEXT DEFAULT '',
                    slot INTEGER DEFAULT 0
                );
                """
            )

    def upsert_spell(self, spell_id: int, name: str, school: int = 1, icon: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO spells (spell_id, name, school, icon) VALUES (?, ?, ?, ?)",
                (int(spell_id), name, int(school), icon),
            )

    def get_spell(self, spell_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM spells WHERE spell_id = ?", (int(spell_id),)).fetchone()
            return dict(row) if row else None

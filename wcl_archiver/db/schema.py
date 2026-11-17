from contextlib import contextmanager
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple


class IndexDB:
    MAX_WORK_ATTEMPTS = 3

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

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

                CREATE TABLE IF NOT EXISTS scan_history (
                    zone_id INTEGER NOT NULL,
                    encounter_id INTEGER NOT NULL,
                    partition INTEGER NOT NULL,
                    difficulty INTEGER NOT NULL DEFAULT 0,
                    class_name TEXT NOT NULL,
                    spec_name TEXT NOT NULL,
                    page INTEGER NOT NULL,
                    refs_found INTEGER NOT NULL DEFAULT 0,
                    is_exhausted INTEGER NOT NULL DEFAULT 0,
                    scanned_at INTEGER DEFAULT (strftime('%s','now')),
                    PRIMARY KEY (zone_id, encounter_id, partition, difficulty, class_name, spec_name, page)
                );

                CREATE TABLE IF NOT EXISTS scan_failures (
                    zone_id INTEGER NOT NULL,
                    encounter_id INTEGER NOT NULL,
                    partition INTEGER NOT NULL,
                    difficulty INTEGER NOT NULL DEFAULT 0,
                    class_name TEXT NOT NULL,
                    spec_name TEXT NOT NULL,
                    page INTEGER NOT NULL,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at INTEGER DEFAULT (strftime('%s','now')),
                    PRIMARY KEY (zone_id, encounter_id, partition, difficulty, class_name, spec_name, page)
                );

                CREATE INDEX IF NOT EXISTS idx_fights_lookup
                    ON fights(content_type, zone_id, encounter_id, status);
                CREATE INDEX IF NOT EXISTS idx_fights_staged_dispatch
                    ON fights(CASE WHEN content_type = 'mplus' THEN 0 ELSE 1 END)
                    WHERE status = 'STAGED';
                CREATE INDEX IF NOT EXISTS idx_participants_spec
                    ON fight_participants(class_name, spec_name, fight_uid);
                """
            )
            # Existing catalogues gain retry metadata in place.
            for table in ("reports", "fights"):
                columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
                for column, declaration in (
                    ("failure_count", "INTEGER NOT NULL DEFAULT 0"),
                    ("last_error", "TEXT NOT NULL DEFAULT ''"),
                ):
                    if column not in columns:
                        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_fights_pending_mplus
                ON fights(report_code)
                WHERE content_type = 'mplus' AND status IN ('QUEUED', 'RETRY')""")
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(fights)")}
            if "staging_rel_path" not in columns:
                conn.execute("ALTER TABLE fights ADD COLUMN staging_rel_path TEXT DEFAULT NULL")
            if "needs_browser" not in columns:
                conn.execute("ALTER TABLE fights ADD COLUMN needs_browser INTEGER NOT NULL DEFAULT 0")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_fights_api_staged_dispatch
                ON fights(CASE WHEN content_type = 'mplus' THEN 0 ELSE 1 END)
                WHERE status = 'API_STAGED'""")
            conn.execute("""
                UPDATE fights SET status = 'FAILED', last_error = 'Report unavailable: ' ||
                    COALESCE((SELECT last_error FROM reports WHERE reports.report_code = fights.report_code), '')
                WHERE status IN ('QUEUED', 'RETRY') AND report_code IN
                    (SELECT report_code FROM reports WHERE archive_status = 'FAILED')
            """)

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
            conn.execute("""
                UPDATE fights SET status = 'FAILED', last_error = 'Report unavailable: ' ||
                    (SELECT last_error FROM reports WHERE report_code = ?)
                WHERE fight_uid = ? AND status IN ('QUEUED', 'RETRY') AND EXISTS
                    (SELECT 1 FROM reports WHERE report_code = ? AND archive_status = 'FAILED')
            """, (report_code, fight_uid, report_code))
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
                "UPDATE reports SET archive_status = ?, last_error = '', updated_at = strftime('%s','now') WHERE report_code = ?",
                (status, report_code),
            )

    def mark_report_failed(self, report_code: str, reason: str = "") -> None:
        with self._connect() as conn:
            conn.execute("INSERT OR IGNORE INTO reports (report_code) VALUES (?)", (report_code,))
            conn.execute("""
                UPDATE reports SET failure_count = failure_count + 1, last_error = ?,
                    archive_status = CASE WHEN failure_count + 1 >= ? THEN 'FAILED' ELSE 'RETRY' END,
                    updated_at = strftime('%s','now') WHERE report_code = ?
            """, (reason, self.MAX_WORK_ATTEMPTS, report_code))
            conn.execute("""
                UPDATE fights SET status = 'FAILED', last_error = ?
                WHERE report_code = ? AND status IN ('QUEUED', 'RETRY') AND EXISTS
                    (SELECT 1 FROM reports WHERE report_code = ? AND archive_status = 'FAILED')
            """, (f"Report unavailable: {reason}", report_code, report_code))

    def mark_fight_packed(
        self,
        report_code: str,
        fight_id: int,
        file_rel_path: str,
        compressed_bytes: int,
        event_count: int,
        duration_ms: Optional[int] = None,
    ) -> None:
        fight_uid = self.make_fight_uid(report_code, fight_id)
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE fights
                SET status = 'PACKED',
                    file_rel_path = ?,
                    compressed_bytes = ?,
                    event_count = ?,
                    duration_ms = COALESCE(?, duration_ms),
                    last_error = ''
                WHERE fight_uid = ?
                """,
                (file_rel_path, compressed_bytes, event_count, duration_ms, fight_uid),
            )

    def get_fight(self, report_code: str, fight_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM fights WHERE fight_uid = ?",
                               (self.make_fight_uid(report_code, fight_id),)).fetchone()
            return dict(row) if row else None

    def mark_fight_staged(self, report_code: str, fight_id: int, staging_rel_path: str) -> None:
        with self._connect() as conn:
            conn.execute("""UPDATE fights SET status = 'STAGED', staging_rel_path = ?, needs_browser = 0, last_error = ''
                WHERE fight_uid = ? AND status != 'PACKED'""",
                (staging_rel_path, self.make_fight_uid(report_code, fight_id)))

    def mark_fight_api_staged(self, report_code: str, fight_id: int, staging_rel_path: str) -> None:
        with self._connect() as conn:
            conn.execute("""UPDATE fights SET status = 'API_STAGED', staging_rel_path = ?, needs_browser = 1, last_error = ''
                WHERE fight_uid = ? AND status != 'PACKED'""",
                (staging_rel_path, self.make_fight_uid(report_code, fight_id)))

    def get_api_staged_fights(self, limit: int = 100, report_code: Optional[str] = None) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        scope = " AND report_code = ?" if report_code is not None else ""
        params = [self.MAX_WORK_ATTEMPTS]
        if report_code is not None:
            params.append(report_code)
        params.append(limit)
        with self._connect() as conn:
            return [dict(row) for row in conn.execute("""SELECT * FROM fights
                WHERE status = 'API_STAGED' AND failure_count < ?""" + scope + """
                ORDER BY CASE WHEN content_type = 'mplus' THEN 0 ELSE 1 END, rowid ASC LIMIT ?""", params)]

    def count_api_staged_fights(self, report_code: Optional[str] = None) -> int:
        with self._connect() as conn:
            scope = " AND report_code = ?" if report_code is not None else ""
            return int(conn.execute("SELECT COUNT(*) FROM fights WHERE status = 'API_STAGED'" + scope,
                                    (report_code,) if report_code is not None else ()).fetchone()[0])

    def clear_fight_staging(self, report_code: str, fight_id: int, staging_rel_path: str) -> None:
        with self._connect() as conn:
            conn.execute("""UPDATE fights SET staging_rel_path = NULL WHERE fight_uid = ?
                AND staging_rel_path = ? AND status = 'PACKED'""",
                (self.make_fight_uid(report_code, fight_id), staging_rel_path))

    def get_staged_fights(self, limit: int = 100, report_code: Optional[str] = None) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        scope = " AND report_code = ?" if report_code is not None else ""
        params = [self.MAX_WORK_ATTEMPTS]
        if report_code is not None:
            params.append(report_code)
        params.append(limit)
        with self._connect() as conn:
            return [dict(row) for row in conn.execute("""SELECT * FROM fights
                WHERE status = 'STAGED' AND failure_count < ?""" + scope + """
                ORDER BY CASE WHEN content_type = 'mplus' THEN 0 ELSE 1 END, rowid ASC LIMIT ?""", params)]

    def mark_fight_failed(self, report_code: str, fight_id: int, reason: str = "") -> None:
        fight_uid = self.make_fight_uid(report_code, fight_id)
        with self._connect() as conn:
            conn.execute(
                """UPDATE fights SET failure_count = failure_count + 1, last_error = ?,
                    status = CASE WHEN staging_rel_path IS NOT NULL OR failure_count + 1 >= ?
                                  THEN 'FAILED' ELSE 'RETRY' END
                    WHERE fight_uid = ?""",
                (reason, self.MAX_WORK_ATTEMPTS, fight_uid),
            )

    def get_queue_counts(self) -> Dict[str, int]:
        with self._connect() as conn:
            counts = {}
            for key, table, column, status in (
                ("pending_reports", "reports", "archive_status", "PENDING"),
                ("queued_fights", "fights", "status", "QUEUED"),
                ("retryable_reports", "reports", "archive_status", "RETRY"),
                ("retryable_fights", "fights", "status", "RETRY"),
                ("failed_reports", "reports", "archive_status", "FAILED"),
                ("failed_fights", "fights", "status", "FAILED"),
                ("staged_fights", "fights", "status", "STAGED"),
                ("api_staged_fights", "fights", "status", "API_STAGED"),
            ):
                counts[key] = int(conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} = ?", (status,)
                ).fetchone()[0])
            counts["failed_scan_pages"] = int(conn.execute("SELECT COUNT(*) FROM scan_failures").fetchone()[0])
            return counts

    def retry_failed_work(self, report_code: Optional[str] = None) -> None:
        """Open a fresh retry budget only when the user starts another harvest."""
        with self._connect() as conn:
            scope = " AND report_code = ?" if report_code is not None else ""
            params = (report_code,) if report_code is not None else ()
            conn.execute("""UPDATE reports SET archive_status = 'PENDING', failure_count = 0
                WHERE archive_status IN ('RETRY', 'FAILED')""" + scope, params)
            conn.execute("""UPDATE fights SET status = CASE WHEN staging_rel_path IS NOT NULL AND needs_browser = 1
                THEN 'API_STAGED' WHEN staging_rel_path IS NOT NULL
                THEN 'STAGED' ELSE 'QUEUED' END, failure_count = 0
                WHERE status IN ('RETRY', 'FAILED')""" + scope, params)

    def _has_pending_mplus(self, conn: sqlite3.Connection) -> bool:
        # The general content index also contains completed historical keys.
        # Frequent scheduler probes must read only the small pending-key index.
        return conn.execute(
            """SELECT 1 FROM fights f INDEXED BY idx_fights_pending_mplus
               JOIN reports r ON r.report_code = f.report_code
               WHERE f.content_type = 'mplus' AND f.status IN ('QUEUED', 'RETRY')
                 AND f.failure_count < ? AND
                 (r.archive_status = 'UNARCHIVED' OR
                  (r.archive_status IN ('PENDING', 'RETRY') AND r.failure_count < ?))
               LIMIT 1""", (self.MAX_WORK_ATTEMPTS, self.MAX_WORK_ATTEMPTS),
        ).fetchone() is not None

    def has_pending_mplus(self) -> bool:
        with self._connect() as conn:
            return self._has_pending_mplus(conn)

    def get_pending_reports_for_unarchive(self, limit: int = 30) -> List[str]:
        if limit <= 0:
            return []
        with self._connect() as conn:
            mplus_first = self._has_pending_mplus(conn)
            scope = " AND f.content_type = 'mplus'" if mplus_first else ""
            # rowid is the existing insertion sequence: enqueue uses INSERT OR
            # IGNORE and status updates never move a fight. Deduplicate reports
            # by their oldest remaining fight, not by the report's age or code.
            rows = conn.execute(
                """SELECT f.report_code FROM fights f
                   JOIN reports r ON r.report_code = f.report_code
                   WHERE f.status IN ('QUEUED', 'RETRY') AND f.failure_count < ?
                     AND r.archive_status IN ('PENDING', 'RETRY') AND r.failure_count < ?"""
                + scope + " ORDER BY f.rowid ASC",
                (self.MAX_WORK_ATTEMPTS, self.MAX_WORK_ATTEMPTS),
            )
            codes, seen = [], set()
            for row in rows:
                code = row["report_code"]
                if code not in seen:
                    codes.append(code)
                    seen.add(code)
                    if len(codes) == limit:
                        return codes
            if mplus_first:
                return codes
            # Failed imports can leave report-only work. Retain its bounded
            # retry path so the global drain can eventually finish.
            rows = conn.execute(
                """SELECT r.report_code FROM reports r
                   WHERE r.archive_status IN ('PENDING', 'RETRY') AND r.failure_count < ?
                     AND NOT EXISTS (SELECT 1 FROM fights f WHERE f.report_code = r.report_code
                         AND f.status IN ('QUEUED', 'RETRY') AND f.failure_count < ?)
                   ORDER BY r.rowid ASC LIMIT ?""",
                (self.MAX_WORK_ATTEMPTS, self.MAX_WORK_ATTEMPTS, limit - len(codes)),
            ).fetchall()
            return codes + [row["report_code"] for row in rows]

    def get_unarchived_pending_fights(self, limit: int = 20, report_code: Optional[str] = None) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        with self._connect() as conn:
            scope = " AND f.report_code = ?" if report_code is not None else ""
            if report_code is None and self._has_pending_mplus(conn):
                scope = " AND f.content_type = 'mplus'"
            params = [self.MAX_WORK_ATTEMPTS]
            if report_code is not None:
                params.append(report_code)
            else:
                # A younger prepared report must not overtake an older fight
                # whose report is still pending or within its retry budget.
                blocker = conn.execute(
                    """SELECT f.rowid FROM fights f
                       JOIN reports r ON r.report_code = f.report_code
                       WHERE f.status IN ('QUEUED', 'RETRY') AND f.failure_count < ?
                         AND r.archive_status IN ('PENDING', 'RETRY') AND r.failure_count < ?"""
                    + scope + " ORDER BY f.rowid ASC LIMIT 1",
                    (self.MAX_WORK_ATTEMPTS, self.MAX_WORK_ATTEMPTS),
                ).fetchone()
                if blocker is not None:
                    scope += " AND f.rowid < ?"
                    params.append(blocker[0])
            params.append(limit)
            rows = conn.execute(
                f"""
                SELECT f.*
                FROM fights f
                JOIN reports r ON r.report_code = f.report_code
                WHERE r.archive_status = 'UNARCHIVED' AND f.status IN ('QUEUED', 'RETRY')
                    AND f.failure_count < ? {scope}
                ORDER BY f.rowid ASC
                LIMIT ?
                """,
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def is_page_scanned(
        self,
        zone_id: int,
        encounter_id: int,
        partition: int,
        class_name: str,
        spec_name: str,
        page: int,
        difficulty: int = 0,
    ) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM scan_history
                WHERE zone_id = ? AND encounter_id = ? AND partition = ?
                  AND difficulty = ? AND class_name = ? AND spec_name = ? AND page = ?
                """,
                (zone_id, encounter_id, partition, difficulty, class_name, spec_name, page),
            ).fetchone()
            return row is not None

    def get_exhausted_page(
        self,
        zone_id: int,
        encounter_id: int,
        partition: int,
        class_name: str,
        spec_name: str,
        difficulty: int = 0,
    ) -> Optional[int]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MIN(page) AS min_exhausted_page FROM scan_history
                WHERE zone_id = ? AND encounter_id = ? AND partition = ?
                  AND difficulty = ? AND class_name = ? AND spec_name = ? AND is_exhausted = 1
                """,
                (zone_id, encounter_id, partition, difficulty, class_name, spec_name),
            ).fetchone()
            if row and row["min_exhausted_page"] is not None:
                return int(row["min_exhausted_page"])
            return None

    def mark_page_failed(
        self,
        zone_id: int,
        encounter_id: int,
        partition: int,
        class_name: str,
        spec_name: str,
        page: int,
        difficulty: int = 0,
        reason: str = "",
        attempts: int = 3,
    ) -> None:
        """Record an exhausted page without certifying it as scanned."""
        with self._connect() as conn:
            conn.execute("""
                INSERT INTO scan_failures (
                    zone_id, encounter_id, partition, difficulty, class_name, spec_name,
                    page, failure_count, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(zone_id, encounter_id, partition, difficulty, class_name, spec_name, page)
                DO UPDATE SET failure_count = scan_failures.failure_count + excluded.failure_count,
                    last_error = excluded.last_error, updated_at = strftime('%s','now')
            """, (zone_id, encounter_id, partition, difficulty, class_name, spec_name,
                  page, max(1, int(attempts)), reason))

    def mark_page_scanned(
        self,
        zone_id: int,
        encounter_id: int,
        partition: int,
        class_name: str,
        spec_name: str,
        page: int,
        refs_found: int,
        is_exhausted: bool = False,
        difficulty: int = 0,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO scan_history (
                    zone_id, encounter_id, partition, difficulty, class_name, spec_name,
                    page, refs_found, is_exhausted, scanned_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s','now'))
                """,
                (
                    zone_id,
                    encounter_id,
                    partition,
                    difficulty,
                    class_name,
                    spec_name,
                    page,
                    int(refs_found),
                    1 if is_exhausted else 0,
                ),
            )
            page_comparison = ">=" if is_exhausted else "="
            conn.execute(f"""
                DELETE FROM scan_failures WHERE zone_id = ? AND encounter_id = ? AND partition = ?
                    AND difficulty = ? AND class_name = ? AND spec_name = ? AND page {page_comparison} ?
            """, (zone_id, encounter_id, partition, difficulty, class_name, spec_name, page))

    def get_scanned_pages_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM scan_history").fetchone()
            return int(row["c"]) if row else 0

    def clear_scan_history(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM scan_history")

    def search_fights(
        self,
        content_type: Optional[str] = None,
        encounter_id: Optional[int] = None,
        class_name: Optional[str] = None,
        spec: Optional[str] = None,
        min_key_level: Optional[int] = None,
        difficulty: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        report_code: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses = ["1=1"]
        params: List[Any] = []
        if report_code is not None:
            clauses.append("f.report_code = ?")
            params.append(report_code)
        if content_type:
            clauses.append("f.content_type = ?")
            params.append(content_type)
        if encounter_id is not None:
            clauses.append("f.encounter_id = ?")
            params.append(encounter_id)
        if min_key_level is not None:
            clauses.append("f.keystone_level >= ?")
            params.append(min_key_level)
        if difficulty is not None:
            clauses.append("f.difficulty = ?")
            params.append(difficulty)
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

        params.extend((limit, max(0, offset)))
        sql = f"""
            SELECT f.*
            FROM fights f
            WHERE {' AND '.join(clauses)}
            ORDER BY f.keystone_level DESC, f.duration_ms ASC, f.fight_uid ASC
            LIMIT ? OFFSET ?
        """
        with self._connect() as conn:
            fight_rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
            for f in fight_rows:
                f["affixes"] = json.loads(f.pop("affixes_json", "[]") or "[]")
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

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

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

    def upsert_spells_batch(self, spells: List[Tuple[int, str, int, str]]) -> None:
        if not spells:
            return
        with self._connect() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO spells (spell_id, name, school, icon) VALUES (?, ?, ?, ?)",
                [(int(sid), name, int(school), icon) for sid, name, school, icon in spells],
            )

    def get_spell(self, spell_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM spells WHERE spell_id = ?", (int(spell_id),)).fetchone()
            return dict(row) if row else None

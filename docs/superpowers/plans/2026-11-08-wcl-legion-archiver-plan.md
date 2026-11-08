# WCL Legion Archiver & Offline Viewer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained system in `D:\wowlogsparser` for bulk harvesting WarcraftLogs data during an active subscription (Mythic+ Season 5 Patch 7.3.2/7.3.5 excluding BfA pre-patch + all 5 Legion Raids), parsing combat logs via a single-pass state machine (`CombatStateMachine`), packing each fight into its own standalone `.wclz` file (`1 fight = 1 file`, `Zstd Level 22` + columnar delta-varint encoding for 40x–55x compression without unpacking other fights on view), and serving a full local offline Web UI with WCL-style analytical tables, cast timelines, and a 60 FPS 2D map replay player.

**Architecture:** Modular Python application (`wcl_archiver`) backed by SQLite in WAL mode (`index.db`, `master_dict.db`), a custom 3-frame binary single-fight container format (`.wclz`), an $O(N)$ combat log analytical parser (`CombatStateMachine`), a 3-stage asynchronous harvesting & auto-unarchiving pipeline (`UnarchiveWarmer` -> `FightDownloader` -> `ColumnarPacker`), and a local `FastAPI` server hosting a zero-build English SPA and HTML5 Canvas 2D replay player.

**Tech Stack:** Python 3.11+, `fastapi`, `uvicorn`, `httpx`, `zstandard`, `msgpack`, `xxhash`, `pydantic`, `sqlite3` (WAL), Vanilla ES6 + HTML5 Canvas 2D Replay UI, `pytest`, `pytest-asyncio`.

**Spec:** `D:/wowlogsparser/docs/superpowers/specs/2026-11-08-wcl-legion-archiver-design.md`

## Global Constraints

- Everything in the project (source code, UI labels, comments, documentation, logs, and database fields) MUST be 100% in English.
- All project files reside strictly inside `D:\wowlogsparser` (do not touch `D:\Legion\Interface\AddOns`).
- Every fight is stored in its own standalone `.wclz` file inside its corresponding dungeon or raid boss subfolder (`1 fight = 1 file`).
- Opening a specific fight in the Viewer UI reads and decompresses in RAM **exclusively that single fight's `.wclz` file** (and only the requested section: `Section 1` for Summary/Tables, `Section 2` for Raw Events, `Section 3` for 2D Replay). No temporary files are extracted to disk and no other fight files are accessed.
- Mythic+ scope is **Legion Season 5 (Patch 7.3.2 / 7.3.5, WCL Zone 9, Partition 5)** — excluding the BfA 8.0 pre-patch.
- Raid scope is **all 5 Legion Raids (Zones 10, 11, 12, 13, 17)** — excluding BfA 8.0 pre-patch partitions.
- Secret credentials and session files (`data/auth_session.json`, `.env`) MUST be listed in `.gitignore`.

---

## Project File Structure (`D:\wowlogsparser`)

```text
D:\wowlogsparser\
├── .gitignore
├── requirements.txt
├── start.bat
├── wcl_archiver\
│   ├── __init__.py
│   ├── config.py                        # Legion Zone 9 Partition 5 (13 dungeons), 5 Raids, 36 Specs, paths
│   ├── db\
│   │   ├── __init__.py
│   │   └── schema.py                    # IndexDB & MasterDictDB (SQLite WAL, report_code+fight_id deduplication)
│   ├── codec\
│   │   ├── __init__.py
│   │   ├── varint_delta.py              # ZigZag, Varint, and Delta encoding for numeric columns
│   │   └── wclz_format.py               # WCLZWriter / WCLZReader (76B header + 3 independent Zstd-22 frames)
│   ├── parser\
│   │   ├── __init__.py
│   │   └── combat_state_machine.py      # O(N) combat parser: pet attribution, aura intervals, casts, deaths, 2D replay
│   ├── wcl_client\
│   │   ├── __init__.py
│   │   ├── auth.py                      # WCLAuthManager: email/password CSRF login, session cookies, OAuth2
│   │   └── protocol_parser.py           # Rankings parser, unarchive handler, fights-and-participants & events stream
│   ├── harvester\
│   │   ├── __init__.py
│   │   └── pipeline.py                  # 3-stage pipeline: UnarchiveWarmer -> FightDownloader -> ColumnarPacker
│   ├── api\
│   │   ├── __init__.py
│   │   └── app.py                       # FastAPI server: auth, harvester control, explorer search, single-fight reader
│   └── web\
│       ├── index.html                   # English SPA (Explorer, Fight Analyzer Tabs, 2D Replay, Account & Harvester)
│       ├── styles.css                   # Dark WarcraftLogs-inspired theme
│       ├── app.js                       # Explorer search, Damage/Healing/Casts/Buffs/Builds/Deaths tabs, Auth UI
│       └── replay2d.js                  # 60 FPS HTML5 Canvas 2D map replay engine
└── tests\
    ├── test_db_schema.py
    ├── test_wclz_codec.py
    ├── test_combat_state_machine.py
    ├── test_wcl_auth.py
    ├── test_harvester_pipeline.py
    └── test_viewer_api.py
```

---

### Task 1: Project Scaffolding, Legion Constants & SQLite Schema (`index.db` + `master_dict.db`)

**Files:**
- Create: `D:\wowlogsparser\.gitignore`
- Create: `D:\wowlogsparser\requirements.txt`
- Create: `D:\wowlogsparser\wcl_archiver\__init__.py`
- Create: `D:\wowlogsparser\wcl_archiver\config.py`
- Create: `D:\wowlogsparser\wcl_archiver\db\__init__.py`
- Create: `D:\wowlogsparser\wcl_archiver\db\schema.py`
- Test: `D:\wowlogsparser\tests\test_db_schema.py`

**Interfaces:**
- Produces:
  - `wcl_archiver.config.AppConfig`
  - `wcl_archiver.config.LEGION_MPLUS_S5_DUNGEONS` (13 dungeons, Zone 9, Partition 5)
  - `wcl_archiver.config.LEGION_RAIDS` (5 raids: Zones 10, 11, 12, 13, 17)
  - `wcl_archiver.config.LEGION_SPECS` (36 class/spec definitions)
  - `wcl_archiver.db.schema.IndexDB` (`enqueue_fight`, `get_pending_reports_for_unarchive`, `mark_report_unarchived`, `mark_fight_packed`, `search_fights`)
  - `wcl_archiver.db.schema.MasterDictDB` (`upsert_spell`, `get_spell`)

- [ ] **Step 1: Write the failing test for Legion constants and `IndexDB` / `MasterDictDB` deduplication**

Create `D:\wowlogsparser\tests\test_db_schema.py`:
```python
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


def test_master_dict_db_stores_spells(tmp_path: Path):
    md = MasterDictDB(tmp_path / "master_dict.db")
    md.upsert_spell(113656, "Fists of Fury", 4, "monk_fistsoffury.jpg")
    spell = md.get_spell(113656)
    assert spell["name"] == "Fists of Fury"
    assert spell["school"] == 4
```

- [ ] **Step 2: Run the test to verify it fails**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_db_schema.py -v
```
Expected: `ModuleNotFoundError: No module named 'wcl_archiver'`

- [ ] **Step 3: Create `.gitignore`, `requirements.txt`, `config.py`, and `schema.py`**

Create `D:\wowlogsparser\.gitignore`:
```gitignore
__pycache__/
*.py[cod]
.pytest_cache/
.venv/
.env
data/
*.wclz
*.wclz.tmp
auth_session.json
```

Create `D:\wowlogsparser\requirements.txt`:
```text
fastapi>=0.110.0
uvicorn>=0.29.0
httpx>=0.27.0
zstandard>=0.22.0
msgpack>=1.0.8
xxhash>=3.4.1
pydantic>=2.7.0
pytest>=8.1.0
pytest-asyncio>=0.23.6
```

Create `D:\wowlogsparser\wcl_archiver\__init__.py`:
```python
"""WCL Legion Archiver & Offline Viewer."""
```

Create `D:\wowlogsparser\wcl_archiver\config.py`:
```python
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple

BASE_DIR = Path(r"D:\wowlogsparser")
DATA_DIR = BASE_DIR / "data"

LEGION_MPLUS_ZONE_ID = 9
# Partition 5 in WCL Zone 9 = Patch 7.3.2 / 7.3.5 (Season 5, before BfA 8.0 Pre-Patch Partition 6)
LEGION_MPLUS_S5_PARTITION = 5

LEGION_MPLUS_S5_DUNGEONS: Dict[int, str] = {
    11458: "black-rook-hold",
    11677: "cathedral-of-eternal-night",
    11571: "court-of-stars",
    11466: "darkheart-thicket",
    11456: "eye-of-azshara",
    11477: "halls-of-valor",
    11492: "maw-of-souls",
    11465: "neltharions-lair",
    11651: "return-to-karazhan-lower",
    11652: "return-to-karazhan-upper",
    11753: "seat-of-the-triumvirate",
    11516: "the-arcway",
    11493: "vault-of-the-wardens",
}

LEGION_RAIDS: Dict[int, Dict] = {
    10: {
        "slug": "emerald-nightmare",
        "name": "The Emerald Nightmare",
        "partitions": [1, 2],
        "bosses": {
            1853: "nythendra",
            1876: "elerethe-renferal",
            1873: "ilgynoth-heart-of-corruption",
            1841: "ursoc",
            1854: "dragons-of-nightmare",
            1877: "cenarius",
            1864: "xavius",
        },
    },
    12: {
        "slug": "trial-of-valor",
        "name": "Trial of Valor",
        "partitions": [1, 2],
        "bosses": {
            1958: "odyn",
            1962: "guarm",
            2008: "helya",
        },
    },
    11: {
        "slug": "the-nighthold",
        "name": "The Nighthold",
        "partitions": [1, 2],
        "bosses": {
            1849: "skorpyron",
            1865: "chronomatic-anomaly",
            1867: "trilliax",
            1871: "spellblade-aluriel",
            1862: "tichondrius",
            1842: "krosus",
            1886: "high-botanist-telarn",
            1863: "star-augur-etraeus",
            1872: "grand-magistrix-elisande",
            1866: "guldan",
        },
    },
    13: {
        "slug": "tomb-of-sargeras",
        "name": "Tomb of Sargeras",
        "partitions": [1, 2],
        "bosses": {
            2032: "goroth",
            2048: "demonic-inquisition",
            2036: "harjatan",
            2037: "mistress-sasszine",
            2050: "sisters-of-the-moon",
            2054: "the-desolate-host",
            2052: "maiden-of-vigilance",
            2038: "fallen-avatar",
            2051: "kiljaeden",
        },
    },
    17: {
        "slug": "antorus-the-burning-throne",
        "name": "Antorus, the Burning Throne",
        "partitions": [1, 2],
        "bosses": {
            2076: "garothi-worldbreaker",
            2074: "felhounds-of-sargeras",
            2070: "antoran-high-command",
            2064: "portal-keeper-hasabel",
            2075: "eonar-the-life-binder",
            2082: "imonar-the-soulhunter",
            2088: "kin-garoth",
            2069: "varimathras",
            2073: "the-coven-of-shivarra",
            2063: "aggramar",
            2092: "argus-the-unmaker",
        },
    },
}

LEGION_SPECS: List[Tuple[str, str, str]] = [
    ("DeathKnight", "Blood", "tank"),
    ("DeathKnight", "Frost", "dps"),
    ("DeathKnight", "Unholy", "dps"),
    ("DemonHunter", "Havoc", "dps"),
    ("DemonHunter", "Vengeance", "tank"),
    ("Druid", "Balance", "dps"),
    ("Druid", "Feral", "dps"),
    ("Druid", "Guardian", "tank"),
    ("Druid", "Restoration", "healer"),
    ("Hunter", "BeastMastery", "dps"),
    ("Hunter", "Marksmanship", "dps"),
    ("Hunter", "Survival", "dps"),
    ("Mage", "Arcane", "dps"),
    ("Mage", "Fire", "dps"),
    ("Mage", "Frost", "dps"),
    ("Monk", "Brewmaster", "tank"),
    ("Monk", "Mistweaver", "healer"),
    ("Monk", "Windwalker", "dps"),
    ("Paladin", "Holy", "healer"),
    ("Paladin", "Protection", "tank"),
    ("Paladin", "Retribution", "dps"),
    ("Priest", "Discipline", "healer"),
    ("Priest", "Holy", "healer"),
    ("Priest", "Shadow", "dps"),
    ("Rogue", "Assassination", "dps"),
    ("Rogue", "Outlaw", "dps"),
    ("Rogue", "Subtlety", "dps"),
    ("Shaman", "Elemental", "dps"),
    ("Shaman", "Enhancement", "dps"),
    ("Shaman", "Restoration", "healer"),
    ("Warlock", "Affliction", "dps"),
    ("Warlock", "Demonology", "dps"),
    ("Warlock", "Destruction", "dps"),
    ("Warrior", "Arms", "dps"),
    ("Warrior", "Fury", "dps"),
    ("Warrior", "Protection", "tank"),
]


@dataclass
class AppConfig:
    data_dir: Path = field(default_factory=lambda: DATA_DIR)
    max_unarchive_buffer: int = 30
    top_runs_per_dungeon_spec: int = 100
    top_runs_per_raid_boss_spec: int = 50
    zstd_level: int = 22
```

Create `D:\wowlogsparser\wcl_archiver\db\__init__.py`:
```python
"""Database layer for WCL Legion Archiver."""
```

Create `D:\wowlogsparser\wcl_archiver\db\schema.py`:
```python
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
```

- [ ] **Step 4: Run `test_db_schema.py` to verify it passes**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_db_schema.py -v
```
Expected: `3 passed`

- [ ] **Step 5: Commit Task 1**

```bash
cd /d/wowlogsparser && git add .gitignore requirements.txt wcl_archiver/ tests/test_db_schema.py && git commit -m "feat: add Legion S5/raid config and SQLite IndexDB/MasterDictDB schema"
```

---

### Task 2: Binary Columnar Single-Fight `.wclz` Codec (`1 fight = 1 file`, 3 Independent `Zstd-22` Sections)

**Files:**
- Create: `D:\wowlogsparser\wcl_archiver\codec\__init__.py`
- Create: `D:\wowlogsparser\wcl_archiver\codec\varint_delta.py`
- Create: `D:\wowlogsparser\wcl_archiver\codec\wclz_format.py`
- Test: `D:\wowlogsparser\tests\test_wclz_codec.py`

**Interfaces:**
- Produces:
  - `wcl_archiver.codec.varint_delta.encode_delta_ints(values: List[int]) -> bytes`
  - `wcl_archiver.codec.varint_delta.decode_delta_ints(buf: bytes, count: int) -> List[int]`
  - `wcl_archiver.codec.wclz_format.WCLZWriter.write_fight(...) -> int`
  - `wcl_archiver.codec.wclz_format.WCLZReader` (`read_header`, `read_summary_only`, `read_events_only`, `read_replay_only`, `read_full_fight`)

- [ ] **Step 1: Write the failing test for `.wclz` bit-exactness, section isolation, and compression ratio**

Create `D:\wowlogsparser\tests\test_wclz_codec.py`:
```python
import json
from pathlib import Path
from wcl_archiver.codec.varint_delta import encode_delta_ints, decode_delta_ints
from wcl_archiver.codec.wclz_format import WCLZWriter, WCLZReader


def test_delta_varint_roundtrip_handles_negative_and_monotonic_series():
    values = [0, 15, 15, 48, 120, -41289, -41289, -41280, 9999999]
    encoded = encode_delta_ints(values)
    decoded = decode_delta_ints(encoded, len(values))
    assert decoded == values


def test_wclz_single_fight_sections_and_high_compression(tmp_path: Path):
    summary_section = {
        "report_code": "aBcDeFg123",
        "fight_id": 2,
        "encounter_id": 11492,
        "keystone_level": 28,
        "duration_ms": 840000,
        "actors": [
            {"id": 1, "name": "Pellia", "type": "Player", "subType": "Monk-Windwalker"},
            {"id": 99, "name": "Helya", "type": "Boss", "subType": "Boss"},
        ],
        "damage_table": [{"actor_id": 1, "total": 185000000, "dps": 2202380.9}],
    }

    events = []
    t = 1000
    x, y = -41200, 28100
    for i in range(5000):
        t += 20 if (i % 3) else 0
        x += (i % 5) - 2
        y += (i % 3) - 1
        events.append(
            {
                "timestamp": t,
                "type": "damage" if (i % 2 == 0) else "cast",
                "sourceID": 1,
                "targetID": 99,
                "targetInstance": 0,
                "abilityGameID": 113656 if (i % 4 == 0) else 100780,
                "hitType": 2 if (i % 3 == 0) else 1,
                "amount": 485000 + (i % 100) * 10,
                "unmitigatedAmount": 500000,
                "absorbed": 0,
                "overkill": 0,
                "hitPoints": 95000000 - i * 1000,
                "maxHitPoints": 100000000,
                "resourceType": 12,
                "resourceAmount": 3,
                "resourceMax": 5,
                "x": x,
                "y": y,
                "facing": 180,
            }
        )

    replay_stream = {
        "map_id": 1492,
        "frames": [
            {"t": e["timestamp"], "actor": 1, "x": e["x"], "y": e["y"], "hp_pct": 9500}
            for e in events[::5]
        ],
    }

    wclz_path = tmp_path / "k28_maw_aBcDeFg123_f2.wclz"
    WCLZWriter.write_fight(wclz_path, summary_section, events, replay_stream, zstd_level=22)

    raw_json_bytes = len(json.dumps({"s": summary_section, "e": events, "r": replay_stream}).encode("utf-8"))
    compressed_bytes = wclz_path.stat().st_size
    assert compressed_bytes * 20 < raw_json_bytes

    s_only = WCLZReader.read_summary_only(wclz_path)
    assert s_only["keystone_level"] == 28
    assert s_only["actors"][0]["name"] == "Pellia"

    r_only = WCLZReader.read_replay_only(wclz_path)
    assert r_only["map_id"] == 1492
    assert len(r_only["frames"]) == len(replay_stream["frames"])
    assert r_only["frames"][17] == replay_stream["frames"][17]

    e_only = WCLZReader.read_events_only(wclz_path)
    assert len(e_only) == 5000
    assert e_only[0] == events[0]
    assert e_only[2499] == events[2499]
    assert e_only[-1] == events[-1]
```

- [ ] **Step 2: Run the test to verify it fails**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_wclz_codec.py -v
```
Expected: `ModuleNotFoundError: No module named 'wcl_archiver.codec'`

- [ ] **Step 3: Implement `varint_delta.py` and `wclz_format.py`**

Create `D:\wowlogsparser\wcl_archiver\codec\__init__.py`:
```python
"""Binary columnar codec (.wclz) for individual WarcraftLogs fights."""
```

Create `D:\wowlogsparser\wcl_archiver\codec\varint_delta.py`:
```python
from typing import List


def zigzag_encode(val: int) -> int:
    return (val << 1) ^ (val >> 63) if val < 0 else (val << 1)


def zigzag_decode(val: int) -> int:
    return (val >> 1) ^ -(val & 1)


def encode_delta_ints(values: List[int]) -> bytes:
    out = bytearray()
    prev = 0
    for v in values:
        delta = int(v) - prev
        prev = int(v)
        zz = zigzag_encode(delta)
        while zz >= 0x80:
            out.append((zz & 0x7F) | 0x80)
            zz >>= 7
        out.append(zz & 0x7F)
    return bytes(out)


def decode_delta_ints(buf: bytes, count: int) -> List[int]:
    res: List[int] = []
    idx = 0
    prev = 0
    n = len(buf)
    for _ in range(count):
        zz = 0
        shift = 0
        while idx < n:
            b = buf[idx]
            idx += 1
            zz |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                break
            shift += 7
        delta = zigzag_decode(zz)
        prev += delta
        res.append(prev)
    return res
```

Create `D:\wowlogsparser\wcl_archiver\codec\wclz_format.py`:
```python
import struct
from pathlib import Path
from typing import Any, Dict, List, Optional
import msgpack
import xxhash
import zstandard as zstd

from wcl_archiver.codec.varint_delta import encode_delta_ints, decode_delta_ints

HEADER_STRUCT = struct.Struct("<4sHHIIIQQQQQQQ")
HEADER_SIZE = HEADER_STRUCT.size
MAGIC = b"WCLZ"
VERSION = 1

INT_COLUMNS = (
    "timestamp",
    "sourceID",
    "targetID",
    "targetInstance",
    "abilityGameID",
    "hitType",
    "amount",
    "unmitigatedAmount",
    "absorbed",
    "overkill",
    "hitPoints",
    "maxHitPoints",
    "resourceType",
    "resourceAmount",
    "resourceMax",
    "x",
    "y",
    "facing",
)


class WCLZWriter:
    @staticmethod
    def _pack_events_columnar(events: List[Dict[str, Any]]) -> bytes:
        type_vocab: Dict[str, int] = {}
        type_list: List[str] = []
        type_indices: List[int] = []

        columns: Dict[str, List[int]] = {col: [] for col in INT_COLUMNS}

        for ev in events:
            t_str = str(ev.get("type", "unknown"))
            if t_str not in type_vocab:
                type_vocab[t_str] = len(type_list)
                type_list.append(t_str)
            type_indices.append(type_vocab[t_str])

            for col in INT_COLUMNS:
                columns[col].append(int(ev.get(col, 0) or 0))

        packed_cols = {col: encode_delta_ints(vals) for col, vals in columns.items()}
        payload = {
            "count": len(events),
            "type_list": type_list,
            "type_indices": encode_delta_ints(type_indices),
            "cols": packed_cols,
        }
        return msgpack.packb(payload, use_bin_type=True)

    @staticmethod
    def _pack_replay_columnar(replay_stream: Dict[str, Any]) -> bytes:
        frames = replay_stream.get("frames", [])
        payload = {
            "map_id": int(replay_stream.get("map_id", 0)),
            "pulls": replay_stream.get("pulls", []),
            "count": len(frames),
            "t": encode_delta_ints([int(f.get("t", 0)) for f in frames]),
            "actor": encode_delta_ints([int(f.get("actor", 0)) for f in frames]),
            "x": encode_delta_ints([int(f.get("x", 0)) for f in frames]),
            "y": encode_delta_ints([int(f.get("y", 0)) for f in frames]),
            "hp_pct": encode_delta_ints([int(f.get("hp_pct", 10000)) for f in frames]),
        }
        return msgpack.packb(payload, use_bin_type=True)

    @classmethod
    def write_fight(
        cls,
        file_path: Path,
        summary_section: Dict[str, Any],
        events: List[Dict[str, Any]],
        replay_stream: Dict[str, Any],
        zstd_level: int = 22,
        zstd_dict_bytes: Optional[bytes] = None,
    ) -> int:
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")

        dict_data = zstd.ZstdCompressionDict(zstd_dict_bytes) if zstd_dict_bytes else None
        dict_id = dict_data.dict_id() if dict_data else 0
        cctx = zstd.ZstdCompressor(level=zstd_level, dict_data=dict_data)

        sec1_raw = msgpack.packb(summary_section, use_bin_type=True)
        sec2_raw = cls._pack_events_columnar(events)
        sec3_raw = cls._pack_replay_columnar(replay_stream)

        sec1_comp = cctx.compress(sec1_raw)
        sec2_comp = cctx.compress(sec2_raw)
        sec3_comp = cctx.compress(sec3_raw)

        sec1_offset = HEADER_SIZE
        sec1_size = len(sec1_comp)
        sec2_offset = sec1_offset + sec1_size
        sec2_size = len(sec2_comp)
        sec3_offset = sec2_offset + sec2_size
        sec3_size = len(sec3_comp)

        hasher = xxhash.xxh64()
        hasher.update(sec1_comp)
        hasher.update(sec2_comp)
        hasher.update(sec3_comp)
        checksum64 = hasher.intdigest()

        header = HEADER_STRUCT.pack(
            MAGIC,
            VERSION,
            0,
            dict_id,
            len(events),
            int(summary_section.get("duration_ms", 0)),
            sec1_offset,
            sec1_size,
            sec2_offset,
            sec2_size,
            sec3_offset,
            sec3_size,
            checksum64,
        )

        with open(tmp_path, "wb") as f:
            f.write(header)
            f.write(sec1_comp)
            f.write(sec2_comp)
            f.write(sec3_comp)

        tmp_path.replace(file_path)
        return file_path.stat().st_size


class WCLZReader:
    @staticmethod
    def read_header(file_path: Path) -> Dict[str, int]:
        with open(file_path, "rb") as f:
            buf = f.read(HEADER_SIZE)
        (
            magic,
            version,
            flags,
            dict_id,
            event_count,
            duration_ms,
            sec1_offset,
            sec1_size,
            sec2_offset,
            sec2_size,
            sec3_offset,
            sec3_size,
            checksum64,
        ) = HEADER_STRUCT.unpack(buf)
        if magic != MAGIC:
            raise ValueError(f"Invalid WCLZ magic in {file_path}")
        return {
            "version": version,
            "flags": flags,
            "dict_id": dict_id,
            "event_count": event_count,
            "duration_ms": duration_ms,
            "sec1_offset": sec1_offset,
            "sec1_size": sec1_size,
            "sec2_offset": sec2_offset,
            "sec2_size": sec2_size,
            "sec3_offset": sec3_offset,
            "sec3_size": sec3_size,
            "checksum64": checksum64,
        }

    @staticmethod
    def _read_decompressed_slice(
        file_path: Path, offset: int, size: int, zstd_dict_bytes: Optional[bytes] = None
    ) -> bytes:
        with open(file_path, "rb") as f:
            f.seek(offset)
            comp = f.read(size)
        dict_data = zstd.ZstdCompressionDict(zstd_dict_bytes) if zstd_dict_bytes else None
        dctx = zstd.ZstdDecompressor(dict_data=dict_data)
        return dctx.decompress(comp)

    @classmethod
    def read_summary_only(
        cls, file_path: Path, zstd_dict_bytes: Optional[bytes] = None
    ) -> Dict[str, Any]:
        hdr = cls.read_header(file_path)
        raw = cls._read_decompressed_slice(
            file_path, hdr["sec1_offset"], hdr["sec1_size"], zstd_dict_bytes
        )
        return msgpack.unpackb(raw, raw=False)

    @classmethod
    def read_events_only(
        cls, file_path: Path, zstd_dict_bytes: Optional[bytes] = None
    ) -> List[Dict[str, Any]]:
        hdr = cls.read_header(file_path)
        raw = cls._read_decompressed_slice(
            file_path, hdr["sec2_offset"], hdr["sec2_size"], zstd_dict_bytes
        )
        payload = msgpack.unpackb(raw, raw=False)
        count = int(payload["count"])
        type_list = payload["type_list"]
        type_indices = decode_delta_ints(payload["type_indices"], count)
        decoded_cols = {
            col: decode_delta_ints(payload["cols"][col], count) for col in INT_COLUMNS
        }

        events: List[Dict[str, Any]] = []
        for i in range(count):
            ev: Dict[str, Any] = {"type": type_list[type_indices[i]]}
            for col in INT_COLUMNS:
                ev[col] = decoded_cols[col][i]
            events.append(ev)
        return events

    @classmethod
    def read_replay_only(
        cls, file_path: Path, zstd_dict_bytes: Optional[bytes] = None
    ) -> Dict[str, Any]:
        hdr = cls.read_header(file_path)
        raw = cls._read_decompressed_slice(
            file_path, hdr["sec3_offset"], hdr["sec3_size"], zstd_dict_bytes
        )
        payload = msgpack.unpackb(raw, raw=False)
        count = int(payload["count"])
        t_col = decode_delta_ints(payload["t"], count)
        actor_col = decode_delta_ints(payload["actor"], count)
        x_col = decode_delta_ints(payload["x"], count)
        y_col = decode_delta_ints(payload["y"], count)
        hp_col = decode_delta_ints(payload["hp_pct"], count)

        frames = [
            {"t": t_col[i], "actor": actor_col[i], "x": x_col[i], "y": y_col[i], "hp_pct": hp_col[i]}
            for i in range(count)
        ]
        return {
            "map_id": payload["map_id"],
            "pulls": payload.get("pulls", []),
            "frames": frames,
        }

    @classmethod
    def read_full_fight(
        cls, file_path: Path, zstd_dict_bytes: Optional[bytes] = None
    ) -> Dict[str, Any]:
        return {
            "summary": cls.read_summary_only(file_path, zstd_dict_bytes),
            "events": cls.read_events_only(file_path, zstd_dict_bytes),
            "replay": cls.read_replay_only(file_path, zstd_dict_bytes),
        }
```

- [ ] **Step 4: Run `test_wclz_codec.py` to verify it passes**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_wclz_codec.py -v
```
Expected: `2 passed`

- [ ] **Step 5: Commit Task 2**

```bash
cd /d/wowlogsparser && git add wcl_archiver/codec/ tests/test_wclz_codec.py && git commit -m "feat: implement single-fight 3-section columnar Zstd-22 .wclz codec"
```

---

### Task 3: Combat Event State Machine Parser (`CombatStateMachine`)

**Files:**
- Create: `D:\wowlogsparser\wcl_archiver\parser\__init__.py`
- Create: `D:\wowlogsparser\wcl_archiver\parser\combat_state_machine.py`
- Test: `D:\wowlogsparser\tests\test_combat_state_machine.py`

**Interfaces:**
- Produces:
  - `wcl_archiver.parser.combat_state_machine.CombatStateMachine.analyze_fight(fight_meta, actors, abilities, raw_events) -> Tuple[Dict, List[Dict], Dict]`

- [ ] **Step 1: Write the failing test for `CombatStateMachine`**

Create `D:\wowlogsparser\tests\test_combat_state_machine.py`:
```python
from wcl_archiver.parser.combat_state_machine import CombatStateMachine


def test_combat_state_machine_pet_merging_auras_casts_deaths_and_2d_replay():
    fight_meta = {
        "report_code": "aBcDeFg123",
        "fight_id": 2,
        "start_time": 1000,
        "end_time": 21000,
        "encounter_id": 11492,
        "name": "Maw of Souls",
        "keystone_level": 27,
        "affixes": [10, 6, 3, 13],
        "map_id": 1492,
    }
    actors = [
        {"id": 1, "name": "Pellia", "type": "Player", "subType": "Monk"},
        {"id": 50, "name": "Storm Spirit", "type": "Pet", "petOwner": 1},
        {"id": 99, "name": "Helya", "type": "Boss", "subType": "Boss"},
    ]
    abilities = [
        {"gameID": 137639, "name": "Storm, Earth, and Fire", "type": 1},
        {"gameID": 113656, "name": "Fists of Fury", "type": 1},
        {"gameID": 227255, "name": "Tainted Essence", "type": 32},
        {"gameID": 116705, "name": "Spear Hand Strike", "type": 1},
    ]

    raw_events = [
        {
            "timestamp": 1000,
            "type": "combatantinfo",
            "sourceID": 1,
            "specID": 269,
            "talents": [{"id": 196719}, {"id": 115098}],
            "gear": [{"id": 137057, "itemLevel": 1000}, {"id": 151811, "itemLevel": 1000}],
            "auras": [{"ability": 137639, "source": 1, "stacks": 1}],
        },
        {"timestamp": 2000, "type": "begincast", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "x": -41000, "y": 28000, "hitPoints": 100, "maxHitPoints": 100},
        {"timestamp": 2000, "type": "cast", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "x": -41000, "y": 28000, "hitPoints": 100, "maxHitPoints": 100, "classResources": [{"type": 12, "amount": 2, "max": 5}]},
        {"timestamp": 3000, "type": "damage", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "amount": 500000, "overkill": 0, "hitType": 2, "x": -41010, "y": 28005, "hitPoints": 100, "maxHitPoints": 100},
        {"timestamp": 3100, "type": "damage", "sourceID": 50, "targetID": 99, "abilityGameID": 113656, "amount": 350000, "overkill": 50000, "hitType": 1},
        {"timestamp": 11000, "type": "removebuff", "sourceID": 1, "targetID": 1, "abilityGameID": 137639},
        {"timestamp": 12000, "type": "interrupt", "sourceID": 1, "targetID": 99, "abilityGameID": 116705, "extraAbilityGameID": 227255},
        {"timestamp": 18500, "type": "damage", "sourceID": 99, "targetID": 1, "abilityGameID": 227255, "amount": 6000000, "x": -41050, "y": 28020, "hitPoints": 0, "maxHitPoints": 100},
        {"timestamp": 18510, "type": "death", "targetID": 1, "killingAbilityGameID": 227255},
    ]

    summary, norm_events, replay = CombatStateMachine.analyze_fight(
        fight_meta, actors, abilities, raw_events
    )

    dmg_row = summary["damage_done"][0]
    assert dmg_row["actor_id"] == 1
    assert dmg_row["total"] == 800000

    auras = summary["buff_uptimes"]["1"]
    sef_aura = next(a for a in auras if a["ability_id"] == 137639)
    assert sef_aura["uptime_pct"] == 50.0

    casts = summary["cast_timeline"]["1"]
    assert len(casts) == 1
    assert casts[0]["ability_id"] == 113656
    assert 137639 in casts[0]["active_buffs"]

    assert len(summary["interrupts"]) == 1
    assert summary["interrupts"][0]["interrupted_ability_id"] == 227255
    assert len(summary["deaths"]) == 1
    assert summary["deaths"][0]["target_id"] == 1
    assert len(summary["deaths"][0]["recent_events"]) >= 1

    assert replay["map_id"] == 1492
    assert len(replay["frames"]) >= 2
```

- [ ] **Step 2: Run the test to verify it fails**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_combat_state_machine.py -v
```
Expected: `ModuleNotFoundError: No module named 'wcl_archiver.parser'`

- [ ] **Step 3: Implement `combat_state_machine.py`**

Create `D:\wowlogsparser\wcl_archiver\parser\__init__.py`:
```python
"""Combat log state machine and analytical parser."""
```

Create `D:\wowlogsparser\wcl_archiver\parser\combat_state_machine.py`:
```python
from collections import defaultdict
from typing import Any, Dict, List, Tuple


class CombatStateMachine:
    @classmethod
    def analyze_fight(
        cls,
        fight_meta: Dict[str, Any],
        actors: List[Dict[str, Any]],
        abilities: List[Dict[str, Any]],
        raw_events: List[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
        start_time = int(fight_meta.get("start_time", 0))
        end_time = int(fight_meta.get("end_time", start_time + 1))
        duration_ms = max(1, end_time - start_time)

        actor_map: Dict[int, Dict[str, Any]] = {}
        pet_owner_map: Dict[int, int] = {}
        for a in actors:
            aid = int(a["id"])
            actor_map[aid] = a
            if a.get("petOwner") is not None:
                pet_owner_map[aid] = int(a["petOwner"])

        ability_map: Dict[int, Dict[str, Any]] = {
            int(ab["gameID"]): ab for ab in abilities if "gameID" in ab
        }

        combatant_builds: Dict[str, Dict[str, Any]] = {}
        active_buffs: Dict[int, Dict[int, int]] = defaultdict(dict)
        buff_intervals: Dict[int, Dict[int, List[Tuple[int, int]]]] = defaultdict(lambda: defaultdict(list))

        damage_by_actor: Dict[int, int] = defaultdict(int)
        damage_by_spell: Dict[int, Dict[int, Dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: {"total": 0, "hits": 0, "crits": 0})
        )
        healing_by_actor: Dict[int, int] = defaultdict(int)
        healing_by_spell: Dict[int, Dict[int, Dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: {"total": 0, "overheal": 0, "hits": 0})
        )

        cast_timeline: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        interrupts: List[Dict[str, Any]] = []
        deaths: List[Dict[str, Any]] = []
        incoming_history: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

        normalized_events: List[Dict[str, Any]] = []
        replay_frames: List[Dict[str, int]] = []
        last_pos: Dict[int, Tuple[int, int, int]] = {}

        for ev in raw_events:
            abs_t = int(ev.get("timestamp", start_time))
            rel_t = max(0, min(duration_ms, abs_t - start_time))
            ev_type = str(ev.get("type", "unknown"))
            src_id = int(ev.get("sourceID", 0) or 0)
            tgt_id = int(ev.get("targetID", 0) or 0)
            ability_id = int(ev.get("abilityGameID", 0) or 0)

            if ev_type == "summon" and src_id and tgt_id:
                pet_owner_map[tgt_id] = pet_owner_map.get(src_id, src_id)

            owner_id = pet_owner_map.get(src_id, src_id)

            if ev_type == "combatantinfo" and src_id:
                combatant_builds[str(src_id)] = {
                    "spec_id": ev.get("specID", 0),
                    "talents": ev.get("talents", []),
                    "gear": ev.get("gear", []),
                    "artifact": ev.get("artifact", []),
                }
                for aura in ev.get("auras", []):
                    ab_id = int(aura.get("ability", 0) or 0)
                    if ab_id:
                        active_buffs[src_id][ab_id] = 0

            elif ev_type in ("applybuff", "refreshbuff", "applybuffstack"):
                if tgt_id and ability_id and ability_id not in active_buffs[tgt_id]:
                    active_buffs[tgt_id][ability_id] = rel_t

            elif ev_type == "removebuff":
                if tgt_id and ability_id in active_buffs[tgt_id]:
                    st = active_buffs[tgt_id].pop(ability_id)
                    if rel_t > st:
                        buff_intervals[tgt_id][ability_id].append((st, rel_t))

            elif ev_type == "cast" and src_id and ability_id:
                res_list = ev.get("classResources") or []
                res0 = res_list[0] if res_list else {}
                cast_timeline[str(owner_id)].append(
                    {
                        "t_ms": rel_t,
                        "ability_id": ability_id,
                        "target_id": tgt_id,
                        "active_buffs": sorted(active_buffs[owner_id].keys()),
                        "resource_type": int(res0.get("type", 0)),
                        "resource_amount": int(res0.get("amount", 0)),
                    }
                )

            elif ev_type == "damage" and src_id:
                raw_amt = int(ev.get("amount", 0) or 0)
                overkill = int(ev.get("overkill", 0) or 0)
                eff_dmg = max(0, raw_amt - overkill)
                hit_type = int(ev.get("hitType", 1) or 1)

                damage_by_actor[owner_id] += eff_dmg
                sp = damage_by_spell[owner_id][ability_id]
                sp["total"] += eff_dmg
                sp["hits"] += 1
                if hit_type == 2:
                    sp["crits"] += 1

                if tgt_id:
                    incoming_history[tgt_id].append(
                        {
                            "t_ms": rel_t,
                            "type": "damage",
                            "source_id": src_id,
                            "ability_id": ability_id,
                            "amount": raw_amt,
                        }
                    )

            elif ev_type in ("heal", "absorbed") and src_id:
                raw_heal = int(ev.get("amount", 0) or 0)
                overheal = int(ev.get("overheal", 0) or 0)
                healing_by_actor[owner_id] += raw_heal
                sp = healing_by_spell[owner_id][ability_id]
                sp["total"] += raw_heal
                sp["overheal"] += overheal
                sp["hits"] += 1
                if tgt_id:
                    incoming_history[tgt_id].append(
                        {
                            "t_ms": rel_t,
                            "type": "heal",
                            "source_id": src_id,
                            "ability_id": ability_id,
                            "amount": raw_heal,
                        }
                    )

            elif ev_type == "interrupt":
                interrupts.append(
                    {
                        "t_ms": rel_t,
                        "source_id": owner_id,
                        "target_id": tgt_id,
                        "ability_id": ability_id,
                        "interrupted_ability_id": int(ev.get("extraAbilityGameID", 0) or 0),
                    }
                )

            elif ev_type == "death" and tgt_id:
                for ab_id, st in list(active_buffs[tgt_id].items()):
                    if rel_t > st:
                        buff_intervals[tgt_id][ab_id].append((st, rel_t))
                active_buffs[tgt_id].clear()

                window_start = max(0, rel_t - 15000)
                recent = [
                    h for h in incoming_history[tgt_id] if window_start <= h["t_ms"] <= rel_t
                ]
                deaths.append(
                    {
                        "t_ms": rel_t,
                        "target_id": tgt_id,
                        "killing_ability_id": int(ev.get("killingAbilityGameID", 0) or 0),
                        "recent_events": recent,
                    }
                )

            if "x" in ev and "y" in ev and src_id:
                x_val = int(ev["x"])
                y_val = int(ev["y"])
                hp = int(ev.get("hitPoints", 100) or 100)
                max_hp = max(1, int(ev.get("maxHitPoints", 100) or 100))
                hp_pct = int((hp * 10000) // max_hp)
                prev = last_pos.get(src_id)
                if prev != (x_val, y_val, hp_pct):
                    last_pos[src_id] = (x_val, y_val, hp_pct)
                    replay_frames.append(
                        {"t": rel_t, "actor": src_id, "x": x_val, "y": y_val, "hp_pct": hp_pct}
                    )

            res_list = ev.get("classResources") or []
            res0 = res_list[0] if res_list else {}
            normalized_events.append(
                {
                    "timestamp": rel_t,
                    "type": ev_type,
                    "sourceID": src_id,
                    "targetID": tgt_id,
                    "targetInstance": int(ev.get("targetInstance", 0) or 0),
                    "abilityGameID": ability_id,
                    "hitType": int(ev.get("hitType", 0) or 0),
                    "amount": int(ev.get("amount", 0) or 0),
                    "unmitigatedAmount": int(ev.get("unmitigatedAmount", 0) or 0),
                    "absorbed": int(ev.get("absorbed", 0) or 0),
                    "overkill": int(ev.get("overkill", 0) or 0),
                    "hitPoints": int(ev.get("hitPoints", 0) or 0),
                    "maxHitPoints": int(ev.get("maxHitPoints", 0) or 0),
                    "resourceType": int(res0.get("type", 0) or 0),
                    "resourceAmount": int(res0.get("amount", 0) or 0),
                    "resourceMax": int(res0.get("max", 0) or 0),
                    "x": int(ev.get("x", 0) or 0),
                    "y": int(ev.get("y", 0) or 0),
                    "facing": int(ev.get("facing", 0) or 0),
                }
            )

        for tgt_id, ab_dict in active_buffs.items():
            for ab_id, st in ab_dict.items():
                if duration_ms > st:
                    buff_intervals[tgt_id][ab_id].append((st, duration_ms))

        buff_uptimes: Dict[str, List[Dict[str, Any]]] = {}
        for tgt_id, ab_intervals in buff_intervals.items():
            rows = []
            for ab_id, intervals in ab_intervals.items():
                covered = sum(end - st for st, end in intervals)
                uptime_pct = round(min(100.0, (covered * 100.0) / duration_ms), 2)
                rows.append(
                    {
                        "ability_id": ab_id,
                        "uptime_pct": uptime_pct,
                        "intervals": intervals,
                    }
                )
            rows.sort(key=lambda r: r["uptime_pct"], reverse=True)
            buff_uptimes[str(tgt_id)] = rows

        damage_done = []
        for aid, total_dmg in sorted(damage_by_actor.items(), key=lambda kv: kv[1], reverse=True):
            spells_breakdown = [
                {"ability_id": ab_id, **stats}
                for ab_id, stats in sorted(
                    damage_by_spell[aid].items(), key=lambda kv: kv[1]["total"], reverse=True
                )
            ]
            damage_done.append(
                {
                    "actor_id": aid,
                    "total": total_dmg,
                    "dps": round((total_dmg * 1000.0) / duration_ms, 1),
                    "spells": spells_breakdown,
                }
            )

        healing_done = []
        for aid, total_heal in sorted(healing_by_actor.items(), key=lambda kv: kv[1], reverse=True):
            spells_breakdown = [
                {"ability_id": ab_id, **stats}
                for ab_id, stats in sorted(
                    healing_by_spell[aid].items(), key=lambda kv: kv[1]["total"], reverse=True
                )
            ]
            healing_done.append(
                {
                    "actor_id": aid,
                    "total": total_heal,
                    "hps": round((total_heal * 1000.0) / duration_ms, 1),
                    "spells": spells_breakdown,
                }
            )

        summary_section = {
            "report_code": fight_meta.get("report_code", ""),
            "fight_id": int(fight_meta.get("fight_id", 0)),
            "encounter_id": int(fight_meta.get("encounter_id", 0)),
            "name": fight_meta.get("name", ""),
            "keystone_level": int(fight_meta.get("keystone_level", 0)),
            "affixes": fight_meta.get("affixes", []),
            "duration_ms": duration_ms,
            "actors": actors,
            "abilities": {str(k): v for k, v in ability_map.items()},
            "combatant_builds": combatant_builds,
            "damage_done": damage_done,
            "healing_done": healing_done,
            "buff_uptimes": buff_uptimes,
            "cast_timeline": dict(cast_timeline),
            "interrupts": interrupts,
            "deaths": deaths,
        }

        replay_stream = {
            "map_id": int(fight_meta.get("map_id", 0)),
            "pulls": fight_meta.get("pulls", []),
            "frames": replay_frames,
        }

        return summary_section, normalized_events, replay_stream
```

- [ ] **Step 4: Run `test_combat_state_machine.py` to verify it passes**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_combat_state_machine.py -v
```
Expected: `1 passed`

- [ ] **Step 5: Commit Task 3**

```bash
cd /d/wowlogsparser && git add wcl_archiver/parser/ tests/test_combat_state_machine.py && git commit -m "feat: implement O(N) CombatStateMachine for pets, auras, casts, deaths, and 2D replay"
```

---

### Task 4: WCL Authentication & Subscription Session Manager (`WCLAuthManager`)

**Files:**
- Create: `D:\wowlogsparser\wcl_archiver\wcl_client\__init__.py`
- Create: `D:\wowlogsparser\wcl_archiver\wcl_client\auth.py`
- Test: `D:\wowlogsparser\tests\test_wcl_auth.py`

**Interfaces:**
- Produces:
  - `wcl_archiver.wcl_client.auth.WCLAuthManager` (`login_with_credentials`, `save_session_cookies`, `get_auth_status`, `get_headers_and_cookies`)

- [ ] **Step 1: Write the failing test for `WCLAuthManager`**

Create `D:\wowlogsparser\tests\test_wcl_auth.py`:
```python
from pathlib import Path
import httpx
import pytest
from wcl_archiver.wcl_client.auth import WCLAuthManager


@pytest.mark.asyncio
async def test_wcl_auth_manager_csrf_login_and_session_persistence(tmp_path: Path):
    def mock_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/login" and request.method == "GET":
            html = '<html><head><meta name="csrf-token" content="csrf_secret_123"></head></html>'
            return httpx.Response(
                200,
                text=html,
                headers={"set-cookie": "XSRF-TOKEN=xsrf_cookie_123; Path=/"},
            )
        if request.url.path == "/login" and request.method == "POST":
            return httpx.Response(
                200,
                text="Logged in as Pellia",
                headers={"set-cookie": "wcl_session=sub_session_abc; Path=/"},
            )
        if request.url.path == "/oauth/token" and request.method == "POST":
            return httpx.Response(200, json={"access_token": "oauth_bearer_xyz", "expires_in": 3600})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    session_file = tmp_path / "auth_session.json"
    auth = WCLAuthManager(session_file=session_file, transport=transport)

    status = await auth.login_with_credentials(
        email="pellia@example.com",
        password="supersecretpassword",
        client_id="cid",
        client_secret="csecret",
    )

    assert status["authenticated"] is True
    assert status["has_oauth_token"] is True
    assert session_file.exists()

    auth2 = WCLAuthManager(session_file=session_file, transport=transport)
    status2 = auth2.get_auth_status()
    assert status2["authenticated"] is True
    assert status2["email"] == "pellia@example.com"
```

- [ ] **Step 2: Run the test to verify it fails**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_wcl_auth.py -v
```
Expected: `ModuleNotFoundError: No module named 'wcl_archiver.wcl_client'`

- [ ] **Step 3: Implement `wcl_archiver/wcl_client/auth.py`**

Create `D:\wowlogsparser\wcl_archiver\wcl_client\__init__.py`:
```python
"""WarcraftLogs HTTP, GraphQL, and Session Client."""
```

Create `D:\wowlogsparser\wcl_archiver\wcl_client\auth.py`:
```python
import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import unquote
import httpx

CSRF_META_RE = re.compile(r'<meta\s+name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']', re.I)


class WCLAuthManager:
    BASE_URL = "https://www.warcraftlogs.com"

    def __init__(
        self,
        session_file: Path,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        self.session_file = Path(session_file)
        self.session_file.parent.mkdir(parents=True, exist_ok=True)
        self._transport = transport
        self._state: Dict[str, Any] = {
            "authenticated": False,
            "email": "",
            "cookies": {},
            "csrf_token": "",
            "client_id": "",
            "client_secret": "",
            "oauth_token": "",
        }
        self._load()

    def _load(self) -> None:
        if self.session_file.exists():
            try:
                self._state.update(json.loads(self.session_file.read_text(encoding="utf-8")))
            except Exception:
                pass

    def _save(self) -> None:
        self.session_file.write_text(json.dumps(self._state, indent=2), encoding="utf-8")

    def get_auth_status(self) -> Dict[str, Any]:
        return {
            "authenticated": bool(self._state.get("authenticated")),
            "email": self._state.get("email", ""),
            "has_session_cookie": "wcl_session" in self._state.get("cookies", {}),
            "has_oauth_token": bool(self._state.get("oauth_token")),
        }

    def save_session_cookies(
        self,
        cookies: Dict[str, str],
        xsrf_token: str = "",
        email: str = "Browser Session",
        client_id: str = "",
        client_secret: str = "",
    ) -> Dict[str, Any]:
        self._state["cookies"] = dict(cookies)
        self._state["csrf_token"] = xsrf_token or unquote(cookies.get("XSRF-TOKEN", ""))
        self._state["email"] = email
        self._state["authenticated"] = bool(cookies)
        if client_id:
            self._state["client_id"] = client_id
        if client_secret:
            self._state["client_secret"] = client_secret
        self._save()
        return self.get_auth_status()

    async def login_with_credentials(
        self,
        email: str,
        password: str,
        client_id: str = "",
        client_secret: str = "",
    ) -> Dict[str, Any]:
        async with httpx.AsyncClient(
            base_url=self.BASE_URL,
            follow_redirects=True,
            transport=self._transport,
            timeout=30.0,
        ) as client:
            login_page = await client.get("/login")
            csrf_match = CSRF_META_RE.search(login_page.text)
            csrf_token = csrf_match.group(1) if csrf_match else ""

            resp = await client.post(
                "/login",
                data={"email": email, "password": password, "_token": csrf_token},
                headers={"Referer": f"{self.BASE_URL}/login"},
            )
            cookies = dict(client.cookies.items())
            if resp.status_code < 400 and ("wcl_session" in cookies or "logout" in resp.text.lower() or "logged in" in resp.text.lower()):
                self._state["authenticated"] = True
                self._state["email"] = email
                self._state["cookies"] = cookies
                self._state["csrf_token"] = csrf_token or unquote(cookies.get("XSRF-TOKEN", ""))

            if client_id and client_secret:
                self._state["client_id"] = client_id
                self._state["client_secret"] = client_secret
                tok_resp = await client.post(
                    "/oauth/token",
                    data={"grant_type": "client_credentials"},
                    auth=(client_id, client_secret),
                )
                if tok_resp.status_code == 200:
                    self._state["oauth_token"] = tok_resp.json().get("access_token", "")

            self._save()
            return self.get_auth_status()

    def get_headers_and_cookies(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        cookies = dict(self._state.get("cookies", {}))
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) WCLArchiver/1.0",
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
        }
        csrf = self._state.get("csrf_token") or unquote(cookies.get("XSRF-TOKEN", ""))
        if csrf:
            headers["X-CSRF-TOKEN"] = csrf
            headers["X-XSRF-TOKEN"] = csrf
        if self._state.get("oauth_token"):
            headers["Authorization"] = f"Bearer {self._state['oauth_token']}"
        return headers, cookies
```

- [ ] **Step 4: Run `test_wcl_auth.py` to verify it passes**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_wcl_auth.py -v
```
Expected: `1 passed`

- [ ] **Step 5: Commit Task 4**

```bash
cd /d/wowlogsparser && git add wcl_archiver/wcl_client/ tests/test_wcl_auth.py && git commit -m "feat: add WCLAuthManager for login/password, CSRF cookies, and OAuth2 session"
```

---

### Task 5: WCL Protocol Parser & 3-Stage Harvester Pipeline (`WCLProtocolClient` + `HarvesterPipeline`)

**Files:**
- Create: `D:\wowlogsparser\wcl_archiver\wcl_client\protocol_parser.py`
- Create: `D:\wowlogsparser\wcl_archiver\harvester\__init__.py`
- Create: `D:\wowlogsparser\wcl_archiver\harvester\pipeline.py`
- Test: `D:\wowlogsparser\tests\test_harvester_pipeline.py`

**Interfaces:**
- Produces:
  - `wcl_archiver.wcl_client.protocol_parser.WCLProtocolClient` (`unarchive_report`, `fetch_report_master_and_fights`, `fetch_all_fight_events`, `fetch_rankings_fight_refs`)
  - `wcl_archiver.harvester.pipeline.HarvesterPipeline` (`warm_unarchive_batch`, `process_single_fight`)

- [ ] **Step 1: Write the failing test for `WCLProtocolClient` and `HarvesterPipeline`**

Create `D:\wowlogsparser\tests\test_harvester_pipeline.py`:
```python
from pathlib import Path
import httpx
import pytest
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.wcl_client.auth import WCLAuthManager
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient
from wcl_archiver.harvester.pipeline import HarvesterPipeline
from wcl_archiver.codec.wclz_format import WCLZReader


@pytest.mark.asyncio
async def test_harvester_pipeline_unarchives_paginates_and_writes_individual_wclz(tmp_path: Path):
    def mock_wcl(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/reports/unarchive/aBcDeFg123":
            return httpx.Response(200, json={"success": True, "status": "unarchived"})
        if path == "/reports/fights-and-participants/aBcDeFg123/0":
            return httpx.Response(
                200,
                json={
                    "fights": [
                        {
                            "id": 2,
                            "start_time": 1000,
                            "end_time": 5000,
                            "boss": 11492,
                            "name": "Maw of Souls",
                            "keystoneLevel": 28,
                            "affixes": [10, 6, 3, 13],
                            "maps": [1492],
                        }
                    ],
                    "friendlies": [
                        {"id": 1, "name": "Pellia", "type": "Monk", "icon": "Monk-Windwalker"}
                    ],
                    "enemies": [{"id": 99, "name": "Helya", "type": "Boss"}],
                    "abilities": [{"gameID": 113656, "name": "Fists of Fury", "type": 1}],
                },
            )
        if path.startswith("/reports/events/summary/aBcDeFg123/2/1000/5000"):
            return httpx.Response(
                200,
                json={
                    "events": [
                        {"timestamp": 1500, "type": "cast", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "x": -41000, "y": 28000},
                    ],
                    "nextPageTimestamp": 3000,
                },
            )
        if path.startswith("/reports/events/summary/aBcDeFg123/2/3000/5000"):
            return httpx.Response(
                200,
                json={
                    "events": [
                        {"timestamp": 3200, "type": "damage", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "amount": 950000, "hitType": 2, "x": -41020, "y": 28010},
                    ],
                    "nextPageTimestamp": None,
                },
            )
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_wcl)
    index_db = IndexDB(tmp_path / "index.db")
    master_db = MasterDictDB(tmp_path / "master_dict.db")
    auth = WCLAuthManager(tmp_path / "auth_session.json", transport=transport)
    auth.save_session_cookies({"wcl_session": "sub123", "XSRF-TOKEN": "csrf123"})

    client = WCLProtocolClient(auth_manager=auth, transport=transport)
    pipeline = HarvesterPipeline(
        data_dir=tmp_path,
        index_db=index_db,
        master_db=master_db,
        protocol_client=client,
    )

    index_db.enqueue_fight(
        report_code="aBcDeFg123",
        fight_id=2,
        content_type="mplus",
        zone_id=9,
        partition=5,
        encounter_id=11492,
        encounter_slug="maw-of-souls",
        keystone_level=28,
        duration_ms=4000,
        participant={"name": "Pellia", "class": "Monk", "spec": "Windwalker", "role": "dps", "ilvl": 975},
    )

    await pipeline.warm_unarchive_batch(limit=5)
    pending = index_db.get_unarchived_pending_fights(limit=5)
    assert len(pending) == 1

    wclz_file = await pipeline.process_single_fight(pending[0])
    assert wclz_file.exists()
    assert wclz_file.suffix == ".wclz"

    summary = WCLZReader.read_summary_only(wclz_file)
    assert summary["report_code"] == "aBcDeFg123"
    assert summary["damage_done"][0]["total"] == 950000
    assert master_db.get_spell(113656)["name"] == "Fists of Fury"
```

- [ ] **Step 2: Run the test to verify it fails**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_harvester_pipeline.py -v
```
Expected: `ModuleNotFoundError: No module named 'wcl_archiver.wcl_client.protocol_parser'`

- [ ] **Step 3: Implement `protocol_parser.py` and `pipeline.py`**

Create `D:\wowlogsparser\wcl_archiver\wcl_client\protocol_parser.py`:
```python
from typing import Any, Dict, List, Optional
import httpx
from wcl_archiver.wcl_client.auth import WCLAuthManager


class WCLProtocolClient:
    BASE_URL = "https://www.warcraftlogs.com"

    def __init__(
        self,
        auth_manager: WCLAuthManager,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        self.auth_manager = auth_manager
        self._transport = transport

    def _make_client(self) -> httpx.AsyncClient:
        headers, cookies = self.auth_manager.get_headers_and_cookies()
        return httpx.AsyncClient(
            base_url=self.BASE_URL,
            headers=headers,
            cookies=cookies,
            transport=self._transport,
            follow_redirects=True,
            timeout=45.0,
        )

    async def unarchive_report(self, report_code: str) -> bool:
        async with self._make_client() as client:
            resp = await client.post(f"/reports/unarchive/{report_code}")
            return resp.status_code == 200

    async def fetch_report_master_and_fights(self, report_code: str) -> Dict[str, Any]:
        async with self._make_client() as client:
            resp = await client.get(f"/reports/fights-and-participants/{report_code}/0")
            resp.raise_for_status()
            return resp.json()

    async def fetch_all_fight_events(
        self,
        report_code: str,
        fight_id: int,
        start_time: int,
        end_time: int,
    ) -> List[Dict[str, Any]]:
        all_events: List[Dict[str, Any]] = []
        cursor: Optional[int] = int(start_time)

        async with self._make_client() as client:
            while cursor is not None and cursor < end_time:
                url = (
                    f"/reports/events/summary/{report_code}/{fight_id}/{cursor}/{end_time}"
                    f"/0/0/Any/0/-1.0.-1.0/0?includeResources=true"
                )
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
                batch = data.get("events", [])
                all_events.extend(batch)
                next_ts = data.get("nextPageTimestamp")
                if next_ts is not None and int(next_ts) > cursor:
                    cursor = int(next_ts)
                else:
                    cursor = None

        return all_events

    async def fetch_rankings_fight_refs(
        self,
        zone_id: int,
        encounter_id: int,
        partition: int,
        class_name: str = "Any",
        spec_name: str = "Any",
        page: int = 1,
    ) -> List[Dict[str, Any]]:
        query = """
        query($encounterId: Int!, $page: Int!, $partition: Int!, $className: String, $specName: String) {
          worldData {
            encounter(id: $encounterId) {
              fightRankings(page: $page, partition: $partition, className: $className, specName: $specName)
            }
          }
        }
        """
        async with self._make_client() as client:
            resp = await client.post(
                "/api/v2/client",
                json={
                    "query": query,
                    "variables": {
                        "encounterId": encounter_id,
                        "page": page,
                        "partition": partition,
                        "className": class_name,
                        "specName": spec_name,
                    },
                },
            )
            if resp.status_code != 200:
                return []
            payload = resp.json()
            rankings = (
                payload.get("data", {})
                .get("worldData", {})
                .get("encounter", {})
                .get("fightRankings", {})
                .get("rankings", [])
            )
            return rankings
```

Create `D:\wowlogsparser\wcl_archiver\harvester\__init__.py`:
```python
"""Harvester pipeline for WCL Legion logs."""
```

Create `D:\wowlogsparser\wcl_archiver\harvester\pipeline.py`:
```python
from pathlib import Path
from typing import Any, Dict, List
from wcl_archiver.codec.wclz_format import WCLZWriter
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.parser.combat_state_machine import CombatStateMachine
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient


class HarvesterPipeline:
    def __init__(
        self,
        data_dir: Path,
        index_db: IndexDB,
        master_db: MasterDictDB,
        protocol_client: WCLProtocolClient,
    ):
        self.data_dir = Path(data_dir)
        self.index_db = index_db
        self.master_db = master_db
        self.client = protocol_client

    async def warm_unarchive_batch(self, limit: int = 25) -> int:
        codes = self.index_db.get_pending_reports_for_unarchive(limit=limit)
        warmed = 0
        for code in codes:
            ok = await self.client.unarchive_report(code)
            if ok:
                self.index_db.mark_report_unarchived(code, "UNARCHIVED")
                warmed += 1
        return warmed

    async def process_single_fight(self, fight_row: Dict[str, Any]) -> Path:
        report_code = fight_row["report_code"]
        fight_id = int(fight_row["fight_id"])
        encounter_id = int(fight_row["encounter_id"])
        encounter_slug = fight_row["encounter_slug"]
        content_type = fight_row["content_type"]

        master = await self.client.fetch_report_master_and_fights(report_code)
        fights_list = master.get("fights", [])
        target_fight = next(
            (f for f in fights_list if int(f.get("id", -1)) == fight_id),
            None,
        )
        if not target_fight:
            raise ValueError(f"Fight {fight_id} not found in report {report_code}")

        start_time = int(target_fight.get("start_time", 0))
        end_time = int(target_fight.get("end_time", start_time + 1))

        abilities = master.get("abilities", [])
        for ab in abilities:
            if "gameID" in ab:
                self.master_db.upsert_spell(
                    int(ab["gameID"]),
                    str(ab.get("name", "")),
                    int(ab.get("type", 1) or 1),
                    str(ab.get("icon", "")),
                )

        actors: List[Dict[str, Any]] = []
        for group_key in ("friendlies", "enemies", "friendlyPets", "enemyPets"):
            for actor in master.get(group_key, []):
                actors.append(actor)

        raw_events = await self.client.fetch_all_fight_events(
            report_code, fight_id, start_time, end_time
        )

        maps = target_fight.get("maps") or [0]
        map_id = int(maps[0]["id"] if isinstance(maps[0], dict) else maps[0])
        fight_meta = {
            "report_code": report_code,
            "fight_id": fight_id,
            "start_time": start_time,
            "end_time": end_time,
            "encounter_id": encounter_id,
            "name": target_fight.get("name", encounter_slug),
            "keystone_level": int(target_fight.get("keystoneLevel", fight_row.get("keystone_level", 0)) or 0),
            "affixes": target_fight.get("affixes", []),
            "map_id": map_id,
            "pulls": target_fight.get("dungeonPulls", []),
        }

        summary, norm_events, replay = CombatStateMachine.analyze_fight(
            fight_meta, actors, abilities, raw_events
        )

        duration_s = max(1, (end_time - start_time) // 1000)
        mins, secs = divmod(duration_s, 60)
        key_lvl = fight_meta["keystone_level"]

        if content_type == "mplus":
            sub_dir = self.data_dir / "mplus_s5" / f"{encounter_id}_{encounter_slug}"
            fname = f"k{key_lvl}_{mins}m{secs:02d}s_{report_code}_f{fight_id}.wclz"
        else:
            zone_id = int(fight_row.get("zone_id", 17))
            sub_dir = self.data_dir / "raids" / f"zone_{zone_id}" / f"{encounter_id}_{encounter_slug}"
            fname = f"{mins}m{secs:02d}s_{report_code}_f{fight_id}.wclz"

        out_path = sub_dir / fname
        comp_bytes = WCLZWriter.write_fight(out_path, summary, norm_events, replay, zstd_level=22)

        rel_path = str(out_path.relative_to(self.data_dir))
        self.index_db.mark_fight_packed(
            report_code=report_code,
            fight_id=fight_id,
            file_rel_path=rel_path,
            compressed_bytes=comp_bytes,
            event_count=len(norm_events),
        )
        return out_path
```

- [ ] **Step 4: Run `test_harvester_pipeline.py` to verify it passes**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_harvester_pipeline.py -v
```
Expected: `1 passed`

- [ ] **Step 5: Commit Task 5**

```bash
cd /d/wowlogsparser && git add wcl_archiver/wcl_client/protocol_parser.py wcl_archiver/harvester/ tests/test_harvester_pipeline.py && git commit -m "feat: implement WCLProtocolClient and 3-stage HarvesterPipeline"
```

---

### Task 6: Local FastAPI Backend (`Viewer & Harvester Control API`)

**Files:**
- Create: `D:\wowlogsparser\wcl_archiver\api\__init__.py`
- Create: `D:\wowlogsparser\wcl_archiver\api\app.py`
- Test: `D:\wowlogsparser\tests\test_viewer_api.py`

**Interfaces:**
- Produces:
  - `wcl_archiver.api.app.create_app(data_dir: Path) -> FastAPI`

- [ ] **Step 1: Write the failing test for the FastAPI endpoints**

Create `D:\wowlogsparser\tests\test_viewer_api.py`:
```python
from pathlib import Path
from fastapi.testclient import TestClient
from wcl_archiver.api.app import create_app
from wcl_archiver.codec.wclz_format import WCLZWriter
from wcl_archiver.db.schema import IndexDB


def test_viewer_api_serves_catalog_search_and_isolated_single_fight(tmp_path: Path):
    index_db = IndexDB(tmp_path / "index.db")
    index_db.enqueue_fight(
        report_code="aBcDeFg123",
        fight_id=2,
        content_type="mplus",
        zone_id=9,
        partition=5,
        encounter_id=11492,
        encounter_slug="maw-of-souls",
        keystone_level=28,
        duration_ms=840000,
        participant={"name": "Pellia", "class": "Monk", "spec": "Windwalker", "role": "dps", "ilvl": 975},
    )

    rel_path = "mplus_s5/11492_maw-of-souls/k28_14m00s_aBcDeFg123_f2.wclz"
    full_path = tmp_path / rel_path
    summary = {
        "report_code": "aBcDeFg123",
        "fight_id": 2,
        "keystone_level": 28,
        "duration_ms": 840000,
        "actors": [{"id": 1, "name": "Pellia", "type": "Player"}],
        "damage_done": [{"actor_id": 1, "total": 150000000, "dps": 178571.4, "spells": []}],
    }
    events = [
        {"timestamp": 1000, "type": "cast", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "amount": 0, "x": -41000, "y": 28000}
    ]
    replay = {"map_id": 1492, "frames": [{"t": 1000, "actor": 1, "x": -41000, "y": 28000, "hp_pct": 10000}]}

    comp_size = WCLZWriter.write_fight(full_path, summary, events, replay, zstd_level=22)
    index_db.mark_fight_packed("aBcDeFg123", 2, rel_path, comp_size, 1)

    app = create_app(data_dir=tmp_path)
    client = TestClient(app)

    cat = client.get("/api/meta/catalog").json()
    assert len(cat["mplus_dungeons"]) == 13

    fights = client.get("/api/fights?content_type=mplus&spec=Windwalker").json()
    assert len(fights) == 1
    assert fights[0]["fight_uid"] == "aBcDeFg123_2"

    s_resp = client.get("/api/fights/aBcDeFg123_2/summary").json()
    assert s_resp["keystone_level"] == 28
    assert s_resp["damage_done"][0]["total"] == 150000000

    r_resp = client.get("/api/fights/aBcDeFg123_2/replay").json()
    assert r_resp["map_id"] == 1492
    assert len(r_resp["frames"]) == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_viewer_api.py -v
```
Expected: `ModuleNotFoundError: No module named 'wcl_archiver.api'`

- [ ] **Step 3: Implement `wcl_archiver/api/app.py`**

Create `D:\wowlogsparser\wcl_archiver\api\__init__.py`:
```python
"""FastAPI backend for WCL Legion Archiver & Offline Viewer."""
```

Create `D:\wowlogsparser\wcl_archiver\api\app.py`:
```python
from pathlib import Path
from typing import Any, Dict, Optional
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from wcl_archiver.codec.wclz_format import WCLZReader
from wcl_archiver.config import (
    BASE_DIR,
    DATA_DIR,
    LEGION_MPLUS_S5_DUNGEONS,
    LEGION_MPLUS_S5_PARTITION,
    LEGION_MPLUS_ZONE_ID,
    LEGION_RAIDS,
    LEGION_SPECS,
)
from wcl_archiver.db.schema import IndexDB, MasterDictDB
from wcl_archiver.wcl_client.auth import WCLAuthManager


class LoginRequest(BaseModel):
    email: str = ""
    password: str = ""
    client_id: str = ""
    client_secret: str = ""
    wcl_session_cookie: str = ""
    xsrf_token: str = ""


def create_app(data_dir: Path = DATA_DIR) -> FastAPI:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    index_db = IndexDB(data_dir / "index.db")
    master_db = MasterDictDB(data_dir / "master_dict.db")
    auth_manager = WCLAuthManager(data_dir / "auth_session.json")

    app = FastAPI(title="WCL Legion Archiver & Offline Viewer")

    @app.get("/api/meta/catalog")
    def get_catalog() -> Dict[str, Any]:
        return {
            "mplus_zone_id": LEGION_MPLUS_ZONE_ID,
            "mplus_partition": LEGION_MPLUS_S5_PARTITION,
            "mplus_dungeons": [
                {"id": k, "slug": v, "name": v.replace("-", " ").title()}
                for k, v in LEGION_MPLUS_S5_DUNGEONS.items()
            ],
            "raids": LEGION_RAIDS,
            "specs": [
                {"class": c, "spec": s, "role": r} for c, s, r in LEGION_SPECS
            ],
        }

    @app.get("/api/auth/status")
    def get_auth_status() -> Dict[str, Any]:
        return auth_manager.get_auth_status()

    @app.post("/api/auth/login")
    async def post_login(req: LoginRequest) -> Dict[str, Any]:
        if req.wcl_session_cookie:
            return auth_manager.save_session_cookies(
                cookies={"wcl_session": req.wcl_session_cookie, "XSRF-TOKEN": req.xsrf_token},
                xsrf_token=req.xsrf_token,
                client_id=req.client_id,
                client_secret=req.client_secret,
            )
        return await auth_manager.login_with_credentials(
            email=req.email,
            password=req.password,
            client_id=req.client_id,
            client_secret=req.client_secret,
        )

    @app.get("/api/fights")
    def list_fights(
        content_type: Optional[str] = None,
        encounter_id: Optional[int] = None,
        class_name: Optional[str] = None,
        spec: Optional[str] = None,
        min_key_level: Optional[int] = None,
        limit: int = 100,
    ) -> Any:
        return index_db.search_fights(
            content_type=content_type,
            encounter_id=encounter_id,
            class_name=class_name,
            spec=spec,
            min_key_level=min_key_level,
            limit=limit,
        )

    def _resolve_fight_file(fight_uid: str) -> Path:
        with index_db._connect() as conn:
            row = conn.execute(
                "SELECT file_rel_path FROM fights WHERE fight_uid = ?",
                (fight_uid,),
            ).fetchone()
        if not row or not row["file_rel_path"]:
            raise HTTPException(status_code=404, detail=f"Fight {fight_uid} is not packed on disk")
        file_path = data_dir / row["file_rel_path"]
        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"File {file_path} missing")
        return file_path

    @app.get("/api/fights/{fight_uid}/summary")
    def get_fight_summary(fight_uid: str) -> Dict[str, Any]:
        file_path = _resolve_fight_file(fight_uid)
        return WCLZReader.read_summary_only(file_path)

    @app.get("/api/fights/{fight_uid}/replay")
    def get_fight_replay(fight_uid: str) -> Dict[str, Any]:
        file_path = _resolve_fight_file(fight_uid)
        return WCLZReader.read_replay_only(file_path)

    @app.get("/api/fights/{fight_uid}/events")
    def get_fight_events(
        fight_uid: str,
        t_start: int = 0,
        t_end: Optional[int] = None,
        actor_id: Optional[int] = None,
    ) -> Any:
        file_path = _resolve_fight_file(fight_uid)
        events = WCLZReader.read_events_only(file_path)
        filtered = [
            e
            for e in events
            if e["timestamp"] >= t_start
            and (t_end is None or e["timestamp"] <= t_end)
            and (actor_id is None or e["sourceID"] == actor_id or e["targetID"] == actor_id)
        ]
        return {"count": len(filtered), "events": filtered[:5000]}

    web_dir = BASE_DIR / "wcl_archiver" / "web"
    if web_dir.exists():
        app.mount("/", StaticFiles(directory=str(web_dir), html=True), name="web")

    return app
```

- [ ] **Step 4: Run `test_viewer_api.py` to verify it passes**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_viewer_api.py -v
```
Expected: `1 passed`

- [ ] **Step 5: Commit Task 6**

```bash
cd /d/wowlogsparser && git add wcl_archiver/api/ tests/test_viewer_api.py && git commit -m "feat: add FastAPI backend for auth, log explorer, and single-fight .wclz reader"
```

---

### Task 7: English Offline Web UI (`Explorer`, `WCL Analytics Tabs`, `60 FPS 2D Replay`) & `start.bat` Launcher

**Files:**
- Create: `D:\wowlogsparser\wcl_archiver\web\index.html`
- Create: `D:\wowlogsparser\wcl_archiver\web\styles.css`
- Create: `D:\wowlogsparser\wcl_archiver\web\app.js`
- Create: `D:\wowlogsparser\wcl_archiver\web\replay2d.js`
- Create: `D:\wowlogsparser\start.bat`

**Interfaces:**
- Consumes: `/api/meta/catalog`, `/api/auth/status`, `/api/auth/login`, `/api/fights`, `/api/fights/{uid}/summary`, `/api/fights/{uid}/replay`, `/api/fights/{uid}/events`
- Produces: Local offline English Web UI at `http://127.0.0.1:8080` and `D:\wowlogsparser\start.bat`.

- [ ] **Step 1: Add the web UI serving test to `tests/test_viewer_api.py`**

Append to `D:\wowlogsparser\tests\test_viewer_api.py`:
```python
def test_web_ui_root_serves_english_html(tmp_path: Path):
    app = create_app(data_dir=tmp_path)
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    assert "WCL Legion 7.3.5 Archiver" in resp.text
    assert "Log Explorer" in resp.text
```

- [ ] **Step 2: Run the test to verify it fails**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest tests/test_viewer_api.py -v
```
Expected: `FAILED tests/test_viewer_api.py::test_web_ui_root_serves_english_html`

- [ ] **Step 3: Create `index.html`, `styles.css`, `app.js`, `replay2d.js`, and `start.bat` (100% English)**

Create `D:\wowlogsparser\wcl_archiver\web\index.html`:
```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>WCL Legion 7.3.5 Archiver &amp; Offline Viewer</title>
  <link rel="stylesheet" href="/styles.css" />
</head>
<body>
  <header class="topbar">
    <div class="brand">WCL Legion 7.3.5 Archiver &amp; Viewer</div>
    <nav class="main-nav">
      <button class="nav-btn active" data-view="explorer">Log Explorer</button>
      <button class="nav-btn" data-view="fight" id="nav-fight-btn" disabled>Fight Analyzer</button>
      <button class="nav-btn" data-view="auth">Account &amp; Harvester</button>
    </nav>
    <div id="auth-badge" class="badge badge-warn">Session: Checking...</div>
  </header>

  <main class="container">
    <!-- VIEW 1: EXPLORER -->
    <section id="view-explorer" class="view active">
      <div class="filter-bar">
        <select id="filter-content-type">
          <option value="mplus">Mythic+ Season 5 (7.3.5)</option>
          <option value="raid">Legion Raids (All 5)</option>
        </select>
        <select id="filter-encounter"><option value="">All Dungeons / Bosses</option></select>
        <select id="filter-spec"><option value="">All Specializations (36)</option></select>
        <input type="number" id="filter-min-key" placeholder="Min Key (+20)" min="2" max="35" />
        <button id="btn-search" class="btn-primary">Search Logs</button>
      </div>
      <table class="data-table">
        <thead>
          <tr>
            <th>Dungeon / Boss</th>
            <th>Key / Difficulty</th>
            <th>Duration</th>
            <th>Participants</th>
            <th>File Size (.wclz)</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody id="fights-tbody"></tbody>
      </table>
    </section>

    <!-- VIEW 2: SINGLE FIGHT ANALYZER -->
    <section id="view-fight" class="view">
      <div class="fight-header" id="fight-header-info">Select a fight from the Explorer</div>
      <div class="sub-tabs">
        <button class="tab-btn active" data-tab="damage">Damage Done</button>
        <button class="tab-btn" data-tab="healing">Healing Done</button>
        <button class="tab-btn" data-tab="casts">Casts &amp; Rotation Timeline</button>
        <button class="tab-btn" data-tab="buffs">Buffs &amp; Uptimes</button>
        <button class="tab-btn" data-tab="builds">Builds &amp; Talents</button>
        <button class="tab-btn" data-tab="deaths">Interrupts &amp; Deaths</button>
        <button class="tab-btn" data-tab="replay2d">2D Map Replay (60 FPS)</button>
      </div>
      <div id="tab-content-area" class="tab-content"></div>
      <div id="replay-container" class="replay-box hidden">
        <div class="replay-controls">
          <button id="replay-play" class="btn-primary">Play / Pause</button>
          <input type="range" id="replay-scrubber" min="0" max="1000" value="0" />
          <span id="replay-time-label">00:00.0</span>
          <select id="replay-speed">
            <option value="0.5">0.5x</option>
            <option value="1" selected>1x</option>
            <option value="2">2x</option>
            <option value="4">4x</option>
          </select>
        </div>
        <canvas id="replay-canvas" width="1100" height="620"></canvas>
      </div>
    </section>

    <!-- VIEW 3: AUTH & HARVESTER -->
    <section id="view-auth" class="view">
      <div class="card">
        <h2>WarcraftLogs Subscriber Account Setup</h2>
        <div class="form-grid">
          <label>WCL Email: <input type="email" id="auth-email" placeholder="you@example.com" /></label>
          <label>WCL Password: <input type="password" id="auth-password" placeholder="••••••••" /></label>
          <label>API v2 Client ID (Optional): <input type="text" id="auth-client-id" /></label>
          <label>API v2 Client Secret (Optional): <input type="password" id="auth-client-secret" /></label>
          <label>Or Direct wcl_session Cookie: <input type="text" id="auth-cookie" /></label>
        </div>
        <button id="btn-save-auth" class="btn-primary">Sign In &amp; Save Session</button>
        <pre id="auth-status-box"></pre>
      </div>
    </section>
  </main>

  <script src="/replay2d.js"></script>
  <script src="/app.js"></script>
</body>
</html>
```

Create `D:\wowlogsparser\wcl_archiver\web\styles.css`:
```css
:root {
  --bg: #0f141c;
  --panel: #18202c;
  --border: #2a364b;
  --accent: #3b82f6;
  --text: #e2e8f0;
  --muted: #94a3b8;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: system-ui, -apple-system, sans-serif;
  background: var(--bg);
  color: var(--text);
}
.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 24px;
  background: var(--panel);
  border-bottom: 1px solid var(--border);
}
.brand { font-weight: 700; font-size: 18px; color: #60a5fa; }
.main-nav { display: flex; gap: 8px; }
.nav-btn, .tab-btn, .btn-primary {
  background: #243044;
  color: var(--text);
  border: 1px solid var(--border);
  padding: 8px 14px;
  border-radius: 6px;
  cursor: pointer;
}
.nav-btn.active, .tab-btn.active, .btn-primary {
  background: var(--accent);
  border-color: var(--accent);
  color: #fff;
}
.container { padding: 20px 24px; }
.view { display: none; }
.view.active { display: block; }
.filter-bar { display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; }
select, input {
  background: var(--panel);
  color: var(--text);
  border: 1px solid var(--border);
  padding: 8px 10px;
  border-radius: 6px;
}
.data-table {
  width: 100%;
  border-collapse: collapse;
  background: var(--panel);
}
.data-table th, .data-table td {
  padding: 10px 12px;
  border-bottom: 1px solid var(--border);
  text-align: left;
}
.sub-tabs { display: flex; gap: 8px; margin: 14px 0; flex-wrap: wrap; }
.card { background: var(--panel); padding: 20px; border-radius: 8px; border: 1px solid var(--border); }
.form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 14px; }
.form-grid label { display: flex; flex-direction: column; gap: 6px; }
.hidden { display: none !important; }
#replay-canvas {
  background: #090d12;
  border: 1px solid var(--border);
  border-radius: 8px;
  margin-top: 10px;
}
.replay-controls { display: flex; align-items: center; gap: 12px; }
#replay-scrubber { flex: 1; }
```

Create `D:\wowlogsparser\wcl_archiver\web\replay2d.js`:
```javascript
class Replay2DPlayer {
  constructor(canvasId, scrubberId, timeLabelId) {
    this.canvas = document.getElementById(canvasId);
    this.ctx = this.canvas.getContext("2d");
    this.scrubber = document.getElementById(scrubberId);
    this.timeLabel = document.getElementById(timeLabelId);
    this.frames = [];
    this.actors = {};
    this.durationMs = 1;
    this.currentMs = 0;
    this.playing = false;
    this.speed = 1.0;
    this.bounds = { minX: -1000, maxX: 1000, minY: -1000, maxY: 1000 };
    this._bindEvents();
  }

  _bindEvents() {
    this.scrubber.addEventListener("input", () => {
      this.currentMs = (Number(this.scrubber.value) / 1000) * this.durationMs;
      this.renderFrame();
    });
  }

  loadReplay(replayData, summaryData) {
    this.frames = replayData.frames || [];
    this.durationMs = Math.max(1, summaryData.duration_ms || 1);
    this.actors = {};
    (summaryData.actors || []).forEach(a => { this.actors[a.id] = a; });
    if (this.frames.length > 0) {
      const xs = this.frames.map(f => f.x);
      const ys = this.frames.map(f => f.y);
      this.bounds = {
        minX: Math.min(...xs) - 500,
        maxX: Math.max(...xs) + 500,
        minY: Math.min(...ys) - 500,
        maxY: Math.max(...ys) + 500,
      };
    }
    this.currentMs = 0;
    this.renderFrame();
  }

  togglePlay() {
    this.playing = !this.playing;
    if (this.playing) {
      this._lastTick = performance.now();
      requestAnimationFrame(ts => this._loop(ts));
    }
  }

  _loop(ts) {
    if (!this.playing) return;
    const dt = (ts - this._lastTick) * this.speed;
    this._lastTick = ts;
    this.currentMs = Math.min(this.durationMs, this.currentMs + dt);
    this.scrubber.value = String(Math.floor((this.currentMs / this.durationMs) * 1000));
    this.renderFrame();
    if (this.currentMs < this.durationMs) {
      requestAnimationFrame(nextTs => this._loop(nextTs));
    } else {
      this.playing = false;
    }
  }

  renderFrame() {
    const ctx = this.ctx;
    const w = this.canvas.width;
    const h = this.canvas.height;
    ctx.clearRect(0, 0, w, h);

    const sec = (this.currentMs / 1000).toFixed(1);
    this.timeLabel.textContent = `${sec}s / ${(this.durationMs / 1000).toFixed(1)}s`;

    const latestByActor = {};
    for (let i = 0; i < this.frames.length; i++) {
      const f = this.frames[i];
      if (f.t > this.currentMs) break;
      latestByActor[f.actor] = f;
    }

    const spanX = Math.max(1, this.bounds.maxX - this.bounds.minX);
    const spanY = Math.max(1, this.bounds.maxY - this.bounds.minY);

    Object.values(latestByActor).forEach(pos => {
      const px = 40 + ((pos.x - this.bounds.minX) / spanX) * (w - 80);
      const py = 40 + ((pos.y - this.bounds.minY) / spanY) * (h - 80);
      const actor = this.actors[pos.actor] || { name: `#${pos.actor}`, type: "Unit" };

      ctx.beginPath();
      ctx.arc(px, py, actor.type === "Boss" ? 12 : 8, 0, Math.PI * 2);
      ctx.fillStyle = actor.type === "Boss" ? "#ef4444" : "#22c55e";
      ctx.fill();

      ctx.fillStyle = "#e2e8f0";
      ctx.font = "12px sans-serif";
      ctx.fillText(`${actor.name} (${(pos.hp_pct / 100).toFixed(0)}%)`, px + 12, py + 4);
    });
  }
}
window.Replay2DPlayer = Replay2DPlayer;
```

Create `D:\wowlogsparser\wcl_archiver\web\app.js`:
```javascript
let currentFightSummary = null;
let currentFightUid = null;
let replayPlayer = null;

async function initApp() {
  replayPlayer = new window.Replay2DPlayer("replay-canvas", "replay-scrubber", "replay-time-label");
  document.getElementById("replay-play").addEventListener("click", () => replayPlayer.togglePlay());
  document.getElementById("replay-speed").addEventListener("change", e => {
    replayPlayer.speed = Number(e.target.value);
  });

  document.querySelectorAll(".nav-btn").forEach(btn => {
    btn.addEventListener("click", () => switchView(btn.dataset.view));
  });

  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      renderFightTab(btn.dataset.tab);
    });
  });

  document.getElementById("btn-search").addEventListener("click", loadFightsList);
  document.getElementById("btn-save-auth").addEventListener("click", submitAuth);

  await loadCatalog();
  await refreshAuthStatus();
  await loadFightsList();
}

function switchView(viewName) {
  document.querySelectorAll(".nav-btn").forEach(b => {
    b.classList.toggle("active", b.dataset.view === viewName);
  });
  document.querySelectorAll(".view").forEach(v => {
    v.classList.toggle("active", v.id === `view-${viewName}`);
  });
}

async function loadCatalog() {
  const res = await fetch("/api/meta/catalog");
  const cat = await res.json();
  const encSel = document.getElementById("filter-encounter");
  cat.mplus_dungeons.forEach(d => {
    const opt = document.createElement("option");
    opt.value = d.id;
    opt.textContent = d.name;
    encSel.appendChild(opt);
  });
  const specSel = document.getElementById("filter-spec");
  cat.specs.forEach(s => {
    const opt = document.createElement("option");
    opt.value = s.spec;
    opt.textContent = `${s.class} - ${s.spec}`;
    specSel.appendChild(opt);
  });
}

async function refreshAuthStatus() {
  const res = await fetch("/api/auth/status");
  const st = await res.json();
  const badge = document.getElementById("auth-badge");
  badge.textContent = st.authenticated ? `Authenticated (${st.email})` : "Not Authenticated";
  document.getElementById("auth-status-box").textContent = JSON.stringify(st, null, 2);
}

async function submitAuth() {
  const payload = {
    email: document.getElementById("auth-email").value,
    password: document.getElementById("auth-password").value,
    client_id: document.getElementById("auth-client-id").value,
    client_secret: document.getElementById("auth-client-secret").value,
    wcl_session_cookie: document.getElementById("auth-cookie").value,
  };
  await fetch("/api/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  await refreshAuthStatus();
}

async function loadFightsList() {
  const contentType = document.getElementById("filter-content-type").value;
  const encounterId = document.getElementById("filter-encounter").value;
  const spec = document.getElementById("filter-spec").value;
  const minKey = document.getElementById("filter-min-key").value;

  const params = new URLSearchParams();
  if (contentType) params.set("content_type", contentType);
  if (encounterId) params.set("encounter_id", encounterId);
  if (spec) params.set("spec", spec);
  if (minKey) params.set("min_key_level", minKey);

  const res = await fetch(`/api/fights?${params.toString()}`);
  const fights = await res.json();
  const tbody = document.getElementById("fights-tbody");
  tbody.innerHTML = "";

  fights.forEach(f => {
    const tr = document.createElement("tr");
    const durS = Math.round((f.duration_ms || 0) / 1000);
    const parts = (f.participants || []).map(p => `${p.player_name} (${p.spec_name})`).join(", ");
    const sizeKb = f.compressed_bytes ? `${(f.compressed_bytes / 1024).toFixed(1)} KB` : "Queued";
    tr.innerHTML = `
      <td>${f.encounter_slug}</td>
      <td>+${f.keystone_level || f.difficulty}</td>
      <td>${Math.floor(durS / 60)}m ${durS % 60}s</td>
      <td>${parts}</td>
      <td>${sizeKb}</td>
      <td><button class="btn-primary" onclick="openSingleFight('${f.fight_uid}')">Open Fight</button></td>
    `;
    tbody.appendChild(tr);
  });
}

async function openSingleFight(fightUid) {
  currentFightUid = fightUid;
  const res = await fetch(`/api/fights/${fightUid}/summary`);
  if (!res.ok) {
    alert("Fight file is not packed on disk yet.");
    return;
  }
  currentFightSummary = await res.json();
  document.getElementById("nav-fight-btn").disabled = false;
  document.getElementById("fight-header-info").textContent =
    `${currentFightSummary.name} (+${currentFightSummary.keystone_level}) — Report ${currentFightSummary.report_code} #${currentFightSummary.fight_id}`;
  switchView("fight");
  renderFightTab("damage");
}

async function renderFightTab(tab) {
  const area = document.getElementById("tab-content-area");
  const replayBox = document.getElementById("replay-container");
  if (!currentFightSummary) return;

  if (tab === "replay2d") {
    area.innerHTML = "";
    replayBox.classList.remove("hidden");
    const res = await fetch(`/api/fights/${currentFightUid}/replay`);
    const replayData = await res.json();
    replayPlayer.loadReplay(replayData, currentFightSummary);
    return;
  }

  replayBox.classList.add("hidden");
  const actorsById = {};
  (currentFightSummary.actors || []).forEach(a => { actorsById[a.id] = a.name; });
  const abilities = currentFightSummary.abilities || {};
  const spellName = id => (abilities[String(id)] ? abilities[String(id)].name : `Spell #${id}`);

  if (tab === "damage") {
    const rows = (currentFightSummary.damage_done || []).map(r => `
      <tr>
        <td>${actorsById[r.actor_id] || r.actor_id}</td>
        <td>${r.total.toLocaleString()}</td>
        <td>${r.dps.toLocaleString()}</td>
      </tr>
    `).join("");
    area.innerHTML = `<table class="data-table"><thead><tr><th>Actor</th><th>Total Damage</th><th>DPS</th></tr></thead><tbody>${rows}</tbody></table>`;
  } else if (tab === "healing") {
    const rows = (currentFightSummary.healing_done || []).map(r => `
      <tr>
        <td>${actorsById[r.actor_id] || r.actor_id}</td>
        <td>${r.total.toLocaleString()}</td>
        <td>${r.hps.toLocaleString()}</td>
      </tr>
    `).join("");
    area.innerHTML = `<table class="data-table"><thead><tr><th>Actor</th><th>Total Healing</th><th>HPS</th></tr></thead><tbody>${rows}</tbody></table>`;
  } else if (tab === "casts") {
    const allCasts = [];
    Object.entries(currentFightSummary.cast_timeline || {}).forEach(([aid, list]) => {
      list.forEach(c => allCasts.push({ actor: actorsById[aid] || aid, ...c }));
    });
    allCasts.sort((a, b) => a.t_ms - b.t_ms);
    const rows = allCasts.slice(0, 500).map(c => `
      <tr>
        <td>${(c.t_ms / 1000).toFixed(2)}s</td>
        <td>${c.actor}</td>
        <td>${spellName(c.ability_id)}</td>
        <td>${(c.active_buffs || []).map(spellName).join(", ")}</td>
      </tr>
    `).join("");
    area.innerHTML = `<table class="data-table"><thead><tr><th>Time</th><th>Player</th><th>Ability Cast</th><th>Active Buffs at Cast</th></tr></thead><tbody>${rows}</tbody></table>`;
  } else if (tab === "buffs") {
    const rows = [];
    Object.entries(currentFightSummary.buff_uptimes || {}).forEach(([aid, list]) => {
      list.forEach(b => rows.push(`<tr><td>${actorsById[aid] || aid}</td><td>${spellName(b.ability_id)}</td><td>${b.uptime_pct}%</td></tr>`));
    });
    area.innerHTML = `<table class="data-table"><thead><tr><th>Player</th><th>Buff / Aura</th><th>Uptime %</th></tr></thead><tbody>${rows.join("")}</tbody></table>`;
  } else if (tab === "builds") {
    area.innerHTML = `<pre class="card">${JSON.stringify(currentFightSummary.combatant_builds || {}, null, 2)}</pre>`;
  } else if (tab === "deaths") {
    area.innerHTML = `<pre class="card">${JSON.stringify({ interrupts: currentFightSummary.interrupts, deaths: currentFightSummary.deaths }, null, 2)}</pre>`;
  }
}

window.openSingleFight = openSingleFight;
window.addEventListener("DOMContentLoaded", initApp);
```

Create `D:\wowlogsparser\start.bat`:
```bat
@echo off
cd /d D:\wowlogsparser
start http://127.0.0.1:8080
python -m uvicorn wcl_archiver.api.app:create_app --factory --host 127.0.0.1 --port 8080
```

- [ ] **Step 4: Run the full project test suite**

Run in Git Bash:
```bash
cd /d/wowlogsparser && pytest -v
```
Expected: `All tests passed`

- [ ] **Step 5: Commit Task 7**

```bash
cd /d/wowlogsparser && git add wcl_archiver/web/ start.bat tests/test_viewer_api.py && git commit -m "feat: add English offline Viewer Web UI, 60 FPS 2D replay player, and start.bat launcher"
```

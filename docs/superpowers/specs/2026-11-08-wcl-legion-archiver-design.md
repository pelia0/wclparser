# System Specification: WCL Legion Archiver & Offline Viewer (`D:\wowlogsparser`)

**Date:** 2026-11-08  
**Status:** Approved  
**Project Directory:** `D:\wowlogsparser`  
**Language Policy:** Everything in the project (source code, UI labels, documentation, logs, database fields, and comments) MUST be 100% in English.

---

## 1. Purpose & Core Requirements

Build a self-contained system for one-off bulk extraction, maximum-ratio compression, and local offline viewing of **WarcraftLogs** combat logs for **World of Warcraft: Legion (7.3.5)** within an active WCL subscription window:

1. **Content Scope:**
   - **Mythic+ Season 5 (Patch 7.3.2 / 7.3.5, WCL Zone 9, Partition 5)** — the final genuine Legion 7.3.5 season **excluding the BfA 8.0 pre-patch** (all 13 Legion dungeons, harvesting top runs per dungeon overall and across all 36 specializations).
   - **All 5 Legion Raids (excluding BfA 8.0 pre-patch partitions):**
     - *The Emerald Nightmare* (Zone 10)
     - *Trial of Valor* (Zone 12)
     - *The Nighthold* (Zone 11)
     - *Tomb of Sargeras* (Zone 13)
     - *Antorus, the Burning Throne* (Zone 17, Partitions 1–2 prior to 8.0 pre-patch)
2. **Full Data Depth per Fight:**
   - Report and fight metadata (duration, keystone level, affixes, boss ID, difficulty, kill/wipe status, dungeon pulls, phases).
   - Complete `CombatantInfo` per participant (7-row talent grid, equipped items, legendaries, trinkets, item level, secondary stats, artifact traits, and relics).
   - Full millisecond-resolution `Raw Events` across the entire fight including class resources (`includeResources=true`) and unit coordinates `(x, y, facing)` for 2D map replay.
3. **Strict Storage & Access Isolation Requirement:**
   - **Every individual fight is stored in its own standalone `.wclz` file (`1 fight = 1 file`).**
   - **Zero bulk archive unpacking on view:** opening a specific fight in the Viewer UI reads and decompresses in RAM **exclusively that single fight's `.wclz` file (~300–700 KB)** in ~10–15 ms. No other fight files are touched, and no temporary uncompressed files are written to disk.
   - **Maximum Compression (40x–55x vs raw JSON):** raw JSON is never written to disk; events are transposed in memory into columnar arrays with delta/varint encoding and compressed with `Zstandard Ultra (Level 22)` (with optional shared `Zstd` dictionary support).

---

## 2. Storage Architecture & `.wclz` Binary Format

### 2.1. On-Disk Directory Layout

```text
D:\wowlogsparser\
├── .gitignore
├── requirements.txt
├── start.bat
├── data\
│   ├── auth_session.json                # Local WCL session cookies & OAuth tokens (gitignored)
│   ├── index.db                         # SQLite (WAL): searchable fight index, participants, specs, talents, queue state
│   ├── master_dict.db                   # SQLite: global dictionary of spells (spellID -> name, school, icon), items, maps
│   ├── mplus_s5\
│   │   ├── 11458_black-rook-hold\
│   │   ├── 11677_cathedral-of-eternal-night\
│   │   ├── 11571_court-of-stars\
│   │   ├── 11466_darkheart-thicket\
│   │   ├── 11456_eye-of-azshara\
│   │   ├── 11477_halls-of-valor\
│   │   ├── 11492_maw-of-souls\
│   │   │   ├── k28_14m12s_aBcDeFg123_f2.wclz
│   │   │   └── k27_15m04s_xYz9876543_f1.wclz
│   │   ├── 11465_neltharions-lair\
│   │   ├── 11651_return-to-karazhan-lower\
│   │   ├── 11652_return-to-karazhan-upper\
│   │   ├── 11753_seat-of-the-triumvirate\
│   │   ├── 11516_the-arcway\
│   │   └── 11493_vault-of-the-wardens\
│   └── raids\
│       ├── zone_10\
│       ├── zone_12\
│       ├── zone_11\
│       ├── zone_13\
│       └── zone_17\
│           ├── 2076_garothi-worldbreaker\
│           └── 2092_argus-the-unmaker\
```

### 2.2. Single-Fight `.wclz` Binary Layout

Each `.wclz` file stores **exactly one fight** and consists of a fixed 76-byte header followed by **3 independently compressed `Zstd` frames**, allowing the UI to read only the required slice of a single fight:

```text
+-----------------------------------------------------------------------+
| HEADER (76 bytes, uncompressed)                                       |
| - Magic: "WCLZ" (4B) | Version: 1 (2B) | Flags (2B) | DictID (4B)     |
| - EventCount (4B) | DurationMs (4B)                                   |
| - Sec1Offset & Sec1Size (16B) — Summary, Builds & Precomputed Tables  |
| - Sec2Offset & Sec2Size (16B) — Columnar Combat Events                |
| - Sec3Offset & Sec3Size (16B) — 2D Replay Spatial & HP Stream         |
| - Checksum64 (8B, xxHash64 over compressed frames)                    |
+-----------------------------------------------------------------------+
| SECTION 1: Fight Summary, Actors & Precomputed Tables (Zstd Frame 1)  |
| - Fight metadata, actor roster, ability table                         |
| - Full CombatantInfo (talents, gear, legendaries, artifact traits)    |
| - Precomputed Damage Done, Healing Done, Casts, Buff Uptimes, Deaths  |
+-----------------------------------------------------------------------+
| SECTION 2: Columnar Combat Events (Zstd Frame 2, Level 22)            |
| - Events transposed into homogeneous integer columns (Struct-of-Arrays|
|   encoded with ZigZag + Delta + Varint):                              |
|   * timestamp, sourceID, targetID, targetInstance, abilityGameID,     |
|     hitType, amount, unmitigatedAmount, absorbed, overkill,           |
|     hitPoints, maxHitPoints, resourceType, resourceAmount,            |
|     resourceMax, x, y, facing                                         |
+-----------------------------------------------------------------------+
| SECTION 3: 2D Replay Spatial & HP Trajectory Stream (Zstd Frame 3)    |
| - Deduplicated movement & HP stream for 60 FPS HTML5 Canvas replay:   |
|   * t[], actor[], x[], y[], hp_pct[] encoded with Delta-Varint        |
|   * Map ID and M+ dungeon pull boundaries                             |
+-----------------------------------------------------------------------+
```

---

## 3. Authentication & Core Parser Engine (`AuthManager` + `WCLProtocolClient` + `CombatStateMachine`)

### 3.0. Authentication & Subscription Session Manager (`WCLAuthManager`)
1. **Login UI & Session Storage:**
   - On startup, the user enters their WarcraftLogs **Email** and **Password** (plus optional v2 `Client ID` / `Client Secret` or direct browser `wcl_session` cookie) in the **Account & Harvester** tab of the web interface.
   - `WCLAuthManager` fetches `/login`, extracts the CSRF `<meta name="csrf-token">`, submits credentials to `POST /login`, captures `wcl_session` and `XSRF-TOKEN` cookies, and persists them locally in `D:\wowlogsparser\data\auth_session.json`.
2. **Subscription & Unarchive Readiness:**
   - Automatically attaches `X-CSRF-TOKEN` / `X-XSRF-TOKEN` and subscriber cookies to `POST /reports/unarchive/{code}` requests so archived Legion logs can be restored on demand.

### 3.1. Layer 1: WCL Network & Protocol Parser (`WCLProtocolClient`)
1. **Rankings & Leaderboard Parser:**
   - Queries WCL rankings for **M+ Season 5 (`zone=9, partition=5`)** across all 13 dungeons and all 36 specs, plus **all 5 Legion Raids (`zones=10, 11, 12, 13, 17`)**.
   - Deduplicates `(report_code, fight_id)` pairs in `index.db` so a single M+ or Raid fight appearing on multiple spec leaderboards is downloaded **only once**.
2. **Zero-Wait Unarchive Warmer:**
   - Maintains a pre-warmed buffer of 25–40 reports by sending `POST /reports/unarchive/{code}` ahead of the download queue.
3. **Report Master & Paginated Event Stream Parser:**
   - Parses `/reports/fights-and-participants/{code}/0` for fight boundaries, actors, pet owners, dungeon pulls, and abilities.
   - Paginates `/reports/events/summary/{code}/{fight_id}/{cursor}/{end_time}?includeResources=true` via `nextPageTimestamp` until 100% of raw events are retrieved.

### 3.2. Layer 2: Analytical Combat Event Parser (`CombatStateMachine`)
Processes all raw events of a fight in a single linear pass $O(N)$ before packing into `.wclz`:
1. **Pet & Guardian Attribution:** Resolves static `petOwner` and dynamic `summon` events so pet/guardian damage and casts (e.g., SEF clones, Xuen, Doomguard, Infernal, Dreadstalkers, totems) are attributed to the owner player.
2. **Aura Interval State Machine:** Combines initial `combatantinfo` auras with `applybuff` / `refreshbuff` / `applybuffstack` / `removebuff` events into exact `[start_ms, end_ms]` intervals, computing Uptime % and capturing active buff snapshots at the millisecond of every cast.
3. **Cast & Channel Timeline Tracker:** Records chronological player casts alongside active buff snapshots and resource levels.
4. **Damage & Healing Attribution:** Computes effective damage (`amount - overkill`), crit counts, DPS/HPS, and healing/absorb breakdowns.
5. **Interrupts & Pre-Death Analyzer:** Extracts all interrupts (`extraAbilityGameID`) and constructs a 15-second incoming damage/healing window prior to every player death.
6. **Spatial Trajectory Builder:** Extracts deduplicated `(t, actor, x, y, hp_pct)` frames for the 2D replay stream.

---

## 4. Offline Viewer Web UI & 60 FPS 2D Replay

Served locally on `http://127.0.0.1:8080` via `FastAPI` and launched via `start.bat` (100% English UI):
1. **Log Explorer:** Filter by Content Type (`Mythic+ Season 5` / `Legion Raids`), Dungeon/Boss, Specialization, and Minimum Key Level.
2. **Single-Fight Analyzer (WCL-Style Tabs):**
   - `Damage Done` & `Healing Done`
   - `Casts & Rotation Timeline` (millisecond cast sequence with active buffs at cast time)
   - `Buffs & Uptimes`
   - `Builds & Talents` (`CombatantInfo` gear, legendaries, talents, artifact traits)
   - `Interrupts & Deaths` (with 15s pre-death breakdown)
3. **Interactive 2D Map Replay (60 FPS HTML5 Canvas):**
   - Reads only Section 3 of the selected fight's `.wclz` file (~10 ms).
   - Renders unit positions, movement interpolation, HP percentages, and playback scrubber (`0.5x` – `4x`).

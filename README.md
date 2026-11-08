# WCL Legion 7.3.5 Archiver & Offline Viewer

Self-contained WarcraftLogs bulk harvester, columnar `Zstd Level 22` single-fight compressor (`.wclz`), $O(N)$ combat log state machine, and local offline viewer with 60 FPS 2D map replay.

## Quick Start

1. **Install dependencies (once):**
   ```bash
   pip install -r requirements.txt
   ```

2. **Launch the server & Web UI:**
   Double-click `start.bat` (or run from terminal):
   ```bash
   python -m uvicorn wcl_archiver.api.app:create_app --factory --host 127.0.0.1 --port 8080
   ```
   Open `http://127.0.0.1:8080` in your browser.

3. **Sign In & Connect Subscription (`Account & Harvester` tab):**
   - Enter your **WCL Email** and **WCL Password** (plus optional **API v2 Client ID & Secret** from `https://www.warcraftlogs.com/api/clients`), or paste your `wcl_session` cookie from your browser.
   - Click **Sign In & Save Session** (saved locally to `data/auth_session.json`).

4. **Harvest Logs (`Account & Harvester` tab):**
   - Click **Start Mythic+ Season 5 (7.3.5) Harvest** to scan and pack all 13 Legion Season 5 dungeons across all 36 specializations.
   - Click **Start All 5 Legion Raids Harvest** to scan and pack all 5 Legion Raids (Emerald Nightmare, Trial of Valor, The Nighthold, Tomb of Sargeras, Antorus).
   - Or paste any specific report URL/code into **Import Single Report Now**.

5. **Browse & Analyze Offline (`Log Explorer` & `Fight Analyzer` tabs):**
   - Filter by Dungeon/Boss, Specialization, or Minimum Key Level.
   - Click **Open Fight** to read only that single fight's `.wclz` file in RAM (~10 ms) without unpacking any other files.
   - Inspect **Damage Done**, **Healing Done**, **Casts & Rotation Timeline**, **Buffs & Uptimes**, **Builds & Talents**, **Interrupts & Deaths**, and **2D Map Replay (60 FPS)**.

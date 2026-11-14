# WCL Legion 7.3.5 Archiver & Offline Viewer

A local WarcraftLogs collector for historical Legion investigations, including TauriWoW bug reports. Each new fight archive is a self-contained compressed file containing every received game-event field, report metadata, supplemental replay records, and provenance. The viewer provides derived fight summaries, damage and healing breakdowns, cast timelines, buffs, builds, deaths, and a 2D replay.

## Install and start

The dependency lock records the installed package versions verified on **Windows with Python 3.13**. `requirements.txt` declares the project's minimum direct dependencies; use the lock for a reproducible setup matching that environment.

From the repository directory in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.lock.txt
python -m playwright install chromium
```

Start the server with `start.bat`, or run:

```powershell
python -m uvicorn wcl_archiver.api.app:create_app --factory --host 127.0.0.1 --port 8080
```

Open [the local viewer](http://127.0.0.1:8080). The launcher and data paths resolve from the repository location, so the checkout does not need to be on a particular drive. The launcher uses the active Python environment and starts without automatic reload. Stop the server before updating its code, then restart it; restarting opens an idle collector and does not automatically start a harvest.

## Connect WarcraftLogs and collect fights

In **Account & Harvester**, select the visible browser or API v2 source. The visible browser can use an installed Chrome/Edge browser; the Playwright Chromium installation provides the fallback browser. Open the browser window, sign in, and complete any requested browser challenge. API v2 credentials can be created at [WarcraftLogs API clients](https://www.warcraftlogs.com/api/clients). Authentication is saved locally in `data/auth_session.json`; the browser uses its local profile.

The **Pages per Spec** field defaults to **2**. Raid collection keeps all five Legion raids, every configured boss and partition, and all four difficulties: Mythic, Heroic, Normal, and LFR. Selecting a specific raid difficulty narrows that request. Setting pages to `0` explicitly requests all available ranking pages. Mythic+ collection remains Legion Season 5, zone 9 / partition 5, across the 13 configured dungeons and 36 specializations. A report URL or code can also be imported directly.

Only one harvest or report import can run at a time. Stop waits for the active collection task to finish its cancellation cleanup; another start or import is rejected while collection or cleanup is active. Shutdown also closes the collection browser and pipeline resources.

Failed work records an attempt count and reason. Retries are bounded within a run. A zero-result packing batch does not finish the job while pending or retryable work remains. Exhausted failures are reported as **incomplete**, with failed report/fight counts. Starting a new collection run makes failed work eligible for another bounded set of attempts.

HTTP 429 and rendered **Too Many Requests** ranking pages apply a shared cooldown across browser navigation, JSON requests, and scan workers. A retry loads the same ranking page. `Retry-After` is honored within bounded delays; otherwise the collector uses exponential backoff. Error pages and unfinished loading placeholders do not certify an empty ranking page. If bounded retries are exhausted, the failure remains explicit and a new collection start permits another attempt.

## What the archive preserves

New collections write **standalone WCLF version 2 (`.wclf`) files**. The fight file is the authoritative record and has no raw-response sidecar dependency:

- Full canonical event objects retain every received key, including unknown fields, nested values, explicit nulls, absent fields, and event ordering. Dynamic columns preserve these distinctions without requiring a predefined event schema.
- Integer and decimal numeric values retain their precision. JSON is parsed with decimal precision before archival; the normalized viewer representation is separate from these full-field records.
- The same file contains full report metadata, supplemental replay records, derived summary data, and compact request/page provenance. Authentication headers and cookies are not provenance.
- Sections are independently compressed with **Zstandard level 9** and protected by SHA-256 checksums. The writer verifies decoded full-event equality before a fight is marked packed. This compression setting favors practical packing time; total throughput and archive size depend on the actual workload.
- Canonical paginated event responses are required. Replay segments are supplemental. Malformed JSON, GraphQL errors, and non-advancing pagination fail collection instead of silently certifying a truncated result.

Response buffering is transient and held in RAM for active work and bounded report metadata caches. New collection does **not** save raw HTTP body text, ranking HTML, error pages, a raw manifest database, or a `data/raw/` tree. Event snapshots are released after a verified write or a failed/cancelled attempt. The archive preserves logical JSON game data; original HTTP whitespace, body bytes, and numeric text spellings are not retained.

The new fight file remains readable and exportable after the collector and its in-memory buffers are gone. Derived analyzer output is separate from the full event fields. Legacy `.wclz` and WCLF version 1 readers remain for compatibility; old WCLF version 1 files may still require their original source sidecars. New collection writes self-contained version 2 files only.

The collector preserves what WarcraftLogs returns through these endpoints. It does not claim access to the uploader's complete original combat-log file, or that every original logging detail is exposed by WarcraftLogs. The final collection size and packing speed depend on the selected reports, event counts, fields, and runtime resources; no fixed final size or full-pipeline speed factor is promised.

## Explore and export

**Log Explorer** defaults to packed fights. Filters include content type, dungeon/boss, specialization, key level, raid difficulty, and stored queue status. Previous/Next controls use stable limit/offset pagination. Only packed rows offer **Open Fight**. The analyzer reads the selected standalone archive rather than opening the entire collection. **Full source records** exposes the archived provenance and logical response records; **Export full event fields** exports the unnormalized canonical events.

The local API exposes the source evidence separately from viewer analysis:

| Endpoint | Result |
| --- | --- |
| `GET /api/fights?status=PACKED&limit=100&offset=0` | Paginated catalogue; `status=all` includes queued, retryable, and failed rows. |
| `GET /api/fights/{fight_uid}/sources` | Manifest and provenance for source records contained in the fight archive. |
| `GET /api/fights/{fight_uid}/sources/{capture_id}` | Reconstructed logical JSON for a source record, read from the standalone file. This is not an export of original HTTP body bytes. |
| `GET /api/fights/{fight_uid}/download` | Download the complete self-contained fight archive. |
| `GET /api/fights/{fight_uid}/full-events` | Full canonical event objects, including all source fields rather than the viewer's normalized event schema. |
| `GET /api/fights/{fight_uid}/raw-events` | Backward-compatible alias for the full-event export; the name does not imply permanent raw-response storage. |
| `GET /api/fights/{fight_uid}/summary` | Derived viewer summary. |
| `GET /api/fights/{fight_uid}/events` | Filtered, normalized viewer events. |
| `GET /api/harvester/status` | Current job state, progress, queue/failure counters, and archive size information. |

`fight_uid` is the report code followed by an underscore and fight ID, for example `aBcDeFg123_2`. Source and full-event exports read the file itself and do not depend on downloader memory or a separate raw store. Legacy viewer-only archives may have no full source records to export.

## Local storage and backups

All collected data stays in the local `data/` directory, which Git ignores. No collection is uploaded by this project. The catalogue, `.wclf` fight files, authentication session, and browser profile are local runtime artifacts rather than repository contents. Source-code publication excludes these local files.

A new WCLF version 2 file alone contains the full records needed to read and export that fight. Back up the **whole data directory while the server is stopped** to preserve the catalogue, collection progress, and account setup as well. Keep authentication and browser profiles private when sharing selected fight archives.

An intentional fresh-collection reset must stop the server and collection workers first. The reset utility removes recognized `mplus_s5/`, `raids/`, legacy `raw/`, and `staging/` collection directories plus catalogue/scan databases, confined to the chosen data directory. It preserves `auth_session.json`, browser profiles, and unrecognized top-level paths. It rejects links and reparse points before deleting anything.

```powershell
# Validate targets and report planned removal counts without deleting:
python -m wcl_archiver.reset_data
# Explicitly perform the reset after stopping the server:
python -m wcl_archiver.reset_data --confirm-reset
```

A reset creates empty catalogues and does not start collection. Leave the server stopped until it is intentionally launched, then begin collection explicitly.

## Verification

```powershell
python -m pytest -q -p no:cacheprovider
node --check wcl_archiver/web/app.js
```

The viewer rendering regression test also uses Node.js. The dependency lock covers the Python runtime and test dependency closure; it does not install Node.js, browser executables, or operating-system components.


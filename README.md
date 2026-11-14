# WCL Legion 7.3.5 Archiver & Offline Viewer

A local WarcraftLogs collector for historical Legion investigations, including TauriWoW bug reports. The archive preserves received source responses before parsing them. The viewer provides derived fight summaries, damage and healing breakdowns, cast timelines, buffs, builds, deaths, and a 2D replay.

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

## What the archive preserves

The authoritative archive is the source-response store under `data/raw/`:

- Original response bodies are captured before JSON parsing or combat-event normalization. HTTP client captures retain decoded response content bytes; browser fetch text is encoded as UTF-8. Browser-rendered rankings are labeled as DOM snapshots.
- SHA-256 addressed Zstandard blobs deduplicate identical response bodies. The SQLite manifest records request context, capture time, response status, report/fight attribution, and provenance. Authentication headers and cookies are not request provenance.
- Received unknown fields, explicit nulls, numeric spellings, and event ordering remain available in the original bodies. Parsing and analysis do not overwrite them.
- Canonical paginated event responses are required to certify a fight's source collection. Replay segments are supplemental. Malformed JSON, GraphQL errors, and non-advancing pagination fail collection instead of silently certifying a truncated result. A source-write failure prevents the fight from being marked packed.

New `.wclf` files contain compressed **derived summaries and references to source captures**. They do not duplicate the complete event stream. Source responses remain independent of derived analysis: received bodies can survive even if a later analysis or packing step fails. Legacy `.wclz` files remain readable as viewer archives, but their normalized event data does not provide the same preservation guarantee as the source archive.

The collector preserves what WarcraftLogs returns through these endpoints. It does not claim access to the uploader's complete original combat-log file, or that every original logging detail is exposed by WarcraftLogs. The final collection size depends on the reports, event counts, available pages, and deduplication; no fixed final-size estimate is promised.

## Explore and export

**Log Explorer** defaults to packed fights. Filters include content type, dungeon/boss, specialization, key level, raid difficulty, and stored queue status. Previous/Next controls use stable limit/offset pagination. Only packed rows offer **Open Fight**. The analyzer reads the selected fight and its required source pages rather than opening the entire collection.

The local API exposes the source evidence separately from viewer analysis:

| Endpoint | Result |
| --- | --- |
| `GET /api/fights?status=PACKED&limit=100&offset=0` | Paginated catalogue; `status=all` includes queued, retryable, and failed rows. |
| `GET /api/fights/{fight_uid}/sources` | The fight's source manifest and capture metadata, including references used for original-body export. |
| `GET /api/archive/responses/{capture_id}` | The original captured response body. Use this to retain exact JSON text, numeric spellings, and response structure. |
| `GET /api/fights/{fight_uid}/raw-events` | Parsed event objects from preserved canonical responses, retaining source fields rather than the viewer's normalized event schema. JSON reserialization is not a byte-for-byte copy of the original response. |
| `GET /api/fights/{fight_uid}/summary` | Derived viewer summary. |
| `GET /api/fights/{fight_uid}/events` | Filtered, normalized viewer events. |
| `GET /api/harvester/status` | Current job state, progress, queue/failure counters, and archive size information. |

`fight_uid` is the report code followed by an underscore and fight ID, for example `aBcDeFg123_2`. Original-body exports are the authority when exact response formatting or number representation matters. Legacy viewer-only fights may have no source manifest to export.

## Local storage and backups

All collected data stays in the local `data/` directory, which Git ignores. No collection is uploaded by this project. The catalogue, source manifest, compressed source objects, `.wclf`/`.wclz` fight files, authentication session, and browser profile are local runtime artifacts rather than repository contents.

Back up the **whole data directory while the server is stopped**. A `.wclf` file alone is not a complete backup: its source references require the matching `raw/manifest.db` and `raw/objects/` files. Keep authentication and browser profiles private when sharing selected source evidence.

An intentional fresh-collection reset must stop the server and collection workers first. Reset only archive/catalogue/scan data confined to the repository's `data/` directory; preserve `auth_session.json` and browser profiles. After a reset, start the viewer idle and begin collection explicitly.

## Verification

```powershell
python -m pytest -q -p no:cacheprovider
node --check wcl_archiver/web/app.js
```

The viewer rendering regression test also uses Node.js. The dependency lock covers the Python runtime and test dependency closure; it does not install Node.js, browser executables, or operating-system components.


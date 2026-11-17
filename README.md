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

In **Account & Harvester**, select the visible browser, API v2, or **Hybrid: API + full browser verification** source. The visible browser can use an installed Chrome/Edge browser; the Playwright Chromium installation provides the fallback browser. Open the browser window, sign in, and complete any requested browser challenge. API v2 credentials can be created at [WarcraftLogs API clients](https://www.warcraftlogs.com/api/clients). Authentication is saved locally in `data/auth_session.json`; the browser uses its local profile.

For archived reports through API v2, connect the subscribed **WCL user account** as well as the API client. Client Credentials alone cannot read archived event data:

1. In the WCL API client's Redirect URLs, register `http://localhost:8080/` exactly. Leave **Public Client** unchecked for this local backend. The dedicated `http://localhost:8080/api/auth/oauth/callback` path also works if registered instead. Use the server's actual port if it differs.
2. Save the Client ID and Client Secret in **Account & Harvester**. Enter the same registered URL in **Registered redirect URL**.
3. Click **Connect WCL account**. The new tab first visits the callback host, then opens the official WCL authorization page. Sign in as the account whose subscription includes archive access and approve the request. The application requests profile permission; private-report permission is not requested.
4. WCL returns to the local application. **Account API** shows the saved user connection separately from the client token. Click **Check archive access** to verify the user API, actual quota, and access to a sample report from the existing catalogue. If there are no saved fights, only API access/quota can be checked. A saved connection alone does not certify an active archive subscription.
5. Select **API v2** when intentionally starting an API collection. Connecting the account does not start collection or change a running browser job.

The Authorization Code callback has a ten-minute, one-time state bound to the initiating browser. Both `localhost` and `127.0.0.1` are supported; the start navigation sets its HttpOnly cookie on the redirect's host so the aliases do not break the return flow. Denied, expired, mismatched and replayed callbacks fail explicitly. User tokens and refresh tokens are stored only in ignored local runtime data, through an atomic filesystem publication. Refresh is shared by concurrent requests and rotates tokens when supplied by WCL. An expired or rejected user grant does not silently downgrade to client or browser access. **Disconnect WCL account** retains the client key and browser session and requires collection to be idle.

User-authorized report metadata and canonical events go directly to `/api/v2/user`. Event pages request 20,000 records and resource fields. Full received logical JSON is captured with decimal precision; API and browser fields have not been certified equivalent until compared on the same live report. A shared API quota gate honors application exhaustion and bounded HTTP 429 retries. Workers wait cancelably for known quota cooldowns without marking queued reports as failed; the status panel shows seconds until retry. A manual archive-access check has its own bounded timeout. Ranking database quotas and source permissions can also prevent requests even when application points remain.

**Strict hybrid** requires the connected WCL user account and a signed-in browser session. It uses three independent phases: API acquisition, browser enrichment, then packing. An API worker saves its complete capture to disk as **API_STAGED** and immediately takes another fight. A separate browser pool loads waiting snapshots from disk, obtains complete browser metadata, canonical pages and supplemental replay segments, and verifies that fight boundaries, event counts, order and identities match. Only a verified combined capture becomes **STAGED** for packing. API workers do not wait for browser startup, browser downloads or compression. The selected worker count controls each network pool independently: 32 means up to 32 API fights and 32 browser enrichments, with the existing shared browser request limit of 64. Waiting snapshots occupy disk rather than an in-memory queue; disk usage can grow when the browser is slower.

The full event export fills missing API fields recursively from browser canonical and replay records. Existing API values remain unchanged; differing source values and their field paths are recorded as conflicts. Every original logical response from both providers stays exportable from the standalone archive, including unknown fields and complete browser report metadata. Mismatched or incomplete sources fail enrichment and retain the API snapshot for retry. A later hybrid run resumes that snapshot without downloading the API events again. Strict preservation still requires all browser canonical pages; this mode does **not** promise fewer browser requests or smaller archives than browser-only collection. Connecting the account does not switch a running collection to hybrid automatically.

The **Pages per Spec** field defaults to **2**. Raid collection keeps all five Legion raids, every configured boss and partition, and all four difficulties: Mythic, Heroic, Normal, and LFR. Selecting a specific raid difficulty narrows that request. Setting pages to `0` explicitly requests all available ranking pages. Mythic+ collection remains Legion Season 5, zone 9 / partition 5, across the 13 configured dungeons and 36 specializations. A report URL or code can also be imported directly.

For browser rankings, collection ends after the current page when there are fewer than **70 visible log rows**, no paginator, or no enabled Next control. Rows are counted before duplicate fights are merged. A recognized empty/private-results message is a successful terminal page. Successfully scanned pages and terminal boundaries are saved in SQLite and reused after stopping, restarting the harvester, or restarting the server; starting a job does not clear that history.

Only one harvest or report import can run at a time. Stop waits for the active collection task to finish its cancellation cleanup; another start or import is rejected while collection or cleanup is active. Shutdown also closes the collection browser and pipeline resources.

Automatic downloading gives queued **Mythic+ priority over raids** at both report preparation and fight downloading. Discovery runs independently: raid ranking pages continue to be scanned and queued while Mythic+ fights download. Discovery does not wait for the Mythic+ backlog to finish. Within each download category, fights are dispatched in their SQLite insertion order; duplicate discoveries and status updates do not move them. A report is prepared according to its oldest remaining fight, rather than its code or its first-ever discovery. An older fight awaiting report preparation also holds its FIFO position through the existing bounded retries. Failed records remain visible and can be retried on a later run. Parallel workers can finish at different times; FIFO governs dispatch. A direct report import remains scoped to the explicitly requested report.

Failed work records an attempt count and reason. Retries are bounded within a run. A zero-result packing batch does not finish the job while pending or retryable work remains. Exhausted failures are reported as **incomplete**, with failed report/fight counts. Starting a new collection run makes failed work eligible for another bounded set of attempts.

Exhausted ranking-page failures remain visible without cancelling unrelated fight downloads. The activity log reports download time separately from packing and verification time, so a long network wait is distinguishable from local archive processing.

The **Download workers** field accepts **any positive whole number without a fixed upper cap**, defaulting to **4**. Apply saves the setting locally in `data/harvester_settings.json` and changes it during a running collection. Automatic collection keeps a continuous queue: when one worker stages its completed fight on disk, that slot immediately takes the next eligible fight without waiting for the other downloads. Increasing the count opens additional slots; decreasing it lets active downloads finish and admits new work only below the new limit. Browser JSON requests share a limit of twice the selected count, so 64 download workers allow up to 128 JSON requests. Resizing preserves active requests, waiting requests, and the shared rate-limit cooldown. Discovery has its own workers and continues independently.

Report preparation runs in a separate bounded pool, so ready fights can download while upcoming reports are being unarchived. Preparation looks ahead by a limited number of fights rather than warming the whole catalogue. Mythic+ priority and FIFO still govern dispatch; an older report awaiting preparation can hold the position of younger fights. Network response time, source availability, and rate limits can therefore leave some slots idle.

The activity log identifies the download worker at each start, completion and error, for example `Started downloading ReportCode_12 (download worker #7; encounter-slug)`. Worker numbers identify active slots and are reused for their next fights. The status panel reports active downloads and report preparations separately from packing. A Start entry means the worker has taken the fight and is fetching its metadata/events; it does not wait for the full download to finish before logging.

Each browser fight crawls two disjoint time windows. Each window follows every canonical pagination cursor; supplemental replay segments download alongside those pages. Captures are validated and restored to their requested page order before packing. This reduces sequential network waits while preserving complete canonical records, including fields that replay segments do not contain. The shared 429 cooldown still applies to every request; the network and WCL limits determine actual throughput. The browser summary endpoint returns roughly 300 events per page. The OAuth API v2 path requests 20,000 events per page; the tested historical report returned about 10,000, so the requested limit is not a guaranteed page size. API access to archived reports requires the user authorization described above.

Downloading and packing have separate queues. A completed download is written to a verified temporary snapshot on disk and becomes **STAGED**. Four background packing workers consume staged fights while free download slots continue with subsequent fights. The status panel shows temporary raw file count and size separately from final archive size, along with active packing workers. Increasing download workers does not increase the packing worker count. If downloading outpaces packing, temporary disk usage grows until packers catch up; downloading does not wait for compression.

HTTP 429 and rendered **Too Many Requests** ranking pages apply a shared cooldown across browser navigation, JSON requests, and scan workers. A retry loads the same ranking page. `Retry-After` is honored within bounded delays; otherwise the collector uses exponential backoff. Error pages and unfinished loading placeholders do not certify an empty ranking page. If bounded retries are exhausted, the failure remains explicit and a new collection start permits another attempt.

## What the archive preserves

New collections write **standalone WCLF version 3 (`.wclf`) files**. The fight file is the authoritative record and has no raw-response sidecar dependency:

- Full canonical event objects retain every received key, including unknown fields, nested values, explicit nulls, absent fields, and event ordering. Dynamic columns preserve these distinctions without requiring a predefined event schema.
- Integer and decimal numeric values retain their precision. JSON is parsed with decimal precision before archival; the normalized viewer representation is separate from these full-field records.
- The same file contains full report metadata, supplemental replay records, derived summary data, and compact request/page provenance. Authentication headers and cookies are not provenance.
- Identical canonical and replay columns are stored once inside the file and referenced by both streams. Different fields, presence bitmaps, and differing event streams remain complete. Exact integer deltas are selected only when they compress better; numeric values are never rounded.
- The default **compact** profile uses **LZMA preset 6**, retaining Zstandard level 9 for a section when it produces a smaller result. The **fast** profile uses Zstandard level 9 with the same lossless shared-column encoding. Sections have SHA-256 checksums, and every reconstructed logical section is verified before publication. The compact profile trades a few seconds of additional compression work for smaller files; total throughput and size depend on the workload.
- Canonical paginated event responses are required. Replay segments are supplemental. Malformed JSON, GraphQL errors, and non-advancing pagination fail collection instead of silently certifying a truncated result.

Completed downloads are staged as uncompressed, checksummed MessagePack snapshots in `data/staging/*.wcls`. Each snapshot preserves the complete captured logical responses and packing metadata, with event arrays stored once. Publication uses a verified temporary file, flush/fsync, and atomic replacement before releasing the completed capture from downloader memory. Active downloads and active packers still require RAM; the waiting packing queue lives on disk.

A snapshot is deleted only after the final standalone archive passes verification and its **PACKED** catalogue update succeeds. During hybrid enrichment, the original API snapshot remains until a separately published, verified combined snapshot has been linked in the catalogue; packers cannot consume an API-only pending snapshot. Stop waits for active writers and keeps queued snapshots. A later start recovers valid staged files, including snapshots written just before an interrupted catalogue update, without downloading their events again. Packing failures retain their snapshots for retry; corrupt snapshots remain on disk and are reported as failures. Cleanup of a leftover snapshot checks the final archive and matching source-attempt identity first.

New collection does **not** save original raw HTTP body text, ranking HTML, error pages, a raw manifest database, or a `data/raw/` tree. The archive and temporary snapshots preserve logical JSON game data; original HTTP whitespace, body bytes, and numeric text spellings are not retained.

The new fight file remains readable and exportable after the collector and its in-memory buffers are gone. Derived analyzer output is separate from the full event fields. WCLF version 2 files remain fully readable. Legacy `.wclz` and WCLF version 1 readers remain for compatibility; old WCLF version 1 files may still require their original source sidecars. New collection writes self-contained version 3 files only.

To favor packing speed, set `WCL_ARCHIVE_COMPRESSION=fast` before starting the server. The default is `compact`; no API credentials are required for either storage profile.

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
| `POST /api/harvester/settings` | Save/apply `{"download_concurrency": 64}`; accepts positive integers with no fixed upper cap. Status also reports request limit, staged fight count, temporary raw file count/bytes, and active pack workers. |

`fight_uid` is the report code followed by an underscore and fight ID, for example `aBcDeFg123_2`. Source and full-event exports read the file itself and do not depend on downloader memory or a separate raw store. Legacy viewer-only archives may have no full source records to export.

## Local storage and backups

All collected data stays in the local `data/` directory, which Git ignores. No collection is uploaded by this project. The catalogue, `.wclf` fight files, authentication session, and browser profile are local runtime artifacts rather than repository contents. Source-code publication excludes these local files.

A standalone WCLF version 2 or 3 file alone contains the full records needed to read and export that fight. Back up the **whole data directory while the server is stopped** to preserve the catalogue, collection progress, and account setup as well. Keep authentication and browser profiles private when sharing selected fight archives.

Existing standalone archives can be repacked with the local server stopped:

```powershell
# Inspect the catalogue without changing stored fight data:
python -m wcl_archiver.repack_data
# Repack verified smaller copies and update only catalogue byte counts:
python -m wcl_archiver.repack_data --apply --compression compact
```

Repacking preserves all four logical sections, full source responses, and scan/queue history. A candidate replaces its original only after exact logical verification and only when it is smaller. Failed candidates leave the original intact. Use the updated application to open version 3 files.

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


# Visible-Browser Harvester — Implementation Plan

Goal: harvest WarcraftLogs rankings and reports by driving a **real, visible browser window** that the user controls for login and for any Cloudflare challenge. No API v2 keys, no Battle.net link.

## Scope and hard rule

- The browser is **headed** (visible). The user solves every Cloudflare / Turnstile challenge and logs in manually.
- Do NOT add: headless mode, fingerprint/UA spoofing, stealth plugins, challenge solvers, or copying `cf_clearance` into `httpx`.
- When a challenge is detected, the code only **pauses and waits** for the user, then continues.

## Why the current approach fails

- `httpx` requests to `/zone/rankings-table/...` return `403` + "Just a moment..." (Cloudflare Turnstile).
- `/api/v2/client` requires OAuth (Client ID/Secret), which the user does not want.
- A real browser session that the user has already cleared is the only path left.

## Architecture

```
FastAPI app ──► HarvesterPipeline ──► BrowserClient (Playwright, headed, persistent profile)
                                          │
                                          ├─ fetch_rankings_fight_refs()   (HTML table parse)
                                          ├─ fetch_report_master_and_fights()
                                          └─ fetch_all_fight_events()
```

`BrowserClient` exposes the **same method names/return shapes** as `WCLProtocolClient`, so `HarvesterPipeline` needs almost no changes.

## Steps

### 1. Dependencies
- `pip install playwright` then `playwright install chromium`.
- Add `playwright` to `requirements.txt`.

### 2. `wcl_archiver/wcl_client/browser_client.py`
- Class `BrowserClient(profile_dir, min_delay=2.0, max_delay=5.0, on_status=None)`.
- `async start()`: `playwright.chromium.launch_persistent_context(user_data_dir=profile_dir, headless=False)`; keep one page open. Persistent profile keeps the login and cleared-challenge state between runs.
- `async stop()`: close context and playwright.
- `async _goto(url)`:
  1. `await page.goto(url, wait_until="domcontentloaded")`.
  2. `await self._wait_if_challenge()`.
  3. Sleep `random.uniform(min_delay, max_delay)` (slow, polite pacing).
- `_wait_if_challenge()`: detect challenge when the title contains "Just a moment" or the URL/DOM contains `challenges.cloudflare.com`. While detected: call `on_status("Solve the challenge in the browser window")`, poll every 2 s, no timeout (or a long one, e.g. 30 min), honour a stop flag.
- `_wait_if_logged_out()`: if the page shows the login form, report "Please log in in the browser window" and poll until the user logs in.

### 3. Rankings parser
- URL: `https://www.warcraftlogs.com/zone/rankings/{zone}#boss={encounter}&class={Class}&spec={Spec}&partition={partition}&page={page}` (this is the URL visible in the browser; verify the exact hash/query format in DevTools).
- The table is rendered by JS, so wait for the table rows: `await page.wait_for_selector("table.rankings-table tbody tr")` (confirm the real selector in DevTools).
- Extract per row: report link `a[href*="/reports/"]` → regex `/reports/([A-Za-z0-9]+)(?:#|\?).*fight=(\d+)`; also key level, duration, and player name.
- Return a list shaped like the API v2 output: `{"report": {"code": ..., "fightID": ...}, "name": ..., "keystoneLevel": ..., "duration": ...}`.
- End of pages: empty tbody or no rows → return `[]` (pipeline already stops on empty when `pages_per_spec == 0`).
- Save a sample of the rendered HTML to `tests/fixtures/rankings_page.html` for the unit test.

### 4. Report / fight data
- Easiest first version: use the browser context's own network: `await context.request.get(url)` (shares cookies/session of the visible browser, so it runs inside the user's cleared session). Try the same JSON endpoints already used in `protocol_parser.py`:
  - `/reports/fights-and-participants/{code}/0`
  - `/reports/events/summary/{code}/{fight}/{start}/{end}/...`
  - `/reports/unarchive/{code}` (POST)
- If a request is answered with a challenge page, call `_wait_if_challenge()` on the visible page and retry.
- Keep the return shapes identical to `WCLProtocolClient`.

### 5. Pipeline wiring
- `HarvesterPipeline` takes a `client` that implements the shared interface; no logic change expected.
- Add `rankings_source: "browser" | "api"` choice in `create_app` (`wcl_archiver/api/app.py`); the app creates one `BrowserClient` lazily.
- New endpoints: `POST /api/browser/open` (start the window), `POST /api/browser/close`, and include `browser_state` (`not_started | waiting_login | waiting_challenge | ready`) in `/api/harvester/status`.

### 6. UI (`index.html`, `app.js`)
- Add "Mode" select: `Visible browser` / `API v2`.
- Add "Open Browser Window" button and a status line showing `browser_state` with a highlighted banner when the user action is needed.
- Keep the progress bar, log, Stop button and "0 = ALL pages".

### 7. Tests
- Parser test: feed `tests/fixtures/rankings_page.html` to the extraction function (pure function, no browser) and assert report codes and fight IDs.
- Challenge-detection test: unit-test `is_challenge_page(title, url, html)` with the real "Just a moment..." snippet.
- Pipeline test with a fake `BrowserClient` implementing the interface.
- Run `pytest -v`; all existing tests must still pass.

### 8. Manual verification
1. `start.bat`, open the UI, choose "Visible browser", click "Open Browser Window".
2. In the opened window log in to WCL and solve any challenge yourself.
3. Start the M+ harvest with pages = 1 and confirm in the log that fights are queued and `.wclz` files are produced.
4. Interrupt with Stop; confirm the harvester ends cleanly.
5. Re-run with pages = 0.

## Risks

- Cloudflare may re-challenge frequently; the design tolerates that by pausing, but long unattended runs may stall.
- Selectors and the hash URL format change over time; keep them in one place (constants at the top of `browser_client.py`).
- Very large crawls are slow by design; keep the delays and do not parallelise pages.

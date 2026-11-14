# Lossless Archive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans task-by-task.

**Goal:** Capture historical source data without normalization loss, fix supplied bugs, and reset old collection for two-page raid discovery.
**Architecture:** Immutable response blobs plus a provenance manifest are authoritative; WCLZ remains a derived viewer cache. Clients capture before parsing and validate pagination; database queue state drives completion.
**Tech Stack:** Existing Python/FastAPI/SQLite/Playwright, Zstandard, SHA-256.
**Spec:** docs/superpowers/specs/2026-11-14-lossless-archive-design.md

## Global Constraints
- Preserve all received fields and source response text/bytes, null/absent distinctions and number spellings.
- Preserve dirty source work and authentication/browser profiles; do not commit or push.
- Reset archives/catalogue only after verification, with local server quiescent and resolved paths confined to data.
- New raid default is 2 pages; retain all raid difficulties and partitions and M+ season 5.

## Review Focus
- Source capture failing must prevent a fight being marked packed.
- Non-advancing pagination, malformed JSON and GraphQL partial/error replies must not silently truncate.
- Stop/start/import must not run concurrent workers sharing mutable state.
- Exhausted retry budgets must report incomplete and permit retry on next start.
- Reset must not follow directory links or delete auth/browser profiles.

### Task 1: Historical source storage and network integration
Files: new codec/raw_archive.py, tests/test_raw_archive.py; modify both WCL clients, API wiring and pipeline integration.
- [x] RED: byte-exact response roundtrip including unknown fields, nulls, large numeric literals; dedup; corruption; capture error blocks parsing; canonical pagination mandatory.
- [x] Implement RawArchive capture/read/list APIs, verified atomic blob writes and SQLite provenance.
- [x] Capture browser/httpx bodies before parsing; validate page cursor and errors; retain replay as supplemental source.
- [x] Expose source evidence/export in API; source archive is independent of derived analysis failures.
- [x] GREEN: targeted tests plus full suite.

### Task 2: Queue and accurate metadata
Files: db/schema.py, harvester/pipeline.py, tests/test_db_schema.py, tests/test_harvester_pipeline.py.
Interfaces: mark_fight_packed(..., duration_ms=None); search_fights(..., offset=0); get_queue_counts() -> pending/retryable/failed counters; retry_failed_work() resets bounded retry budgets at explicit new job; drain_queue_batch remains int for compatibility but callers consult counts.
- [x] RED: exact duration; first failed batch with later pending work; transient failures recover; permanent failures bounded with saved reasons; stable pagination.
- [x] Implement persistent failure counts/reasons and fair bounded retry selection; exact duration propagation.
- [x] GREEN: targeted and full suite; describe new interfaces to API implementer.

### Task 3: Viewer/job lifecycle and defaults
Files: api/app.py, web/app.js, web/index.html, config.py, start.bat, tests/test_viewer_api.py.
- [x] RED: packed-only default, page 2 accessible, nonzero queued after zero batch does not finish, exhausted failures report incomplete, concurrent job rejected, default request discovers two raid pages.
- [x] Integrate Task 2 interfaces; own and cancel/await worker on stop/shutdown; status exposes failure counts and persistent source size.
- [x] Add explorer pagination/status controls, source export link and two-page default; use portable paths.
- [x] GREEN: targeted/full suite and JS syntax/browser smoke.

### Task 4: Review, documentation, verified reset
Files: README.md, requirements lock, new reset utility and tests; task ledger under .superpowers/sdd/2026-11-14-lossless-archive/.
- [x] Document source vs cache, completeness limits, API, dependencies and restart.
- [x] Fresh independent review; fix material findings with RED/GREEN tests.
- [x] Test reset confinement/preservation, then stop confirmed server and perform authorized reset; report exact removed files/bytes and zero DB counts.
- [x] Restart viewer idle; verify API defaults, empty catalogue and preserved credentials/profile.

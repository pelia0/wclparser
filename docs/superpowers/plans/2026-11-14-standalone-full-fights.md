# Standalone Full-Field Fights Implementation Plan

> Required execution: superpowers:subagent-driven-development; tests first; preserve prior code.

**Goal:** Replace permanent raw-response storage with self-contained, full-field, fast compressed fight files and correct429 retry handling.
**Spec:** docs/superpowers/specs/2026-11-14-standalone-full-fights-design.md
**Constraints:** all game fields retained, no raw sidecars, reset authorized, credentials/profiles preserved, no auto-start.

## Task 1: Standalone archive (queue_fixes)
- [x] New codec/fight_archive.py: FightArchive(root), capture/read_body/capture_info/fight_capture/complete_fight/write_fight/stats compatible enough for current clients; in-memory capture only (no mkdir/database/blobs). Ignore ranking HTML and non200 responses. Capture bodies parsed with Decimal precision; retain original full report objects.
- [x] PackedFightReader(path) read_summary_only/read_raw_events/read_events_only/read_replay_only/source_manifest/read_source_response methods; direct file-only operation.
- [x] WCLF2 separate compressed sections, hashes and full-schema dynamic columns; unknown/null/missing/nested/large numeric roundtrip; no event duplication in metadata templates.
- [x] Tests RED/GREEN for self-contained copy after buffers discarded, numerical/field preservation, corruption, concurrent capture contexts, and released buffers after verified packing. Zstd9 is default (observed ~0.15s per130k events for columns).

## Task 2: Browser429 (viewer_fixes)
- [x] RateLimitError, navigation HTTPstatus+RetryAfter backoff and shared cooldown, rendered HTML/AJAX429 detection; script translations excluded.
- [x] Invalid/loading pages raise, explicit empty rankings valid. Bounded retries always same URL, exhausted failures visible and retryable next job. RED/GREEN + existing browser suite.

## Task 3: Integration (root)
- [x] Wire FightArchive instead of RawArchive in app; reader dispatch WCLF1/2; file-only exports/download link; accurate storage status; docs remove permanent/raw source promises.
- [x] Source archival survives derived analyzer failure; stop/cleanup releases in-memory work. Regression tests plus fullsuite.
- [x] Independent review with emphasis all-field persistence, no hidden raw dependencies, and false429 emptiness.

## Task 4: Verified reset and delivery (root)
- [x] Stop confirmed local server/browser. Reset recognized collection paths, verify zero fights and no raw/staging directories, auth/profile unchanged.
- [x] Smoke new isolated archive/API/browser. Final checks, commit/push source-only changes (prior explicit user request); leave server stopped for user.

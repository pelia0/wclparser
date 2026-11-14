# Parallel full event download

## Problem and evidence

Live harvesting takes 88–213 seconds to download a fight, then 2.5–6.8 seconds to pack and verify it. Four active fights each crawl hundreds of sequential summary-event pages of roughly 300 events, after obtaining the supplemental replay stream.

Replacing canonical pages with replay events was investigated and rejected. An exhaustive comparison of 52 saved fights and 5,297,497 canonical events found two fights with canonical-only death fields and one replay stream missing 8,283 events. Boundary sampling missed interior differences. No sampled replacement will ship.

The browser summary endpoint ignores `limit=10000` and `count=10000`. The current session has no OAuth API credentials. The existing GraphQL path accepts a 10,000-event limit but is not available to this browser session.

## Implementation

- Keep every canonical summary-event page and every supplemental replay segment. Preserve unknown fields, Decimal precision, source responses, and the single-file archive format.
- For the in-memory FightArchive only, crawl two disjoint inclusive integer time windows per fight, `[start, midpoint]` and `[midpoint + 1, end]`. Each window follows every canonical pagination cursor until termination. Preserve original order within each page and concatenate windows by requested range.
- Sort canonical captures by requested cursor before packing; validate page/report/fight/range bindings and complete contiguous cursor chains. This makes asynchronous capture completion safe for standalone source reconstruction.
- Run the complete canonical and replay streams concurrently, with cancellation and failure awaiting every sibling task before capture buffers are released.
- Keep four fight workers and four pack workers. Give their eight canonical streams a shared ceiling of eight browser requests. Existing shared HTTP 429 cooldown and same-request retries still apply.
- Reuse the archive's parsed response body instead of parsing the same JSON twice. Preserve exact Decimal values.
- Leave legacy archive acquisition compatible. Preserve all current files, queue, scan history, and authentication; no conversion or reset.

## Verification and rollout

Use deterministic tests for complete multi-page streams, timestamp groups and midpoint boundaries, empty windows, missing fields, malformed or stalled cursors, failure and cancellation, source reconstruction, and shared concurrency. Compare one full live parallel download against its existing archived canonical and replay records, including every field/value/order. Record actual network and packing times; do not extrapolate small cached-request timings into throughput claims. Run the full suite and independent review. Commit and push source only under existing authorization; stop the local server and leave startup to the user.

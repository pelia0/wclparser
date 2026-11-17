# Continuous downloads implementation plan

**Goal:** Keep every available download slot busy with the next FIFO fight, independently of slow downloads, report preparation and disk packing. Identify the worker in activity logs.

**Architecture:** Automatic discovery/drain calls opt into a persistent scheduler. It owns at most the configured number of active fight downloads and a bounded independent report preparation pool. Completed slots are refilled without waiting for other slots. Existing finite batch/import APIs retain their synchronous contract. All downloading shares the existing durable staging helper, source capture, request limiter and cooldown.

**Constraints:** Mythic+ priority, FIFO dispatch, full source preservation, raw deletion only after verified PACKED commit, discovery independence, bounded retries and manual server restart remain. Live resize never cancels acquired work. No production data edits or server interruption.

- [x] Add failing regressions: blocked first fight does not prevent third fight starting on worker #2; ready work starts while preparation of a younger report is blocked; logs identify the worker before network completion.
- [x] Extract one-fight durable staging; use numbered finite workers for imports and a persistent refill scheduler for automatic collection. Bound report lookahead, preserve UID claims and propagate scheduler errors.
- [x] Integrate scheduler cancellation before packing cleanup, expose active download/preparation counters and apply resize on the next free slot. Test stop during staging, retries, priority, discovery arrivals and limits.
- [x] Add an index for pending-Mythic+ existence checks and verify probes skip 5,000 packed keys and 5,000 queued raids. Update automatic callers and activity log text/documentation.
- [x] Run targeted and complete tests and review final diff. Source-only publication excludes runtime data; live acceptance and server restart remain with the user.

## Verification

Full suite: 341 passed. Focused tests cover individual slot refill across the old batch boundary, ready downloads during report preparation, start logs before blocked network requests, live resize without cancellation, newly discovered keys taking the next slot, discovery arrivals while idle, repeated stop during a blocked staging thread, bounded failures and scheduler restart after a transient catalogue error. Independent review reproduced and verified the cancellation fix and approved the scheduling/lifecycle changes. A mocked headless browser checks active download/preparation counters and worker-labelled activity entries. The running collector and real data were left untouched; no live throughput claim is made for the new code before manual restart.

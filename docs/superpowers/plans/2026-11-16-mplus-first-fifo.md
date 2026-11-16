# Mythic+ first, insertion-ordered collection

User priority: collect Mythic+ first using the friend's subscription; keep raid work for later. Within each category, dispatch by insertion order rather than report code, key level, duration, or retry count. Preserve all existing records and archives.

The current reports and fights are ordinary SQLite rowid tables. Enqueue uses INSERT OR IGNORE, updates do not move rows, and the application contains no table rebuild, VACUUM, or individual fight/report deletion. Existing rowids retain the insertion sequence, so no data rewrite or queue-column backfill is needed.

- [x] Regression tests for both selection stages: reverse-alphabetical IDs, unrelated key levels/durations, old report with newly added fights, duplicate enqueue/reopen, bounded retries, ready raids while Mythic+ reports are pending, and transition to raids after all eligible Mythic+ work finishes.
- [x] Add read-only scheduling queries using pending fights as the source of order. If any eligible Mythic+ fight exists under a nonfailed report, restrict both report preparation and fight dispatch to Mythic+. Sort by fight rowid and deduplicate report codes in that order. Preserve the explicit report-import scope. Keep existing retry budgets and stored reasons.
- [x] Avoid a data migration or additional index: actual catalogue selection measured approximately 13 ms for report preparation and 9 ms for ready fights. Whole database/auth/master file hashes and all 3924 archive byte hashes remained unchanged after the actual read-only proof.
- [x] Full suite, independent review, read-only proof on actual catalogue, source-only commit/push. Leave the server stopped for the user's manual start and recommend Mythic+ mode to avoid spending scan requests on raids.

The actual-catalogue proof selected the oldest 25 pending Mythic+ report groups. Preparing only those reports in an in-memory mirror produced 25 Mythic+ fights in original insertion order, while ready raid fights remained blocked. No original report or fight status was changed. Independent review found and then verified the fix for a batch-size-25 retry bypass: dispatch now stops at the oldest eligible fight still awaiting report preparation. The focused suite passed 50 tests.

The full suite passed 259 tests with two existing dependency deprecation warnings. JavaScript syntax and whitespace checks passed. No schema migration, new index, queue reset, record rewrite, or archive change was required.

Parallel workers may finish out of order; FIFO describes selection and dispatch. Retryable old work retains its place until its existing bounded retry budget is exhausted. Failed jobs remain recorded and a new run can retry them.

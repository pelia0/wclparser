# Hybrid responsiveness under concurrent collection

Trigger: live user collection with 32 API and 32 browser workers. Status requests repeatedly exceeded 8, 15 and 30 seconds. Read-only database inspection confirmed API snapshots continued appearing; browser and packing thread stacks were active. A ten-second nonblocking py-spy profile identified repeated synchronous HTTPX SSL trust-store initialization on the application loop and per-leaf MessagePack comparison in source validation workers.

Scope: reuse the existing verified HTTPX SSL context across a protocol client's request clients without changing authentication, trust-store configuration or request ownership. Preserve exact encoded-value semantics while avoiding repeated MessagePack serialization for equal basic JSON scalar types. Stop doing disk/DB statistics collection on every activity-log entry; status polling remains authoritative. Do not change configured concurrency, production data, API grants or live server lifecycle.

- [x] Add failing regression and implement protocol SSL context reuse; preserve configured certificate trust and transport behavior.
- [x] Add typed/Decimal/null/cross-type equality regressions and optimize exact leaf comparisons with compatibility fallback.
- [x] Remove synchronous statistics collection from activity logging, keeping status counters available through their existing endpoint.
- [x] Run focused and full regression checks; publication is source-only under existing authorization. User controls production restart.

Verification: **605 tests passed**, with two existing dependency deprecations. Independent review found no P1/P2 issues; 168 focused tests passed independently. Actual default HTTPX transports load the certificate trust store once across three request clients, preserving CERT_REQUIRED, hostname validation, SSL_CERT_FILE and SSL_CERT_DIR. Trust configuration is captured when the protocol instance is constructed at application startup.

On a saved 59,710-event browser archive, the full canonical/replay merge took 5.4108 seconds with the previous comparator and 2.5332 seconds with the optimized comparator in one local measurement. The full merged data and conflict encodings had identical SHA-256 hashes; the original archive SHA-256 stayed unchanged. This proves equality for that input, not live collection throughput. Synthetic benchmark results are separate and must not be presented as server latency.

During live read-only inspection, the catalogue advanced from 7,074 to 7,075 packed fights; a later snapshot had 25 API_STAGED fights, six STAGED fights and 22 retryable fights. Three failed reports (twelve dependent fights) returned the explicit WCL API error `You do not have permission to view this report.` Those failures do not establish missing or corrupted data. API snapshots and source files were not edited or deleted. The existing server was not restarted, so improved live status latency remains unverified until it loads the update.

# Selective hybrid enrichment implementation plan

**Goal:** API remains the full primary event source; browser enrichment stops downloading the full canonical stream and obtains only report metadata, replay segments, and canonical timestamp windows for deaths or events not covered by replay.

**Architecture:** Retain independent API_STAGED and browser worker pools. Add an explicitly versioned browser supplement capture, rather than fabricating full canonical completeness. All received source fields/responses remain lossless in WCLF3. Required timestamp windows and replay coverage are validated against API event identities, including duplicate occurrences. Existing strict hybrid archives remain readable. No automatic full canonical fallback is allowed. Failed validation retains the API snapshot.

**Evidence and limits:** Eleven saved real browser fights across Mythic+ and every raid difficulty had full replay identity coverage. Canonical fields absent from replay occurred only on death records in one fight (killer/killingAbility/deathSave fields). This is empirical coverage, not a universal undocumented endpoint contract. API/browser endpoints do not expose a documented field projection; replay responses still include whole event records. The implementation must not label preservation of arbitrary unseen fields from unfetched canonical responses as proven. Full original responses actually obtained must remain exact. Existing archives, server state and production catalogue stay unchanged during verification.

## Tasks

- [x] Codec: introduce `complete_supplement(report_code,fight_id,fetch_id,start,end,api_events)`, `supplement_windows(api_events,replay_events)`, validate explicit `source_mode=browser_supplement_v1`, full replay range and exact required canonical windows; mode-aware merge/full view with exact typed originals, fields/conflicts, duplicates, corruption checks and backwards compatibility.
- [x] Browser: introduce `fetch_fight_supplement(report_code,fight_id,start,end,api_events)` on its owned browser loop/capture; download replay first, calculate required single-timestamp windows, fetch only those windows, complete supplement; require valid boss metadata and fail incomplete captures without canonical fallback. HybridClient calls this method and retains off-thread merge/cancellation ownership.
- [x] UI/docs: replace misleading full browser verification copy with selective enrichment, expose source strategy in provenance, preserve independent worker controls and lifecycle.
- [x] Proof: use saved original canonical data as an independent oracle for selective reconstruction, all fields including nested/unknown values and original death-specific fields; compare real API and real selective browser sources and exact source response exports. Report limits honestly.
- [x] Full suite and independent review. Publication is source-only under existing authorization. User owns server restart.

## Verification outcome

- Full suite: **560 passed**, two existing dependency deprecation warnings. JavaScript syntax and headless Chrome UI controls passed.
- Live API plus live selective browser for one 59,710-event Mythic+ fight: full merged fields exactly matched an independent saved full-canonical/replay oracle. All 47 actually received logical source responses survived disk staging, packing and fresh reader reconstruction exactly. Original archive SHA-256 stayed unchanged; temporary proof snapshots were deleted after verification.
- Browser requests: **1 master + 6 replay + 33 timestamp windows = 40**, versus the saved **1 master + 6 replay + 198 canonical pages = 205**. Final file: **1,073,679 bytes**, compared with **1,643,073 bytes** for the previous strict hybrid. API acquisition: 3.788 seconds; selective browser acquisition including merge: 14.582 seconds; packing: 7.749 seconds. One-fight observations do not establish a collection-wide throughput factor.
- Offline full-field reconstruction on eleven saved browser fights also matched the full-source oracle, using an identity-only synthetic primary to ensure fields came from replay/targeted supplements. These eleven checks are offline source-coverage checks, not eleven live API/browser downloads.
- Review corrections: selective completion rejects replay-only identities and excess occurrences; codec planning and completion run in owned threads so other browser requests continue, with repeated cancellation draining writers before capture cleanup. Existing strict archive compatibility remains intact.
- Unfetched arbitrary canonical fields remain outside the empirical guarantee. Duplicate identity association retains the existing occurrence-order assumption. Neither limitation is concealed by a full-browser-completeness marker.

## Expanded live verification requested by the user

Twelve selected fights from six reports passed: two Mythic+ reports plus one raid report each at Mythic, Heroic, Normal and LFR, two fights per report. Fresh API and selective browser data were compared with the complete browser sources already saved locally. The check covered **586,533 events**, all full nested field values/types, **890 exact source-response reconstructions** across the packed files, staging reloads, and unchanged original archive/API snapshot SHA-256 hashes. All temporary proof snapshots and output copies were deleted after verification. The production catalogue remained read-only.

| Report category | Fights | Events | Old browser event/replay requests | New browser event/replay requests |
| --- | ---: | ---: | ---: | ---: |
| Mythic+ report 1 | 2 | 114,922 | 392 | 260 |
| Mythic+ report 2 | 2 | 104,443 | 361 | 317 |
| Raid Mythic | 2 | 101,279 | 334 | 61 |
| Raid Heroic | 2 | 95,904 | 313 | 22 |
| Raid Normal | 2 | 91,535 | 303 | 52 |
| Raid LFR | 2 | 78,450 | 256 | 88 |
| Total | 12 | 586,533 | 1,959 | 800 |

The counts exclude cached report metadata; 66 additional API event requests were made. Browser requests declined 59.16%, which is not a whole-pipeline throughput factor. Combined new archives were 10,443,126 bytes versus 10,189,180 bytes for the original browser-only files (+2.49%). All six report metadata bodies matched after aligning unordered actor/ability/fight lists; current response ordering itself was preserved exactly in the archives. Complete event data was checked only for the selected twelve fights, not all fights listed in those reports.

Final regression run: **560 passed**, two existing dependency deprecations. The UI now checks `hybrid_strategy=selective`; an older still-running backend explicitly shows a restart notice instead of claiming the new strategy is active. Source publication excludes local evidence, runtime data, archives and authentication. User retains control of server restart.

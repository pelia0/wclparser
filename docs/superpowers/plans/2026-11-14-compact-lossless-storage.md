# Compact lossless storage implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development for the independent codec and repacking tasks, followed by a fresh whole-change review.

**Goal:** Reduce each standalone historical fight archive without losing any canonical event, replay field, report metadata, summary, or provenance.

**Architecture:** WCLF3 retains the four logical WCLF2 sections. Identical replay columns reference canonical columns inside the same file; differing columns remain complete. Exact integer deltas are used only when their compressed representation is smaller. Section compression supports Zstd 9 (fast) and LZMA 6 (compact). The reader reconstructs precisely the existing logical section objects and reads both WCLF2 and WCLF3.

**Tech stack:** Existing Python, MessagePack, Zstandard and standard-library LZMA. No new dependencies.

## Constraints and evidence

- Preserve every received field, value, numeric type/precision, absent/null distinction, and event order. No sampled substitution, normalized-only source, rounding, or permanent raw sidecars.
- Each file remains independently readable, with its complete report master. Current sampled metadata sections are approximately 19–42 KB; canonical/replay duplication is the larger target.
- Four read-only sample benchmarks found replay-relative encoding saves approximately 27–28%; relative encoding plus LZMA 6 saves approximately 37–38%. Compression-only LZMA time was 1.7–4.6 seconds versus 0.1–0.3 seconds with relative Zstd 9. These are sample results, not guaranteed collection-wide ratios.
- Whole-column exact MessagePack equality can remove repeated columns without assuming identical event streams. Unequal event counts use full replay storage. Unknown fields and mismatched streams must round-trip.
- Keep old files readable and prevent old readers from silently interpreting encoded columns: use new magic WCLF3.
- Current collection may be running; do not alter existing files or catalogue during development. Prepare and verify a dry-run repacking utility first. No reset, deletion of logical data, or auth changes.

## Tasks

- [x] Codec: tests first for V2 compatibility, standalone V3 full-field/source-response roundtrip, differing/missing fields and counts, exact scalar types, integer deltas, reference validation, corrupt compression, and atomic write cleanup. Implement V3 writer/reader and a reusable verified archive-copy writer. Keep the public logical section interface stable.
- [x] Repacking: tests first for dry-run immutability, refusal of unsafe paths/active server, verified atomic replacement, smaller-only updates, byte-count catalogue updates, and failure keeping original intact. Implement source-only CLI with explicit apply option and compression profile.
- [x] Root integration: compare full logical sections and source records on copies of real fights; run all tests and independent review. Select profile using user preference; publish source only. Apply no collection reset.

## Verified outcome

The user selected the compact profile for new archives. Five real fight copies preserved all four logical sections with exact MessagePack equality, reconstructed source samples, canonical records, replay records, and unchanged original file hashes. Representative final compact sizes were 1,548,477 → 1,080,543 bytes and 4,386,616 → 2,977,571 bytes. An intentionally mismatched 127,751-canonical / 119,468-replay stream stayed complete and shrank from 2,998,780 to 2,588,059 bytes. Final savings vary with the data; the initial prototype's 37–38% is not a promised collection-wide ratio.

The full suite passed 246 tests with two existing dependency warnings. An independent focused review passed 89 tests and found no remaining important issues after fixing the LZMA decoder memory limit. The existing collection was not repacked or reset; the optional CLI provides that separate operation with a dry-run default.

## Review focus

Typed equality must distinguish booleans, integers and Decimal values; shared references must retain missing fields and dictionary ordering within the logical section interface; readers must reject missing references or cycles; conversion must never replace an unverified original; old running servers must not encounter newly encoded files.

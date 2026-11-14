# Paper AI prefix reuse and smaller dynamic inputs

## Problem

The two-hour baseline before release contained 24 successful paper-autopilot
calls: average input 6,185.46 tokens, average cache miss 5,161.46 tokens,
and a token-weighted hit rate of 16.55%. Every call reported 1,024 hit tokens.
These are input-token counts, not currency charges or complete account billing.

Previously, rapidly changing `current.markets` preceded positions and realized
feedback. Every symbol repeated fourteen feature names and fixed execution-cost
fields. Prefix caching cannot independently reuse an identical suffix after a
changed prefix. Redis market caching is separate from provider input caching.

## Implementation

`quant.paper_prompt` packs the wire request as `paper.cache.v1`. The internal
decision payload, trade validation, timestamps, risk budget, exit journal,
300-second batch interval and daily 288-attempt ceiling are retained.

- Fixed instructions, limits, column definitions, common fee/slippage values and
  reinvestment accounting definitions form a deterministic prefix. Sorted mapping
  keys prevent incidental insertion order from changing it. No padding is added.
- Freshly reread realized feedback, pause state and position-management parameters
  precede rapidly changing account marks, quote timestamps and market vectors.
  Position marks are joined to management by symbol; entry prices are never used
  as substitutes for missing current quotes.
- The fourteen features use a vector with one explicit column definition.
  Completed-trade and per-symbol statistics use defined column arrays. Common
  execution costs are sent once; each market retains spread and break-even move.
  Slow candidate context and community excerpts are preserved separately.
- Values retain the existing ten-significant-digit wire representation, including
  tiny prices/returns. No coarser price rounding, time bucketing, stale-state TTL,
  response replay or suppressed risk update is used to manufacture cache hits.
- Unknown future input fields are retained. Missing prices/vectors remain null;
  inconsistent fixed fees fail rather than silently using the wrong cost.
- Telemetry records schema, character counts, candidate count and a SHA-256 prefix
  fingerprint. Provider usage remains the authority for billable token counts.
- The UI shows current-call usage and a persisted, schema-specific last-24-call
  window, with token-weighted hit rate and average missed tokens. Existing lifetime
  totals are preserved and include pre-optimization usage.

The alternative of freezing prices or replaying decisions could increase apparent
reuse while making trading inputs stale. Adding irrelevant fixed text could inflate
the hit percentage while increasing cost. Neither is used. Prefix reuse remains
provider-controlled and is not guaranteed; fewer missed tokens matters alongside
the percentage.

## Verification

- Same eleven-symbol public/paper snapshot, no provider calls and no live-ledger
  writes: full input shrank from 14,918 to 11,263 characters (24.5%). This includes
  the new encoding guide. Characters are not an exact token or billing estimate.
- Local backend: 559 passed, 2 skipped. Release-image regressions: 183 passed with
  networking disabled, temporary databases and the non-root runtime user.
- TypeScript/Vite production build passed. Runtime server-file and deployed UI
  SHA-256 hashes matched local artifacts.
- Tests cover feature-column inversion, every original wire value, position/mark
  and budget reconstruction, all per-symbol statistics, null values, tiny prices,
  stable prefixes under quote changes, immediate stop/feedback/budget changes,
  mapping-order determinism, fee mismatch, bounded weighted telemetry and
  preservation of lifetime totals across a controller restart.

## Release and observations

Built `ai-trader-quant:cache-layout-20261114` from the existing exit-repair image.
Only API and paper-worker were recreated and frontend assets replaced. The real
observer retained its previous image and remained read-only. Authenticated paper,
observer and live-account endpoints returned HTTP 200 with fresh heartbeats.
Paper settings retained live=false, no Binance credentials and the 0.001 risk
fraction. DS retained final strategy direction authority.

Source/config/UI backup:
`deploy/backups/cache-layout-20261114/source-before.tar.gz`.
Online SQLite backup: `/data/cache-layout-before-20261114.db`.
Before and immediately after release: 2,892 orders and 13,752 fills; comparison
against the backup found zero changed or missing prior orders/fills. The active
ledger was not replaced and the last-request interval was not reset.

First scheduled new-format call completed at 2026-11-14 17:13:47 Asia/Shanghai:
six candidates, 3,924 input tokens, 768 hit and 3,156 missed (19.57% hit rate).
It parsed successfully; no new order was forced. The smaller candidate count
confounds comparison against the old-window mean. Later calls are needed to
measure prefix warming and normal live-input variation; do not treat the first
call as a controlled billing-savings estimate.

To roll back, restore prior code/config/UI and recreate only API/paper-worker with
the exit-repair image. Do not restore the database backup over later trades.

Provider reference checked during diagnosis:
https://api-docs.deepseek.com/guides/kv_cache

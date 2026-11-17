# Paper short-horizon mover scan

The paper account remains the only execution target. This change makes the
existing DeepSeek autopilot act on the selected market leaders sooner and
preserves the market scanner's ranking when building its portfolio request.

## Changes

- Lower the autopilot's hard minimum decision interval from 300 seconds to
  60 seconds. The configured daily request ceiling remains 288.
- Skip a DeepSeek request when every candidate and managed position lacks a
  fresh usable quote. Local stop, target and pending-exit handling continues.
- Keep every open paper position first, then order eligible candidates by the
  fresh potential radar's stronger long/short score and the all-market flow
  ranking. The AI still sees no more than twelve candidates per request.
- Clarify that the simulation is collecting fills and exits before model
  approval. Ask DS to compare long and short opportunities, take a small paper
  exploration when at least two independent direction signals agree, and
  normally target a 1-15 minute holding period. Costs remain in the PnL record;
  they are not treated as proof that an unvalidated opportunity must be rejected.
- Preserve real simulated fees, slippage, the existing account and order caps,
  and the real-account read-only boundary.
- Enable the previously disabled paper AI autopilot and DS authority flags.
  Keep dynamic exits enabled as required by configuration validation.

## Validation

- `test_paper_autopilot.py`: 59 passed.
- Full backend suite: 563 passed, 2 skipped.
- The prompt prefix changes, so provider-side cache reuse may need to warm up
  again. Daily maximum AI requests are unchanged.

## Deployment

Built and deployed as `ai-trader-quant:paper-mover-scan-20261117-r3`.
Recreated only API and paper-worker. The paper AI autopilot, DS authority,
dynamic exits, and 60-second decision interval are enabled; live execution is
disabled and the paper worker has no Binance API credentials. The existing
SQLite ledger was backed up before rollout and kept in place.

At 2026-11-17 10:11 Asia/Shanghai, API health was green and the paper worker was
running. The latest DS batch evaluated nine fresh candidates and chose to hold;
there were no open positions at that check. Decisions are recorded as paper
exploration and do not imply a validated or profitable model. An earlier
sample had no fresh quote and now skips the AI request entirely.

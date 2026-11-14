# Durable paper exits and realized cost feedback

## Incident and limits

The original simulated account remains the only execution target. Real-account
observation stays read-only, with real execution disabled. This release does not
reset balances, remove historical losses, force trades, or validate profitability.
DS retains final strategy direction authority inside the existing execution and
capital constraints; no trend, flow, confidence or confirmation voting veto was
restored.

The November 14 audit found eleven closing IOC orders with zero fills and
`No fresh reconstructed depth`. An expired exit could disappear when the price
rebounded. One ARB take-profit attempt expired and eventually exited later by
horizon instead. Existing positions could also be omitted from AI requests when
the entry feature gate failed, despite usable quotes or depth.

Before this repair, the first 38 completed DS-authority positions had gross PnL
-92.8862 USDT, fees 64.5827 USDT and net PnL -157.4688 USDT. Long entries
accounted for most losses. Three short results were insufficient to justify a
short-only strategy. These are historical descriptive observations, not forecasts.

## Changes

- Persist each exit in SQLite before checking pending orders or depth. Bind the
  intent to the position's opening timestamp and direction. Preserve the first
  trigger; liquidation may upgrade it. Never cancel it on a price rebound or AI
  parameter update.
- Retry the remaining position at most once per second on a new valid, uncrossed
  depth revision no older than two seconds. Preserve reduce-only, participation,
  fees, adverse slippage and transactional partial-fill accounting. Never fill on
  fabricated prices or missing depth. Resume after restart and recover legacy
  failed closing attempts for the current position.
- Prioritize exits over entries. Manage all held symbols even after pool rotation;
  latch holding-horizon exits even when marks or entry features are unavailable.
  Entry-analysis errors cannot skip the paper worker's protection pass.
- Include held symbols in the AI request independently of the entry feature gate.
  Missing vectors and prices remain null. Such rows are management-only; fresh
  quotes are required to update protection. New entry requirements remain intact.
- Give DS explicit round-trip fees, slippage and spread, and the last 100 completed
  DS-authority position results by direction and exit reason. Exclude legacy
  decisions and cash deposits. Explain the signal horizon and missing slow-cycle
  evidence. Statistical feedback does not train or approve a model automatically.
- Reduce the deployed compounding risk fraction from 0.005 to 0.001. With the
  6,000-USDT reference capital this starts at 6 USDT per trade and 18 USDT for the
  portfolio, before the existing loss/drawdown adjustment. These are sizing
  budgets, not guaranteed loss limits under price gaps. Existing position sizes
  and prior losses remain unchanged; no daily-loss latch was added to paper mode.
- Show durable pending exits and the DS direction feedback on the paper screen.
  Retain one batched request per 300 seconds and the 288-request daily ceiling;
  no per-tick AI requests were added.

SQLite is used for durable intent alongside the existing ledger. Redis remains
the disposable live-data layer: putting exit intent only in Redis would lose it
on expiry or restart. Repeatedly submitting against the same depth would also
reuse displayed liquidity, so a new revision is required before another attempt.

## Verification and deployment

- Local backend: 548 passed, 2 skipped. Frontend TypeScript/Vite build passed.
- Release-image regression: 172 passed with networking disabled, no credentials,
  temporary test databases and the normal non-root container user.
- Regression coverage includes expired IOC, rebound after exit trigger, partial
  fills, restart and legacy recovery, fresh-depth requirements, no duplicate
  matching of the same book revision, position incarnation identity, exit priority,
  missing mark/features, protected AI updates, costs and smaller risk sizing.
- Built `ai-trader-quant:exit-repair-20261114` from the installed prior image.
  Recreated only API and paper-worker. Real observer and data collectors retained
  their existing images. Source/UI/config backup:
  `deploy/backups/exit-repair-20261114/source-before.tar.gz`.
- Stopped the paper worker before the online SQLite backup:
  `/data/exit-repair-before-20261114.db`. Original active ledger:
  `/data/quant-aggressive-paper.db`. Baseline: 2,877 orders and 13,697 fills.
- At 2026-11-14 10:55:23 Asia/Shanghai the new DS request included the held
  PIEVERSE position and the realized direction/cost feedback. DS updated the
  holding horizon to 1,200 seconds; the position had already exceeded it.
- At 10:55:28.886 Asia/Shanghai, reduce-only exit
  `qt_d17d8f0ec6497c07b479ed10688d` filled the remaining approximately 52 units
  across 14 real-depth simulated fill records. Reason: `ds_horizon`.
  Gross PnL +0.580327208 USDT, both-side fees 0.087471225 USDT, net
  +0.492855983 USDT. This was an automatic horizon exit, not an AI `close` action.
- At 10:55:57 the ledger contained 2,878 orders and 13,711 fills, no open
  positions and no pending exits. Compared every prior order and fill against
  the backup: zero changed or missing rows. The 40 completed DS-authority
  positions still had cumulative net PnL -151.999515 USDT. One profitable close
  verifies execution, not strategy profitability or live incident-path retry.
- Authenticated paper/status/live-account routes returned HTTP 200 with fresh
  heartbeats. Paper settings confirmed no Binance credentials and live=false;
  live-account reported read_only=true and execution_enabled=false. Paper/API
  containers were healthy/running. Deployed frontend hashes matched the build.

To roll back code/configuration, restore the source backup and recreate only
API and paper-worker with the prior image. Never overwrite the active ledger
with the backup to undo a release: doing so would erase subsequent fills. Inspect
any existing pending exits before rolling back to code without durable retries.

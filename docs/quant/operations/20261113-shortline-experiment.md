# AI plans, event-triggered paper entries and bounded reinvestment

This is a paper-only execution experiment, not a profitable-strategy release or
an exchange HFT implementation. The original simulated ledger is retained.
Real trading stays disabled and its observer is not redeployed for this change.

## Frozen rules

- Keep one bounded AI portfolio request per 300 seconds. An entry plan can wait
  up to 300 seconds from request start, with at most one entry signal per plan
  and symbol. Close/update actions keep their original short expiration.
- Require a fresh, stronger matching 1–4 hour radar direction, fresh collector
  features, 30/60-second trend agreement, matching OFI and trade imbalance,
  volume above its recent baseline, and a check against chasing a short spike.
- Require observed ATR-derived space of at least 2.5 times estimated round-trip
  costs and target-to-risk after costs of at least 1.2. These are heuristic
  filters, not calibrated probabilities or estimates of positive expectancy.
- Require two distinct increasing feature timestamps within ten seconds of
  each other; repeated processing of one Redis sample is not confirmation.
- Recheck entry quality in risk sizing, preserving existing correlation,
  participation, margin, notional, liquidation and lot-size constraints.
- After this policy fully closes a symbol, persist a 120-second cooldown.
- Estimate the protected exit trigger using actual allocated entry fees,
  expected exit fees, adverse slippage and half-spread, with a small cushion.
  This estimate does not guarantee breakeven through jumps or missing depth.

## Reinvestment and accounting

Reference capital is 6,000 virtual USDT inside the original account, not a new
deposit or a new ledger. Initial risk ceilings are 30 USDT per trade and 90 USDT
for the portfolio. They scale with this policy's settled net PnL and shrink
continuously under drawdown. Open gains do not increase the budget; open losses
and remaining entry fees reduce it. The scale is capped at 2, and the original
2,000 USDT per-position / 6,000 USDT aggregate margin ceilings still apply.

Only positions tagged `ai-plan-flow-v1` contribute to its reinvestment statistics.
Each partial close allocates entry fees proportionally in the same transaction
as its durable fill. A full position is counted once across partial fills.
Deposits, prior strategies and positions opened before this policy are excluded.
Historical balances, fills and losses are never reset. The current simulator
does not accrue funding fees; all performance claims must preserve that limit.

The dashboard separately exposes policy net PnL, fees, completed positions,
wins, scaling and risk budgets. The execution-policy fingerprint includes the
new mode so old validation proofs cannot be reused as proof for this policy.

## Historical diagnostic — failed to establish an edge

Before deployment, 80 completed historical AI entries had matching historical
features available. Checking the fixed new filter at their original entry times
retained 6 original outcomes totaling **-2.916280391 USDT**, and excluded 74
totaling **-184.893870643 USDT**. Exclusion reasons: 46 cost/volatility/reward-risk,
11 flow disagreement, 8 low volume, 8 trend disagreement, 1 chase check.

This is retrospective entry diagnosis, not a replay of the new policy: deferred
entries, the two-sample confirmation and changed exits were not simulated.
No thresholds were retuned on these results. It is not an OOS result, and the
retained trades remain negative. Do not describe the policy as profitable.

## Validation and deployment

214 related tests passed; frontend TypeScript and Vite production build passed.
Tests cover directional symmetry, fees and adverse fills at the protected
trigger, plan expiry, distinct-sample confirmation, cooldown, size caps,
deposit exclusion, partial-fill accounting, restart persistence and paper isolation.

Pre-deployment snapshot: `/data/shortline-before-20261113.db`, made with SQLite's
online backup API. At that snapshot there were 2,787 execution orders, 12,761
execution fills and no open positions. The snapshot is evidence, not a reset.
Source/static-assets backup is under
`deploy/backups/shortline-20261113/source-before.tar.gz`.

Deploy only `api` and `paper-worker` with the existing Compose files and the
`ai-trader-quant:shortline-20261113` image. Other containers keep their images.
To disable the experiment, set `QUANT_PAPER_FAST_EXECUTION=false` for both
services and recreate them. Existing paper position protection stays active;
neither rollback nor disabling the mode should overwrite the ledger snapshot.

At 2026-11-13 14:48 Asia/Shanghai, the API was healthy, paper worker was running,
WebSocket marks were fresh for all 12 selected symbols, and authenticated status
endpoints returned HTTP 200. Paper config confirmed no trading credentials and
live execution disabled. Risk budgets were 30/90 USDT, multiplier 1.0. Execution
order/fill counts were unchanged from the snapshot; no new-policy orders or
positions existed yet. The AI was waiting for overlap between the slow radar
and fresh candidate features. Deployment success is not an actual fill or profit.

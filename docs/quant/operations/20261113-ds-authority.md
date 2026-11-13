# DeepSeek final strategy authority in the existing paper account

The previous AI path could wait indefinitely for slow radar overlap, then veto
an AI entry on trend, order flow, cost heuristics or two-sample confirmation.
`QUANT_PAPER_DS_AUTHORITY=true` makes those signals optional context for DS.
An unexpired, structurally valid DS instruction goes directly to paper sizing
and execution, without another strategy vote or the shortline cooldown.

DS selects among fresh candidates and controls direction, entry/close/update,
requested margin and leverage ceilings, stop/target distances and holding time.
It can still choose to hold. This mode does not force trades or prove profit.
New positions persist their authority flag across partial fills and restarts.
Protection follows DS targets without automatic trailing or breakeven changes;
DS updates may tighten stops and change targets and the total holding horizon.

Execution still enforces data freshness, decision expiry, position identity,
entry-price drift, balance, position/margin/notional caps, correlation risk,
liquidity participation, lot rules and liquidation distance. The existing
settled-net-PnL reinvestment accounting and risk ceilings remain in force.
No real trading access is granted. Paper configuration rejects exchange
credentials, live mode, or missing protective exits.

## Cost and storage

Retain one fixed-prefix JSON portfolio request per 300 seconds, at most twelve
fresh candidates with held positions first, and the persisted daily 288-request
limit. Redis/WebSocket events drive local execution and protection. No per-tick
AI calls were added. The new prompt can cold-miss once; cache hits are not
guaranteed for changing account and market inputs.

The original SQLite paper ledger is unchanged. This mode continues the existing
shortline reinvestment series; the policy fingerprint includes the new flag,
so previous model validation does not validate this changed decision policy.

## Validation and release

- 166 related local tests passed across autopilot, shortline, paper simulation,
  forward trial, lab, engine and observer coverage (including the added partial
  fill variant). The final image passed all 62 autopilot/shortline tests.
- TypeScript and Vite production build passed.
- Tests cover bypassed strategy vetoes, long/short symmetry, one-shot decisions,
  stale data and decision rejection, paper-only isolation, risk ceilings,
  DS stop/target triggers and authority persistence across partial fills.
- Built `ai-trader-quant:ds-authority-20261113` on the installed shortline image
  with the changed server files. Recreated only API and paper-worker, and
  installed the rebuilt frontend. The real observer container was not replaced.
- Before deployment: 2,787 execution orders, 12,761 fills, zero open positions.
  Online SQLite backup: `/data/ds-authority-before-20261113.db`.
  Source and UI backup: `deploy/backups/ds-authority-20261113/source-before.tar.gz`.
- At 2026-11-13 15:21:47 Asia/Shanghai, DS completed its first request in the new
  mode and chose HOLD for BTC, SOL, SUI and UNI, citing limited net-of-cost edge.
  The original ledger still had 2,787 orders and 12,761 fills. This confirms a
  working decision cycle, not a new fill or profitability.
- Both authenticated status endpoints returned HTTP 200 with fresh heartbeats.
  Paper configuration confirmed DS authority enabled and no exchange credentials;
  the account observer remained disabled for trading and observation-only.

To revert the decision mode, disable `QUANT_PAPER_DS_AUTHORITY` for API and
paper-worker and recreate those services. Existing DS-tagged positions retain
their original protection semantics until closed. Never restore the backup over
the active ledger merely to undo configuration; that would erase later fills.

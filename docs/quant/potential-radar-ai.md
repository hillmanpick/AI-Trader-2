# Potential radar AI commentary

The radar keeps its numerical rule score and observation labels. A separate
`radar-ai` process produces read-only Chinese commentary from at most five fresh
candidate feature vectors with rule score >= 20. It cannot change rankings,
approve a model, adjust leverage, submit orders or control existing positions.
Only the official DeepSeek HTTPS origin is accepted. No Binance credentials or
operator token are supplied to this process.

Deploy with both Compose files, preserving the server's existing base config:

```sh
docker compose -f deploy/compose.yaml -f deploy/radar.compose.yaml config -q
docker compose -f deploy/compose.yaml -f deploy/radar.compose.yaml up -d --no-deps api potential-radar radar-ai
```

The existing server `.env` supplies `DEEPSEEK_API_KEY`, `DEEPSEEK_MODEL` and
`DEEPSEEK_BASE_URL`. `radar-ai` is commentary only. The overlay separately
enables the existing DeepSeek autopilot on the original simulated account;
live trading, Binance private keys and ordinary entry-review AI remain off.
Stop `radar-ai` to stop commentary, or remove the `paper-worker` override to
stop AI-managed paper positions.

Calls are bounded to once per 900 seconds and at most 48 attempts per UTC day.
Attempts are persisted in `potential-radar.db` before network I/O, including
errors; there is no immediate provider retry. Each call has at most 1400 output
tokens, a response size cap and request timeout. The stable system prefix and
compact feature batch reduce changing prompt content. Cache hits are measured
from provider usage, not promised; reducing request frequency and input size is
the primary cost control. Request counts bound usage, not a fixed currency cost.

`GET /api/quant/radar/status` includes `ai_analysis` with input timestamps,
model, candidate reasoning, risks, invalidation conditions, usage and daily
attempt totals. A commentary based on data older than 20 minutes is marked
stale. Commentary may refer to an earlier watchlist snapshot, which the UI
explicitly distinguishes from the live ranking. Neither a rule score nor an AI
view is an empirically validated probability or guaranteed return.

## Paper-trading selection

The all-market collector ranks positive and negative short-term flow, then
reserves part of its bounded 12-contract depth pool for fresh radar extremes.
The batch prompt combines radar context (5m/15m and 1h/4h returns and
long/short scores) with current 5s-60s order-flow features and managed positions.
Before a new simulated entry, code requires a fresh same-side radar score of at
least 20 that exceeds the opposite-side score. No matching fresh candidate
means no new position; exits for held positions still run.

The DeepSeek paper autopilot can choose long, short, hold, close or tighten
protection on the existing simulated account. Execution continues through the
existing simulation ledger, depth matcher, fee/slippage calculation, dynamic
leverage, exposure and correlation risk checks. It does not use Binance private
credentials or submit real orders. The overlay enforces a 300 second minimum
between batch requests and 288 requests per UTC day; that counter is persisted
before network I/O. The separate read-only commentary process remains bounded
at 48 requests per UTC day. This request bound limits call count, not a fixed
currency cost.
